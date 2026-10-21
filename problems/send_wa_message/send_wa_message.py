# Send a message to WhatsApp (sync recording in db vs. async WA sending - how to make them atomic ?!)
# can the message be sent twice ?!
# can the message be created twice in the db ?!

def send_whatsapp_message(request):
    message = Message.objects.create(
        customer=request.user,
        phone_number=request.data["phone_number"],
        body=request.data["body"],
        status="PENDING",
    )
    

    # Problem #1:
    # sending to WhatsApp fails:
    # if the message is created in the db but the sending to WhatsApp fails,
    # the message will be stuck in PENDING status and will never be sent.




    
    # Solution #1:
    # we can use a retry mechanism to retry sending the message if it fails,
    # and set that in the status of the message to FAILED if it fails after a certain number of retries, 
    # and we can have a separate task to retry sending the failed messages.
    
    """
    Problem #2:
        Sending to WA can cost time!

        This can slow down the system.

    Solution #2:
        - we can use a separate task to send the message to WhatsApp asynchronously.
    """
    response = whatsapp_client.send_message(
        phone_number=message.phone_number,
        body=message.body,
    )

    """
    Problem #3:
        Async WA sending task can fail and the message is stored in DB but not sent!.
    
    Solution #3:
        - we can use a retry mechanism to retry finding the message in the db if it's not found, 
        after a wait time, but this is not a good solution because it will 
        delay the task and can cause a timeout or worker exhausting.

        but,
        WA can still fail, and we don't have enough visibility else the logs in this case to know what's happening.

        We will have to dig into this problem manually in the logs and there will be
        no automated way to handle it or reconcile it.

        - Outbox pattern:
        we can use outbox pattern to handle the sending of the message to WhatsApp asynchronously,
        and we can have a separate task to retry sending the failed messages.

        The simplest definition of the outbox pattern:
        
        The transactional outbox prevents an external action from being forgotten 
        after a database transaction commits.

        It atomically saves the business change and a durable record of the 
        external work that must happen later.

    """
    

    message.status = "SENT"
    
    # here we could receive the webhook from WA before the message_id is set in the db
    message.provider_message_id = response["message_id"]
    message.save(
        update_fields=[
            "status",
            "provider_message_id",
        ]
    )

    return JsonResponse({
        "message_id": message.id,
        "status": message.status,
    })

# sending logic when use outbox pattern:
def send_whatsapp_message_outbox():
    with transaction.atomic():
        message = Message.objects.create(
            customer=request.user,
            phone_number=request.data["phone_number"],
            body=request.data["body"],
            status="PENDING",
        )

        # create an outbox entry for the message
        outbox_entry = OutboxEntry.objects.create(
            message=message,
            status="PENDING",
        )

    transaction.on_commit(lambda: send_whatsapp_message_outbox_task.delay(outbox_entry.id))


def send_whatsapp_message_outbox_task(outbox_entry_id):
    outbox_entry = OutboxEntry.objects.get(id=outbox_entry_id)

    try:
        response = whatsapp_client.send_message(
            phone_number=outbox_entry.message.phone_number,
            body=outbox_entry.message.body,
        )
    except Exception as e:
        # log the exception and retry later
        outbox_entry.status = "FAILED"
        outbox_entry.save(update_fields=["status"])
        raise e

    outbox_entry.status = "SENT"
    outbox_entry.message.status = "SENT"
    outbox_entry.message.provider_message_id = response["message_id"]
    outbox_entry.save(update_fields=["status"])
    outbox_entry.message.save(
        update_fields=[
            "status",
            "provider_message_id",
        ]
    )

# 3 sec to be fast enough to send the multiple messages
@celery.periodic_task(run_every=timedelta(seconds=3))
def handle_outbox_entries():
    """
    Problem #1:
        Same message can be sent twice:

        The poller enqueues ALL PENDING entries every 3 sec, including entries already enqueued
        by on_commit or currently being sent (status stays PENDING until WhatsApp responds).
        -> two tasks for the same entry run concurrently -> sent twice.

    Solution #1:
        Atomic claim in the task: PENDING -> PROCESSING with a conditional update.
        Only the worker that changed the row sends; the others skip.

            claimed = OutboxEntry.objects.filter(
                id=outbox_entry_id, status="PENDING"
            ).update(status="PROCESSING")

            if not claimed:
                return  # another worker has it, or it's already sent

        ("check status then send" is not enough: both workers can read PENDING at the same time)
    """
    """
    Problem #2:
        Messages can be sent out of order:
        - multiple workers send in parallel -> a later message can finish first
        - a failed message retried later goes after newer ones

    Solution #2:
        Per-conversation guard (customer + phone number):
        don't send an entry while an older entry for the same conversation is not finished yet
        (anything not SENT) -> release it back to PENDING and try later.

        Take care: a FAILED entry blocks the conversation forever!
        -> retry it N times, then mark it FAILED_PERMANENTLY (+ alert); the guard ignores final states.

    """
    
    for outbox_entry in OutboxEntry.objects.filter(status="PENDING"):
        send_whatsapp_message_outbox_task.delay(outbox_entry.id)