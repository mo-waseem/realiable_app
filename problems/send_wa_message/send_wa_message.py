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
        Async WA sending task could not found the message in the db!
        or it can fail for any other reason.
    
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

        if this task is taking more than 3 seconds, another worker can pick the next handle_outbox_entries 
        when this one is not finished yet, and the same outbox entry can be sent twice!!
    
    Solution #1:
        - make the task idempotent by checking the status of the outbox entry before sending it, 
        and if it's already sent, skip it.
    """
    """
    Problem #2:
        Messages can be sent out of order:

        Messages can be sent out of order if the outbox entries are not processed in 
        the order they were created.

    Solution #2:
        - Add a guard in send_whatsapp_message_outbox_task to check if there are any 
        pending outbox entries for the same customer and phone number, 
        and if so, skip sending the current outbox entry until the previous ones are sent.

    """
    
    for outbox_entry in OutboxEntry.objects.filter(status="PENDING"):
        send_whatsapp_message_outbox_task.delay(outbox_entry.id)