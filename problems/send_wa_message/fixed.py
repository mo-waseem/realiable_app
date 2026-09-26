def send_whatsapp_message_outbox(request):
    with transaction.atomic():
        message = Message.objects.create(
            customer=request.user,
            phone_number=request.data["phone_number"],
            body=request.data["body"],
            status="PENDING",
        )

        outbox_entry = OutboxEntry.objects.create(
            message=message,
            status="PENDING",
        )

    transaction.on_commit(
        lambda: send_whatsapp_message_outbox_task.delay(outbox_entry.id)
    )


@celery.task
def send_whatsapp_message_outbox_task(outbox_entry_id):
    # Atomically claim the outbox entry.
    claimed = OutboxEntry.objects.filter(
        id=outbox_entry_id,
        status="PENDING",
    ).update(
        status="PROCESSING",
    )

    # Another worker already claimed it,
    # or the message was already processed.
    if not claimed:
        return

    outbox_entry = OutboxEntry.objects.select_related("message").get(
        id=outbox_entry_id,
    )

    # Don't send this message if an older message for the same
    # customer + phone number hasn't finished yet.
    previous_entry_exists = (
        OutboxEntry.objects.filter(
            message__customer=outbox_entry.message.customer,
            message__phone_number=outbox_entry.message.phone_number,
            id__lt=outbox_entry.id,
        )
        .exclude(status="SENT")
        .exists()
    )

    if previous_entry_exists:
        # Release the claim so it can be picked up later.
        OutboxEntry.objects.filter(
            id=outbox_entry_id,
            status="PROCESSING",
        ).update(
            status="PENDING",
        )
        return

    try:
        response = whatsapp_client.send_message(
            phone_number=outbox_entry.message.phone_number,
            body=outbox_entry.message.body,
        )

    except Exception:
        outbox_entry.status = "FAILED"
        outbox_entry.save(update_fields=["status"])
        raise

    outbox_entry.status = "SENT"

    outbox_entry.message.status = "SENT"
    outbox_entry.message.provider_message_id = response["message_id"]

    outbox_entry.save(
        update_fields=["status"],
    )

    outbox_entry.message.save(
        update_fields=[
            "status",
            "provider_message_id",
        ]
    )


# 3 sec to be fast enough to send multiple messages
@celery.periodic_task(run_every=timedelta(seconds=3))
def handle_outbox_entries():
    for outbox_entry in OutboxEntry.objects.filter(status="PENDING"):
        send_whatsapp_message_outbox_task.delay(outbox_entry.id)