# Receive a webhook from WhatsApp (timeout? duplicate webhooks?)
# can the webhook endpoint be slow enough to cause a timeout ?!
# can the webhook be received twice ?!
# can the webhook be received out of order ?!

def webhook_handler(request):
    received_data = request.data

    """
    Problem #1:
        process_webhook_data can be slow and can cause a timeout in the webhook response,
        and the webhook will be retried by WhatsApp, and we will receive the same webhook
        again and again, and we will process the same webhook multiple times!!

    
    Solution #1:
        we can define process_webhook_data as a celery task and call it asynchronously,
        so the webhook response will be sent immediately and we will not receive the same webhook again.
    """

    """
    Problem #2:
        Once we return 200, the provider considers the webhook delivered and will never retry it.

        With Solution #1 the event lives only in the broker / worker memory, so it can be lost:
        - the broker loses the task (Redis restart, enqueue fails)
        - the worker crashes or the task fails permanently
        - a bug in processing

        Result: the webhook is lost forever, and we can't retry or replay it.

    Solution #2:
        Inbox pattern:

            Save the webhook in the db (InboxEvent, status=NEW) BEFORE returning 200,
            then process it asynchronously.

            If processing fails, the event is still stored -> we can retry, inspect or replay it.

            Add a periodic polling job that picks up events with status NEW or TO_RETRY
            and processes them (covers lost tasks, failed tasks and events waiting to be retried).

            Bonus: a unique constraint on the provider's event id drops duplicate deliveries.
    """

    """
    Problem #3:
        webhooks can be received out of order! 
        
        and this is not acceptable in the services that the order
        of the messages is important.
        Like payment service:
        - we can receive REFUNDED webhook before CAPTURED webhook
        - our system will set the local status as REFUNDED and then as PAID
        - the customer's order will be shipped based on this!! and this is wrong, we should cancel the order
        instead.

    Solution #3:
        Multiple solutions can be used here:

        1- Event versions:
            if the provider support event versioning, we can only process the newer versions and ignore
            older ones.
            
            Example:
                REFUNDED version 7 arrives first
                → Apply it
                → provider_version = 7

                CAPTURED version 6 arrives later
                → 6 is older than 7
                → Ignore it

        2- State machine:
            Define a state machine that our system can transit the payment state only through it.

            Example:

                ALLOWED_TRANSITIONS = {
                    Payment.Status.PENDING: {
                        Payment.Status.PAID,
                        Payment.Status.FAILED,
                    },
                    Payment.Status.PAID: {
                        Payment.Status.PARTIALLY_REFUNDED,
                        Payment.Status.REFUNDED,
                    },
                    Payment.Status.PARTIALLY_REFUNDED: {
                        Payment.Status.REFUNDED,
                    },
                    Payment.Status.REFUNDED: set(),
                    Payment.Status.FAILED: set(),
                }
            
                then in the code:

                def change_payment_status(payment, new_status):
                    allowed = ALLOWED_TRANSITIONS[payment.status]

                    if new_status not in allowed:
                        logger.warning(
                            "Rejected invalid payment transition",
                            extra={
                                "payment_id": payment.id,
                                "current_status": payment.status,
                                "requested_status": new_status,
                            },
                        )
                        return False

                    payment.status = new_status
                    payment.save(update_fields=["status"])
                    return True
        
        3- Inbox pattern and reconciliation:
            We can store the webhook as event, check then if we can apply it
            and if not --> set the status of event as TO_RECONCILE or OUT_OF_ORDER.

            Example:
            --------

            if not can_apply(payment.status, new_status):
                webhook.status = WebhookEvent.Status.OUT_OF_ORDER
                webhook.save(update_fields=["status"])

                reconcile_payment.delay(payment.id)
                return
            
            @shared_task
            def reconcile_payment(payment_id):
                payment = Payment.objects.get(id=payment_id)

                provider_payment = payment_provider.get_payment(
                    payment.provider_payment_id
                )

                payment.status = map_provider_status(
                    provider_payment["status"]
                )
                payment.save(update_fields=["status"])

    """

    """
    Problem #4:
        Two workers can pick up the same inbox events!
        - Two poller runs overlap and fetch the same batch of NEW / TO_RETRY events
        - The poller picks an event that the immediate task is still processing

        Result: the same event is processed twice -> data inconsistency.

    Solution #4:
        Atomic claim: batch claim in the poller + single claim in inbox_handle (scales to high volume)

            Poller - claim a batch in ONE query, then enqueue each event:

            with transaction.atomic():
                ids = list(
                    InboxEvent.objects.select_for_update(skip_locked=True)
                    .filter(
                        status__in=["NEW", "TO_RETRY"],
                        created_at__lt=timezone.now() - timedelta(minutes=2),  # leave fresh events to the immediate task
                    )
                    .order_by("id")
                    .values_list("id", flat=True)[:500]
                )
                InboxEvent.objects.filter(id__in=ids).update(
                    status="ENQUEUED", enqueued_at=timezone.now()
                )

            for event_id in ids:
                inbox_handle.delay(event_id)

            inbox_handle - the ONLY place an event becomes PROCESSING (used by the webhook and the poller):

            @shared_task(queue="webhooks")
            def inbox_handle(inbox_event_id):
                claimed = InboxEvent.objects.filter(
                    id=inbox_event_id, status__in=["NEW", "TO_RETRY", "ENQUEUED"]
                ).update(status="PROCESSING", processing_started_at=timezone.now())

                if not claimed:
                    return  # already claimed by another worker or already done
                ...

            Why:
            - one query claims a whole batch
            - ENQUEUED status -> the next poll doesn't enqueue the same events again
            - skip_locked -> several pollers can run in parallel without blocking each other
            - one task per event -> parallel processing, one bad event doesn't affect the others
            - the claim is a short status update; never hold a row lock during processing (slow external calls)

            Also needed:
            - Stuck events: reset events stuck in ENQUEUED / PROCESSING for too long back to TO_RETRY
              (lost task or crashed worker)
            - Partial index on (status, created_at) for the pending statuses -> fast poll query
            - Archive / delete DONE events regularly to keep the table small
            - Separate "webhooks" queue so webhook processing doesn't block other tasks

            Processing is still "at least once" -> handlers must be idempotent (see #5).

    """

    """
    Problem #5:
        The provider can deliver the same webhook more than once (at-least-once delivery):
        retries on timeout / non-2xx, or the connection drops after we already processed it.

        And even with one inbox row, our processing is at-least-once too
        (worker crashes before marking DONE, or the stuck-event reset in #4 runs it again).

        Result: the same event is applied twice -> e.g. points added twice, order paid twice.

    Solution #5:
        Two layers of idempotency:

        1- Dedup on receive (inbox):
            Unique constraint on (provider, provider_event_id):

            inbox_event, created = InboxEvent.objects.get_or_create(
                provider="payments",
                provider_event_id=received_data["id"],
                defaults={"payload": received_data},
            )

            if not created:
                return JsonResponse({"status": "duplicate"})  # still 200, otherwise the provider retries forever

        2- Idempotent processing (handler):
            - Apply the DB effects and mark the event DONE in the SAME transaction:

                with transaction.atomic():
                    change_payment_status(payment, Payment.Status.PAID)  # conditional transition (#3)
                    inbox_event.status = "DONE"
                    inbox_event.save(update_fields=["status"])

            - Conditional updates / state machine (#3): PENDING -> PAID only, a 2nd run does nothing
            - Unique constraint on the effect itself, e.g. LoyaltyPoints unique on source_event_id
            - External calls: pass an idempotency key (e.g. the event id) if the API supports it

    """
    """
    Problem #6:
        The WhatsApp status webhook can arrive before we save the message_id in the db
        (we get the message_id from the send API response, the webhook can be faster).

        The webhook can't find the message -> the message is never marked SENT,
        it stays PENDING forever even though it was actually sent.

        This can lead to:
        - wrong message status shown to users / reports
        - the message sent twice if the user (or a retry job) re-sends PENDING messages

    Solution #6:
        1- Send our own reference value:
            Save the message row in the db BEFORE calling WhatsApp,
            and send our own id with the message (e.g. WhatsApp's biz_opaque_callback_data,
            if the API supports it). It comes back in the status webhook.

            message = Message.objects.create(status="PENDING", ...)
            send_whatsapp_message(..., biz_opaque_callback_data=str(message.id))

            In the webhook: find the message by our id, not by WhatsApp's message_id
            -> the message always exists, no race.

        2- Retry later (inbox status TO_RETRY):
            If the message is not found, don't drop the event:

            if not message:
                inbox_event.status = "TO_RETRY"
                inbox_event.retry_count += 1
                inbox_event.next_retry_at = timezone.now() + backoff(inbox_event.retry_count)
                if inbox_event.retry_count >= MAX_RETRIES:
                    inbox_event.status = "FAILED"  # + alert
                inbox_event.save()
                return

            The poller (#2, #4) picks it up again when next_retry_at has passed.
            Max retries -> FAILED, so an event for a message that really doesn't exist doesn't retry forever.

    """

    process_webhook_data(received_data)

    return JsonResponse({"status": "success"})