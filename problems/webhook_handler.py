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
        webhooks can be received out of order! 
        
        and this is not acceptable in the services that the order
        of the messages is important.
        Like payment service:
        - we can receive REFUNDED webhook before CAPTURED webhook
        - our system will set the local status as REFUNDED and then as PAID
        - the customer's order will be shipped based on this!! and this is wrong, we should cancel the order
        instead.

    Solution #2:
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
    Problem #3:

        Two workers can handle the same webhook! 

        So, we will have duplicate processing for the same webhook event.

        This can cause data inconsistency.
    
    Solution #3:
        Inbox pattern + DB-row lock:

            Using the inbox pattern, we can check if any other worker is handling the current event by:

            with transaction.atomic:
                inbox_event = InboxEvent.objects.select_for_update(skip_locked=True).filter(id=inbox_event_id, 
                            status="NEW")

                if not inbox_event:
                    return "Event is locked, most probably it's in processing by other worker"
                
                inbox_event.status = "PROCESSING"
                inbox_event.save(update_fields=['status'])
            
            

    """

    """
    Problem #4:
        Webhook event can be delivered twice!
        
        this code will handle it twice and this can cause data inconsistency.

    Solution #4:
        Inbox pattern + idempotency:

            process_webhook_data should have idempotency_key for each event, so matched events will have
            the same idempotency key

            Example:
            --------
            Charged event with id: 123 
            received #1 ---> get_or_create idempotency key with "payment_event_123"
            

    
    """
    """
        Problem #5:
            Asynchronous webhook from WhatsApp can be received before the message_id is set in the db:
    
            the webhook may fail to find the message in the db and will not update its status to SENT,
            and the message will be stuck in PENDING status and will never be in sent status.
    
            This could lead to bunch of problems like:
            - the message will be stuck in PENDING status and will never be sent.
            - the message will be sent twice if the user retries sending the message.
            
        Solution #5:
            We need a way that guarantee that we will effectively receive and process the webhook one time only:
            
            - we can use a retry in the webhook handler to retry finding the message in the db if it's not found,
            after a wait time, but this is not a good solution because it will 
            delay the webhook response and can cause a timeout.
    
            - we can use inbox pattern to handle the webhook and update the message status in the db.
    
            - Inbox can provide us with event status so in this case we can keep the inboxevent status to NEW, so it can 
            be processed again till the the message is appear in the db
        
        """

    process_webhook_data(received_data)

    return JsonResponse({"status": "success"})