









@celery.periodic_task(run_every=timedelta(seconds=30))
def get_and_create_leads():
    for customer in Customer.objects.all():
        response = requests.get(f"https://api.example.com/{customer.id}/leads")
        for lead in response.json():
            lead_obj = Lead.objects.create(**lead)










# Get and create a lead 
# can be created twice ?!
# can block other leads from being created ?!
@celery.periodic_task(run_every=timedelta(seconds=30))
def get_and_create_leads():
    for customer in Customer.objects.all():
        response = requests.get(f"https://api.example.com/{customer.id}/leads")
        """
        Problem #1:
            if there is some exception here, others leads from other customers will be blocked
            like a dirty lead who doesn't have a valid data

        Solution #1:
            so better to catch the exception and log it, and continue with the next customer
            timeout is recommended to be set to avoid blocking the task for too long
        """

        for lead in response.json():
            """
            Problem #2:
                this can create a lead twice:
                - Another worker pick the next get_and_create_leads when this one is not finished yet
                - the task is retried
            
            Solution #2: 
                so better to use get_or_create or check if the lead already exists before creating it
                get_or_create needs unique field(s) constraint(s) to be set in the model, 
                otherwise it will create duplicates so we can check if the lead already exists 
                before creating it by its external_id or any other unique field(s)
            """

            """
            Problem #3:
                One malformed lead can raise an exception and prevent all
                remaining leads for this customer from being processed.
            
            Solution #3:
                Catch expected validation or database errors at the lead
                level, log enough context to investigate, and continue.
                
                Do not blindly catch every possible exception if the error
                indicates a programming or infrastructure failure.
            
            """
            lead_obj = Lead.objects.create(**lead)

    """
    Problem #4:
        if the task is taking too long, it can be killed by the celery worker because of timeout,
        so not all leads will be created in this case.
    
    Solution #4:
        Optimizing the task:
        - do it in batches and each batch is a separate task and can have 10 customers' leads for example.
        - creating the leads in bulk instead of one by one, but this can be tricky if we want to catch the exceptions for each lead and continue with the next lead.
        - for catching the exceptions for each lead we can use pydantic to validate the lead data before creating it, and if it's invalid we can log it and continue with the next lead.
        - we can consider this task as a long running task and enqueue it to a separate queue with a higher timeout, and the main queue can have a lower timeout to avoid blocking other tasks.
    """

    """
    # Problem #5:
        Celery Beat publishes this task every 30 seconds, even when the
        previous execution is still running.
        
        Another worker can therefore execute the same synchronization
        concurrently and attempt to create the same leads.
    
    # Solution #5:
        Use a distributed lock with one stable key, such as:

        locks:sync_all_leads

        Acquire it atomically with SET NX EX, use a unique ownership token,
        and release it only if the token still belongs to this worker.
        
        The expiration prevents a crashed worker from leaving a stale lock.
        
        However, the lock is not the primary protection against duplicates.
        Lead creation must still be idempotent and protected by a database
        unique constraint because locks can expire or fail.
    
    """









