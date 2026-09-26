"""
Problem #1:

    A feature needs to be rolled out today, services we have:
        - web
        - 3 workers: for background jobs
    
    normal deployment process (simple):

        - push to branch
        - pull the branch in server
        - restart all 4 services

    
    wait...

    what if one worker was in a middle of a task??

    if we follow the above process as it's, the worker will shutdown 
    immediately and the task will be stopped

    what if the task can be sensitive and had a specific pipeline needs to be completed?

Solution #1:

    Graceful shutdown:

        the workers here will have a sigterm signal and celery will wait for the workers
        to finish their current tasks and stop consuming any new tasks.


"""


"""
Problem #2:
    Our platform has some features needs a real-time response and a delay of more than 2 seconds
    is not acceptable and the requirements is Zero down-time.

    When we have a graceful shutdown, the workers will not consume new tasks 
    and this process can take time till the current tasks finishes.

    What if there is a long-running task can run for minutes or more ??


Solution #2:
    Rolling deployment:

        In this case we can use the rolling deployment strategy, deploying the new workers
        before shutting down the old workers.

        So that, any new tasks will be consumed by the new workers and we will have zero down-time
        system.
"""

"""
Problem #3:
    Old tasks in the queue can be the first to consume by the new workers:

    The queues could still have old queued tasks that are changed in the new code, and the new workers
    should consume it.

    Example:
    --------

    1- Old:
    
        @task
        def send_report(report_id):
            # some logic
    
    2- New:

        @task
        def send_report(report_id, provider):
            # some logic
    
    The new workers consider that the task has two args not one, so it may fail.

Solution #3:
    Backward Compatible:

    Change for a task needed ? Make it backward compatible:

    It's preferable to add a totally new task instead of changing in the 
    definition of the current one.
    
    

"""


"""
Problem #4:
    Old and New DB models:
    
    Now, we have two set of workers, the first one work with the old DB models and the new ones
    work with the new DB models.

    The current state is:

    - Deployment done, new DB models is rolled out.
    - Old workers could produce failure because they still working with the old code and consider
    that the old DB models are still there!


Solution #4:
    Expand-and-contract:

    When designing the new solution that needs to be deployed we need to take in consideration
    that it should be an addition on the old DB models to not cause failure for the old workers.

    A probation period (compatibility window) for the old DB schema should be defined 
    and eventually deleted after we make sure that no workers or services are still using it.

    We should do it specifically as the following:

    1- Expand:
        Existing column: phone_number
        New column:      recipient_phone_number (nullable -for old rows values- or we can fill it eventually)

        At this point:

        Old code → continues using phone_number
        New code → can understand both columns

    2- Deploy Compatible code:
        Deploy code that can handle both fields:

        - Write (to both to let old and new codes pass):
            message.phone_number = phone_number
            message.recipient_phone_number = phone_number

            message.save(
                update_fields=[
                    "phone_number",
                    "recipient_phone_number",
                ]
            )

        - Read (read from both):
            phone_number = (
                message.recipient_phone_number
                or message.phone_number
            )
        
    3- Backfill existing data to the new column:

        We can have here a script to do this, 

        Take care!!

        Large tables needs to do it in batches to avoid DB overwhelming 
        (long transactions and expected blocking!)

        OR...

        We can follow an eventual backfill way

        on each read:

        check the new column if not has a value -> fill it

        on write:

        write for both columns

        this for hurry devs that don't have time ;-)
    
    4- Switch code fully to use the new schema:

        After finishing backfilling, we can switch the code fully to use the new schema.

    5- Contract:

        In a later deployment, we can remove the old schema from the DB safely.
        

"""