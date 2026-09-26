import logging
import uuid
from datetime import timedelta

import redis
import requests
from celery import chord
from pydantic import BaseModel, ValidationError

logger = logging.getLogger(__name__)

redis_client = redis.Redis.from_url("redis://localhost:6379/0")

CUSTOMER_BATCH_SIZE = 10

LOCK_KEY = "locks:sync_all_leads"
LOCK_TTL = 300


class LeadData(BaseModel):
    external_id: str
    # Add the other required Lead fields here


def release_lock(lock_key, lock_token):
    redis_client.eval(
        """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """,
        1,
        lock_key,
        lock_token,
    )


@celery.periodic_task(run_every=timedelta(seconds=30), queue="long_running")
def get_and_create_leads():
    lock_token = str(uuid.uuid4())

    acquired = redis_client.set(
        LOCK_KEY,
        lock_token,
        nx=True,
        ex=LOCK_TTL,
    )

    if not acquired:
        logger.info("Lead synchronization is already running.")
        return

    try:
        customer_ids = list(
            Customer.objects.values_list("id", flat=True)
        )

        batches = [
            customer_ids[i:i + CUSTOMER_BATCH_SIZE]
            for i in range(0, len(customer_ids), CUSTOMER_BATCH_SIZE)
        ]

        if not batches:
            release_lock(LOCK_KEY, lock_token)
            return

        tasks = [
            process_leads_batch.s(batch).set(queue="long_running")
            for batch in batches
        ]

        chord(tasks)(
            release_sync_lock.s(lock_token).set(queue="long_running")
        )

    except Exception:
        release_lock(LOCK_KEY, lock_token)
        raise


@celery.task(time_limit=300, queue="LONG_TASKS")
def process_leads_batch(customer_ids):
    for customer_id in customer_ids:
        response = None

        try:
            response = requests.get(
                f"https://api.example.com/{customer_id}/leads",
                timeout=10,
            )
            response.raise_for_status()

            lead_objects = []

            for lead in response.json():
                try:
                    validated_lead = LeadData.model_validate(lead)

                    lead_objects.append(
                        Lead(**validated_lead.model_dump())
                    )

                except ValidationError:
                    logger.exception(
                        "Invalid lead data for customer %s. Lead: %s",
                        customer_id,
                        lead,
                    )
                    continue

            Lead.objects.bulk_create(
                lead_objects,
                ignore_conflicts=True,
            )

        except Exception:
            logger.exception(
                "Failed to process leads for customer %s. Response: %s",
                customer_id,
                response.content if response is not None else None,
            )
            continue


@celery.task
def release_sync_lock(results, lock_token):
    release_lock(LOCK_KEY, lock_token)