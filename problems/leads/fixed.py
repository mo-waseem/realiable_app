import logging
import uuid
from datetime import timedelta

import redis
import requests
from pydantic import BaseModel, ValidationError

logger = logging.getLogger(__name__)

redis_client = redis.Redis.from_url("redis://localhost:6379/0")

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
        for customer in Customer.objects.all():
            response = None

            try:
                response = requests.get(
                    f"https://api.example.com/{customer.id}/leads",
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
                            customer.id,
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
                    customer.id,
                    response.content if response is not None else None,
                )
                continue

    finally:
        release_lock(LOCK_KEY, lock_token)