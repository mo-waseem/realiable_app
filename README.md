# Building Reliable Applications

An interactive workshop on designing applications that stay correct when requests are duplicated, workers crash, dependencies slow down, and deployments overlap.

The materials combine a **three-hour session plan**, presentation slides, and Python examples. Each scenario starts with a failure, invites participants to propose a fix, and explores the trade-offs introduced by that fix.

## Start here

- **[Session plan](application-reliability-session.md)** — learning goals, 14 failure scenarios, discussion prompts, a timed agenda, and a closing reliability checklist.
- **[Presentation slides](Building%20Reliable%20Applications.pptx)** — download and open in PowerPoint or a compatible presentation tool.
- **[Code exercises](problems/)** — annotated examples covering lead synchronization, WhatsApp messaging, webhooks, and deployments.

## Who this is for

Backend developers and teams working with APIs, relational databases, background jobs, and external services. Basic familiarity with Python, database transactions, and task queues will help; the examples use Django ORM and Celery conventions.

## Topics covered

- **Request reliability:** idempotency, duplicate requests, timeouts, uncertain outcomes, and safe retries.
- **Data integrity:** atomic updates, unique constraints, row locking, and deadlocks.
- **Background processing:** task redelivery, acknowledgements, batching, retry limits, and backpressure.
- **Distributed workflows:** partial failures, compensation, transactional outboxes, inboxes, and reconciliation.
- **Infrastructure and operations:** database bottlenecks, dependency isolation, observability, and recovery.
- **Deployment reliability:** graceful shutdown, rolling deployments, backward-compatible tasks, and expand-and-contract migrations.

The session plan follows a payment/order application as its architecture grows:

```text
Client → API → PostgreSQL → Redis → Queue → Worker → External Provider
```

The code exercises apply related ideas to lead imports and messaging workflows.

## Exercise guide

| Scenario | Start with | Follow-up | Discussion focus |
| --- | --- | --- | --- |
| Lead synchronization | [Original example](problems/leads/get_and_create_leads.py) | [Revised example](problems/leads/fixed.py), then [batched version](problems/leads/fixed_with_batches.py) | Overlapping scheduled jobs, duplicate records, validation, timeouts, Redis locks, and workload isolation |
| WhatsApp message sending | [Original example and outbox walkthrough](problems/send_wa_message/send_wa_message.py) | [Revised example](problems/send_wa_message/fixed.py) | Saving intent transactionally, claiming work atomically, concurrent workers, and message ordering |
| Webhook handling | [Annotated scenario](problems/webhook_handler.py) | Discuss the proposed approaches in the file | Duplicate and out-of-order events, inboxes, state transitions, and reconciliation |
| Deployments | [Deployment scenarios](problems/deployment.py) | Walk through each rollout failure | In-flight tasks, worker replacement, task compatibility, and schema evolution |

## How to use the materials

For self-study, read the session plan and then work through the exercises in the order above. Read each original example before opening its revised version.

For a group session:

1. Show the initial code or failure scenario without revealing the solution.
2. Give participants **2–4 minutes** to identify the failure and propose a fix.
3. Challenge the fix: what if two workers run at once, the response is lost, or the process crashes immediately after an external action succeeds?
4. Walk through the proposed approach and discuss its guarantees and limits.
5. Identify a metric, log, or alert that would reveal the failure in production.

Use the [suggested three-hour agenda](application-reliability-session.md#3-suggested-three-hour-agenda) to pace the workshop.

## About the code examples

The Python files are teaching snippets intended for reading, discussion, and adaptation. The repository does not include a runnable Django project, dependency manifest, database models, service configuration, or a test suite.

The examples assume application-provided objects such as `Customer`, `Lead`, `Message`, `OutboxEntry`, a configured Celery app, and a WhatsApp client. Lead synchronization examples also use Redis, Requests, and Pydantic v2-style validation. External URLs are placeholders.

Files named `fixed.py` illustrate improvements within an exercise; they are intermediate designs with further failure cases to discuss. For example:

- Database uniqueness constraints are still needed to prevent duplicate leads when locks expire or executions overlap.
- An outbox claim does not by itself resolve a worker crash after the provider accepts a message, recover abandoned `PROCESSING` entries, or retry `FAILED` entries.
- Moving webhook processing to a queue shortens response time, but duplicate delivery still requires idempotent processing.

To turn an exercise into a runnable application, supply the models and constraints, framework imports, provider integration, and database/queue configuration, and adapt task registration and scheduling to your Celery version.

## Core takeaway

Reliability means expecting failures, limiting their impact, preserving business correctness, making uncertainty visible, and giving the system a safe path to recovery.
