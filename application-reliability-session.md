# Building Reliable Applications

## Interactive 3-Hour Session Plan

## Session goal

Introduce application reliability as a sequence of real engineering failures. For every scenario, participants first diagnose the problem, propose solutions, discuss trade-offs, and then see how the application evolves.

The session follows one payment/order application as it grows from a simple API into a distributed system:

```text
Client → API → PostgreSQL → Redis → Queue → Worker → External Provider
```

Each new component solves a problem but also introduces a new failure mode.

## Teaching format

- **60% problem solving and discussion**
- **40% explanation and solution walkthrough**
- Give participants **2–4 minutes** to discuss each problem before revealing the solution.
- Let some solutions create the next failure. For example, retrying a timed-out payment can create a duplicate charge.
- Use live diagrams, short code samples, audience voting, and small-group discussions.

---

## 1. Main subjects

| # | Subject | What we explore |
|---|---|---|
| 1 | Reliability fundamentals | What reliability means, failure assumptions, availability, and failure domains |
| 2 | Reliable request processing | Duplicate requests, timeouts, uncertain outcomes, retries, and idempotency |
| 3 | Concurrency and data integrity | Race conditions, transactions, concurrent updates, row locking, and deadlocks |
| 4 | Reliable asynchronous processing | Queues, workers, acknowledgements, redelivery, retries, poison tasks, and backpressure |
| 5 | Distributed-system failures | Partial failures, cross-service consistency, message consistency, and compensation |
| 6 | Database and infrastructure reliability | Bottlenecks, database overload, connection exhaustion, caching, and scaling |
| 7 | Production reliability and recovery | Observability, health checks, deployments, graceful degradation, rollback, and reconciliation |

---

## 2. Problems and solutions

### Problem 1 — The user clicked Pay twice

Two requests arrive almost simultaneously:

```text
POST /payments
POST /payments

→ Charge #1
→ Charge #2
```

**Question for participants:** How do we know these requests represent the same logical operation?

**What can go wrong:** The customer is charged twice even though they intended to make only one payment.

**Solution:**

- Require an idempotency key for operations that must happen only once.
- Store the key with the operation and its result.
- Enforce uniqueness at the database level.
- Return the previous result when the same key is received again.
- Define the scope and lifetime of each key.

---

### Problem 2 — The request timed out. Did the payment happen?

```text
Our API → Payment provider
               ↓
          Payment succeeds
               ↓
        Response gets lost
```

Our system only sees:

```text
TimeoutError
```

**Question for participants:** Is it safe to retry?

**What can go wrong:** The provider may already have charged the customer. A blind retry can create a second charge.

**Solution:**

- Treat a timeout as an **unknown outcome**, not an automatic failure.
- Use the same idempotency key when retrying.
- Query the provider using the operation or correlation ID.
- Move the payment to a state such as `PROCESSING` or `UNKNOWN` until confirmed.
- Reconcile uncertain payments asynchronously.
- Use bounded timeouts, retry limits, exponential backoff, and jitter.

---

### Problem 3 — We only have one item left

```python
if product.stock > 0:
    product.stock -= 1
    product.save()
```

Two requests both read:

```text
stock = 1
```

Both requests then succeed.

**Question for participants:** Why did valid code produce invalid business state?

**What can go wrong:** The system sells more stock than it owns because checking and updating were separate concurrent operations.

**Solution:**

- Make the validation and update atomic.
- Use a conditional database update such as `UPDATE ... WHERE stock > 0`.
- Alternatively, lock the product row inside a transaction.
- Add a database constraint when practical so invalid state cannot be committed.

---

### Problem 4 — The balance became negative

```text
Balance: $100

Worker A withdraws $80
Worker B withdraws $80
```

Each worker independently sees enough balance.

**Question for participants:** Which resource must be protected, and for how long?

**What can go wrong:** Both withdrawals pass validation and the final balance becomes invalid.

**Solution:**

- Begin a database transaction.
- Lock the balance or account row using `SELECT ... FOR UPDATE`.
- Re-read and validate the balance after acquiring the lock.
- Perform the subtraction within the same transaction.
- Add a database constraint such as `balance >= 0` as a final safety layer.

---

### Problem 5 — Everything suddenly stopped

Transaction A:

```text
lock account 1
wait for account 2
```

Transaction B:

```text
lock account 2
wait for account 1
```

**Question for participants:** Why can neither transaction continue?

**What can go wrong:** A circular wait creates a deadlock. The database aborts one transaction, or requests remain blocked until a timeout.

**Solution:**

- Lock shared resources in a consistent order, such as ascending account ID.
- Keep transactions short.
- Avoid network calls while holding database locks.
- Detect deadlock errors and retry the aborted transaction safely.
- Monitor lock wait time and transaction duration.

---

### Problem 6 — The worker died halfway through the job

```text
Queue
  ↓
Worker
  ↓
Charge customer
  ↓
Worker crashes
```

**Questions for participants:** Should the queue run the task again? What if the charge already happened?

**What can go wrong:** Acknowledging before execution can lose work. Acknowledging after execution can cause redelivery and duplicate side effects.

**Solution:**

- Acknowledge after successful processing when redelivery is required.
- Make the task idempotent because late acknowledgement provides at-least-once delivery, not exactly-once execution.
- Persist task or operation state.
- Use provider-side idempotency for external side effects.
- Configure visibility timeouts appropriately.

---

### Problem 7 — The same task ran twice

```text
process_payment(123)
process_payment(123)
```

Both workers begin processing.

**Question for participants:** Is duplicate delivery itself a bug?

**What can go wrong:** Queues can redeliver messages because acknowledgements are lost, workers crash, or visibility timeouts expire.

**Solution:**

- Assume messages can be delivered more than once.
- Store a stable event or operation ID.
- Enforce uniqueness in the database.
- Use state transitions such as `NEW → PROCESSING → SUCCEEDED`.
- Lock or atomically claim the record before processing.
- Return safely when an operation is already complete.

---

### Problem 8 — One bad task keeps coming back forever

```text
Task → crash → retry → crash → retry → crash → retry
```

Meanwhile:

```text
Queue: 10
Queue: 100
Queue: 10,000
```

**Question for participants:** When should we stop retrying?

**What can go wrong:** Poison tasks and aggressive retries consume workers, increase queue latency, overload dependencies, and block healthy work.

**Solution:**

- Set a maximum retry count.
- Use exponential backoff with jitter.
- Retry only errors likely to be transient.
- Send exhausted tasks to a dead-letter queue or failure store.
- Separate critical and non-critical workloads into different queues.
- Add rate limits, queue-depth alerts, and backpressure.

---

### Problem 9 — Service A succeeded, but Service B failed

```text
Create order
   ↓
Save order ✓
   ↓
Reserve inventory ✓
   ↓
Send payment request ✗
```

**Question for participants:** Can we roll everything back across separate services and databases?

**What can go wrong:** The business operation becomes partially complete, while a local transaction cannot undo remote commits.

**Solution:**

- Model the operation as a stateful workflow or saga.
- Define compensating actions, such as releasing inventory.
- Make every step idempotent.
- Persist progress so execution can resume after a crash.
- Prefer eventual consistency where immediate global consistency is impractical.

---

### Problem 10 — The database committed, but the message was not sent

```python
payment.save()

# The application crashes here.

send_task(payment.id)
```

**Question for participants:** How do we keep the database and queue consistent without one shared transaction?

**What can go wrong:** The database says the payment exists, but no worker knows it should be processed.

**Solution:**

- Write the business record and an outbox event in the same database transaction.
- Have a publisher relay unsent outbox records to the queue.
- Mark records as published only after the broker accepts them.
- Make consumers idempotent because the publisher may send an event more than once.
- Use `transaction.on_commit` for simpler cases, while recognizing that it does not survive a crash after commit and before publish.

---

### Problem 11 — The external provider disagrees with our database

Our database:

```text
Payment #123: PROCESSING
```

Provider:

```text
Payment #123: CAPTURED
```

**Question for participants:** Which system is the source of truth for the charge?

**What can go wrong:** Lost webhooks, crashes, and network failures leave local state stale even when the external operation succeeded.

**Solution:**

- Define the source of truth for every important state.
- Process provider webhooks idempotently.
- Periodically reconcile incomplete or stale records with the provider.
- Correct local state from verified provider data.
- Alert on discrepancies that cannot be resolved automatically.

---

### Problem 12 — Traffic increased 10× and everything became slow

```text
10 req/s   → fine
100 req/s  → fine
1000 req/s → failure
```

Investigation:

```text
API CPU?   fine
Redis?     fine
Workers?   fine
Postgres?  100%
```

**Question for participants:** Will adding more API servers help?

**What can go wrong:** Horizontal application scaling creates more database queries and connections, making the real bottleneck worse.

**Solution:**

- Measure before scaling: latency percentiles, throughput, saturation, slow queries, locks, and connection counts.
- Optimize query patterns and remove N+1 queries.
- Add appropriate indexes based on real access patterns.
- Use connection pooling with carefully chosen limits.
- Cache suitable reads and move non-urgent work out of request paths.
- Apply rate limiting and backpressure.
- Load-test the full system, not only the API layer.

---

### Problem 13 — One dependency becomes slow and takes down everything else

Normally:

```text
API → Service B: 100 ms
```

Today:

```text
API → Service B: 30 seconds
```

Requests, workers, threads, and connections begin accumulating.

**Question for participants:** Why does Service A fail when only Service B is unhealthy?

**What can go wrong:** Resource exhaustion causes a cascading failure across otherwise healthy components.

**Solution:**

- Set explicit connection and response timeouts.
- Bound retries with backoff and jitter.
- Use circuit breakers to stop repeatedly calling an unhealthy dependency.
- Limit concurrency per dependency.
- Isolate resource pools so one dependency cannot consume everything.
- Degrade gracefully when the feature is non-critical.

---

### Problem 14 — We deployed successfully and production broke

```text
Tests ✓
Build ✓
Deploy ✓

New version
    ↓
Database migration
    ↓
Old workers still running
    ↓
Failure
```

**Question for participants:** What does a successful deployment actually mean?

**What can go wrong:** New code, old code, and the database schema temporarily coexist during rolling deployments. A migration or incompatible message can break running instances.

**Solution:**

- Use backward-compatible expand-and-contract migrations.
- Deploy code that can work with both old and new schemas during transition.
- Keep queue messages backward compatible or version them.
- Separate liveness from readiness checks.
- Use rolling, canary, or blue-green deployments.
- Monitor error rates and latency during rollout.
- Automate rollback when safe, while treating irreversible migrations carefully.

---

## 3. Suggested three-hour agenda

| Time | Activity |
|---|---|
| 00:00–00:15 | Introduction, reliability mindset, application baseline, and session rules |
| 00:15–00:40 | Problems 1–2: duplicate requests and uncertain outcomes |
| 00:40–01:10 | Problems 3–5: race conditions, critical state, and deadlocks |
| 01:10–01:20 | Break |
| 01:20–01:55 | Problems 6–8: workers, duplicate delivery, and retry storms |
| 01:55–02:25 | Problems 9–11: partial failures, outbox consistency, and reconciliation |
| 02:25–02:35 | Break |
| 02:35–02:55 | Problems 12–13: bottlenecks and cascading failures |
| 02:55–03:00 | Problem 14 summary, reliability checklist, and closing takeaway |

> If deployment reliability needs a full exercise, reduce the number of earlier scenarios or extend the last section by 10–15 minutes.

---

## 4. Interactive exercise pattern

Use the same cycle for every scenario:

1. **Show the failure** without naming the solution.
2. **Ask participants to predict** the system state and customer impact.
3. **Let groups propose a fix** for 2–4 minutes.
4. **Challenge the proposed fix** with a second failure or edge case.
5. **Introduce the reliability pattern** and explain its guarantees and limits.
6. **Update the architecture** to include the solution.
7. **Record one operational signal** that would reveal this failure in production.

Useful audience prompts:

- What exactly failed?
- What does the caller know, and what remains uncertain?
- Could the operation happen twice?
- What happens if the process crashes at this exact line?
- Which component is the source of truth?
- What protects the business invariant?
- Is this retry safe?
- How would we detect this in production?
- How does this solution fail under load?

---

## 5. Final reliability checklist

By the end of the session, participants should ask these questions when designing a system:

- What failures are expected?
- What happens when a request is duplicated?
- What happens when a response is lost?
- Which actions are safe to retry?
- Are business invariants protected atomically?
- Can workers execute the same task more than once?
- Can a bad task overwhelm the queue?
- Can database state and emitted messages disagree?
- What is the authoritative source for external state?
- How does the system recover from partial completion?
- What happens when a dependency becomes slow?
- Where are the bottlenecks under load?
- Can old and new versions run together during deployment?
- What metrics, logs, traces, and alerts reveal failure?
- How does the system reconcile and recover without manual intervention?

## Closing takeaway

Reliability does not mean preventing every failure. It means expecting failures, limiting their impact, preserving business correctness, making uncertainty visible, and giving the system a safe path to recovery.
