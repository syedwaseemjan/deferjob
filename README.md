# deferjob

A small Python library for work that must happen later — hours later, or months later — and still be there when the day arrives.

The schedule lives in **Postgres**, as rows in a table. You insert a job when you book the event. You change the date when the event moves. You cancel the job when a dispute opens. A worker process asks Postgres, once a minute, “what is due?” and runs those rows.

It is a pip package (`pip install deferjob`), not a Postgres extension. You need Postgres 13+ and Python 3.11+.

This exists because [Celery `eta` is the wrong place to keep work that is months away](https://waseem.is-a.dev/blog/scheduling-months-ahead-without-celery/).

---

## What it is

deferjob is a **durable delayed-job table** plus a **poller**.

- **Durable** means the future work is data in your database. A deploy, a crash, or a new server does not forget that a wedding closes in November.
- **Delayed** means “run at this timestamp,” not “run as soon as a worker is free.”
- **Job** means a named piece of your code (close the event, complete the order, auto-resolve the dispute) plus a small JSON payload, usually just ids.

It is not a general job queue, not a workflow engine, and not a cron replacement. Those tools solve different problems. See [What it cannot do](#what-it-cannot-do).

---

## Why it exists

Imagine a customer books a chef in August for a wedding in November. On the event date you close the event. A few hours later you mark the order complete and pay the chef, unless someone opened a dispute.

That is not “run this in a few seconds.” That is “remember this for three months, then do it, and let me change my mind in between.”

A common shortcut is Celery’s `eta`: tell the worker a timestamp, walk away. That looks like one line. The promise is actually being kept in a worker process’s memory, or in Redis treated as a cache. Then:

- A deploy restarts the workers. The November job was sitting inside August’s process.
- Redis visibility timeout cannot tell “this worker is waiting until November” from “this worker died.” The same job gets handed out again and again, or a real crash sits unhealed for as long as your longest delay.
- Moving the wedding means revoking a broker task id you stored on the booking. You cannot `SELECT` “everything due next week.” The schedule is a side effect you chase with ids.

The thing you actually care about — *this event closes on this date* — is a fact about the booking. It should be a row next to the booking.

That is all deferjob is. Postgres remembers. A worker checks the clock.

---

## What it does, and why

### 1. Store the job as a row

Scheduling is an `INSERT` into `defer_jobs`.

```python
from datetime import datetime
from zoneinfo import ZoneInfo
from deferjob import Defer, Job

jobs = Defer("postgresql://localhost/app")
jobs.install()

jobs.schedule(
    "close_event",
    run_at=datetime(2024, 11, 12, 9, 0, tzinfo=ZoneInfo("UTC")),
    payload={"event_id": 42},
    key="event:42:close",
)
```

**Why:** A row survives deploys, new machines, and Redis failovers. You can open psql and see it. You can back it up with the rest of the database. The delay is the `run_at` column, not a process that has to stay alive until November.

`run_at` must be timezone-aware. Naive datetimes are rejected. Scheduling is a clock problem; guessing the timezone is how you close a wedding on the wrong day.

`payload` should be identifiers (`event_id`, `order_id`), not a copy of the order as it looked in August. In November the handler loads the live row.

### 2. Give the job a name you chose (`key`)

`key` is optional, but it is the way you should talk about a job.

```text
event:42:close
order:99:complete
dispute:7:autoresolve
```

**Why:** In the Celery version, every table grew a `*_task_id` column that pointed at a message inside the broker. To move a date you had to find that id and hope `revoke` reached the worker holding it.

Here the key *is* the name. One pending job per key (enforced by a unique index). You do not store a broker id on the booking.

### 3. Move a date, or cancel, like any other data

```python
jobs.reschedule(key="event:42:close", run_at=new_end)
jobs.cancel(key="order:99:complete")
jobs.list(key_prefix="event:42:")
```

**Why:** Customers move weddings. Disputes open. “What is still scheduled for this booking?” is a product question. Those are updates and selects, not control-plane RPCs to a worker fleet.

`cancel` and `reschedule` only work while the job is **pending**. If a worker has already claimed it (`running`), these calls raise `JobNotPending`. They do not stop a handler that is already in progress. See [What it cannot do](#what-it-cannot-do).

### 4. Put the job in the same transaction as the booking

```python
with psycopg.connect(DSN) as conn:
    booking_id = insert_booking(conn, ...)
    jobs.schedule(
        "close_event",
        run_at=event_ends_at,
        payload={"event_id": booking_id},
        key=f"event:{booking_id}:close",
        conn=conn,
    )
    conn.commit()
```

**Why:** You do not want a booking with no close job, or a close job with no booking. If the request fails after the insert, both roll back.

If you pass `conn=`, deferjob **does not commit it**. That connection is yours. If you omit `conn=`, deferjob opens its own connection and commits.

### 5. Run what is due, without holding the future in memory

A worker process loops:

1. Give back jobs stuck in `running` for too long (a worker crashed mid-job).
2. Ask Postgres for the next pending row whose `run_at` is in the past.
3. Mark it `running` and run your handler.
4. Mark it `done`, or put it back as `pending` for a retry, or mark it `failed`.

```python
@jobs.job("close_event")
def close_event(job: Job) -> None:
    event = events.get(job.payload["event_id"])
    if event.already_closed:
        return
    event.close()


jobs.run()  # process; polls about every 60 seconds
```

Or:

```bash
deferjob worker --app myapp.jobs:jobs
```

**Why:** The worker can die at any time. The November jobs are not inside it. They are still pending rows. The next worker will see them when November comes.

Several workers can run at once. Claiming uses `FOR UPDATE SKIP LOCKED`: if one worker has a row, the next worker skips it and takes a different one. They do not block each other.

A job may run up to one poll interval late (60 seconds by default). For closing an event or paying a chef the next day, a minute does not matter. Still having the job in November does.

### 6. Retry a failed handler, then stop

If the handler raises, the job goes back to `pending` with `run_at` pushed forward (60s, 120s, 240s, … capped at an hour). After `max_attempts` (default 5) it becomes `failed` and stays that way so you can look at `last_error`.

**Why:** Networks blip. The payment API is down for two minutes. Automatic retries cover that. They should not retry forever and they should not hide the failure. A missing handler (you deployed a job name the worker does not know) is marked `failed` immediately, not retried. That is a sharp edge during rolling deploys — see below.

### 7. Recover a worker that died mid-job

When a job is claimed, `locked_at` is set. If the process is killed, the row stays `running`. After `reclaim_after` (15 minutes by default), another worker sets it back to `pending` and someone else can take it.

**Why:** This is the correct use of a visibility timeout. It answers “how long do we wait before assuming this in-flight run is dead?” It does **not** answer “how far ahead may I schedule?” Those are different questions. Celery-on-Redis conflates them. deferjob does not. A job scheduled for November does not need a three-month timeout.

---

## The life of a job

```text
schedule() ──► pending ──► running ──► done
                 │            │
                 │            ├── handler error, attempts left ──► pending (later)
                 │            └── handler error, no attempts left ──► failed
                 │
                 └── cancel() ──► cancelled

reschedule() only while pending
```

| Status      | Meaning                                      | You can cancel / reschedule? |
| ----------- | -------------------------------------------- | ---------------------------- |
| `pending`   | Waiting for `run_at`                         | Yes                          |
| `running`   | A worker has claimed it                      | No                           |
| `done`      | Handler returned without raising             | No                           |
| `failed`    | Handler raised until `max_attempts`, or no handler is registered | No            |
| `cancelled` | You called `cancel` while it was pending     | No                           |

`get(key=...)` only returns a job that is still `pending` or `running` — the live one for that key. History (`done`, `failed`, `cancelled`) is still in the table; use `list(key=..., status="done")` or SQL.

The same `key` can be scheduled again after the previous row is `done` / `failed` / `cancelled`. The unique index only covers live rows.

---

## How the worker claims a row

This is the query. It is the whole trick.

```sql
UPDATE defer_jobs
SET status = 'running', attempts = attempts + 1, locked_at = now()
WHERE id = (
    SELECT id FROM defer_jobs
    WHERE status = 'pending' AND run_at <= now()
    ORDER BY run_at, id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING *;
```

`SKIP LOCKED` means “if another worker already has this row in a transaction, do not wait, take the next one.” Postgres shipped that in 9.5.

The future work is never loaded into the worker until it is due. Ten workers and twenty November events does not mean twenty Python objects living in RAM from August.
