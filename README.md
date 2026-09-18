# deferjob

A small Python library for work that must happen later, hours later or months later, and still be there when the day arrives.

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

The thing you actually care about (*this event closes on this date*) is a fact about the booking. It should be a row next to the booking.

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

**Why:** Networks blip. The payment API is down for two minutes. Automatic retries cover that. They should not retry forever and they should not hide the failure. A missing handler (you deployed a job name the worker does not know) is marked `failed` immediately, not retried. That is a sharp edge during rolling deploys. See below.

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

`get(key=...)` only returns a job that is still `pending` or `running`: the live one for that key. History (`done`, `failed`, `cancelled`) is still in the table; use `list(key=..., status="done")` or SQL.

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

---

## At least once, not exactly once

Everything that runs jobs this way can deliver **at least once**:

- The worker crashes after the handler committed a payment but before the row was marked `done`. Reclaim runs the handler again.
- Two workers overlap if a job runs longer than `reclaim_after`.
- A retry runs after a failure you thought had succeeded.

deferjob will not save you from a double payout. Your handler must look at current state and refuse to act twice:

```python
@jobs.job("complete_order")
def complete_order(job: Job) -> None:
    order = orders.get(job.payload["order_id"])
    if not order.is_live or order.dispute:
        return
    order.complete()
```

That check is not polish. It is the line between “the job ran twice” and “we paid the chef twice.” Write it on purpose.

---

## The table

`jobs.install()` creates this. For a real app, copy `install_sql()` into your own migrations so the table is versioned with the rest of the schema.

```sql
CREATE TABLE defer_jobs (
    id            bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    name          text NOT NULL,          -- handler to run
    key           text,                   -- optional business name
    run_at        timestamptz NOT NULL,   -- when it becomes due
    status        text NOT NULL DEFAULT 'pending',
    attempts      integer NOT NULL DEFAULT 0,
    max_attempts  integer NOT NULL DEFAULT 5,
    payload       jsonb NOT NULL DEFAULT jsonb_build_object(),
    last_error    text,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    locked_at     timestamptz             -- set while running
);
```

Indexes:

- pending rows by `run_at` (so “what is due?” is cheap)
- unique `key` while status is `pending` or `running`
- `locked_at` on running rows (so reclaim is cheap)

Default table name is `defer_jobs`. You can pass `table="..."` if you need another name. It must be a simple identifier, not `schema.table`.

---

## API in short

| You want to…                         | Call                                      |
| ------------------------------------ | ----------------------------------------- |
| Create the table                     | `jobs.install()` or `install_sql()`       |
| Register a handler                   | `@jobs.job("close_event")`                |
| Schedule work                        | `jobs.schedule(name, run_at=..., ...)`    |
| Replace an existing pending job      | `schedule(..., key=..., replace=True)`    |
| Move the time                        | `jobs.reschedule(key=..., run_at=...)`    |
| Stop a pending job                   | `jobs.cancel(key=...)`                    |
| Read the live job                    | `jobs.get(key=...)` or `get(id=...)`      |
| List by booking, status, etc.        | `jobs.list(key_prefix=..., status=...)`   |
| Run due jobs once (tests, cron)      | `jobs.run_once()`                         |
| Run until the process is stopped     | `jobs.run()` or `deferjob worker`         |

`schedule` / `cancel` / `reschedule` / `get` / `list` all take optional `conn=` so they can share your transaction.

Errors you will see:

| Error           | When                                              |
| --------------- | ------------------------------------------------- |
| `JobExists`     | That `key` already has a pending or running job   |
| `JobNotFound`   | No matching id, or no live job for that key       |
| `JobNotPending` | You tried to cancel or move a job that is not waiting |
| `UnknownJob`    | You asked for a handler name that was never registered |
| `NotConfigured` | No connection string and no `connect=`            |

---

## What it cannot do

This list is the product boundary, plus the current limits of this library. Read it before you put money on the path.

### It is not a queue for work that should run now

If the user clicks “export CSV” and a worker should start in the next second, use a queue (Celery without far-future `eta`, RQ, SQS, Postgres listen/notify queues, …). deferjob will do it if you set `run_at` to now, but it polls, runs one job after another in a single process, and opens a new database connection for each step. That is the wrong shape for a hot path.

### It is not exactly-once

See above. Handlers must be idempotent. There is no distributed transaction around “your side effect + mark the job done.”

### It cannot stop a job that has already started

`cancel` only works in `pending`. If the worker has claimed `complete_order` and a dispute lands in the same minute, cancel raises `JobNotPending` and the handler still runs. The handler has to check `order.dispute` itself.

`cancel` and `reschedule` are also not one atomic `UPDATE ... WHERE status = 'pending'`. A claim can sneak in between the read and the write. Do not treat cancel as a lock on the business action.

### It cannot promise a job will survive a rolling deploy unchanged

If you enqueue `name="payout_v2"` and an old worker is still running, that worker does not have the handler. deferjob marks the job **failed immediately** (no retries). A new job name and an old worker is a lost job unless you drain workers or keep the old handler registered.

A job queued in August always runs against **November’s code**. That is a feature (you can fix the handler) and a constraint (do not put positional arguments you will rename into `payload` and then forget). Keep payloads as stable ids.

### It cannot run a hung handler forever, but it also cannot stop one

There is no per-job timeout. A handler that blocks on a network call sits in `running` until `reclaim_after` (15 minutes). Then a second worker may start the same job while the first is still going. Your handler must tolerate that overlap.

### It cannot hide Postgres from you

You need a database you already trust. deferjob does not ship:

- connection pooling on the default path (each API call may open and close a connection; pass `connect=` with a pool if you care)
- async
- Django or SQLAlchemy session helpers (you can pass the raw psycopg connection)
- a schema name (`app.defer_jobs`)
- automatic purging of old `done` / `failed` / `cancelled` rows (the table grows until you delete them)
- versioned migrations (only `CREATE TABLE IF NOT EXISTS`)
- metrics, an admin UI, or alerts when a job fails
- `LISTEN/NOTIFY` (the worker wakes on a timer, not when you insert a near-term job)
- cron / repeating jobs (one `run_at` per row; to repeat, the handler schedules the next one)
- workflows (wait, then branch, then wait again). That is Step Functions or Temporal. A dispute with several deadlines is several rows, or one row that reschedules itself.

### It cannot replace “wake me up”

Celery Beat, systemd timers, EventBridge, or Kubernetes cron can start or poke the worker. They should not *store* the November wedding. The schedule stays in the table. Something still has to run `jobs.run()` in a process that stays up.

### It cannot make a bad handler safe

If `complete_order` pays the chef without checking state, a retry or a reclaim double-pays. The library will not notice. That is the same rule as SQS, Celery, and EventBridge.

---

## When this is the right tool

Use it when:

- The work is tied to a row you already keep in Postgres (booking, order, dispute).
- The delay is long enough that a process restart is certain (hours to months).
- You need to change or cancel the work as data changes.
- A minute late is acceptable.
- The handler can check “is this still the right thing to do?”

Closing an event on its date, completing an order a few hours later, auto-rejecting a booking nobody accepted, and resolving a quiet dispute are that shape.

Do not use it when you need sub-second dispatch, exactly-once side effects, multi-step sagas, or a high-throughput queue. Use the tool that is for that.

---

## Install and run

```bash
pip install deferjob
```

```python
# myapp/jobs.py
from deferjob import Defer, Job

jobs = Defer("postgresql://localhost/app")

@jobs.job("close_event")
def close_event(job: Job) -> None:
    ...
```

```bash
deferjob install --dsn postgresql://localhost/app
deferjob worker --app myapp.jobs:jobs
```

`Defer(conninfo)` or `Defer(connect=pool.connection)` if you already have a pool. `jobs.configure(dsn)` is there for app factories that register handlers before they have a URL.

---

## Develop

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest
```

Tests start Postgres with Docker unless `DATABASE_URL` is set.

Releases publish to PyPI through GitHub Actions (Trusted Publishing). Bump the version, then create a GitHub release named `v0.1.1` (or later). There is no upload token on a laptop.

---

## Status

**0.1.1** stores future work in a table and a small worker runs what is due, and you still handle shared database connections, jobs that have already started, and old finished rows yourself
