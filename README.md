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
