from __future__ import annotations

from datetime import UTC, datetime, timedelta

import psycopg
from psycopg import sql

from deferjob import Defer, Job


def _now() -> datetime:
    return datetime.now(UTC)


def test_run_once_executes_due_job(jobs: Defer) -> None:
    seen: list[int] = []

    @jobs.job("close_event")
    def close_event(job: Job) -> None:
        seen.append(job.payload["event_id"])

    jobs.schedule(
        "close_event",
        run_at=_now() - timedelta(seconds=1),
        payload={"event_id": 44},
        key="event:44:close",
    )
    assert jobs.run_once() == 1
    assert seen == [44]
    done = jobs.list(status="done")
    assert len(done) == 1
    assert done[0].key == "event:44:close"


def test_future_job_is_not_claimed(jobs: Defer) -> None:
    @jobs.job("close_event")
    def close_event(job: Job) -> None:
        raise AssertionError("should not run")

    jobs.schedule(
        "close_event",
        run_at=_now() + timedelta(days=30),
        key="event:1:close",
    )
    assert jobs.claim() is None
    assert jobs.run_once() == 0
    pending = jobs.get(key="event:1:close")
    assert pending is not None
    assert pending.status == "pending"
    assert pending.attempts == 0


def test_missing_handler_fails_without_retry(jobs: Defer) -> None:
    jobs.schedule("ghost", run_at=_now() - timedelta(seconds=1), key="ghost:1")
    assert jobs.run_once() == 1
    rows = jobs.list(status="failed")
    assert len(rows) == 1
    assert rows[0].last_error is not None
    assert "no handler" in rows[0].last_error


def test_handler_error_retries_then_fails(jobs: Defer) -> None:
    jobs.backoff = lambda attempts: timedelta(hours=1)

    @jobs.job("pay_chef")
    def pay_chef(job: Job) -> None:
        raise RuntimeError("card declined")

    jobs.schedule(
        "pay_chef",
        run_at=_now() - timedelta(seconds=1),
        key="order:9:pay",
        max_attempts=2,
    )
    assert jobs.run_once() == 1
    retry = jobs.get(key="order:9:pay")
    assert retry is not None
    assert retry.status == "pending"
    assert retry.attempts == 1
    assert retry.last_error is not None
    assert "card declined" in retry.last_error
    assert retry.run_at > _now()

    jobs.reschedule(key="order:9:pay", run_at=_now() - timedelta(seconds=1))
    assert jobs.run_once() == 1
    assert jobs.get(key="order:9:pay") is None
    failed = jobs.list(status="failed", key="order:9:pay")
    assert len(failed) == 1
    assert failed[0].attempts == 2


def test_skip_locked_gives_each_worker_a_different_job(jobs: Defer, dsn: str) -> None:
    jobs.schedule("close_event", run_at=_now() - timedelta(seconds=1), key="a")
    jobs.schedule("close_event", run_at=_now() - timedelta(seconds=1), key="b")

    first = psycopg.connect(dsn)
    second = psycopg.connect(dsn)
    try:
        claimed = jobs.claim(conn=first)
        other = jobs.claim(conn=second)
        assert claimed is not None
        assert other is not None
        assert {claimed.key, other.key} == {"a", "b"}
    finally:
        first.rollback()
        first.close()
        second.rollback()
        second.close()


def test_reclaim_returns_stale_running_jobs(jobs: Defer, dsn: str) -> None:
    created = jobs.schedule(
        "close_event",
        run_at=_now() - timedelta(seconds=1),
        key="event:7:close",
    )
    with psycopg.connect(dsn) as conn:
        conn.execute(
            sql.SQL(
                """
                UPDATE {t}
                SET status = 'running',
                    attempts = 1,
                    locked_at = now() - interval '1 hour'
                WHERE id = %s
                """
            ).format(t=sql.Identifier(jobs.table)),
            (created.id,),
        )
        conn.commit()

    assert jobs.reclaim(after=timedelta(minutes=15)) == 1
    job = jobs.get(id=created.id)
    assert job is not None
    assert job.status == "pending"
    assert job.locked_at is None


def test_handler_can_be_registered_by_function_name(jobs: Defer) -> None:
    seen: list[str] = []

    @jobs.job
    def ping(job: Job) -> None:
        seen.append(job.name)

    jobs.schedule("ping", run_at=_now() - timedelta(seconds=1))
    assert jobs.run_once() == 1
    assert seen == ["ping"]
