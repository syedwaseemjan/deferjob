from __future__ import annotations

from datetime import UTC, datetime, timedelta

import psycopg
import pytest

from deferjob import Defer, JobExists, JobNotFound, JobNotPending


def _soon(seconds: float = 0) -> datetime:
    return datetime.now(UTC) + timedelta(seconds=seconds)


def test_schedule_requires_aware_datetime(jobs: Defer) -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        jobs.schedule("close_event", run_at=datetime(2024, 11, 12, 9, 0, 0))


def test_schedule_and_get_by_key(jobs: Defer) -> None:
    when = _soon(3600)
    created = jobs.schedule(
        "close_event",
        run_at=when,
        payload={"event_id": 9},
        key="event:9:close",
    )
    fetched = jobs.get(key="event:9:close")
    assert fetched is not None
    assert fetched.id == created.id
    assert fetched.name == "close_event"
    assert fetched.payload == {"event_id": 9}
    assert fetched.status == "pending"


def test_duplicate_key_raises(jobs: Defer) -> None:
    jobs.schedule("close_event", run_at=_soon(60), key="event:1:close")
    with pytest.raises(JobExists):
        jobs.schedule("close_event", run_at=_soon(120), key="event:1:close")


def test_replace_moves_the_existing_job(jobs: Defer) -> None:
    first = jobs.schedule(
        "close_event",
        run_at=_soon(60),
        payload={"event_id": 1},
        key="event:1:close",
    )
    later = _soon(86400)
    second = jobs.schedule(
        "close_event",
        run_at=later,
        payload={"event_id": 1, "moved": True},
        key="event:1:close",
        replace=True,
    )
    assert second.id == first.id
    assert second.payload["moved"] is True
    assert second.run_at == later


def test_reschedule_updates_run_at(jobs: Defer) -> None:
    jobs.schedule("close_event", run_at=_soon(60), key="event:2:close")
    later = _soon(10_000)
    moved = jobs.reschedule(key="event:2:close", run_at=later)
    assert moved.run_at == later
    assert moved.status == "pending"


def test_cancel_pending_job(jobs: Defer) -> None:
    jobs.schedule("complete_order", run_at=_soon(60), key="order:3:complete")
    cancelled = jobs.cancel(key="order:3:complete")
    assert cancelled.status == "cancelled"
    assert jobs.get(key="order:3:complete") is None


def test_cancel_missing_job(jobs: Defer) -> None:
    with pytest.raises(JobNotFound):
        jobs.cancel(key="missing")


def test_cannot_reschedule_cancelled_job(jobs: Defer) -> None:
    created = jobs.schedule("close_event", run_at=_soon(60), key="event:4:close")
    jobs.cancel(id=created.id)
    with pytest.raises(JobNotPending):
        jobs.reschedule(id=created.id, run_at=_soon(120))


def test_same_key_can_be_scheduled_again_after_cancel(jobs: Defer) -> None:
    jobs.schedule("close_event", run_at=_soon(60), key="event:5:close")
    jobs.cancel(key="event:5:close")
    again = jobs.schedule("close_event", run_at=_soon(90), key="event:5:close")
    assert again.status == "pending"


def test_list_by_key_prefix(jobs: Defer) -> None:
    jobs.schedule("close_event", run_at=_soon(60), key="event:10:close")
    jobs.schedule("complete_order", run_at=_soon(120), key="event:10:complete")
    jobs.schedule("close_event", run_at=_soon(180), key="event:11:close")
    rows = jobs.list(key_prefix="event:10:")
    assert {row.key for row in rows} == {"event:10:close", "event:10:complete"}


def test_schedule_in_caller_transaction(jobs: Defer, dsn: str) -> None:
    when = _soon(60)
    with psycopg.connect(dsn) as conn:
        jobs.schedule(
            "close_event",
            run_at=when,
            key="event:txn:close",
            conn=conn,
        )
        other = Defer(dsn, table=jobs.table)
        assert other.get(key="event:txn:close") is None
        conn.commit()
    assert jobs.get(key="event:txn:close") is not None
