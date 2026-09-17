from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

STATUSES = ("pending", "running", "done", "failed", "cancelled")


@dataclass(frozen=True, slots=True)
class Job:
    """One row from the job table.

    ``payload`` should be identifiers, not a snapshot of business state.
    The handler looks the live row up and checks it before acting.
    """

    id: int
    name: str
    key: str | None
    run_at: datetime
    status: str
    attempts: int
    max_attempts: int
    payload: dict[str, Any]
    last_error: str | None
    created_at: datetime
    updated_at: datetime
    locked_at: datetime | None


def job_from_mapping(row: dict[str, Any]) -> Job:
    payload = row["payload"]
    if payload is None:
        payload = {}
    if not isinstance(payload, dict):
        raise TypeError(f"payload must be an object, got {type(payload).__name__}")
    return Job(
        id=row["id"],
        name=row["name"],
        key=row["key"],
        run_at=row["run_at"],
        status=row["status"],
        attempts=row["attempts"],
        max_attempts=row["max_attempts"],
        payload=payload,
        last_error=row["last_error"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        locked_at=row["locked_at"],
    )
