from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any

import psycopg
from psycopg import sql

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from deferjob.errors import (
    JobExists,
    JobNotFound,
    JobNotPending,
    NotConfigured,
    UnknownJob,
)
from deferjob.models import Job, job_from_mapping
from deferjob.schema import check_table, install_sql

_RETURNING = """
    id, name, key, run_at, status, attempts, max_attempts,
    payload, last_error, created_at, updated_at, locked_at
"""

Connect = Callable[[], Any]
Conn = psycopg.Connection[Any]
Handler = Callable[[Job], None]


def default_backoff(attempts: int) -> timedelta:
    """60s, 120s, 240s, ... capped at one hour."""
    seconds = min(60 * (2 ** max(attempts - 1, 0)), 3600)
    return timedelta(seconds=seconds)


def _like_prefix(prefix: str) -> str:
    escaped = prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"{escaped}%"


def require_aware(value: datetime, name: str = "run_at") -> datetime:
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value


class Defer:
    """Schedule, change, and run delayed jobs stored in Postgres.

    Pass a connection into mutating methods to keep the job in the same
    transaction as the row it belongs to.
    """

    def __init__(
        self,
        conninfo: str | None = None,
        *,
        connect: Connect | None = None,
        table: str = "defer_jobs",
        backoff: Callable[[int], timedelta] = default_backoff,
        reclaim_after: timedelta = timedelta(minutes=15),
        poll_interval: float = 60.0,
    ) -> None:
        self.conninfo = conninfo
        self._connect = connect
        self.table = check_table(table)
        self.backoff = backoff
        self.reclaim_after = reclaim_after
        self.poll_interval = poll_interval
        self._handlers: dict[str, Handler] = {}

    def configure(self, conninfo: str) -> None:
        self.conninfo = conninfo

    def job(self, name: str | Handler | None = None) -> Any:
        """Register a handler. Use as @jobs.job or @jobs.job(\"name\")."""

        def register(fn: Handler, job_name: str) -> Handler:
            self._handlers[job_name] = fn
            return fn

        if callable(name):
            return register(name, name.__name__)

        def decorator(fn: Handler) -> Handler:
            return register(fn, name or fn.__name__)

        return decorator

    def handler(self, name: str) -> Handler:
        try:
            return self._handlers[name]
        except KeyError:
            raise UnknownJob(f"no handler registered for {name!r}") from None

    def install(self, *, conn: Conn | None = None) -> None:
        with self._connection(conn) as c:
            c.execute(install_sql(self.table))

    def schedule(
        self,
        name: str,
        *,
        run_at: datetime,
        payload: dict[str, Any] | None = None,
        key: str | None = None,
        max_attempts: int = 5,
        replace: bool = False,
        conn: Conn | None = None,
    ) -> Job:
        """Insert a job. With key and replace=True, move an existing one."""
        require_aware(run_at)
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        body = payload or {}
        with self._connection(conn) as c:
            if key is not None:
                existing = self._get(c, key=key)
                if existing is not None:
                    if existing.status == "running":
                        raise JobExists(
                            f"job {key!r} is already running (id={existing.id})"
                        )
                    if existing.status == "pending":
                        if not replace:
                            raise JobExists(
                                f"job {key!r} is already scheduled (id={existing.id})"
                            )
                        return self._update_pending(
                            c,
                            existing.id,
                            run_at=run_at,
                            payload=body,
                            max_attempts=max_attempts,
                            name=name,
                        )
            return self._insert(
                c,
                name=name,
                key=key,
                run_at=run_at,
                payload=body,
                max_attempts=max_attempts,
            )

    def cancel(
        self,
        *,
        key: str | None = None,
        id: int | None = None,
        conn: Conn | None = None,
    ) -> Job:
        """Mark a pending job cancelled. Raises if it is missing or not pending."""
        self._need_id_or_key(id, key)
        with self._connection(conn) as c:
            job = self._get(c, id=id, key=key)
            if job is None:
                raise JobNotFound(self._missing_msg(id, key))
            if job.status != "pending":
                raise JobNotPending(f"job id={job.id} is {job.status}, not pending")
            return self._set_status(c, job.id, "cancelled")

    def reschedule(
        self,
        *,
        run_at: datetime,
        key: str | None = None,
        id: int | None = None,
        conn: Conn | None = None,
    ) -> Job:
        require_aware(run_at)
        self._need_id_or_key(id, key)
        with self._connection(conn) as c:
            job = self._get(c, id=id, key=key)
            if job is None:
                raise JobNotFound(self._missing_msg(id, key))
            if job.status != "pending":
                raise JobNotPending(f"job id={job.id} is {job.status}, not pending")
            return self._update_pending(c, job.id, run_at=run_at)

    def _insert(
        self,
        conn: Conn,
        *,
        name: str,
        key: str | None,
        run_at: datetime,
        payload: dict[str, Any],
        max_attempts: int,
    ) -> Job:
        if key is None:
            conflict = sql.SQL("")
        else:
            conflict = sql.SQL(
                """
                ON CONFLICT (key)
                WHERE key IS NOT NULL AND status IN ('pending', 'running')
                DO NOTHING
                """
            )
        query = sql.SQL(
            """
            INSERT INTO {t} (name, key, run_at, payload, max_attempts)
            VALUES (%(name)s, %(key)s, %(run_at)s, %(payload)s, %(max_attempts)s)
            {conflict}
            RETURNING {cols}
            """
        ).format(t=self._t(), cols=sql.SQL(_RETURNING), conflict=conflict)
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                query,
                {
                    "name": name,
                    "key": key,
                    "run_at": run_at,
                    "payload": Jsonb(payload),
                    "max_attempts": max_attempts,
                },
            )
            row = cur.fetchone()
            if row is None:
                raise JobExists(f"job {key!r} is already scheduled")
            return job_from_mapping(row)

    def get(
        self,
        *,
        key: str | None = None,
        id: int | None = None,
        conn: Conn | None = None,
    ) -> Job | None:
        self._need_id_or_key(id, key)
        with self._connection(conn) as c:
            return self._get(c, id=id, key=key)

    def list(
        self,
        *,
        status: str | None = None,
        key: str | None = None,
        key_prefix: str | None = None,
        limit: int = 100,
        conn: Conn | None = None,
    ) -> list[Job]:
        if limit < 1:
            raise ValueError("limit must be at least 1")
        clauses = [sql.SQL("TRUE")]
        params: dict[str, Any] = {"limit": limit}
        if status is not None:
            clauses.append(sql.SQL("status = %(status)s"))
            params["status"] = status
        if key is not None:
            clauses.append(sql.SQL("key = %(key)s"))
            params["key"] = key
        if key_prefix is not None:
            clauses.append(sql.SQL("key LIKE %(key_prefix)s ESCAPE '\\'"))
            params["key_prefix"] = _like_prefix(key_prefix)
        query = sql.SQL(
            "SELECT {cols} FROM {t} WHERE {where} "
            "ORDER BY run_at, id LIMIT %(limit)s"
        ).format(
            cols=sql.SQL(_RETURNING),
            t=self._t(),
            where=sql.SQL(" AND ").join(clauses),
        )
        with self._connection(conn) as c, c.cursor(row_factory=dict_row) as cur:
            cur.execute(query, params)
            return [job_from_mapping(row) for row in cur.fetchall()]

    def next_run_at(self, *, conn: Conn | None = None) -> datetime | None:
        query = sql.SQL(
            "SELECT min(run_at) AS run_at FROM {t} WHERE status = 'pending'"
        ).format(t=self._t())
        with self._connection(conn) as c, c.cursor(row_factory=dict_row) as cur:
            cur.execute(query)
            row = cur.fetchone()
            if row is None:
                return None
            value = row["run_at"]
            if value is None:
                return None
            if not isinstance(value, datetime):
                raise TypeError(f"run_at must be datetime, got {type(value).__name__}")
            return value

    def claim(self, *, conn: Conn | None = None) -> Job | None:
        """Mark the next due job running and return it. Safe across workers."""
        query = sql.SQL(
            """
            UPDATE {t}
            SET status = 'running',
                attempts = attempts + 1,
                locked_at = now(),
                updated_at = now()
            WHERE id = (
                SELECT id FROM {t}
                WHERE status = 'pending' AND run_at <= now()
                ORDER BY run_at, id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING {cols}
            """
        ).format(t=self._t(), cols=sql.SQL(_RETURNING))
        with self._connection(conn) as c, c.cursor(row_factory=dict_row) as cur:
            cur.execute(query)
            row = cur.fetchone()
            if row is None:
                return None
            return job_from_mapping(row)

    def reclaim(
        self, *, after: timedelta | None = None, conn: Conn | None = None
    ) -> int:
        """Return jobs stuck in running to pending after a crash."""
        delay = after if after is not None else self.reclaim_after
        query = sql.SQL(
            """
            UPDATE {t}
            SET status = 'pending',
                locked_at = NULL,
                updated_at = now()
            WHERE status = 'running'
              AND locked_at < now() - %(after)s
            """
        ).format(t=self._t())
        with self._connection(conn) as c, c.cursor() as cur:
            cur.execute(query, {"after": delay})
            return cur.rowcount or 0

    def complete(self, job: Job, *, conn: Conn | None = None) -> Job:
        with self._connection(conn) as c:
            return self._set_status(c, job.id, "done")

    def fail(
        self, job: Job, error: BaseException | str, *, conn: Conn | None = None
    ) -> Job:
        message = (
            error if isinstance(error, str) else f"{type(error).__name__}: {error}"
        )
        with self._connection(conn) as c:
            if job.attempts >= job.max_attempts:
                return self._set_status(c, job.id, "failed", last_error=message)
            query = sql.SQL(
                """
                UPDATE {t}
                SET status = 'pending',
                    run_at = now() + %(delay)s,
                    last_error = %(last_error)s,
                    locked_at = NULL,
                    updated_at = now()
                WHERE id = %(id)s
                RETURNING {cols}
                """
            ).format(t=self._t(), cols=sql.SQL(_RETURNING))
            with c.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    query,
                    {
                        "id": job.id,
                        "delay": self.backoff(job.attempts),
                        "last_error": message,
                    },
                )
                row = cur.fetchone()
                assert row is not None
                return job_from_mapping(row)

    def _get(
        self,
        conn: Conn,
        *,
        id: int | None = None,
        key: str | None = None,
    ) -> Job | None:
        if id is not None:
            clause = sql.SQL("id = %(id)s")
            params: dict[str, Any] = {"id": id}
        else:
            clause = sql.SQL("key = %(key)s AND status IN ('pending', 'running')")
            params = {"key": key}
        query = sql.SQL("SELECT {cols} FROM {t} WHERE {where}").format(
            cols=sql.SQL(_RETURNING), t=self._t(), where=clause
        )
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, params)
            row = cur.fetchone()
            if row is None:
                return None
            return job_from_mapping(row)

    def _update_pending(
        self,
        conn: Conn,
        job_id: int,
        *,
        run_at: datetime | None = None,
        payload: dict[str, Any] | None = None,
        max_attempts: int | None = None,
        name: str | None = None,
    ) -> Job:
        assignments = [sql.SQL("updated_at = now()")]
        params: dict[str, Any] = {"id": job_id}
        if run_at is not None:
            assignments.append(sql.SQL("run_at = %(run_at)s"))
            params["run_at"] = run_at
        if payload is not None:
            assignments.append(sql.SQL("payload = %(payload)s"))
            params["payload"] = Jsonb(payload)
        if max_attempts is not None:
            assignments.append(sql.SQL("max_attempts = %(max_attempts)s"))
            params["max_attempts"] = max_attempts
        if name is not None:
            assignments.append(sql.SQL("name = %(name)s"))
            params["name"] = name
        query = sql.SQL(
            """
            UPDATE {t}
            SET {sets}
            WHERE id = %(id)s AND status = 'pending'
            RETURNING {cols}
            """
        ).format(
            t=self._t(),
            sets=sql.SQL(", ").join(assignments),
            cols=sql.SQL(_RETURNING),
        )
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, params)
            row = cur.fetchone()
            if row is None:
                raise JobNotPending(f"job id={job_id} is not pending")
            return job_from_mapping(row)

    def _set_status(
        self,
        conn: Conn,
        job_id: int,
        status: str,
        *,
        last_error: str | None = None,
    ) -> Job:
        query = sql.SQL(
            """
            UPDATE {t}
            SET status = %(status)s,
                last_error = %(last_error)s,
                locked_at = NULL,
                updated_at = now()
            WHERE id = %(id)s
            RETURNING {cols}
            """
        ).format(t=self._t(), cols=sql.SQL(_RETURNING))
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                query,
                {"id": job_id, "status": status, "last_error": last_error},
            )
            row = cur.fetchone()
            if row is None:
                raise JobNotFound(f"job id={job_id} not found")
            return job_from_mapping(row)

    @staticmethod
    def _need_id_or_key(id: int | None, key: str | None) -> None:
        if (id is None) == (key is None):
            raise ValueError("pass exactly one of id= or key=")

    @staticmethod
    def _missing_msg(id: int | None, key: str | None) -> str:
        if id is not None:
            return f"job id={id} not found"
        return f"job key={key!r} not found"

    def _t(self) -> sql.Identifier:
        return sql.Identifier(self.table)

    def _need_db(self) -> None:
        if not self.conninfo and self._connect is None:
            raise NotConfigured("pass a connection string or connect=")

    @contextmanager
    def _connection(self, conn: Conn | None = None) -> Iterator[Conn]:
        if conn is not None:
            yield conn
            return
        self._need_db()
        owned: Any
        if self._connect is not None:
            owned = self._connect()
        else:
            assert self.conninfo is not None
            owned = psycopg.connect(self.conninfo)
        if hasattr(owned, "__enter__"):
            with owned as opened:
                try:
                    yield opened
                    opened.commit()
                except Exception:
                    opened.rollback()
                    raise
            return
        try:
            yield owned
            owned.commit()
        except Exception:
            owned.rollback()
            raise
        finally:
            owned.close()
