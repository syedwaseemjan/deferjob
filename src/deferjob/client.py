from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any

import psycopg
from psycopg import sql

from deferjob.errors import NotConfigured
from deferjob.schema import check_table, install_sql

Connect = Callable[[], Any]
Conn = psycopg.Connection[Any]


def default_backoff(attempts: int) -> timedelta:
    """60s, 120s, 240s, ... capped at one hour."""
    seconds = min(60 * (2 ** max(attempts - 1, 0)), 3600)
    return timedelta(seconds=seconds)


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

    def configure(self, conninfo: str) -> None:
        self.conninfo = conninfo

    def install(self, *, conn: Conn | None = None) -> None:
        with self._connection(conn) as c:
            c.execute(install_sql(self.table))

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
