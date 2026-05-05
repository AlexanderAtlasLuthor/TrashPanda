"""V2.10.11 — Persistent SMTP retry queue.

Per-job SQLite store under ``<run_dir>/smtp_retry_queue.sqlite`` for
rows that returned an operationally-transient SMTP outcome. The
:class:`SMTPVerificationStage` enqueues such rows after the
in-process retry budget (P1.1) is exhausted; a separate worker
(:mod:`app.smtp_retry_worker`) drains the queue with a longer-form
backoff schedule (15min / 30min / 60min) — the timeframe where real
greylisting actually resolves.

Why SQLite (not SQLAlchemy / Postgres)
--------------------------------------

The existing ``app.storage.StagingDB`` pattern uses raw SQLite per
run. Retry queue data is *per-job runtime state*, not durable SaaS
billing data, so it inherits the same store: one ``.sqlite`` file
alongside the run's other artifacts. Cleaning up a run = deleting
the run directory; no cross-job migrations or org tenancy needed.

State machine
-------------

Each row goes through:

    pending → running → {succeeded, exhausted, expired}
                      ↘ pending (next attempt scheduled)

* ``pending``    — waiting for the next retry; ``next_retry_at`` is
                   the earliest moment the worker may probe it.
* ``running``    — the worker has claimed the row; should not stay
                   in this state for long.
* ``succeeded``  — the probe returned a terminal verdict
                   (valid / invalid / catch_all_possible / 5xx).
* ``exhausted``  — ``attempt`` reached ``max_retries`` without a
                   terminal verdict.
* ``expired``    — ``created_at`` is older than the TTL (default
                   24h) and the row no longer reflects useful
                   campaign timing.

Concurrency note: the worker uses ``UPDATE … RETURNING`` to claim
rows atomically. Two workers running against the same DB will not
double-probe the same row.

Public API
----------

* :class:`SMTPRetryQueue` — open / create the per-run DB.
* :func:`open_for_run` — convenience constructor that resolves
  ``<run_dir>/smtp_retry_queue.sqlite``.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence


SMTP_RETRY_QUEUE_FILENAME: str = "smtp_retry_queue.sqlite"
SMTP_RETRY_CONFIG_FILENAME: str = "smtp_retry_queue_config.json"


# State constants. String-typed for trivial inspection in raw SQL.
STATE_PENDING: str = "pending"
STATE_RUNNING: str = "running"
STATE_SUCCEEDED: str = "succeeded"
STATE_EXHAUSTED: str = "exhausted"
STATE_EXPIRED: str = "expired"

ALL_STATES: tuple[str, ...] = (
    STATE_PENDING,
    STATE_RUNNING,
    STATE_SUCCEEDED,
    STATE_EXHAUSTED,
    STATE_EXPIRED,
)


# Default schedule. Element ``i`` is the wait *before* attempt ``i+1``
# (i.e. retry #1 waits 15min after enqueue, retry #2 waits 30min
# after that, retry #3 waits 60min after that). After
# ``len(schedule)`` retries the row is marked ``exhausted``.
DEFAULT_RETRY_SCHEDULE_MINUTES: tuple[int, ...] = (15, 30, 60)
DEFAULT_TTL_HOURS: int = 24


_CREATE_TABLE_SQL = """\
CREATE TABLE IF NOT EXISTS smtp_retry_queue (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id          TEXT NOT NULL,
    source_row      INTEGER NOT NULL,
    email           TEXT NOT NULL,
    domain          TEXT NOT NULL,
    provider_family TEXT NOT NULL DEFAULT 'corporate_unknown',
    attempt         INTEGER NOT NULL DEFAULT 0,
    next_retry_at   TEXT NOT NULL,
    last_status     TEXT,
    last_response_code INTEGER,
    last_response_message TEXT,
    state           TEXT NOT NULL DEFAULT 'pending',
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    UNIQUE (job_id, source_row, email)
)
"""

_INDEX_SQL = (
    "CREATE INDEX IF NOT EXISTS idx_retry_pending ON smtp_retry_queue "
    "(state, next_retry_at)",
)


@dataclass(frozen=True, slots=True)
class RetryRow:
    """A single queue row, returned by ``claim_pending`` and ``snapshot``."""

    id: int
    job_id: str
    source_row: int
    email: str
    domain: str
    provider_family: str
    attempt: int
    next_retry_at: datetime
    last_status: str | None
    last_response_code: int | None
    last_response_message: str | None
    state: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class RetryQueueCounts:
    """Aggregated state counts for a run's retry queue."""

    pending: int = 0
    running: int = 0
    succeeded: int = 0
    exhausted: int = 0
    expired: int = 0

    @property
    def total(self) -> int:
        return (
            self.pending
            + self.running
            + self.succeeded
            + self.exhausted
            + self.expired
        )

    def to_dict(self) -> dict[str, int]:
        return {
            "pending": self.pending,
            "running": self.running,
            "succeeded": self.succeeded,
            "exhausted": self.exhausted,
            "expired": self.expired,
            "total": self.total,
        }


@dataclass(slots=True)
class RetryQueueConfig:
    """Per-job knobs for the retry worker.

    ``auto_retry_enabled`` is the opt-in flag the operator sets at
    upload time. When ``False`` the worker ignores the queue entirely
    (the operator-triggered ``run`` endpoint still drains on demand).
    """

    auto_retry_enabled: bool = False
    retry_schedule_minutes: tuple[int, ...] = field(
        default_factory=lambda: DEFAULT_RETRY_SCHEDULE_MINUTES
    )
    ttl_hours: int = DEFAULT_TTL_HOURS

    @property
    def max_retries(self) -> int:
        return len(self.retry_schedule_minutes)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def _format_dt(value: datetime) -> str:
    """Persist datetimes as ISO-8601 with UTC offset for stable ordering."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


def _parse_dt(value: str | None) -> datetime:
    """Inverse of ``_format_dt``; defaults to UTC epoch on bad input."""
    if not value:
        return datetime.fromtimestamp(0, tz=timezone.utc)
    try:
        dt = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return datetime.fromtimestamp(0, tz=timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _row_from_db(db_row: Sequence[Any]) -> RetryRow:
    return RetryRow(
        id=int(db_row[0]),
        job_id=str(db_row[1]),
        source_row=int(db_row[2]),
        email=str(db_row[3]),
        domain=str(db_row[4]),
        provider_family=str(db_row[5] or "corporate_unknown"),
        attempt=int(db_row[6]),
        next_retry_at=_parse_dt(db_row[7]),
        last_status=db_row[8],
        last_response_code=(
            int(db_row[9]) if db_row[9] is not None else None
        ),
        last_response_message=db_row[10],
        state=str(db_row[11]),
        created_at=_parse_dt(db_row[12]),
        updated_at=_parse_dt(db_row[13]),
    )


_SELECT_COLUMNS = (
    "id, job_id, source_row, email, domain, provider_family, "
    "attempt, next_retry_at, last_status, last_response_code, "
    "last_response_message, state, created_at, updated_at"
)


# --------------------------------------------------------------------------- #
# Queue
# --------------------------------------------------------------------------- #


class SMTPRetryQueue:
    """Per-run SQLite queue. Open with :func:`open_for_run`.

    The class never raises on caller error: malformed inserts are
    silently skipped, unknown row ids are no-ops. Tests / operator
    tooling can call ``snapshot()`` to inspect the full table.
    """

    def __init__(self, db_path: Path) -> None:
        self._path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(db_path))
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._init_schema()

    @property
    def path(self) -> Path:
        return self._path

    def _init_schema(self) -> None:
        self._conn.execute(_CREATE_TABLE_SQL)
        for stmt in _INDEX_SQL:
            self._conn.execute(stmt)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "SMTPRetryQueue":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    # --- Enqueue -------------------------------------------------------- #

    def enqueue(
        self,
        *,
        job_id: str,
        source_row: int,
        email: str,
        domain: str,
        provider_family: str,
        last_status: str | None,
        last_response_code: int | None,
        last_response_message: str | None,
        schedule_minutes: tuple[int, ...] = DEFAULT_RETRY_SCHEDULE_MINUTES,
        now: datetime | None = None,
    ) -> bool:
        """Insert a new pending row. Returns False on duplicate-key.

        ``next_retry_at`` is set to ``now + schedule_minutes[0]``. If
        the schedule is empty (operator disabled retries entirely),
        the row is enqueued already in ``exhausted`` so the snapshot
        still records the operational outcome without scheduling a
        probe.
        """
        if not email or not domain:
            return False
        ts = now or _utcnow()
        if not schedule_minutes:
            state = STATE_EXHAUSTED
            next_retry = ts
        else:
            state = STATE_PENDING
            next_retry = ts + timedelta(minutes=int(schedule_minutes[0]))
        try:
            self._conn.execute(
                """
                INSERT INTO smtp_retry_queue (
                    job_id, source_row, email, domain, provider_family,
                    attempt, next_retry_at, last_status,
                    last_response_code, last_response_message, state,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    int(source_row),
                    email,
                    domain,
                    provider_family or "corporate_unknown",
                    0,
                    _format_dt(next_retry),
                    last_status,
                    last_response_code,
                    last_response_message,
                    state,
                    _format_dt(ts),
                    _format_dt(ts),
                ),
            )
            self._conn.commit()
            return True
        except sqlite3.IntegrityError:
            # Duplicate (job_id, source_row, email) — already queued.
            return False

    # --- Claim / drain -------------------------------------------------- #

    def claim_pending(
        self,
        *,
        limit: int = 100,
        now: datetime | None = None,
    ) -> list[RetryRow]:
        """Atomically move up to ``limit`` due rows from pending → running.

        Uses a single ``UPDATE … RETURNING`` to claim. SQLite's
        ``RETURNING`` is supported from 3.35; we fall back to a
        select+update if not available. Two workers running against
        the same file therefore won't double-probe.
        """
        ts = now or _utcnow()
        ts_str = _format_dt(ts)

        # Detect RETURNING support.
        cursor = self._conn.execute("SELECT sqlite_version()")
        version = cursor.fetchone()[0]
        major, minor, *_ = version.split(".")
        has_returning = (int(major), int(minor)) >= (3, 35)

        if has_returning:
            cursor = self._conn.execute(
                f"""
                UPDATE smtp_retry_queue
                SET state = ?, updated_at = ?
                WHERE id IN (
                    SELECT id FROM smtp_retry_queue
                    WHERE state = ? AND next_retry_at <= ?
                    ORDER BY next_retry_at
                    LIMIT ?
                )
                RETURNING {_SELECT_COLUMNS}
                """,
                (STATE_RUNNING, ts_str, STATE_PENDING, ts_str, int(limit)),
            )
            rows = [_row_from_db(r) for r in cursor.fetchall()]
            self._conn.commit()
            return rows

        # Fallback: select then update inside a transaction.
        cursor = self._conn.execute(
            f"""
            SELECT {_SELECT_COLUMNS} FROM smtp_retry_queue
            WHERE state = ? AND next_retry_at <= ?
            ORDER BY next_retry_at
            LIMIT ?
            """,
            (STATE_PENDING, ts_str, int(limit)),
        )
        candidates = cursor.fetchall()
        if not candidates:
            return []
        ids = tuple(int(r[0]) for r in candidates)
        placeholder = ",".join("?" for _ in ids)
        self._conn.execute(
            f"""
            UPDATE smtp_retry_queue
            SET state = ?, updated_at = ?
            WHERE id IN ({placeholder})
            """,
            (STATE_RUNNING, ts_str, *ids),
        )
        self._conn.commit()
        # Refresh state on the returned rows.
        return [
            RetryRow(
                **{
                    **_row_from_db(r).__dict__,
                    "state": STATE_RUNNING,
                    "updated_at": ts,
                }
            )
            for r in candidates
        ]

    def mark_succeeded(
        self,
        row_id: int,
        *,
        last_status: str,
        last_response_code: int | None = None,
        last_response_message: str | None = None,
        now: datetime | None = None,
    ) -> None:
        ts = now or _utcnow()
        self._conn.execute(
            """
            UPDATE smtp_retry_queue
            SET state = ?,
                last_status = ?,
                last_response_code = ?,
                last_response_message = ?,
                attempt = attempt + 1,
                updated_at = ?
            WHERE id = ?
            """,
            (
                STATE_SUCCEEDED,
                last_status,
                last_response_code,
                last_response_message,
                _format_dt(ts),
                int(row_id),
            ),
        )
        self._conn.commit()

    def reschedule(
        self,
        row_id: int,
        *,
        schedule_minutes: tuple[int, ...],
        last_status: str,
        last_response_code: int | None = None,
        last_response_message: str | None = None,
        now: datetime | None = None,
    ) -> str:
        """Bump ``attempt`` and either reschedule or mark exhausted.

        Returns the new state (``pending`` or ``exhausted``).
        """
        ts = now or _utcnow()
        # Read current attempt to decide the next slot.
        cursor = self._conn.execute(
            "SELECT attempt FROM smtp_retry_queue WHERE id = ?",
            (int(row_id),),
        )
        record = cursor.fetchone()
        if record is None:
            return STATE_EXHAUSTED
        next_attempt = int(record[0]) + 1
        max_retries = len(schedule_minutes)

        if next_attempt >= max_retries:
            self._conn.execute(
                """
                UPDATE smtp_retry_queue
                SET state = ?,
                    attempt = ?,
                    last_status = ?,
                    last_response_code = ?,
                    last_response_message = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    STATE_EXHAUSTED,
                    next_attempt,
                    last_status,
                    last_response_code,
                    last_response_message,
                    _format_dt(ts),
                    int(row_id),
                ),
            )
            self._conn.commit()
            return STATE_EXHAUSTED

        # Schedule the next attempt at ts + the *next* slot's wait.
        wait_minutes = int(schedule_minutes[next_attempt])
        next_retry = ts + timedelta(minutes=wait_minutes)
        self._conn.execute(
            """
            UPDATE smtp_retry_queue
            SET state = ?,
                attempt = ?,
                next_retry_at = ?,
                last_status = ?,
                last_response_code = ?,
                last_response_message = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                STATE_PENDING,
                next_attempt,
                _format_dt(next_retry),
                last_status,
                last_response_code,
                last_response_message,
                _format_dt(ts),
                int(row_id),
            ),
        )
        self._conn.commit()
        return STATE_PENDING

    def expire_old(
        self,
        *,
        ttl: timedelta,
        now: datetime | None = None,
    ) -> int:
        """Mark every pending row older than ``ttl`` as expired.

        Returns the number of rows transitioned. The worker calls
        this at startup so a queue file abandoned mid-run doesn't
        keep emails alive forever.
        """
        ts = now or _utcnow()
        cutoff = ts - ttl
        cursor = self._conn.execute(
            """
            UPDATE smtp_retry_queue
            SET state = ?, updated_at = ?
            WHERE state IN (?, ?) AND created_at <= ?
            """,
            (
                STATE_EXPIRED,
                _format_dt(ts),
                STATE_PENDING,
                STATE_RUNNING,
                _format_dt(cutoff),
            ),
        )
        self._conn.commit()
        return int(cursor.rowcount or 0)

    # --- Inspection ----------------------------------------------------- #

    def counts(self) -> RetryQueueCounts:
        cursor = self._conn.execute(
            "SELECT state, COUNT(*) FROM smtp_retry_queue GROUP BY state"
        )
        result: dict[str, int] = {}
        for state, count in cursor.fetchall():
            result[str(state)] = int(count)
        return RetryQueueCounts(
            pending=result.get(STATE_PENDING, 0),
            running=result.get(STATE_RUNNING, 0),
            succeeded=result.get(STATE_SUCCEEDED, 0),
            exhausted=result.get(STATE_EXHAUSTED, 0),
            expired=result.get(STATE_EXPIRED, 0),
        )

    def snapshot(
        self,
        *,
        states: Iterable[str] | None = None,
        limit: int | None = None,
    ) -> list[RetryRow]:
        """Return rows matching ``states`` (default: all) up to ``limit``."""
        sql = f"SELECT {_SELECT_COLUMNS} FROM smtp_retry_queue"
        params: list[Any] = []
        if states:
            states_tuple = tuple(states)
            placeholder = ",".join("?" for _ in states_tuple)
            sql += f" WHERE state IN ({placeholder})"
            params.extend(states_tuple)
        sql += " ORDER BY id"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(int(limit))
        cursor = self._conn.execute(sql, params)
        return [_row_from_db(r) for r in cursor.fetchall()]


# --------------------------------------------------------------------------- #
# Convenience constructor
# --------------------------------------------------------------------------- #


def open_for_run(run_dir: str | Path) -> SMTPRetryQueue:
    """Open the per-run retry queue at ``<run_dir>/smtp_retry_queue.sqlite``.

    The DB file is created on first call and reused thereafter. The
    caller is responsible for ``close()`` when done — the worker and
    operator endpoints both use a short-lived ``with closing(...)``
    pattern via :func:`contextlib.closing`.
    """
    return SMTPRetryQueue(Path(run_dir) / SMTP_RETRY_QUEUE_FILENAME)


__all__ = [
    "ALL_STATES",
    "DEFAULT_RETRY_SCHEDULE_MINUTES",
    "DEFAULT_TTL_HOURS",
    "RetryQueueConfig",
    "RetryQueueCounts",
    "RetryRow",
    "SMTP_RETRY_CONFIG_FILENAME",
    "SMTP_RETRY_QUEUE_FILENAME",
    "SMTPRetryQueue",
    "STATE_EXHAUSTED",
    "STATE_EXPIRED",
    "STATE_PENDING",
    "STATE_RUNNING",
    "STATE_SUCCEEDED",
    "open_for_run",
]
