"""SQLite-backed persistence for the V2 Domain Historical Memory layer.

This module intentionally keeps one responsibility: read and write
:class:`DomainHistoryRecord` instances, indexed by domain, in a local
SQLite database. Business rules (labels, adjustments, explanations)
live in sibling modules.

The schema is deliberately minimal — one table, integer counters, two
timestamps — so that future migrations are cheap.
"""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

from .history_models import DomainHistoryRecord, DomainObservation


# --------------------------------------------------------------------------- #
# Schema                                                                      #
# --------------------------------------------------------------------------- #


_SCHEMA_SQL: str = """
CREATE TABLE IF NOT EXISTS domain_history (
    domain                TEXT    PRIMARY KEY,
    first_seen_at         TEXT    NOT NULL,
    last_seen_at          TEXT    NOT NULL,
    total_seen_count      INTEGER NOT NULL DEFAULT 0,
    mx_present_count      INTEGER NOT NULL DEFAULT 0,
    a_fallback_count      INTEGER NOT NULL DEFAULT 0,
    dns_failure_count     INTEGER NOT NULL DEFAULT 0,
    timeout_count         INTEGER NOT NULL DEFAULT 0,
    typo_corrected_count  INTEGER NOT NULL DEFAULT 0,
    review_count          INTEGER NOT NULL DEFAULT 0,
    invalid_count         INTEGER NOT NULL DEFAULT 0,
    ready_count           INTEGER NOT NULL DEFAULT 0,
    hard_fail_count       INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_domain_history_last_seen
    ON domain_history (last_seen_at);
"""


_COLUMNS: tuple[str, ...] = (
    "domain",
    "first_seen_at",
    "last_seen_at",
    "total_seen_count",
    "mx_present_count",
    "a_fallback_count",
    "dns_failure_count",
    "timeout_count",
    "typo_corrected_count",
    "review_count",
    "invalid_count",
    "ready_count",
    "hard_fail_count",
)


def _row_to_record(row: sqlite3.Row) -> DomainHistoryRecord:
    return DomainHistoryRecord(
        domain=row["domain"],
        first_seen_at=datetime.fromisoformat(row["first_seen_at"]),
        last_seen_at=datetime.fromisoformat(row["last_seen_at"]),
        total_seen_count=int(row["total_seen_count"]),
        mx_present_count=int(row["mx_present_count"]),
        a_fallback_count=int(row["a_fallback_count"]),
        dns_failure_count=int(row["dns_failure_count"]),
        timeout_count=int(row["timeout_count"]),
        typo_corrected_count=int(row["typo_corrected_count"]),
        review_count=int(row["review_count"]),
        invalid_count=int(row["invalid_count"]),
        ready_count=int(row["ready_count"]),
        hard_fail_count=int(row["hard_fail_count"]),
    )


def _record_to_tuple(record: DomainHistoryRecord) -> tuple[Any, ...]:
    return (
        record.domain,
        record.first_seen_at.isoformat(),
        record.last_seen_at.isoformat(),
        record.total_seen_count,
        record.mx_present_count,
        record.a_fallback_count,
        record.dns_failure_count,
        record.timeout_count,
        record.typo_corrected_count,
        record.review_count,
        record.invalid_count,
        record.ready_count,
        record.hard_fail_count,
    )


# --------------------------------------------------------------------------- #
# Store                                                                       #
# --------------------------------------------------------------------------- #


class DomainHistoryStore:
    """Thread-safe SQLite repository for domain history records.

    The connection is lazy: the file is created (along with any missing
    parent directory) the first time the store is used. Call :meth:`close`
    when the owning process shuts down — or use it as a context manager.

    Passing ``db_path=":memory:"`` is supported and is the preferred
    option for unit tests.
    """

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = Path(db_path) if str(db_path) != ":memory:" else Path(":memory:")
        self._lock = threading.RLock()
        self._conn: sqlite3.Connection | None = None

    # ── Connection management ──────────────────────────────────────── #

    def _ensure_connection(self) -> sqlite3.Connection:
        if self._conn is not None:
            return self._conn
        if str(self._db_path) != ":memory:":
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(
            str(self._db_path),
            check_same_thread=False,
            isolation_level=None,  # autocommit; we manage transactions ourselves
        )
        conn.row_factory = sqlite3.Row
        conn.executescript(_SCHEMA_SQL)
        self._conn = conn
        return conn

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            conn = self._ensure_connection()
            conn.execute("BEGIN")
            try:
                yield conn
            except Exception:
                conn.execute("ROLLBACK")
                raise
            else:
                conn.execute("COMMIT")

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def __enter__(self) -> "DomainHistoryStore":
        self._ensure_connection()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[no-untyped-def]
        self.close()

    # ── Read API ───────────────────────────────────────────────────── #

    def exists(self, domain: str) -> bool:
        key = (domain or "").strip().lower()
        if not key:
            return False
        with self._lock:
            conn = self._ensure_connection()
            cur = conn.execute(
                "SELECT 1 FROM domain_history WHERE domain = ? LIMIT 1",
                (key,),
            )
            return cur.fetchone() is not None

    def get(self, domain: str) -> DomainHistoryRecord | None:
        key = (domain or "").strip().lower()
        if not key:
            return None
        with self._lock:
            conn = self._ensure_connection()
            cur = conn.execute(
                f"SELECT {', '.join(_COLUMNS)} FROM domain_history WHERE domain = ?",
                (key,),
            )
            row = cur.fetchone()
            return _row_to_record(row) if row is not None else None

    def iter_all(self) -> Iterator[DomainHistoryRecord]:
        """Yield every record in the store, ordered by last_seen_at desc.

        Used by the report writer; for very large stores, prefer
        :meth:`iter_domains` (see below) for a targeted query.
        """
        with self._lock:
            conn = self._ensure_connection()
            cur = conn.execute(
                f"SELECT {', '.join(_COLUMNS)} FROM domain_history "
                "ORDER BY last_seen_at DESC"
            )
            for row in cur:
                yield _row_to_record(row)

    def get_many(self, domains: Iterable[str]) -> dict[str, DomainHistoryRecord]:
        keys = sorted({(d or "").strip().lower() for d in domains if d})
        if not keys:
            return {}
        placeholders = ",".join("?" * len(keys))
        with self._lock:
            conn = self._ensure_connection()
            cur = conn.execute(
                f"SELECT {', '.join(_COLUMNS)} FROM domain_history "
                f"WHERE domain IN ({placeholders})",
                keys,
            )
            return {row["domain"]: _row_to_record(row) for row in cur.fetchall()}

    # ── Write API ──────────────────────────────────────────────────── #

    def upsert(self, record: DomainHistoryRecord) -> None:
        """Insert-or-replace the given record verbatim.

        Used primarily by tests; normal callers should prefer
        :meth:`update_from_observation` / :meth:`bulk_update` which
        preserve monotonic counts.
        """
        with self._transaction() as conn:
            conn.execute(
                f"""
                INSERT INTO domain_history ({', '.join(_COLUMNS)})
                VALUES ({', '.join('?' * len(_COLUMNS))})
                ON CONFLICT(domain) DO UPDATE SET
                    first_seen_at        = excluded.first_seen_at,
                    last_seen_at         = excluded.last_seen_at,
                    total_seen_count     = excluded.total_seen_count,
                    mx_present_count     = excluded.mx_present_count,
                    a_fallback_count     = excluded.a_fallback_count,
                    dns_failure_count    = excluded.dns_failure_count,
                    timeout_count        = excluded.timeout_count,
                    typo_corrected_count = excluded.typo_corrected_count,
                    review_count         = excluded.review_count,
                    invalid_count        = excluded.invalid_count,
                    ready_count          = excluded.ready_count,
                    hard_fail_count      = excluded.hard_fail_count
                """,
                _record_to_tuple(record),
            )

    def update_from_observation(
        self,
        observation: DomainObservation,
        now: datetime | None = None,
    ) -> DomainHistoryRecord:
        """Apply a single observation and return the post-update record."""
        return self.bulk_update([observation], now=now)[observation.domain]

    def bulk_update(
        self,
        observations: Iterable[DomainObservation],
        now: datetime | None = None,
    ) -> dict[str, DomainHistoryRecord]:
        """Apply many observations transactionally.

        Observations are aggregated by domain in-memory first so we only
        issue one UPDATE per unique domain regardless of input size.
        """
        when = now or datetime.now()
        grouped: dict[str, list[DomainObservation]] = {}
        for obs in observations:
            if not obs.domain:
                continue
            grouped.setdefault(obs.domain, []).append(obs)

        if not grouped:
            return {}

        updated: dict[str, DomainHistoryRecord] = {}
        with self._transaction() as conn:
            for domain, obs_list in grouped.items():
                cur = conn.execute(
                    f"SELECT {', '.join(_COLUMNS)} FROM domain_history WHERE domain = ?",
                    (domain,),
                )
                row = cur.fetchone()
                if row is None:
                    record = DomainHistoryRecord(
                        domain=domain,
                        first_seen_at=when,
                        last_seen_at=when,
                    )
                else:
                    record = _row_to_record(row)

                for obs in obs_list:
                    record.apply_observation(obs, now=when)

                conn.execute(
                    f"""
                    INSERT INTO domain_history ({', '.join(_COLUMNS)})
                    VALUES ({', '.join('?' * len(_COLUMNS))})
                    ON CONFLICT(domain) DO UPDATE SET
                        last_seen_at         = excluded.last_seen_at,
                        total_seen_count     = excluded.total_seen_count,
                        mx_present_count     = excluded.mx_present_count,
                        a_fallback_count     = excluded.a_fallback_count,
                        dns_failure_count    = excluded.dns_failure_count,
                        timeout_count        = excluded.timeout_count,
                        typo_corrected_count = excluded.typo_corrected_count,
                        review_count         = excluded.review_count,
                        invalid_count        = excluded.invalid_count,
                        ready_count          = excluded.ready_count,
                        hard_fail_count      = excluded.hard_fail_count
                    """,
                    _record_to_tuple(record),
                )
                updated[domain] = record
        return updated

    # ── Housekeeping ───────────────────────────────────────────────── #

    def count(self) -> int:
        with self._lock:
            conn = self._ensure_connection()
            cur = conn.execute("SELECT COUNT(*) FROM domain_history")
            row = cur.fetchone()
            return int(row[0]) if row else 0

    def delete(self, domain: str) -> bool:
        key = (domain or "").strip().lower()
        if not key:
            return False
        with self._transaction() as conn:
            cur = conn.execute("DELETE FROM domain_history WHERE domain = ?", (key,))
            return cur.rowcount > 0


__all__ = ["DomainHistoryStore"]
