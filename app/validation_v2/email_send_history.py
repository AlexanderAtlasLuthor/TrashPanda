"""Persistent per-email send history for SMTP probe deduplication.

Why this exists
---------------
Operators typically iterate over a single dataset many times — running the
same CSV (or a copy / sub-sample of it) through the pipeline to compare
results, tune thresholds or re-export. The in-process :class:`SMTPCache`
already prevents duplicate probes *within* a run, but it is wiped between
processes.

Without a cross-run memory of "we already touched this address", a second
run of the same data re-opens the SMTP handshake to every candidate
mailbox, which:

  * burns sender reputation on the egress IP;
  * spends SMTP quota that real campaigns need;
  * keeps re-deriving a result we already know.

This module adds a small SQLite table — one row per
``email_normalized`` — that records the canonical V2.2 probe outcome
plus the timestamp and a monotonic ``send_count``. The
:class:`SMTPVerificationStage` consults the store *before* each live
probe; on a fresh hit (within ``ttl_days``) the persisted result is
replayed into the per-run cache and emitted as if it had just been
probed, so no SMTP traffic is generated.

Design constraints
------------------
  * Mirrors the :class:`app.validation_v2.history_store.DomainHistoryStore`
    pattern: lazy connection, threading.RLock, ``isolation_level=None``,
    transactional writes.
  * Identifier is ``email_normalized`` (lowercased, trimmed) — same key
    the per-run cache uses, so the two layers agree on identity.
  * The store is opt-in (``email_send_history.enabled`` config) and
    only writes for **live** probes — dry-run results are never
    persisted because no email actually went on the wire.
  * Records expose a small monotonic surface (``send_count``,
    ``first_sent_at``, ``last_sent_at``) so audits can see how many
    times a row would have been re-probed without dedup.
"""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path


# --------------------------------------------------------------------------- #
# Schema                                                                      #
# --------------------------------------------------------------------------- #


_SCHEMA_SQL: str = """
CREATE TABLE IF NOT EXISTS email_send_history (
    email_normalized       TEXT    PRIMARY KEY,
    domain                 TEXT    NOT NULL,
    first_sent_at          TEXT    NOT NULL,
    last_sent_at           TEXT    NOT NULL,
    send_count             INTEGER NOT NULL DEFAULT 1,
    last_status            TEXT    NOT NULL,
    last_smtp_result       TEXT    NOT NULL,
    last_response_code     INTEGER,
    last_response_message  TEXT    NOT NULL DEFAULT '',
    last_was_success       INTEGER NOT NULL DEFAULT 0,
    last_is_catch_all      INTEGER NOT NULL DEFAULT 0,
    last_inconclusive      INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_email_send_history_last_sent
    ON email_send_history (last_sent_at);
CREATE INDEX IF NOT EXISTS idx_email_send_history_domain
    ON email_send_history (domain);
"""


_COLUMNS: tuple[str, ...] = (
    "email_normalized",
    "domain",
    "first_sent_at",
    "last_sent_at",
    "send_count",
    "last_status",
    "last_smtp_result",
    "last_response_code",
    "last_response_message",
    "last_was_success",
    "last_is_catch_all",
    "last_inconclusive",
)


# --------------------------------------------------------------------------- #
# Record dataclass                                                            #
# --------------------------------------------------------------------------- #


@dataclass(slots=True)
class EmailSendRecord:
    """One row in :class:`EmailSendHistoryStore`.

    Mirrors the canonical V2.2 SMTP outcome plus enough metadata to
    decide whether the persisted result is fresh enough to reuse.
    """

    email_normalized: str
    domain: str
    first_sent_at: datetime
    last_sent_at: datetime
    send_count: int
    last_status: str
    last_smtp_result: str
    last_response_code: int | None
    last_response_message: str
    last_was_success: bool
    last_is_catch_all: bool
    last_inconclusive: bool

    def is_fresh(self, ttl_days: int | None, now: datetime | None = None) -> bool:
        """Return True iff this record was written within the TTL window.

        ``ttl_days`` of None / 0 means "never expire": all records are
        considered fresh.
        """
        if ttl_days is None or ttl_days <= 0:
            return True
        when = now or datetime.now()
        return when - self.last_sent_at <= timedelta(days=ttl_days)


def _row_to_record(row: sqlite3.Row) -> EmailSendRecord:
    return EmailSendRecord(
        email_normalized=row["email_normalized"],
        domain=row["domain"],
        first_sent_at=datetime.fromisoformat(row["first_sent_at"]),
        last_sent_at=datetime.fromisoformat(row["last_sent_at"]),
        send_count=int(row["send_count"]),
        last_status=row["last_status"],
        last_smtp_result=row["last_smtp_result"],
        last_response_code=(
            int(row["last_response_code"])
            if row["last_response_code"] is not None
            else None
        ),
        last_response_message=row["last_response_message"] or "",
        last_was_success=bool(row["last_was_success"]),
        last_is_catch_all=bool(row["last_is_catch_all"]),
        last_inconclusive=bool(row["last_inconclusive"]),
    )


# --------------------------------------------------------------------------- #
# Store                                                                       #
# --------------------------------------------------------------------------- #


def _normalize_key(email: str) -> str:
    return (email or "").strip().lower()


class EmailSendHistoryStore:
    """Thread-safe SQLite repository for per-email send history.

    Use as a context manager in tests::

        with EmailSendHistoryStore(":memory:") as store:
            ...

    In production the store is opened once per pipeline run from
    ``configs/default.yaml`` and stashed in
    ``context.extras["email_send_history_store"]`` so every chunk shares
    one connection.
    """

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = (
            Path(db_path) if str(db_path) != ":memory:" else Path(":memory:")
        )
        self._lock = threading.RLock()
        self._conn: sqlite3.Connection | None = None
        # Process-local counters; not persisted. ``SMTPVerificationStage``
        # increments these so the runtime summary can report dedup
        # savings without round-tripping the DB.
        self.history_hits: int = 0
        self.records_written: int = 0

    # ── Connection management ──────────────────────────────────────── #

    def _ensure_connection(self) -> sqlite3.Connection:
        if self._conn is not None:
            return self._conn
        if str(self._db_path) != ":memory:":
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(
            str(self._db_path),
            check_same_thread=False,
            isolation_level=None,
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

    def __enter__(self) -> "EmailSendHistoryStore":
        self._ensure_connection()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[no-untyped-def]
        self.close()

    # ── Read API ───────────────────────────────────────────────────── #

    def lookup(self, email_normalized: str) -> EmailSendRecord | None:
        key = _normalize_key(email_normalized)
        if not key:
            return None
        with self._lock:
            conn = self._ensure_connection()
            cur = conn.execute(
                f"SELECT {', '.join(_COLUMNS)} FROM email_send_history "
                "WHERE email_normalized = ?",
                (key,),
            )
            row = cur.fetchone()
            return _row_to_record(row) if row is not None else None

    def lookup_fresh(
        self,
        email_normalized: str,
        ttl_days: int | None,
        now: datetime | None = None,
    ) -> EmailSendRecord | None:
        """Return the record only if it is within ``ttl_days``; else None."""
        record = self.lookup(email_normalized)
        if record is None:
            return None
        return record if record.is_fresh(ttl_days, now=now) else None

    def count(self) -> int:
        with self._lock:
            conn = self._ensure_connection()
            cur = conn.execute("SELECT COUNT(*) FROM email_send_history")
            row = cur.fetchone()
            return int(row[0]) if row else 0

    def iter_all(self) -> Iterator[EmailSendRecord]:
        with self._lock:
            conn = self._ensure_connection()
            cur = conn.execute(
                f"SELECT {', '.join(_COLUMNS)} FROM email_send_history "
                "ORDER BY last_sent_at DESC"
            )
            for row in cur:
                yield _row_to_record(row)

    # ── Write API ──────────────────────────────────────────────────── #

    def record(
        self,
        *,
        email_normalized: str,
        domain: str,
        status: str,
        smtp_result: str,
        response_code: int | None,
        response_message: str,
        was_success: bool,
        is_catch_all: bool,
        inconclusive: bool,
        now: datetime | None = None,
    ) -> EmailSendRecord:
        """Upsert a probe outcome for ``email_normalized``.

        On insert: ``send_count=1`` and ``first_sent_at == last_sent_at``.
        On update: ``send_count`` is incremented monotonically;
        ``first_sent_at`` is preserved and ``last_*`` is overwritten.
        """
        key = _normalize_key(email_normalized)
        if not key:
            raise ValueError("email_normalized cannot be empty")
        domain_key = (domain or "").strip().lower()
        when = now or datetime.now()
        when_iso = when.isoformat()
        message = (response_message or "")[:500]

        with self._transaction() as conn:
            cur = conn.execute(
                f"SELECT {', '.join(_COLUMNS)} FROM email_send_history "
                "WHERE email_normalized = ?",
                (key,),
            )
            existing = cur.fetchone()
            if existing is None:
                conn.execute(
                    f"""
                    INSERT INTO email_send_history ({', '.join(_COLUMNS)})
                    VALUES ({', '.join('?' * len(_COLUMNS))})
                    """,
                    (
                        key,
                        domain_key,
                        when_iso,
                        when_iso,
                        1,
                        status,
                        smtp_result,
                        response_code,
                        message,
                        int(bool(was_success)),
                        int(bool(is_catch_all)),
                        int(bool(inconclusive)),
                    ),
                )
                record = EmailSendRecord(
                    email_normalized=key,
                    domain=domain_key,
                    first_sent_at=when,
                    last_sent_at=when,
                    send_count=1,
                    last_status=status,
                    last_smtp_result=smtp_result,
                    last_response_code=response_code,
                    last_response_message=message,
                    last_was_success=bool(was_success),
                    last_is_catch_all=bool(is_catch_all),
                    last_inconclusive=bool(inconclusive),
                )
            else:
                new_count = int(existing["send_count"]) + 1
                conn.execute(
                    """
                    UPDATE email_send_history SET
                        domain                = ?,
                        last_sent_at          = ?,
                        send_count            = ?,
                        last_status           = ?,
                        last_smtp_result      = ?,
                        last_response_code    = ?,
                        last_response_message = ?,
                        last_was_success      = ?,
                        last_is_catch_all     = ?,
                        last_inconclusive     = ?
                    WHERE email_normalized = ?
                    """,
                    (
                        domain_key or existing["domain"],
                        when_iso,
                        new_count,
                        status,
                        smtp_result,
                        response_code,
                        message,
                        int(bool(was_success)),
                        int(bool(is_catch_all)),
                        int(bool(inconclusive)),
                        key,
                    ),
                )
                record = EmailSendRecord(
                    email_normalized=key,
                    domain=domain_key or existing["domain"],
                    first_sent_at=datetime.fromisoformat(existing["first_sent_at"]),
                    last_sent_at=when,
                    send_count=new_count,
                    last_status=status,
                    last_smtp_result=smtp_result,
                    last_response_code=response_code,
                    last_response_message=message,
                    last_was_success=bool(was_success),
                    last_is_catch_all=bool(is_catch_all),
                    last_inconclusive=bool(inconclusive),
                )
        self.records_written += 1
        return record

    def delete(self, email_normalized: str) -> bool:
        key = _normalize_key(email_normalized)
        if not key:
            return False
        with self._transaction() as conn:
            cur = conn.execute(
                "DELETE FROM email_send_history WHERE email_normalized = ?",
                (key,),
            )
            return cur.rowcount > 0

    def purge_expired(
        self,
        ttl_days: int,
        now: datetime | None = None,
    ) -> int:
        """Delete records older than ``ttl_days``; returns rows removed.

        ``ttl_days`` <= 0 is a no-op (mirrors :meth:`lookup_fresh`'s
        "never expire" semantics).
        """
        if ttl_days is None or ttl_days <= 0:
            return 0
        cutoff = (now or datetime.now()) - timedelta(days=ttl_days)
        with self._transaction() as conn:
            cur = conn.execute(
                "DELETE FROM email_send_history WHERE last_sent_at < ?",
                (cutoff.isoformat(),),
            )
            return int(cur.rowcount)


__all__ = ["EmailSendHistoryStore", "EmailSendRecord"]
