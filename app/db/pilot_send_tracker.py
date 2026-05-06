"""V2.10.12 — Pilot send tracker.

Per-run SQLite store under ``<run_dir>/pilot_send_tracker.sqlite``
that records which rows TrashPanda actually sent a real message to,
and what bounce verdict came back. The tracker is the source of
truth for the V2.10.12 ``delivery_verified`` and ``do_not_send``
post-pilot updates.

State machine
-------------

    pending_send → sent → verdict_ready
                       ↘ expired (waited too long for a DSN)

* ``pending_send`` — selected for the batch but not yet sent.
* ``sent``         — handed to the SMTP transport. Awaiting bounce
                     polling.
* ``verdict_ready``— DSN parsed (or wait window elapsed without one)
                     and ``dsn_status`` is one of the canonical
                     verdicts.
* ``expired``      — wait window elapsed past the long retention
                     and we no longer trust silence as a positive
                     signal.

Verdicts
--------

* ``delivered``    — wait window elapsed without a bounce. The
                     conservative-but-actionable signal we use for
                     the ``delivery_verified`` cohort.
* ``hard_bounce``  — DSN with ``Action: failed`` and a 5xx
                     ``Status:`` code (5.x.x SMTP enhanced status).
* ``soft_bounce``  — DSN with ``Action: failed`` and a 4xx code.
* ``blocked``      — DSN diagnostic mentions blocked/policy/spam.
* ``deferred``     — DSN with ``Action: delayed``.
* ``complaint``    — ARF-style abuse report.
* ``unknown``      — DSN couldn't be parsed.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence


PILOT_TRACKER_FILENAME: str = "pilot_send_tracker.sqlite"
PILOT_CONFIG_FILENAME: str = "pilot_send_config.json"


# State constants.
STATE_PENDING_SEND: str = "pending_send"
STATE_SENT: str = "sent"
STATE_VERDICT_READY: str = "verdict_ready"
STATE_EXPIRED: str = "expired"

ALL_STATES: tuple[str, ...] = (
    STATE_PENDING_SEND,
    STATE_SENT,
    STATE_VERDICT_READY,
    STATE_EXPIRED,
)


# Verdict constants. Mirrors the bounce_ingestion vocabulary so the
# finalize step can hand the same labels to ``ingest_bounce_feedback``
# without a translation layer.
VERDICT_DELIVERED: str = "delivered"
VERDICT_HARD_BOUNCE: str = "hard_bounce"
VERDICT_SOFT_BOUNCE: str = "soft_bounce"
VERDICT_BLOCKED: str = "blocked"
VERDICT_DEFERRED: str = "deferred"
VERDICT_COMPLAINT: str = "complaint"
VERDICT_UNKNOWN: str = "unknown"

ALL_VERDICTS: tuple[str, ...] = (
    VERDICT_DELIVERED,
    VERDICT_HARD_BOUNCE,
    VERDICT_SOFT_BOUNCE,
    VERDICT_BLOCKED,
    VERDICT_DEFERRED,
    VERDICT_COMPLAINT,
    VERDICT_UNKNOWN,
)


# Verdicts that route the row to the customer-facing ``do_not_send``
# bucket post-finalize. ``delivered`` becomes ``delivery_verified``.
# ``soft_bounce`` and ``deferred`` stay neutral — they're transient
# and surfaced separately in ``pilot_soft_bounces.xlsx`` /
# ``pilot_blocked_or_deferred.xlsx`` for operator review.
DO_NOT_SEND_VERDICTS: frozenset[str] = frozenset({
    VERDICT_HARD_BOUNCE,
    VERDICT_BLOCKED,
    VERDICT_COMPLAINT,
})

DELIVERY_VERIFIED_VERDICTS: frozenset[str] = frozenset({
    VERDICT_DELIVERED,
})


# Default conservative wait before we declare an unbounced row
# delivery_verified. 48h covers Yahoo/Gmail's typical bounce latency
# without making the operator wait days.
DEFAULT_WAIT_WINDOW_HOURS: int = 48
DEFAULT_EXPIRY_HOURS: int = 168  # 7 days — DSNs rarely arrive past this.


_CREATE_TABLE_SQL = """\
CREATE TABLE IF NOT EXISTS pilot_send_tracker (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id          TEXT NOT NULL,
    batch_id        TEXT NOT NULL,
    source_row      INTEGER NOT NULL,
    email           TEXT NOT NULL,
    domain          TEXT NOT NULL,
    provider_family TEXT NOT NULL DEFAULT 'corporate_unknown',
    verp_token      TEXT NOT NULL UNIQUE,
    message_id      TEXT,
    sent_at         TEXT,
    state           TEXT NOT NULL DEFAULT 'pending_send',
    dsn_status      TEXT,
    dsn_received_at TEXT,
    dsn_diagnostic  TEXT,
    dsn_smtp_code   TEXT,
    last_polled_at  TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    UNIQUE (job_id, batch_id, email)
)
"""

_INDEX_SQL = (
    "CREATE INDEX IF NOT EXISTS idx_pilot_state ON pilot_send_tracker (state)",
    "CREATE INDEX IF NOT EXISTS idx_pilot_token ON pilot_send_tracker (verp_token)",
)


@dataclass(frozen=True, slots=True)
class PilotRow:
    id: int
    job_id: str
    batch_id: str
    source_row: int
    email: str
    domain: str
    provider_family: str
    verp_token: str
    message_id: str | None
    sent_at: datetime | None
    state: str
    dsn_status: str | None
    dsn_received_at: datetime | None
    dsn_diagnostic: str | None
    dsn_smtp_code: str | None
    last_polled_at: datetime | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class PilotCounts:
    pending_send: int = 0
    sent: int = 0
    verdict_ready: int = 0
    expired: int = 0
    # Per-verdict counts for the verdict_ready cohort. Always sum to
    # ``verdict_ready``; surfaced separately so the UI can show the
    # bounce-rate breakdown without re-querying.
    delivered: int = 0
    hard_bounce: int = 0
    soft_bounce: int = 0
    blocked: int = 0
    deferred: int = 0
    complaint: int = 0
    unknown: int = 0

    @property
    def total(self) -> int:
        return self.pending_send + self.sent + self.verdict_ready + self.expired

    @property
    def hard_bounce_rate(self) -> float:
        denom = self.delivered + self.hard_bounce
        return (self.hard_bounce / denom) if denom > 0 else 0.0

    def to_dict(self) -> dict[str, float | int]:
        return {
            "pending_send": self.pending_send,
            "sent": self.sent,
            "verdict_ready": self.verdict_ready,
            "expired": self.expired,
            "delivered": self.delivered,
            "hard_bounce": self.hard_bounce,
            "soft_bounce": self.soft_bounce,
            "blocked": self.blocked,
            "deferred": self.deferred,
            "complaint": self.complaint,
            "unknown": self.unknown,
            "total": self.total,
            "hard_bounce_rate": round(self.hard_bounce_rate, 4),
        }


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def _format_dt(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


_SELECT_COLUMNS = (
    "id, job_id, batch_id, source_row, email, domain, provider_family, "
    "verp_token, message_id, sent_at, state, dsn_status, "
    "dsn_received_at, dsn_diagnostic, dsn_smtp_code, last_polled_at, "
    "created_at, updated_at"
)


def _row_from_db(db_row: Sequence) -> PilotRow:
    return PilotRow(
        id=int(db_row[0]),
        job_id=str(db_row[1]),
        batch_id=str(db_row[2]),
        source_row=int(db_row[3]),
        email=str(db_row[4]),
        domain=str(db_row[5]),
        provider_family=str(db_row[6] or "corporate_unknown"),
        verp_token=str(db_row[7]),
        message_id=db_row[8],
        sent_at=_parse_dt(db_row[9]),
        state=str(db_row[10]),
        dsn_status=db_row[11],
        dsn_received_at=_parse_dt(db_row[12]),
        dsn_diagnostic=db_row[13],
        dsn_smtp_code=db_row[14],
        last_polled_at=_parse_dt(db_row[15]),
        created_at=_parse_dt(db_row[16]) or _utcnow(),
        updated_at=_parse_dt(db_row[17]) or _utcnow(),
    )


# --------------------------------------------------------------------------- #
# Tracker
# --------------------------------------------------------------------------- #


class PilotSendTracker:
    """Per-run pilot send tracker. Open via :func:`open_for_run`."""

    def __init__(self, db_path: Path) -> None:
        self._path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(db_path))
        self._conn.execute("PRAGMA journal_mode=WAL")
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

    def __enter__(self) -> "PilotSendTracker":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    # --- Insert ------------------------------------------------------- #

    def add_candidate(
        self,
        *,
        job_id: str,
        batch_id: str,
        source_row: int,
        email: str,
        domain: str,
        provider_family: str,
        verp_token: str,
        now: datetime | None = None,
    ) -> bool:
        """Add a row to the batch in ``pending_send``. False on duplicate."""
        if not email or not verp_token:
            return False
        ts = now or _utcnow()
        try:
            self._conn.execute(
                """
                INSERT INTO pilot_send_tracker (
                    job_id, batch_id, source_row, email, domain,
                    provider_family, verp_token, state,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    batch_id,
                    int(source_row),
                    email,
                    domain,
                    provider_family or "corporate_unknown",
                    verp_token,
                    STATE_PENDING_SEND,
                    _format_dt(ts),
                    _format_dt(ts),
                ),
            )
            self._conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    # --- Transitions -------------------------------------------------- #

    def mark_sent(
        self,
        row_id: int,
        *,
        message_id: str | None,
        now: datetime | None = None,
    ) -> None:
        ts = now or _utcnow()
        self._conn.execute(
            """
            UPDATE pilot_send_tracker
            SET state = ?, message_id = ?, sent_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                STATE_SENT,
                message_id,
                _format_dt(ts),
                _format_dt(ts),
                int(row_id),
            ),
        )
        self._conn.commit()

    def record_dsn(
        self,
        verp_token: str,
        *,
        dsn_status: str,
        dsn_diagnostic: str | None = None,
        dsn_smtp_code: str | None = None,
        now: datetime | None = None,
    ) -> bool:
        """Apply a parsed DSN by VERP token. Returns True if a row matched.

        Idempotent: re-applying the same DSN is a no-op (the row is
        already in ``verdict_ready`` so the WHERE filter excludes
        it). Late-arriving DSNs after ``mark_expired`` still update
        ``dsn_status`` so the audit trail reflects reality even
        though the customer-facing verdict was already made.
        """
        ts = now or _utcnow()
        cursor = self._conn.execute(
            """
            UPDATE pilot_send_tracker
            SET state = ?,
                dsn_status = ?,
                dsn_diagnostic = ?,
                dsn_smtp_code = ?,
                dsn_received_at = ?,
                updated_at = ?
            WHERE verp_token = ? AND state = ?
            """,
            (
                STATE_VERDICT_READY,
                dsn_status,
                dsn_diagnostic,
                dsn_smtp_code,
                _format_dt(ts),
                _format_dt(ts),
                verp_token,
                STATE_SENT,
            ),
        )
        self._conn.commit()
        return cursor.rowcount > 0

    def mark_delivered_after_wait(
        self,
        *,
        wait_window_hours: int = DEFAULT_WAIT_WINDOW_HOURS,
        now: datetime | None = None,
    ) -> int:
        """Mark sent rows older than ``wait_window`` as delivered.

        The conservative "no bounce after N hours = it must have
        landed" assumption. Returns the number of rows transitioned.
        """
        ts = now or _utcnow()
        cutoff = ts - timedelta(hours=int(wait_window_hours))
        cursor = self._conn.execute(
            """
            UPDATE pilot_send_tracker
            SET state = ?,
                dsn_status = ?,
                updated_at = ?
            WHERE state = ? AND sent_at IS NOT NULL AND sent_at <= ?
            """,
            (
                STATE_VERDICT_READY,
                VERDICT_DELIVERED,
                _format_dt(ts),
                STATE_SENT,
                _format_dt(cutoff),
            ),
        )
        self._conn.commit()
        return int(cursor.rowcount or 0)

    def mark_expired(
        self,
        *,
        expiry_hours: int = DEFAULT_EXPIRY_HOURS,
        now: datetime | None = None,
    ) -> int:
        """Move rows still in ``sent`` past the long retention into
        ``expired``. ``dsn_status`` becomes ``unknown`` so downstream
        consumers don't read silence as a positive signal."""
        ts = now or _utcnow()
        cutoff = ts - timedelta(hours=int(expiry_hours))
        cursor = self._conn.execute(
            """
            UPDATE pilot_send_tracker
            SET state = ?,
                dsn_status = COALESCE(dsn_status, ?),
                updated_at = ?
            WHERE state = ? AND sent_at IS NOT NULL AND sent_at <= ?
            """,
            (
                STATE_EXPIRED,
                VERDICT_UNKNOWN,
                _format_dt(ts),
                STATE_SENT,
                _format_dt(cutoff),
            ),
        )
        self._conn.commit()
        return int(cursor.rowcount or 0)

    def update_last_polled(
        self,
        verp_tokens: Iterable[str],
        *,
        now: datetime | None = None,
    ) -> None:
        ts = now or _utcnow()
        for token in verp_tokens:
            self._conn.execute(
                """
                UPDATE pilot_send_tracker
                SET last_polled_at = ?
                WHERE verp_token = ?
                """,
                (_format_dt(ts), token),
            )
        self._conn.commit()

    # --- Inspection --------------------------------------------------- #

    def by_token(self, verp_token: str) -> PilotRow | None:
        cursor = self._conn.execute(
            f"SELECT {_SELECT_COLUMNS} FROM pilot_send_tracker "
            "WHERE verp_token = ? LIMIT 1",
            (verp_token,),
        )
        record = cursor.fetchone()
        return _row_from_db(record) if record else None

    def snapshot(
        self,
        *,
        states: Iterable[str] | None = None,
        verdicts: Iterable[str] | None = None,
        batch_id: str | None = None,
        limit: int | None = None,
    ) -> list[PilotRow]:
        sql = f"SELECT {_SELECT_COLUMNS} FROM pilot_send_tracker"
        clauses: list[str] = []
        params: list = []
        if states:
            states_t = tuple(states)
            clauses.append(
                f"state IN ({','.join('?' for _ in states_t)})"
            )
            params.extend(states_t)
        if verdicts:
            verdicts_t = tuple(verdicts)
            clauses.append(
                f"dsn_status IN ({','.join('?' for _ in verdicts_t)})"
            )
            params.extend(verdicts_t)
        if batch_id:
            clauses.append("batch_id = ?")
            params.append(batch_id)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY id"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(int(limit))
        cursor = self._conn.execute(sql, params)
        return [_row_from_db(r) for r in cursor.fetchall()]

    def counts(self, *, batch_id: str | None = None) -> PilotCounts:
        sql = (
            "SELECT state, dsn_status, COUNT(*) FROM pilot_send_tracker"
        )
        params: list = []
        if batch_id:
            sql += " WHERE batch_id = ?"
            params.append(batch_id)
        sql += " GROUP BY state, dsn_status"
        cursor = self._conn.execute(sql, params)
        bucket = {
            STATE_PENDING_SEND: 0,
            STATE_SENT: 0,
            STATE_VERDICT_READY: 0,
            STATE_EXPIRED: 0,
        }
        verdict_bucket: dict[str, int] = {v: 0 for v in ALL_VERDICTS}
        for state, dsn_status, count in cursor.fetchall():
            n = int(count)
            bucket[state] = bucket.get(state, 0) + n
            if state == STATE_VERDICT_READY and dsn_status:
                verdict_bucket[dsn_status] = (
                    verdict_bucket.get(dsn_status, 0) + n
                )
        return PilotCounts(
            pending_send=bucket[STATE_PENDING_SEND],
            sent=bucket[STATE_SENT],
            verdict_ready=bucket[STATE_VERDICT_READY],
            expired=bucket[STATE_EXPIRED],
            delivered=verdict_bucket[VERDICT_DELIVERED],
            hard_bounce=verdict_bucket[VERDICT_HARD_BOUNCE],
            soft_bounce=verdict_bucket[VERDICT_SOFT_BOUNCE],
            blocked=verdict_bucket[VERDICT_BLOCKED],
            deferred=verdict_bucket[VERDICT_DEFERRED],
            complaint=verdict_bucket[VERDICT_COMPLAINT],
            unknown=verdict_bucket[VERDICT_UNKNOWN],
        )


def open_for_run(run_dir: str | Path) -> PilotSendTracker:
    return PilotSendTracker(Path(run_dir) / PILOT_TRACKER_FILENAME)


__all__ = [
    "ALL_STATES",
    "ALL_VERDICTS",
    "DEFAULT_EXPIRY_HOURS",
    "DEFAULT_WAIT_WINDOW_HOURS",
    "DELIVERY_VERIFIED_VERDICTS",
    "DO_NOT_SEND_VERDICTS",
    "PILOT_CONFIG_FILENAME",
    "PILOT_TRACKER_FILENAME",
    "PilotCounts",
    "PilotRow",
    "PilotSendTracker",
    "STATE_EXPIRED",
    "STATE_PENDING_SEND",
    "STATE_SENT",
    "STATE_VERDICT_READY",
    "VERDICT_BLOCKED",
    "VERDICT_COMPLAINT",
    "VERDICT_DEFERRED",
    "VERDICT_DELIVERED",
    "VERDICT_HARD_BOUNCE",
    "VERDICT_SOFT_BOUNCE",
    "VERDICT_UNKNOWN",
    "open_for_run",
]
