"""V2.10.14 — Sender IP reputation tracking.

TrashPanda's pilot quality depends on the sending IP's reputation
with the major providers. The May 2026 pilot proved this the hard
way: every Microsoft and Yahoo response was about the IP, not the
recipient. Without a place to track and gate on reputation, every
new pilot risks a repeat.

This module is a small, honest piece of the puzzle:

* SQLite store of ``ReputationSnapshot`` rows (IP × source × time).
* Manual CSV import (Microsoft SNDS, Yahoo Sender Hub, etc. — none
  of these have public APIs, so the operator downloads CSVs and
  uploads them here).
* ``latest_for_ip`` / ``is_safe_to_pilot`` lookups.
* A pre-pilot gate that returns ``(safe, reasons)`` so the launch
  orchestrator can warn the operator (we do NOT auto-block — the
  operator decides).

Sources we track:

* ``snds``      Microsoft Smart Network Data Services
                (https://sendersupport.olc.protection.outlook.com/snds/)
* ``yahoo``     Yahoo Sender Hub
* ``google``    Google Postmaster Tools
* ``rbl``       Public RBL listing (Spamhaus, Barracuda, SORBS, etc.)
* ``manual``    Operator-recorded note (e.g. "we got delisted today")

Status values follow a green / yellow / red traffic light:

* ``green``     OK to pilot. No known reputation issues.
* ``yellow``    Warning. Pilot proceeds but operator should monitor.
* ``red``       Stop. Pilot would waste effort; resolve reputation first.
"""

from __future__ import annotations

import csv
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator


REPUTATION_DB_FILENAME: str = "sender_reputation.sqlite"
DEFAULT_REPUTATION_DIR: Path = Path("runtime") / "reputation"


# Sources.
SOURCE_SNDS: str = "snds"
SOURCE_YAHOO: str = "yahoo"
SOURCE_GOOGLE: str = "google"
SOURCE_RBL: str = "rbl"
SOURCE_MANUAL: str = "manual"

ALL_SOURCES: tuple[str, ...] = (
    SOURCE_SNDS, SOURCE_YAHOO, SOURCE_GOOGLE, SOURCE_RBL, SOURCE_MANUAL,
)


# Statuses.
STATUS_GREEN: str = "green"
STATUS_YELLOW: str = "yellow"
STATUS_RED: str = "red"

ALL_STATUSES: tuple[str, ...] = (STATUS_GREEN, STATUS_YELLOW, STATUS_RED)


# How old a snapshot can be before we treat it as stale (no signal).
DEFAULT_FRESHNESS_HOURS: int = 72


@dataclass(frozen=True, slots=True)
class ReputationSnapshot:
    ip: str
    source: str
    captured_at: datetime
    status: str
    score: float | None = None       # numeric reputation, source-dependent
    complaint_rate: float | None = None
    notes: str = ""

    def is_stale(
        self, *, freshness_hours: int = DEFAULT_FRESHNESS_HOURS,
        now: datetime | None = None,
    ) -> bool:
        ref = now or datetime.now(tz=timezone.utc)
        return (ref - self.captured_at) > timedelta(hours=freshness_hours)


_CREATE_TABLE_SQL = """\
CREATE TABLE IF NOT EXISTS sender_reputation (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ip              TEXT NOT NULL,
    source          TEXT NOT NULL,
    captured_at     TEXT NOT NULL,
    status          TEXT NOT NULL,
    score           REAL,
    complaint_rate  REAL,
    notes           TEXT NOT NULL DEFAULT '',
    UNIQUE (ip, source, captured_at)
)
"""

_INDEX_SQL = (
    "CREATE INDEX IF NOT EXISTS idx_rep_ip ON sender_reputation (ip)",
    "CREATE INDEX IF NOT EXISTS idx_rep_ip_source "
    "ON sender_reputation (ip, source, captured_at DESC)",
)


def _ensure_db(conn: sqlite3.Connection) -> None:
    conn.execute(_CREATE_TABLE_SQL)
    for sql in _INDEX_SQL:
        conn.execute(sql)


def _format_dt(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _parse_dt(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


@contextmanager
def open_store(
    db_path: str | Path | None = None,
) -> Iterator[sqlite3.Connection]:
    """Open the reputation SQLite store (creating it if needed).

    Default path: ``runtime/reputation/sender_reputation.sqlite``.
    """
    if db_path is None:
        db_path = DEFAULT_REPUTATION_DIR / REPUTATION_DB_FILENAME
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    try:
        conn.row_factory = sqlite3.Row
        _ensure_db(conn)
        yield conn
        conn.commit()
    finally:
        conn.close()


def record_snapshot(
    conn: sqlite3.Connection,
    snapshot: ReputationSnapshot,
) -> None:
    """Insert a snapshot. Duplicate (ip, source, captured_at) rows
    are silently ignored — useful when re-importing the same CSV."""
    conn.execute(
        "INSERT OR IGNORE INTO sender_reputation "
        "(ip, source, captured_at, status, score, complaint_rate, notes) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            snapshot.ip,
            snapshot.source,
            _format_dt(snapshot.captured_at),
            snapshot.status,
            snapshot.score,
            snapshot.complaint_rate,
            snapshot.notes,
        ),
    )


def latest_for_ip(
    conn: sqlite3.Connection,
    ip: str,
    *,
    source: str | None = None,
) -> ReputationSnapshot | None:
    """Return the most recent snapshot for ``ip`` (optionally filtered
    by source)."""
    sql = (
        "SELECT ip, source, captured_at, status, score, complaint_rate, notes "
        "FROM sender_reputation WHERE ip = ?"
    )
    params: list[object] = [ip]
    if source is not None:
        sql += " AND source = ?"
        params.append(source)
    sql += " ORDER BY captured_at DESC LIMIT 1"
    cursor = conn.execute(sql, params)
    row = cursor.fetchone()
    if row is None:
        return None
    return _row_to_snapshot(row)


def latest_per_source(
    conn: sqlite3.Connection, ip: str,
) -> dict[str, ReputationSnapshot]:
    """Return ``{source -> latest snapshot}`` for the given IP."""
    cursor = conn.execute(
        "SELECT ip, source, captured_at, status, score, complaint_rate, notes "
        "FROM sender_reputation WHERE ip = ? "
        "ORDER BY source, captured_at DESC",
        (ip,),
    )
    out: dict[str, ReputationSnapshot] = {}
    for row in cursor.fetchall():
        snap = _row_to_snapshot(row)
        out.setdefault(snap.source, snap)
    return out


def _row_to_snapshot(row: sqlite3.Row) -> ReputationSnapshot:
    return ReputationSnapshot(
        ip=row["ip"],
        source=row["source"],
        captured_at=_parse_dt(row["captured_at"]),
        status=row["status"],
        score=row["score"],
        complaint_rate=row["complaint_rate"],
        notes=row["notes"] or "",
    )


@dataclass(frozen=True, slots=True)
class GateDecision:
    """Pre-pilot gate result. ``safe`` is the operator's go/no-go
    summary; ``reasons`` is the per-source detail for the audit log."""

    ip: str
    safe: bool
    overall_status: str  # green / yellow / red / unknown
    reasons: tuple[str, ...]


def is_safe_to_pilot(
    conn: sqlite3.Connection,
    ip: str,
    *,
    freshness_hours: int = DEFAULT_FRESHNESS_HOURS,
    now: datetime | None = None,
) -> GateDecision:
    """Aggregate the latest snapshots per source into a go/no-go.

    Rules:
      * Any non-stale ``red`` → not safe, overall=red.
      * Any non-stale ``yellow`` → safe (operator warning), overall=yellow.
      * All sources stale or missing → safe (no signal blocks
        operations), overall=unknown.
      * Otherwise → safe, overall=green.
    """
    snapshots = latest_per_source(conn, ip)
    reasons: list[str] = []
    has_red = False
    has_yellow = False
    has_fresh = False

    for source, snap in sorted(snapshots.items()):
        if snap.is_stale(freshness_hours=freshness_hours, now=now):
            reasons.append(
                f"{source}: stale (captured {snap.captured_at.isoformat()})"
            )
            continue
        has_fresh = True
        reasons.append(
            f"{source}: {snap.status}"
            + (f" score={snap.score}" if snap.score is not None else "")
            + (f" complaint_rate={snap.complaint_rate}" if snap.complaint_rate is not None else "")
            + (f" — {snap.notes}" if snap.notes else "")
        )
        if snap.status == STATUS_RED:
            has_red = True
        elif snap.status == STATUS_YELLOW:
            has_yellow = True

    if not has_fresh:
        reasons.append("no fresh reputation data for this IP")
        overall = "unknown"
        safe = True
    elif has_red:
        overall = STATUS_RED
        safe = False
    elif has_yellow:
        overall = STATUS_YELLOW
        safe = True
    else:
        overall = STATUS_GREEN
        safe = True

    return GateDecision(
        ip=ip,
        safe=safe,
        overall_status=overall,
        reasons=tuple(reasons),
    )


# ---------------------------------------------------------------------------
# CSV importers
# ---------------------------------------------------------------------------


def _classify_snds_row(
    *, complaint_rate: float | None, trap_count: int | None,
) -> str:
    """Map SNDS metrics onto green/yellow/red.

    Microsoft's published guidance:
      * complaint rate < 0.3% → green
      * 0.3% – 0.5%           → yellow
      * > 0.5%                → red
    Trap hits dominate: any trap hit forces red.
    """
    if trap_count is not None and trap_count > 0:
        return STATUS_RED
    if complaint_rate is None:
        return STATUS_GREEN
    if complaint_rate > 0.005:
        return STATUS_RED
    if complaint_rate > 0.003:
        return STATUS_YELLOW
    return STATUS_GREEN


def import_snds_csv(
    conn: sqlite3.Connection,
    csv_path: str | Path,
    *,
    captured_at: datetime | None = None,
) -> int:
    """Import a Microsoft SNDS CSV.

    SNDS CSV columns (per Microsoft docs, order may vary):
      * ``IP Address``
      * ``Activity start (UTC)`` / ``Activity end (UTC)``
      * ``RCPT commands``
      * ``DATA commands``
      * ``Filter result`` (Green / Yellow / Red — Microsoft's own bucket)
      * ``Complaint rate``  (decimal, e.g. 0.0045 = 0.45%)
      * ``Trap message count``
      * ``Sample HELO``     (string — sometimes useful as note)

    Unknown columns are tolerated. Returns count of rows ingested.
    """
    captured_at = captured_at or datetime.now(tz=timezone.utc)
    path = Path(csv_path)
    if not path.is_file():
        raise FileNotFoundError(csv_path)

    written = 0
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        for raw in reader:
            ip = (
                raw.get("IP Address")
                or raw.get("IP")
                or raw.get("ip")
                or ""
            ).strip()
            if not ip:
                continue
            complaint_rate = _parse_float(
                raw.get("Complaint rate")
                or raw.get("complaint_rate")
            )
            trap_count = _parse_int(
                raw.get("Trap message count")
                or raw.get("trap_count")
            )
            filter_result = (
                raw.get("Filter result")
                or raw.get("filter_result")
                or ""
            ).strip().lower()
            # Trust Microsoft's own bucket if present, else compute.
            if filter_result in {STATUS_GREEN, STATUS_YELLOW, STATUS_RED}:
                status = filter_result
            else:
                status = _classify_snds_row(
                    complaint_rate=complaint_rate,
                    trap_count=trap_count,
                )
            notes_parts: list[str] = []
            if trap_count:
                notes_parts.append(f"trap_count={trap_count}")
            sample_helo = (raw.get("Sample HELO") or "").strip()
            if sample_helo:
                notes_parts.append(f"sample_helo={sample_helo}")
            record_snapshot(
                conn,
                ReputationSnapshot(
                    ip=ip,
                    source=SOURCE_SNDS,
                    captured_at=captured_at,
                    status=status,
                    score=None,
                    complaint_rate=complaint_rate,
                    notes="; ".join(notes_parts),
                ),
            )
            written += 1
    return written


def _parse_float(raw: object) -> float | None:
    if raw is None:
        return None
    s = str(raw).strip().rstrip("%")
    if not s:
        return None
    try:
        v = float(s)
    except ValueError:
        return None
    # Treat values >1 as percent (operator pasted "0.45%" → 0.45 → 0.0045).
    if v > 1:
        v = v / 100.0
    return v


def _parse_int(raw: object) -> int | None:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    try:
        return int(float(s))  # tolerates "0.0"
    except ValueError:
        return None


__all__ = [
    "ALL_SOURCES",
    "ALL_STATUSES",
    "DEFAULT_FRESHNESS_HOURS",
    "DEFAULT_REPUTATION_DIR",
    "GateDecision",
    "REPUTATION_DB_FILENAME",
    "ReputationSnapshot",
    "SOURCE_GOOGLE",
    "SOURCE_MANUAL",
    "SOURCE_RBL",
    "SOURCE_SNDS",
    "SOURCE_YAHOO",
    "STATUS_GREEN",
    "STATUS_RED",
    "STATUS_YELLOW",
    "import_snds_csv",
    "is_safe_to_pilot",
    "latest_for_ip",
    "latest_per_source",
    "open_store",
    "record_snapshot",
]
