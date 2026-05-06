"""V2.10.12 — Pilot send finalize: XLSX outputs + bounce_ingestion bridge.

After the IMAP poller has applied DSNs and ``mark_delivered_after_wait``
has flipped silent rows to ``delivered``, the finalize step
materializes the customer-facing XLSX deliverables:

* ``delivery_verified.xlsx``     — pilot-proven good. The headline
                                    new tier of V2.10.12.
* ``pilot_hard_bounces.xlsx``    — DSN said ``hard_bounce``. Move to
                                    do_not_send.
* ``pilot_soft_bounces.xlsx``    — transient bounces. Operator
                                    decides whether to retry later.
* ``pilot_blocked_or_deferred.xlsx`` — policy / deferred.
* ``pilot_summary_report.xlsx``  — per-batch counts + bounce-rate.
* ``pilot_send_candidates.xlsx`` — the snapshot of who was in the
                                    batch (always included).
* ``updated_do_not_send.xlsx``   — union of the existing
                                    ``do_not_send.xlsx`` + the new
                                    ``pilot_hard_bounces``,
                                    ``pilot_blocked_or_deferred``,
                                    and complaints. Customer's
                                    canonical "do not send" list
                                    after the pilot.

Like every other re-classification step, this is **operator-
triggered**, never automatic. Bundles a customer has already
downloaded never silently change content.

Bounce-ingestion bridge
-----------------------

The pilot results also feed the existing V2.7 ``bounce_ingestion``
module so per-domain risk levels in ``runtime/feedback/
bounce_outcomes.sqlite`` reflect the real outcomes. We synthesize
a CSV row per pilot result and call the existing
``ingest_bounce_feedback`` — no new ingestion path, no per-domain
aggregator duplicated.
"""

from __future__ import annotations

import csv
import logging
import tempfile
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from ..db.pilot_send_tracker import (
    DELIVERY_VERIFIED_VERDICTS,
    DO_NOT_SEND_VERDICTS,
    PILOT_TRACKER_FILENAME,
    PilotRow,
    VERDICT_BLOCKED,
    VERDICT_COMPLAINT,
    VERDICT_DEFERRED,
    VERDICT_DELIVERED,
    VERDICT_HARD_BOUNCE,
    VERDICT_SOFT_BOUNCE,
    open_for_run,
    PilotCounts,
)


_LOGGER = logging.getLogger(__name__)


PILOT_SEND_CANDIDATES_XLSX: str = "pilot_send_candidates.xlsx"
DELIVERY_VERIFIED_XLSX: str = "delivery_verified.xlsx"
PILOT_HARD_BOUNCES_XLSX: str = "pilot_hard_bounces.xlsx"
PILOT_SOFT_BOUNCES_XLSX: str = "pilot_soft_bounces.xlsx"
PILOT_BLOCKED_OR_DEFERRED_XLSX: str = "pilot_blocked_or_deferred.xlsx"
PILOT_SUMMARY_REPORT_XLSX: str = "pilot_summary_report.xlsx"
UPDATED_DO_NOT_SEND_XLSX: str = "updated_do_not_send.xlsx"


@dataclass(slots=True)
class FinalizeResult:
    run_dir: Path
    files_written: dict[str, Path]
    counts: PilotCounts
    bounce_ingestion_csv: Path | None = None
    bounce_ingestion_report_path: Path | None = None


# --------------------------------------------------------------------------- #
# XLSX helpers
# --------------------------------------------------------------------------- #


_PILOT_COLUMNS: tuple[str, ...] = (
    "email",
    "domain",
    "provider_family",
    "batch_id",
    "verp_token",
    "message_id",
    "sent_at",
    "state",
    "dsn_status",
    "dsn_smtp_code",
    "dsn_diagnostic",
    "dsn_received_at",
)


def _row_to_dict(row: PilotRow) -> dict:
    return {
        "email": row.email,
        "domain": row.domain,
        "provider_family": row.provider_family,
        "batch_id": row.batch_id,
        "verp_token": row.verp_token,
        "message_id": row.message_id or "",
        "sent_at": row.sent_at.isoformat() if row.sent_at else "",
        "state": row.state,
        "dsn_status": row.dsn_status or "",
        "dsn_smtp_code": row.dsn_smtp_code or "",
        "dsn_diagnostic": (row.dsn_diagnostic or "")[:300],
        "dsn_received_at": (
            row.dsn_received_at.isoformat() if row.dsn_received_at else ""
        ),
    }


def _write_xlsx(rows: list[PilotRow], path: Path, sheet_name: str) -> None:
    if not rows:
        # Skip writing empty files so the package isn't cluttered with
        # blank workbooks.
        return
    df = pd.DataFrame([_row_to_dict(r) for r in rows], columns=_PILOT_COLUMNS)
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)


def _write_summary(
    path: Path,
    counts: PilotCounts,
    *,
    batch_ids: list[str],
) -> None:
    rows = [
        {"metric": "total_pilot_rows", "value": counts.total},
        {"metric": "pending_send", "value": counts.pending_send},
        {"metric": "sent", "value": counts.sent},
        {"metric": "verdict_ready", "value": counts.verdict_ready},
        {"metric": "expired", "value": counts.expired},
        {"metric": "delivered", "value": counts.delivered},
        {"metric": "hard_bounce", "value": counts.hard_bounce},
        {"metric": "soft_bounce", "value": counts.soft_bounce},
        {"metric": "blocked", "value": counts.blocked},
        {"metric": "deferred", "value": counts.deferred},
        {"metric": "complaint", "value": counts.complaint},
        {"metric": "unknown", "value": counts.unknown},
        {
            "metric": "hard_bounce_rate",
            "value": round(counts.hard_bounce_rate, 4),
        },
        {"metric": "batch_ids", "value": ", ".join(batch_ids)},
    ]
    df = pd.DataFrame(rows, columns=["metric", "value"])
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="summary", index=False)


def _merge_into_do_not_send(
    *,
    run_dir: Path,
    new_rows: list[PilotRow],
) -> Path | None:
    """Emit ``updated_do_not_send.xlsx`` = existing do_not_send +
    pilot hard_bounces / blocked / complaints. Existing file may or
    may not be present — when missing we just write the pilot
    additions."""
    if not new_rows:
        return None

    additions = pd.DataFrame(
        [_row_to_dict(r) for r in new_rows], columns=_PILOT_COLUMNS,
    )
    existing_path = run_dir / "do_not_send.xlsx"
    out_path = run_dir / UPDATED_DO_NOT_SEND_XLSX

    if existing_path.is_file():
        try:
            existing = pd.read_excel(existing_path, sheet_name=0, dtype=str)
        except Exception:
            existing = pd.DataFrame()
        combined = pd.concat([existing, additions], ignore_index=True)
    else:
        combined = additions

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        combined.to_excel(writer, sheet_name="do_not_send", index=False)
    return out_path


# --------------------------------------------------------------------------- #
# Bounce-ingestion bridge
# --------------------------------------------------------------------------- #


def _verdict_to_outcome(verdict: str | None) -> str | None:
    """Map our verdict vocabulary onto bounce_ingestion's CSV
    ``outcome`` column. ``unknown`` and ``None`` are skipped."""
    if not verdict or verdict == "unknown":
        return None
    return verdict


def _write_ingestion_csv(
    rows: list[PilotRow],
    *,
    path: Path,
) -> int:
    """Write a bounce_ingestion-shaped CSV. Returns row count."""
    written = 0
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["email", "outcome", "smtp_code", "reason"])
        for row in rows:
            outcome = _verdict_to_outcome(row.dsn_status)
            if outcome is None:
                continue
            writer.writerow([
                row.email,
                outcome,
                row.dsn_smtp_code or "",
                (row.dsn_diagnostic or "")[:200],
            ])
            written += 1
    return written


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #


def finalize_pilot(
    run_dir: str | Path,
    *,
    wait_window_hours: int | None = None,
    expiry_hours: int | None = None,
    config_path: str | Path | None = None,
    feed_bounce_ingestion: bool = True,
    now: datetime | None = None,
) -> FinalizeResult:
    """Apply wait-window verdicts and emit the XLSX deliverables.

    Steps (idempotent — safe to call repeatedly):

    1. ``mark_delivered_after_wait`` — silent rows older than
       ``wait_window_hours`` flip to ``delivered``.
    2. ``mark_expired`` — rows past ``expiry_hours`` flip to
       ``expired`` so they don't count as ``sent`` forever.
    3. Snapshot every row by category and write the XLSX files.
    4. Synthesize a bounce-ingestion-shaped CSV and call
       ``ingest_bounce_feedback`` so per-domain risk levels in
       ``runtime/feedback/bounce_outcomes.sqlite`` reflect the
       pilot outcomes.
    """
    run_dir_path = Path(run_dir)
    queue_path = run_dir_path / PILOT_TRACKER_FILENAME
    files_written: dict[str, Path] = {}
    if not queue_path.is_file():
        # Nothing to finalize — tracker never built.
        return FinalizeResult(
            run_dir=run_dir_path,
            files_written=files_written,
            counts=PilotCounts(),
        )

    from .config import read_pilot_config

    config = read_pilot_config(run_dir_path)
    wait_h = wait_window_hours or config.wait_window_hours
    expiry_h = expiry_hours or config.expiry_hours

    with closing(open_for_run(run_dir_path)) as tracker:
        tracker.mark_delivered_after_wait(
            wait_window_hours=wait_h, now=now,
        )
        tracker.mark_expired(expiry_hours=expiry_h, now=now)

        all_rows = tracker.snapshot()
        counts = tracker.counts()

        delivered = [r for r in all_rows if r.dsn_status in DELIVERY_VERIFIED_VERDICTS]
        hard_bounces = [r for r in all_rows if r.dsn_status == VERDICT_HARD_BOUNCE]
        soft_bounces = [r for r in all_rows if r.dsn_status == VERDICT_SOFT_BOUNCE]
        blocked_or_deferred = [
            r for r in all_rows
            if r.dsn_status in {VERDICT_BLOCKED, VERDICT_DEFERRED}
        ]
        do_not_send_additions = [
            r for r in all_rows if r.dsn_status in DO_NOT_SEND_VERDICTS
        ]

        # Always-emit candidates snapshot.
        cand_path = run_dir_path / PILOT_SEND_CANDIDATES_XLSX
        _write_xlsx(all_rows, cand_path, "pilot_send_candidates")
        if all_rows:
            files_written["pilot_send_candidates"] = cand_path

        if delivered:
            p = run_dir_path / DELIVERY_VERIFIED_XLSX
            _write_xlsx(delivered, p, "delivery_verified")
            files_written["delivery_verified"] = p
        if hard_bounces:
            p = run_dir_path / PILOT_HARD_BOUNCES_XLSX
            _write_xlsx(hard_bounces, p, "pilot_hard_bounces")
            files_written["pilot_hard_bounces"] = p
        if soft_bounces:
            p = run_dir_path / PILOT_SOFT_BOUNCES_XLSX
            _write_xlsx(soft_bounces, p, "pilot_soft_bounces")
            files_written["pilot_soft_bounces"] = p
        if blocked_or_deferred:
            p = run_dir_path / PILOT_BLOCKED_OR_DEFERRED_XLSX
            _write_xlsx(blocked_or_deferred, p, "pilot_blocked_or_deferred")
            files_written["pilot_blocked_or_deferred"] = p

        # Always-emit summary.
        summary_path = run_dir_path / PILOT_SUMMARY_REPORT_XLSX
        batch_ids = sorted({r.batch_id for r in all_rows if r.batch_id})
        _write_summary(summary_path, counts, batch_ids=batch_ids)
        files_written["pilot_summary_report"] = summary_path

        # Update do_not_send merge.
        merged = _merge_into_do_not_send(
            run_dir=run_dir_path, new_rows=do_not_send_additions,
        )
        if merged is not None:
            files_written["updated_do_not_send"] = merged

    # Bounce-ingestion bridge — feed per-domain aggregate.
    bounce_csv: Path | None = None
    bounce_report: Path | None = None
    if feed_bounce_ingestion:
        verdict_rows = [
            r for r in all_rows if _verdict_to_outcome(r.dsn_status)
        ]
        if verdict_rows:
            tmp_dir = run_dir_path / "_pilot_send_tmp"
            tmp_dir.mkdir(parents=True, exist_ok=True)
            bounce_csv = tmp_dir / "pilot_bounce_outcomes.csv"
            written = _write_ingestion_csv(verdict_rows, path=bounce_csv)
            if written > 0:
                try:
                    from ..api_boundary import ingest_bounce_feedback

                    bounce_report = ingest_bounce_feedback(
                        feedback_csv_path=bounce_csv,
                        config_path=config_path,
                    )
                except Exception as exc:  # pragma: no cover - defensive
                    _LOGGER.warning(
                        "bounce_ingestion bridge failed: %s", exc,
                    )

    return FinalizeResult(
        run_dir=run_dir_path,
        files_written=files_written,
        counts=counts,
        bounce_ingestion_csv=bounce_csv,
        bounce_ingestion_report_path=bounce_report,
    )


__all__ = [
    "DELIVERY_VERIFIED_XLSX",
    "FinalizeResult",
    "PILOT_BLOCKED_OR_DEFERRED_XLSX",
    "PILOT_HARD_BOUNCES_XLSX",
    "PILOT_SEND_CANDIDATES_XLSX",
    "PILOT_SOFT_BOUNCES_XLSX",
    "PILOT_SUMMARY_REPORT_XLSX",
    "UPDATED_DO_NOT_SEND_XLSX",
    "finalize_pilot",
]
