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

from .evidence import (
    SMTP_EVIDENCE_REPORT_FILENAME,
    write_smtp_evidence_report,
)
from ..db.pilot_send_tracker import (
    DELIVERY_VERIFIED_VERDICTS,
    DO_NOT_SEND_VERDICTS,
    INFRA_RETEST_VERDICTS,
    PILOT_TRACKER_FILENAME,
    PilotRow,
    VERDICT_BLOCKED,
    VERDICT_COMPLAINT,
    VERDICT_DEFERRED,
    VERDICT_DELIVERED,
    VERDICT_HARD_BOUNCE,
    VERDICT_INFRA_BLOCKED,
    VERDICT_PROVIDER_DEFERRED,
    VERDICT_SOFT_BOUNCE,
    VERDICT_UNKNOWN,
    open_for_run,
    PilotCounts,
)


_LOGGER = logging.getLogger(__name__)


PILOT_SEND_CANDIDATES_XLSX: str = "pilot_send_candidates.xlsx"
DELIVERY_VERIFIED_XLSX: str = "delivery_verified.xlsx"
PILOT_HARD_BOUNCES_XLSX: str = "pilot_hard_bounces.xlsx"
PILOT_SOFT_BOUNCES_XLSX: str = "pilot_soft_bounces.xlsx"
PILOT_BLOCKED_OR_DEFERRED_XLSX: str = "pilot_blocked_or_deferred.xlsx"
PILOT_INFRA_RETEST_XLSX: str = "pilot_infrastructure_blocked.xlsx"
PILOT_SUMMARY_REPORT_XLSX: str = "pilot_summary_report.xlsx"
UPDATED_DO_NOT_SEND_XLSX: str = "updated_do_not_send.xlsx"


_TECHNICAL_BUCKET_FILES: dict[str, str] = {
    "clean_high_confidence": "clean_high_confidence.csv",
    "review_medium_confidence": "review_medium_confidence.csv",
    "removed_invalid": "removed_invalid.csv",
}

_PILOT_REVIEW_VERDICTS: frozenset[str] = frozenset({
    VERDICT_SOFT_BOUNCE,
    VERDICT_DEFERRED,
    VERDICT_UNKNOWN,
    # Sender-side rejections leave the recipient verdict undetermined,
    # so they belong in operator review rather than removed_invalid.
    VERDICT_INFRA_BLOCKED,
    VERDICT_PROVIDER_DEFERRED,
})


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
            "metric": "infrastructure_blocked",
            "value": counts.infrastructure_blocked,
        },
        {"metric": "provider_deferred", "value": counts.provider_deferred},
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
# Technical CSV re-bucketing
# --------------------------------------------------------------------------- #


def _normalize_email(value: object) -> str:
    text = str(value or "").strip().lower()
    return text if "@" in text else ""


def _read_bucket_csv(path: Path) -> pd.DataFrame:
    if not path.is_file() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(
            path,
            dtype=str,
            keep_default_na=False,
            na_filter=False,
        )
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _write_bucket_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def _source_row_for_email(
    frames: dict[str, pd.DataFrame],
    email: str,
) -> dict[str, object] | None:
    """Return the best existing technical CSV row for ``email``.

    Pilot send candidates originate from the review action XLSXs, so
    prefer the review bucket when a row still exists there. On a
    re-run, the same email may already have been moved to clean or
    removed; those are valid sources too, which keeps finalize
    idempotent.
    """
    for bucket in (
        "review_medium_confidence",
        "clean_high_confidence",
        "removed_invalid",
    ):
        df = frames.get(bucket, pd.DataFrame())
        if df.empty or "email" not in df.columns:
            continue
        mask = df["email"].map(_normalize_email) == email
        if bool(mask.any()):
            return df.loc[mask].iloc[0].to_dict()
    return None


def _fallback_row(row: PilotRow) -> dict[str, object]:
    return {
        "email": row.email,
        "source_row_number": str(row.source_row or ""),
        "domain": row.domain,
        "provider_family": row.provider_family or "corporate_unknown",
    }


def _response_message(row: PilotRow) -> str:
    diagnostic = (row.dsn_diagnostic or "").strip()
    if diagnostic:
        return diagnostic[:300]
    verdict = row.dsn_status or row.state
    return f"pilot_send:{verdict}"


def _apply_pilot_verdict_columns(
    data: dict[str, object],
    row: PilotRow,
    *,
    target_bucket: str,
) -> dict[str, object]:
    """Stamp the technical row with the post-pilot decision columns."""
    verdict = row.dsn_status or "unknown"
    code = row.dsn_smtp_code or ""
    message = _response_message(row)
    out = dict(data)
    out["email"] = row.email
    out["provider_family"] = (
        out.get("provider_family") or row.provider_family or "corporate_unknown"
    )
    out["smtp_response_code"] = code
    out["smtp_response_message"] = message
    out["pilot_send_verdict"] = verdict
    out["pilot_send_batch_id"] = row.batch_id
    out["pilot_send_finalized_at"] = datetime.now(timezone.utc).isoformat()

    if target_bucket == "clean_high_confidence":
        out.update({
            "final_action": "auto_approve",
            "decision_reason": "pilot_delivery_verified",
            "decision_confidence": "1.0",
            "deliverability_probability": "1.0",
            "smtp_status": "valid",
            "smtp_confirmed_valid": "true",
            "smtp_response_type": "pilot_delivered",
            "client_reason": "Pilot send verified delivery.",
            "final_output_reason": "pilot_delivery_verified",
        })
    elif target_bucket == "removed_invalid":
        reason = f"pilot_{verdict}"
        smtp_status = "blocked" if verdict == VERDICT_BLOCKED else "invalid"
        if verdict == VERDICT_COMPLAINT:
            smtp_status = "complaint"
        out.update({
            "final_action": "auto_reject",
            "decision_reason": reason,
            "decision_confidence": "1.0",
            "deliverability_probability": "0.0",
            "smtp_status": smtp_status,
            "smtp_confirmed_valid": "false",
            "smtp_response_type": verdict,
            "client_reason": (
                "Pilot send produced a hard bounce or block. Do not use."
            ),
            "final_output_reason": reason,
        })
    else:
        reason = f"pilot_{verdict}"
        if verdict == VERDICT_SOFT_BOUNCE:
            smtp_status = "temp_fail"
            client_reason = (
                "Pilot send returned a transient bounce. Keep in review "
                "before retrying."
            )
        elif verdict == VERDICT_UNKNOWN:
            smtp_status = "unknown"
            client_reason = (
                "Pilot send returned an inconclusive bounce. Keep in "
                "review before retrying."
            )
        elif verdict == VERDICT_INFRA_BLOCKED:
            smtp_status = "infrastructure_blocked"
            client_reason = (
                "Recipient provider rejected our sending IP/network, not "
                "the recipient. Re-test from a different sender before "
                "deciding."
            )
        elif verdict == VERDICT_PROVIDER_DEFERRED:
            smtp_status = "provider_deferred"
            client_reason = (
                "Recipient provider deferred mail due to sender volume "
                "or reputation. Re-test from a clean sender later."
            )
        else:
            smtp_status = "deferred"
            client_reason = (
                "Pilot send returned a transient or inconclusive bounce. "
                "Keep in review before retrying."
            )
        out.update({
            "final_action": "manual_review",
            "decision_reason": reason,
            "decision_confidence": "0.8",
            "smtp_status": smtp_status,
            "smtp_confirmed_valid": "false",
            "smtp_response_type": verdict,
            "client_reason": client_reason,
            "final_output_reason": reason,
        })
    return out


def _target_bucket_for_pilot_row(row: PilotRow) -> str | None:
    verdict = row.dsn_status
    if verdict in DELIVERY_VERIFIED_VERDICTS:
        return "clean_high_confidence"
    if verdict in DO_NOT_SEND_VERDICTS:
        return "removed_invalid"
    if verdict in _PILOT_REVIEW_VERDICTS or row.state == "expired":
        return "review_medium_confidence"
    return None


def _apply_pilot_results_to_technical_buckets(
    *,
    run_dir: Path,
    rows: list[PilotRow],
) -> dict[str, Path]:
    """Move pilot-result rows into the base CSV buckets.

    This is the step that makes "Re-clean with pilot results" update
    the dashboard counts and regenerated client package. Hard bounces
    and blocks become removed rows, transient bounces remain review,
    and delivery-verified rows become clean. Pending/sent rows are
    intentionally left untouched until the operator has a verdict.
    """
    targets: dict[str, str] = {}
    by_email: dict[str, PilotRow] = {}
    for row in rows:
        email = _normalize_email(row.email)
        if not email:
            continue
        bucket = _target_bucket_for_pilot_row(row)
        if bucket is None:
            continue
        targets[email] = bucket
        by_email[email] = row

    if not targets:
        return {}

    frames = {
        bucket: _read_bucket_csv(run_dir / filename)
        for bucket, filename in _TECHNICAL_BUCKET_FILES.items()
    }
    moved_rows: dict[str, list[dict[str, object]]] = {
        bucket: [] for bucket in _TECHNICAL_BUCKET_FILES
    }

    for email, bucket in targets.items():
        pilot_row = by_email[email]
        source = _source_row_for_email(frames, email) or _fallback_row(pilot_row)
        moved_rows[bucket].append(
            _apply_pilot_verdict_columns(
                source,
                pilot_row,
                target_bucket=bucket,
            )
        )

    target_emails = set(targets)
    written: dict[str, Path] = {}
    for bucket, filename in _TECHNICAL_BUCKET_FILES.items():
        path = run_dir / filename
        current = frames.get(bucket, pd.DataFrame())
        if not current.empty and "email" in current.columns:
            keep_mask = ~current["email"].map(_normalize_email).isin(target_emails)
            current = current.loc[keep_mask].copy()
        additions = moved_rows[bucket]
        if additions:
            current = pd.concat(
                [current, pd.DataFrame(additions)],
                ignore_index=True,
                sort=False,
            )
        _write_bucket_csv(path, current)
        written[bucket] = path
    return written


# --------------------------------------------------------------------------- #
# Bounce-ingestion bridge
# --------------------------------------------------------------------------- #


def _verdict_to_outcome(verdict: str | None) -> str | None:
    """Map our verdict vocabulary onto bounce_ingestion's CSV
    ``outcome`` column. ``unknown`` and sender-side verdicts are
    skipped: they describe the sender, not the recipient domain, so
    they would pollute the per-domain risk aggregator (counting them
    as ``unknown`` would still drag medium/high-risk thresholds)."""
    if not verdict or verdict == "unknown":
        return None
    if verdict in {VERDICT_INFRA_BLOCKED, VERDICT_PROVIDER_DEFERRED}:
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
        infra_retest = [
            r for r in all_rows if r.dsn_status in INFRA_RETEST_VERDICTS
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
        if infra_retest:
            p = run_dir_path / PILOT_INFRA_RETEST_XLSX
            _write_xlsx(infra_retest, p, "pilot_infrastructure_blocked")
            files_written["pilot_infrastructure_blocked"] = p

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

        bucket_updates = _apply_pilot_results_to_technical_buckets(
            run_dir=run_dir_path,
            rows=all_rows,
        )
        for bucket, path in bucket_updates.items():
            files_written[f"{bucket}_csv"] = path

        # Always emit the per-row evidence report — the audit trail
        # for every routing decision the pilot makes.
        evidence_path = run_dir_path / SMTP_EVIDENCE_REPORT_FILENAME
        write_smtp_evidence_report(all_rows, path=evidence_path)
        files_written["smtp_evidence_report"] = evidence_path

        # Build the customer-language bundle (clean_deliverable /
        # review_provider_limited / high_risk_removed + README) from
        # whatever artifacts now exist in the run directory. Safe to
        # run repeatedly.
        try:
            from ..customer_bundle import emit_customer_bundle

            bundle_result = emit_customer_bundle(run_dir_path)
            for name, path in bundle_result.files_written.items():
                files_written[f"customer_bundle.{name}"] = path
        except Exception as exc:  # pragma: no cover - defensive
            _LOGGER.warning("customer bundle emit failed: %s", exc)

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
    "PILOT_INFRA_RETEST_XLSX",
    "PILOT_SEND_CANDIDATES_XLSX",
    "PILOT_SOFT_BOUNCES_XLSX",
    "PILOT_SUMMARY_REPORT_XLSX",
    "UPDATED_DO_NOT_SEND_XLSX",
    "finalize_pilot",
]
