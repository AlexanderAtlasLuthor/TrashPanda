"""Glue between the V1 pipeline outputs and the V2 history layer.

The V1 pipeline is not modified. Instead, after a run finishes, we read
its technical CSVs (``clean_high_confidence.csv``,
``review_medium_confidence.csv``, ``removed_invalid.csv``), build one
:class:`DomainObservation` per row, and upsert them into the store.

This keeps the per-row cost of V2 effectively zero during the pipeline
and localises all V2 side-effects in a single post-run function.
"""

from __future__ import annotations

import csv
import logging
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .domain_memory import (
    DEFAULT_THRESHOLDS,
    LabelThresholds,
    classify_domain,
    compute_adjustment,
)
from .explanation_v2 import explain_domain_history
from .history_models import DomainObservation, FinalDecision
from .history_store import DomainHistoryStore
from .scoring_adjustment import (
    AdjustmentConfig,
    AdjustmentStats,
    enrich_run_outputs,
    write_adjustment_summary,
)


_LOGGER = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# CSV → observation mapping                                                   #
# --------------------------------------------------------------------------- #


# Map from the pipeline's ``final_output_reason`` values (see
# app/pipeline.py, Phase 3 classification) to our compact
# :class:`FinalDecision` vocabulary.
_REASON_TO_DECISION: dict[str, str] = {
    "kept_high_confidence": FinalDecision.READY,
    "kept_review": FinalDecision.REVIEW,
    "removed_low_score": FinalDecision.INVALID,
    "removed_duplicate": FinalDecision.INVALID,
    "removed_hard_fail": FinalDecision.HARD_FAIL,
}


_CSV_NAMES_BY_DEFAULT_DECISION: tuple[tuple[str, str], ...] = (
    ("clean_high_confidence.csv", FinalDecision.READY),
    ("review_medium_confidence.csv", FinalDecision.REVIEW),
    ("removed_invalid.csv", FinalDecision.INVALID),
)


def _is_truthy_csv(value: Any) -> bool:
    """CSV booleans are written as the strings ``True`` / ``False``."""
    if value is None:
        return False
    s = str(value).strip().lower()
    return s in ("1", "true", "t", "yes", "y")


def _pick_domain(row: dict[str, str]) -> str:
    """Domain of record for a row.

    Prefer ``corrected_domain`` (the domain actually used downstream)
    over the raw ``domain`` column. Fall back to ``domain_from_email``.
    """
    for key in ("corrected_domain", "domain", "domain_from_email"):
        value = (row.get(key) or "").strip().lower()
        if value:
            return value
    return ""


def _row_to_observation(row: dict[str, str], default_decision: str) -> DomainObservation | None:
    domain = _pick_domain(row)
    if not domain:
        return None

    # Preferred decision comes from pipeline's own final_output_reason;
    # fall back to the per-file default if unknown.
    reason_key = (row.get("final_output_reason") or "").strip()
    decision = _REASON_TO_DECISION.get(reason_key, default_decision)

    had_mx = _is_truthy_csv(row.get("has_mx_record"))
    had_a = _is_truthy_csv(row.get("has_a_record"))
    had_a_fallback = (not had_mx) and had_a
    dns_performed = _is_truthy_csv(row.get("dns_check_performed"))
    dns_error = (row.get("dns_error") or "").strip()
    had_dns_failure = dns_performed and (not had_mx) and (not had_a)
    dns_error_lower = dns_error.lower()
    had_timeout = "timeout" in dns_error_lower or "timed out" in dns_error_lower
    was_typo = _is_truthy_csv(row.get("typo_corrected"))
    had_hard_fail = _is_truthy_csv(row.get("hard_fail")) or decision == FinalDecision.HARD_FAIL

    return DomainObservation(
        domain=domain,
        had_mx=had_mx,
        had_a_fallback=had_a_fallback,
        had_dns_failure=had_dns_failure,
        had_timeout=had_timeout,
        was_typo_corrected=was_typo,
        had_hard_fail=had_hard_fail,
        final_decision=decision,
    )


def _iter_rows(csv_path: Path) -> Iterator[dict[str, str]]:
    if not csv_path.is_file():
        return
    with csv_path.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            yield row


def build_observations_from_run(run_dir: Path) -> list[DomainObservation]:
    """Read the three technical CSVs and yield observations.

    Rows without an identifiable domain are skipped. Output is NOT
    deduplicated by domain — callers that want per-domain aggregation
    should pass the list to :meth:`DomainHistoryStore.bulk_update`, which
    groups internally.
    """
    run_dir = Path(run_dir)
    observations: list[DomainObservation] = []
    for csv_name, default_decision in _CSV_NAMES_BY_DEFAULT_DECISION:
        for row in _iter_rows(run_dir / csv_name):
            obs = _row_to_observation(row, default_decision)
            if obs is not None:
                observations.append(obs)
    return observations


# --------------------------------------------------------------------------- #
# Orchestration: update history + write report                                #
# --------------------------------------------------------------------------- #


@dataclass(slots=True)
class HistoryUpdateResult:
    """Return payload for :func:`update_history_from_run`."""

    observations_processed: int
    domains_updated: int
    report_path: Path | None
    adjustment_report_path: Path | None = None
    adjustment_stats: AdjustmentStats | None = None


def update_history_from_run(
    run_dir: Path,
    store: DomainHistoryStore,
    *,
    write_summary_report: bool = True,
    thresholds: LabelThresholds = DEFAULT_THRESHOLDS,
    max_positive_adjustment: int = 3,
    max_negative_adjustment: int = 5,
    # Phase 2 kwargs — all default to "no adjustment, backwards-compatible".
    adjustment_config: AdjustmentConfig | None = None,
    write_adjustment_report: bool = False,
    now: datetime | None = None,
    logger: logging.Logger | None = None,
) -> HistoryUpdateResult:
    """Read the run's outputs, update history, and write summary CSVs.

    Phase 2 extension: when ``adjustment_config.apply`` is True, the
    three technical CSVs are rewritten with historical columns BEFORE
    the store is updated (so the snapshot used for adjustment reflects
    prior runs only, not the current one).

    Safe to call from ``run_cleaning_job``'s post-run phase. Callers are
    expected to wrap this in try/except themselves — this function only
    raises on programmer error.
    """
    log = logger or _LOGGER
    run_dir = Path(run_dir)

    observations = build_observations_from_run(run_dir)
    if not observations:
        log.info("history: no observations built from run (run_dir=%s)", run_dir)
        return HistoryUpdateResult(0, 0, None)

    # ── Phase 2: enrich outputs using PRE-update snapshot ──────────── #
    adjustment_report_path: Path | None = None
    adjustment_stats: AdjustmentStats | None = None
    if adjustment_config is not None and adjustment_config.apply:
        domains_in_run = {obs.domain for obs in observations if obs.domain}
        pre_records = store.get_many(domains_in_run)
        adjustment_stats = enrich_run_outputs(
            run_dir=run_dir,
            pre_records=pre_records,
            config=adjustment_config,
            thresholds=thresholds,
            logger=log,
        )
        if write_adjustment_report:
            adjustment_report_path = write_adjustment_summary(run_dir, adjustment_stats)

    # ── Phase 1: update store (original behaviour) ─────────────────── #
    updated = store.bulk_update(observations, now=now)

    report_path: Path | None = None
    if write_summary_report:
        report_path = write_domain_history_summary(
            run_dir=run_dir,
            records=updated.values(),
            thresholds=thresholds,
            max_positive_adjustment=max_positive_adjustment,
            max_negative_adjustment=max_negative_adjustment,
        )

    log.info(
        "history: observations=%d domains_updated=%d report=%s adjustment=%s",
        len(observations),
        len(updated),
        report_path.name if report_path else "none",
        adjustment_report_path.name if adjustment_report_path else "none",
    )
    return HistoryUpdateResult(
        observations_processed=len(observations),
        domains_updated=len(updated),
        report_path=report_path,
        adjustment_report_path=adjustment_report_path,
        adjustment_stats=adjustment_stats,
    )


# --------------------------------------------------------------------------- #
# Report writer                                                               #
# --------------------------------------------------------------------------- #


_REPORT_HEADER: tuple[str, ...] = (
    "domain",
    "total_seen_count",
    "mx_rate",
    "invalid_rate",
    "review_rate",
    "ready_rate",
    "timeout_rate",
    "dns_failure_rate",
    "readiness_label",
    "confidence_adjustment",
    "historical_explanation",
)


def write_domain_history_summary(
    run_dir: Path,
    records: Iterable,
    *,
    thresholds: LabelThresholds = DEFAULT_THRESHOLDS,
    max_positive_adjustment: int = 3,
    max_negative_adjustment: int = 5,
) -> Path:
    """Write ``domain_history_summary.csv`` into ``run_dir``.

    Accepts an iterable of :class:`DomainHistoryRecord`. Rows are sorted
    by ``total_seen_count`` desc so the most-observed domains surface
    first when a human opens the CSV.
    """
    run_dir = Path(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    out_path = run_dir / "domain_history_summary.csv"

    materialised = list(records)
    materialised.sort(key=lambda r: r.total_seen_count, reverse=True)

    with out_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(_REPORT_HEADER)
        for record in materialised:
            label = classify_domain(record, thresholds)
            adjustment = compute_adjustment(
                record,
                max_positive=max_positive_adjustment,
                max_negative=max_negative_adjustment,
                thresholds=thresholds,
            )
            writer.writerow([
                record.domain,
                record.total_seen_count,
                round(record.mx_rate, 3),
                round(record.invalid_rate, 3),
                round(record.review_rate, 3),
                round(record.ready_rate, 3),
                round(record.timeout_rate, 3),
                round(record.dns_failure_rate, 3),
                label,
                adjustment,
                explain_domain_history(record, thresholds),
            ])
    return out_path


__all__ = [
    "HistoryUpdateResult",
    "build_observations_from_run",
    "update_history_from_run",
    "write_domain_history_summary",
]
