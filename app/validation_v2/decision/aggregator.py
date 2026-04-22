"""V2 Phase 6 — decision-engine CSV enrichment + summary report.

Reads the three technical CSVs (already enriched by Phases 1–5),
applies :func:`apply_decision_policy` to every row, appends four new
columns, and writes ``decision_summary.csv`` next to them.

Never alters V1 row placement. When ``enable_bucket_override`` is True,
the decision engine populates ``overridden_bucket`` as an annotation;
physically moving rows between files is a downstream concern.
"""

from __future__ import annotations

import csv
import logging
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

from .decision_engine import (
    DecisionResult,
    apply_decision_policy,
    inputs_from_row,
)
from .decision_explanation import explain_decision
from .policy import DEFAULT_DECISION_POLICY, DecisionPolicy, FinalAction


_LOGGER = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Column contract                                                             #
# --------------------------------------------------------------------------- #


DECISION_COLUMNS: tuple[str, ...] = (
    "final_action",
    "decision_reason",
    "decision_confidence",
    "overridden_bucket",
    "decision_note",
)


_CSV_NAMES: tuple[str, ...] = (
    "clean_high_confidence.csv",
    "review_medium_confidence.csv",
    "removed_invalid.csv",
)


# --------------------------------------------------------------------------- #
# Config / stats / result                                                     #
# --------------------------------------------------------------------------- #


@dataclass(slots=True, frozen=True)
class DecisionConfig:
    enabled: bool = True
    approve_threshold: float = 0.80
    review_threshold: float = 0.50
    enable_bucket_override: bool = False
    write_summary_report: bool = True

    def to_policy(self) -> DecisionPolicy:
        return DecisionPolicy(
            approve_threshold=self.approve_threshold,
            review_threshold=self.review_threshold,
            enable_bucket_override=self.enable_bucket_override,
        )


@dataclass(slots=True)
class DecisionStats:
    total_rows_scanned: int = 0
    total_auto_approved: int = 0
    total_manual_review: int = 0
    total_auto_rejected: int = 0
    rejected_hard_fail: int = 0
    rejected_duplicate: int = 0
    rejected_low_probability: int = 0
    bucket_overrides_to_ready: int = 0
    bucket_overrides_to_invalid: int = 0

    def record(self, result: DecisionResult) -> None:
        self.total_rows_scanned += 1
        if result.final_action == FinalAction.AUTO_APPROVE:
            self.total_auto_approved += 1
        elif result.final_action == FinalAction.MANUAL_REVIEW:
            self.total_manual_review += 1
        else:
            self.total_auto_rejected += 1
            if result.decision_reason == "hard_fail":
                self.rejected_hard_fail += 1
            elif result.decision_reason == "duplicate":
                self.rejected_duplicate += 1
            elif result.decision_reason == "low_probability":
                self.rejected_low_probability += 1

        if result.overridden_bucket == "ready":
            self.bucket_overrides_to_ready += 1
        elif result.overridden_bucket == "invalid":
            self.bucket_overrides_to_invalid += 1


@dataclass(slots=True)
class DecisionPassResult:
    rows_processed: int
    stats: DecisionStats
    report_path: Path | None


# --------------------------------------------------------------------------- #
# Row enrichment                                                              #
# --------------------------------------------------------------------------- #


def _row_fields(
    result: DecisionResult, inputs_for_explanation,
) -> dict[str, str]:
    return {
        "final_action": result.final_action,
        "decision_reason": result.decision_reason,
        "decision_confidence": f"{result.decision_confidence:.3f}",
        "overridden_bucket": result.overridden_bucket,
        "decision_note": explain_decision(result, inputs_for_explanation),
    }


def _enrich_one_csv(
    csv_path: Path, policy: DecisionPolicy, stats: DecisionStats,
) -> bool:
    if not csv_path.is_file():
        return False
    fd, tmp_name = tempfile.mkstemp(
        prefix=csv_path.stem + ".decision.",
        suffix=".tmp.csv",
        dir=str(csv_path.parent),
    )
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as fw, csv_path.open(
            encoding="utf-8", newline=""
        ) as fr:
            reader = csv.DictReader(fr)
            existing = list(reader.fieldnames or [])
            fieldnames = existing + [c for c in DECISION_COLUMNS if c not in existing]
            writer = csv.DictWriter(fw, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for row in reader:
                inputs = inputs_from_row(row)
                result = apply_decision_policy(inputs, policy)
                stats.record(result)
                row.update(_row_fields(result, inputs))
                writer.writerow(row)
        os.replace(tmp_path, csv_path)
        return True
    except Exception:
        try:
            tmp_path.unlink()
        except OSError:
            pass
        raise


# --------------------------------------------------------------------------- #
# Summary report                                                              #
# --------------------------------------------------------------------------- #


def write_decision_summary(run_dir: Path, stats: DecisionStats) -> Path:
    run_dir = Path(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    path = run_dir / "decision_summary.csv"
    rows: list[tuple[str, object]] = [
        ("total_rows_scanned", stats.total_rows_scanned),
        ("total_auto_approved", stats.total_auto_approved),
        ("total_manual_review", stats.total_manual_review),
        ("total_auto_rejected", stats.total_auto_rejected),
        ("rejected_hard_fail", stats.rejected_hard_fail),
        ("rejected_duplicate", stats.rejected_duplicate),
        ("rejected_low_probability", stats.rejected_low_probability),
        ("bucket_overrides_to_ready", stats.bucket_overrides_to_ready),
        ("bucket_overrides_to_invalid", stats.bucket_overrides_to_invalid),
    ]
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(("metric", "value"))
        for k, v in rows:
            writer.writerow((k, v))
    return path


# --------------------------------------------------------------------------- #
# Orchestrator                                                                #
# --------------------------------------------------------------------------- #


def run_decision_pass(
    run_dir: Path,
    config: DecisionConfig,
    *,
    logger: logging.Logger | None = None,
) -> DecisionPassResult | None:
    """Top-level Phase-6 entry. Returns None when disabled."""
    log = logger or _LOGGER
    if not config.enabled:
        return None

    run_dir = Path(run_dir)
    policy = config.to_policy()
    stats = DecisionStats()

    enriched_any = False
    for name in _CSV_NAMES:
        try:
            if _enrich_one_csv(run_dir / name, policy, stats):
                enriched_any = True
        except Exception as exc:  # pragma: no cover - defensive guard
            log.warning("decision: failed to enrich %s (%s)", name, exc)

    report: Path | None = None
    if config.write_summary_report and enriched_any:
        report = write_decision_summary(run_dir, stats)

    log.info(
        "decision: rows=%d approve=%d review=%d reject=%d "
        "(overrides: ready=%d, invalid=%d)",
        stats.total_rows_scanned, stats.total_auto_approved,
        stats.total_manual_review, stats.total_auto_rejected,
        stats.bucket_overrides_to_ready, stats.bucket_overrides_to_invalid,
    )
    return DecisionPassResult(
        rows_processed=stats.total_rows_scanned,
        stats=stats,
        report_path=report,
    )


__all__ = [
    "DECISION_COLUMNS",
    "DecisionConfig",
    "DecisionPassResult",
    "DecisionStats",
    "run_decision_pass",
    "write_decision_summary",
]
