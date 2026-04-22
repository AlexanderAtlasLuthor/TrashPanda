"""V2 Phase 5 — CSV enrichment + summary report for the probability model.

Reads the three technical CSVs (already enriched by Phases 2-4), runs
:func:`compute_deliverability_probability` on every row, appends four
new columns, and writes ``deliverability_summary.csv`` next to them.

Never alters V1/V2 bucket placement — probability is strictly an
informational signal.
"""

from __future__ import annotations

import csv
import logging
import os
import tempfile
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path

from .row_explanation import explain_deliverability
from .row_model import (
    DEFAULT_PROBABILITY_THRESHOLDS,
    DeliverabilityComputation,
    ProbabilityThresholds,
    compute_deliverability_probability,
    inputs_from_row,
)


_LOGGER = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Column contract                                                             #
# --------------------------------------------------------------------------- #


PROBABILITY_COLUMNS: tuple[str, ...] = (
    "deliverability_probability",
    "deliverability_label",
    "deliverability_factors",
    "deliverability_note",
)


_CSV_NAMES: tuple[str, ...] = (
    "clean_high_confidence.csv",
    "review_medium_confidence.csv",
    "removed_invalid.csv",
)


# --------------------------------------------------------------------------- #
# Config + stats + result                                                     #
# --------------------------------------------------------------------------- #


@dataclass(slots=True, frozen=True)
class ProbabilityConfig:
    enabled: bool = True
    high_threshold: float = 0.70
    medium_threshold: float = 0.40
    write_summary_report: bool = True


@dataclass(slots=True)
class ProbabilityStats:
    total_rows_scanned: int = 0
    rows_high: int = 0
    rows_medium: int = 0
    rows_low: int = 0
    rows_overridden_hard_fail: int = 0
    rows_overridden_duplicate: int = 0
    rows_overridden_no_mx: int = 0
    probability_sum: float = 0.0
    factors_seen: dict[str, int] = field(default_factory=dict)

    @property
    def mean_probability(self) -> float:
        if self.total_rows_scanned == 0:
            return 0.0
        return self.probability_sum / float(self.total_rows_scanned)

    def record(self, computation: DeliverabilityComputation) -> None:
        self.total_rows_scanned += 1
        self.probability_sum += computation.probability

        if computation.label == "high":
            self.rows_high += 1
        elif computation.label == "medium":
            self.rows_medium += 1
        else:
            self.rows_low += 1

        if computation.override_reason == "hard_fail":
            self.rows_overridden_hard_fail += 1
        elif computation.override_reason == "duplicate":
            self.rows_overridden_duplicate += 1
        elif computation.override_reason == "no_mx_record":
            self.rows_overridden_no_mx += 1

        for factor in computation.factors:
            self.factors_seen[factor.name] = self.factors_seen.get(factor.name, 0) + 1


@dataclass(slots=True)
class ProbabilityPassResult:
    rows_processed: int
    stats: ProbabilityStats
    report_path: Path | None


# --------------------------------------------------------------------------- #
# CSV enrichment                                                              #
# --------------------------------------------------------------------------- #


def _thresholds_from_config(config: ProbabilityConfig) -> ProbabilityThresholds:
    return ProbabilityThresholds(
        high_threshold=config.high_threshold,
        medium_threshold=config.medium_threshold,
        # Remaining defaults come from the model's own defaults.
        smtp_deliverable_multiplier=DEFAULT_PROBABILITY_THRESHOLDS.smtp_deliverable_multiplier,
        smtp_undeliverable_multiplier=DEFAULT_PROBABILITY_THRESHOLDS.smtp_undeliverable_multiplier,
        smtp_catch_all_multiplier=DEFAULT_PROBABILITY_THRESHOLDS.smtp_catch_all_multiplier,
        smtp_inconclusive_multiplier=DEFAULT_PROBABILITY_THRESHOLDS.smtp_inconclusive_multiplier,
        historical_reliable_multiplier=DEFAULT_PROBABILITY_THRESHOLDS.historical_reliable_multiplier,
        historical_unstable_multiplier=DEFAULT_PROBABILITY_THRESHOLDS.historical_unstable_multiplier,
        historical_risky_multiplier=DEFAULT_PROBABILITY_THRESHOLDS.historical_risky_multiplier,
        catch_all_strong_threshold=DEFAULT_PROBABILITY_THRESHOLDS.catch_all_strong_threshold,
        catch_all_strong_multiplier=DEFAULT_PROBABILITY_THRESHOLDS.catch_all_strong_multiplier,
        catch_all_moderate_threshold=DEFAULT_PROBABILITY_THRESHOLDS.catch_all_moderate_threshold,
        catch_all_moderate_multiplier=DEFAULT_PROBABILITY_THRESHOLDS.catch_all_moderate_multiplier,
    )


def _row_fields(computation: DeliverabilityComputation) -> dict[str, str]:
    factor_names = "|".join(f.name for f in computation.factors)
    if computation.override_reason:
        factor_names = f"override:{computation.override_reason}"
    return {
        "deliverability_probability": f"{computation.probability:.3f}",
        "deliverability_label": computation.label,
        "deliverability_factors": factor_names,
        "deliverability_note": explain_deliverability(computation),
    }


def _enrich_one_csv(
    csv_path: Path,
    thresholds: ProbabilityThresholds,
    stats: ProbabilityStats,
) -> bool:
    if not csv_path.is_file():
        return False

    fd, tmp_name = tempfile.mkstemp(
        prefix=csv_path.stem + ".prob.",
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
            fieldnames = existing + [
                c for c in PROBABILITY_COLUMNS if c not in existing
            ]
            writer = csv.DictWriter(fw, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for row in reader:
                inputs = inputs_from_row(row)
                computation = compute_deliverability_probability(inputs, thresholds)
                stats.record(computation)
                row.update(_row_fields(computation))
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


def write_probability_summary(run_dir: Path, stats: ProbabilityStats) -> Path:
    run_dir = Path(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    path = run_dir / "deliverability_summary.csv"

    rows: list[tuple[str, object]] = [
        ("total_rows_scanned", stats.total_rows_scanned),
        ("rows_high", stats.rows_high),
        ("rows_medium", stats.rows_medium),
        ("rows_low", stats.rows_low),
        ("rows_overridden_hard_fail", stats.rows_overridden_hard_fail),
        ("rows_overridden_duplicate", stats.rows_overridden_duplicate),
        ("rows_overridden_no_mx", stats.rows_overridden_no_mx),
        ("mean_probability", round(stats.mean_probability, 4)),
    ]
    for factor, count in sorted(stats.factors_seen.items()):
        rows.append((f"factor:{factor}", count))

    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(("metric", "value"))
        for k, v in rows:
            writer.writerow((k, v))
    return path


# --------------------------------------------------------------------------- #
# Orchestrator                                                                #
# --------------------------------------------------------------------------- #


def run_probability_pass(
    run_dir: Path,
    config: ProbabilityConfig,
    *,
    logger: logging.Logger | None = None,
) -> ProbabilityPassResult | None:
    """Top-level Phase-5 entry point. Returns None when disabled."""
    log = logger or _LOGGER
    if not config.enabled:
        return None

    run_dir = Path(run_dir)
    thresholds = _thresholds_from_config(config)
    stats = ProbabilityStats()

    enriched_any = False
    for name in _CSV_NAMES:
        try:
            if _enrich_one_csv(run_dir / name, thresholds, stats):
                enriched_any = True
        except Exception as exc:  # pragma: no cover - defensive guard
            log.warning("probability: failed to enrich %s (%s)", name, exc)

    report_path: Path | None = None
    if config.write_summary_report and enriched_any:
        report_path = write_probability_summary(run_dir, stats)

    log.info(
        "probability: rows=%d high=%d medium=%d low=%d mean=%.3f",
        stats.total_rows_scanned, stats.rows_high, stats.rows_medium,
        stats.rows_low, stats.mean_probability,
    )
    return ProbabilityPassResult(
        rows_processed=stats.total_rows_scanned,
        stats=stats,
        report_path=report_path,
    )


__all__ = [
    "PROBABILITY_COLUMNS",
    "ProbabilityConfig",
    "ProbabilityPassResult",
    "ProbabilityStats",
    "run_probability_pass",
    "write_probability_summary",
]
