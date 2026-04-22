"""V2 Phase 2 — conservative score adjustment from domain history.

Design
------
The V1 pipeline's scoring and bucketing logic are never modified. This
module runs *after* the pipeline has written its technical CSVs. For
each row it looks up the domain's pre-update history record and
produces an :class:`AdjustmentDecision` that captures:

* the V1 score (``score_pre_history``),
* a small, bounded integer adjustment (``confidence_adjustment_applied``),
* the post-adjustment score (``score_post_history``),
* the historical label (``historical_label``),
* the original bucket and the final bucket, plus the reason any flip
  was blocked.

The decision is then written back into the CSV as a set of additive
columns; existing columns and row placement on disk are preserved.

Guardrails (deterministic)
--------------------------
1. **Hard-fails and duplicates are immutable.** History never rescues
   a row that V1 explicitly removed on syntax/disposable/hard-stop
   grounds, and never un-deduplicates a non-canonical row.
2. **Adjustment disabled → no-op.** When
   ``AdjustmentConfig.apply`` is False the post score equals the pre
   score; no column value changes relative to what V1 produced.
3. **Insufficient data → no-op.** A domain with fewer observations
   than ``min_observations_for_adjustment`` receives adjustment 0.
4. **Flips are conservative.** Only ``review ↔ ready`` transitions are
   permitted and only when ``allow_bucket_flip_from_history`` is True.
   ``invalid → ready`` (or the reverse) is never allowed via history.
"""

from __future__ import annotations

import csv
import logging
import os
import tempfile
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .catch_all import (
    DEFAULT_CATCH_ALL_THRESHOLDS,
    CatchAllSignal,
    CatchAllThresholds,
    classify_review_subclass,
    detect_catch_all_signals,
)
from .domain_memory import (
    DEFAULT_THRESHOLDS,
    LabelThresholds,
    classify_domain,
    compute_adjustment,
)
from .history_models import DomainHistoryRecord, HistoricalLabel


_LOGGER = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Bucket helpers                                                              #
# --------------------------------------------------------------------------- #


READY = "ready"
REVIEW = "review"
INVALID = "invalid"
HARD_FAIL = "hard_fail"
DUPLICATE = "duplicate"
UNKNOWN = "unknown"


_BUCKET_FROM_REASON: dict[str, str] = {
    "kept_high_confidence": READY,
    "kept_review": REVIEW,
    "removed_low_score": INVALID,
    "removed_hard_fail": HARD_FAIL,
    "removed_duplicate": DUPLICATE,
}


_SAFE_FLIPS: frozenset[tuple[str, str]] = frozenset(
    {(REVIEW, READY), (READY, REVIEW)}
)


def bucket_from_output_reason(reason: str) -> str:
    """Translate V1's ``final_output_reason`` string to our bucket vocabulary."""
    return _BUCKET_FROM_REASON.get((reason or "").strip(), UNKNOWN)


def bucket_from_score(
    score: int,
    high_confidence_threshold: int,
    review_threshold: int,
) -> str:
    """Classify a score by thresholds. Pure function — no history involved."""
    if score >= high_confidence_threshold:
        return READY
    if score >= review_threshold:
        return REVIEW
    return INVALID


# --------------------------------------------------------------------------- #
# Config + decision dataclasses                                               #
# --------------------------------------------------------------------------- #


@dataclass(slots=True, frozen=True)
class AdjustmentConfig:
    """Runtime knobs governing the Phase-2 adjustment pass."""

    apply: bool = False
    max_positive_adjustment: int = 3
    max_negative_adjustment: int = 5
    min_observations_for_adjustment: int = 5
    allow_bucket_flip_from_history: bool = False
    high_confidence_threshold: int = 70
    review_threshold: int = 40


@dataclass(slots=True)
class AdjustmentDecision:
    """Per-row decision produced by :func:`compute_row_adjustment`."""

    score_pre_history: int
    score_post_history: int
    confidence_adjustment_applied: int
    historical_label: str
    historical_total_seen_count: int
    historical_ready_rate: float
    historical_invalid_rate: float
    historical_timeout_rate: float
    original_bucket: str
    final_bucket: str
    historical_bucket_flipped: bool
    flip_blocked_reason: str  # "", "hard_fail", "duplicate", "config_disabled",
                              # "insufficient_data", "flips_disabled", "cross_tier"


# --------------------------------------------------------------------------- #
# Row-level decision                                                          #
# --------------------------------------------------------------------------- #


def _clamp(value: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, value))


def compute_row_adjustment(
    *,
    score: int,
    original_bucket: str,
    hard_fail: bool,
    record: DomainHistoryRecord | None,
    config: AdjustmentConfig,
    thresholds: LabelThresholds = DEFAULT_THRESHOLDS,
) -> AdjustmentDecision:
    """Produce an :class:`AdjustmentDecision` for one row.

    Pure function: no I/O, no globals. Exhaustively tested for every
    guardrail branch in ``tests/test_v2_scoring_adjustment.py``.
    """
    score_pre = _clamp(int(score) if score is not None else 0, 0, 100)
    total_seen = record.total_seen_count if record is not None else 0
    label = (
        classify_domain(record, thresholds)
        if record is not None
        else HistoricalLabel.INSUFFICIENT_DATA
    )

    base = AdjustmentDecision(
        score_pre_history=score_pre,
        score_post_history=score_pre,
        confidence_adjustment_applied=0,
        historical_label=label,
        historical_total_seen_count=total_seen,
        historical_ready_rate=record.ready_rate if record else 0.0,
        historical_invalid_rate=record.invalid_rate if record else 0.0,
        historical_timeout_rate=record.timeout_rate if record else 0.0,
        original_bucket=original_bucket,
        final_bucket=original_bucket,
        historical_bucket_flipped=False,
        flip_blocked_reason="",
    )

    # Guardrails: these rows NEVER receive adjustment.
    if hard_fail or original_bucket == HARD_FAIL:
        base.flip_blocked_reason = "hard_fail"
        return base
    if original_bucket == DUPLICATE:
        base.flip_blocked_reason = "duplicate"
        return base
    if not config.apply:
        base.flip_blocked_reason = "config_disabled"
        return base
    if total_seen < config.min_observations_for_adjustment:
        base.flip_blocked_reason = "insufficient_data"
        return base

    # Compute adjustment.
    adjustment = compute_adjustment(
        record,
        max_positive=config.max_positive_adjustment,
        max_negative=config.max_negative_adjustment,
        thresholds=thresholds,
    )
    score_post = _clamp(score_pre + adjustment, 0, 100)

    # Bucket decision.
    if not config.allow_bucket_flip_from_history:
        base.confidence_adjustment_applied = adjustment
        base.score_post_history = score_post
        base.flip_blocked_reason = "flips_disabled"
        return base

    proposed = bucket_from_score(
        score_post,
        config.high_confidence_threshold,
        config.review_threshold,
    )
    if proposed == original_bucket:
        base.confidence_adjustment_applied = adjustment
        base.score_post_history = score_post
        return base

    transition = (original_bucket, proposed)
    if transition in _SAFE_FLIPS:
        base.confidence_adjustment_applied = adjustment
        base.score_post_history = score_post
        base.final_bucket = proposed
        base.historical_bucket_flipped = True
        return base

    # Unsafe cross-tier flip (e.g. invalid ↔ ready). Keep original bucket
    # but still record the score change for auditability.
    base.confidence_adjustment_applied = adjustment
    base.score_post_history = score_post
    base.flip_blocked_reason = "cross_tier"
    return base


# --------------------------------------------------------------------------- #
# Aggregate stats                                                             #
# --------------------------------------------------------------------------- #


@dataclass(slots=True)
class AdjustmentStats:
    """Aggregate counters written into ``historical_adjustment_summary.csv``."""

    total_rows_scanned: int = 0
    rows_with_positive_adjustment: int = 0
    rows_with_negative_adjustment: int = 0
    rows_with_zero_adjustment: int = 0
    rows_by_label: dict[str, int] = field(default_factory=dict)
    bucket_flips_review_to_ready: int = 0
    bucket_flips_ready_to_review: int = 0
    flips_blocked_hard_fail: int = 0
    flips_blocked_duplicate: int = 0
    flips_blocked_cross_tier: int = 0
    flips_blocked_config: int = 0
    flips_blocked_insufficient_data: int = 0
    # Phase 3 counters.
    rows_with_possible_catch_all: int = 0
    review_subclasses: dict[str, int] = field(default_factory=dict)

    def record(
        self,
        decision: AdjustmentDecision,
        *,
        catch_all: CatchAllSignal | None = None,
        review_subclass: str | None = None,
    ) -> None:
        self.total_rows_scanned += 1

        adj = decision.confidence_adjustment_applied
        if adj > 0:
            self.rows_with_positive_adjustment += 1
        elif adj < 0:
            self.rows_with_negative_adjustment += 1
        else:
            self.rows_with_zero_adjustment += 1

        self.rows_by_label[decision.historical_label] = (
            self.rows_by_label.get(decision.historical_label, 0) + 1
        )

        if decision.historical_bucket_flipped:
            pair = (decision.original_bucket, decision.final_bucket)
            if pair == (REVIEW, READY):
                self.bucket_flips_review_to_ready += 1
            elif pair == (READY, REVIEW):
                self.bucket_flips_ready_to_review += 1

        blocked = decision.flip_blocked_reason
        if blocked == "hard_fail":
            self.flips_blocked_hard_fail += 1
        elif blocked == "duplicate":
            self.flips_blocked_duplicate += 1
        elif blocked == "cross_tier":
            self.flips_blocked_cross_tier += 1
        elif blocked in ("config_disabled", "flips_disabled"):
            self.flips_blocked_config += 1
        elif blocked == "insufficient_data":
            self.flips_blocked_insufficient_data += 1

        if catch_all is not None and catch_all.is_possible_catch_all:
            self.rows_with_possible_catch_all += 1
        if review_subclass is not None:
            self.review_subclasses[review_subclass] = (
                self.review_subclasses.get(review_subclass, 0) + 1
            )


# --------------------------------------------------------------------------- #
# CSV enrichment                                                              #
# --------------------------------------------------------------------------- #


# Columns appended to each technical CSV in deterministic order.
_NEW_COLUMNS: tuple[str, ...] = (
    # Phase 2 — scoring adjustment + human text.
    "score_pre_history",
    "score_post_history",
    "confidence_adjustment_applied",
    "historical_label",
    "historical_total_seen_count",
    "historical_ready_rate",
    "historical_invalid_rate",
    "historical_timeout_rate",
    "historical_bucket_flipped",
    "v2_final_bucket",
    "flip_blocked_reason",
    "human_reason",
    "human_risk",
    "human_recommendation",
    # Phase 3 — catch-all + review intelligence.
    "possible_catch_all",
    "catch_all_confidence",
    "catch_all_reason",
    "review_subclass",
)


NEW_COLUMNS: tuple[str, ...] = _NEW_COLUMNS  # public re-export


def _truthy(value: Any) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() in ("1", "true", "t", "yes", "y")


def _int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _pick_domain(row: dict[str, str]) -> str:
    for key in ("corrected_domain", "domain", "domain_from_email"):
        value = (row.get(key) or "").strip().lower()
        if value:
            return value
    return ""


def _decision_row_fields(
    decision: AdjustmentDecision,
    human: dict[str, str],
) -> dict[str, str]:
    return {
        "score_pre_history": str(decision.score_pre_history),
        "score_post_history": str(decision.score_post_history),
        "confidence_adjustment_applied": str(decision.confidence_adjustment_applied),
        "historical_label": decision.historical_label,
        "historical_total_seen_count": str(decision.historical_total_seen_count),
        "historical_ready_rate": f"{decision.historical_ready_rate:.3f}",
        "historical_invalid_rate": f"{decision.historical_invalid_rate:.3f}",
        "historical_timeout_rate": f"{decision.historical_timeout_rate:.3f}",
        "historical_bucket_flipped": str(decision.historical_bucket_flipped),
        "v2_final_bucket": decision.final_bucket,
        "flip_blocked_reason": decision.flip_blocked_reason,
        "human_reason": human.get("human_reason", ""),
        "human_risk": human.get("human_risk", ""),
        "human_recommendation": human.get("human_recommendation", ""),
    }


def _dns_error_implies_timeout(dns_error: str | None) -> bool:
    if not dns_error:
        return False
    lowered = dns_error.lower()
    return "timeout" in lowered or "timed out" in lowered


def enrich_csv(
    csv_path: Path,
    pre_records: dict[str, DomainHistoryRecord],
    config: AdjustmentConfig,
    stats: AdjustmentStats,
    *,
    default_bucket: str,
    thresholds: LabelThresholds = DEFAULT_THRESHOLDS,
    catch_all_thresholds: CatchAllThresholds = DEFAULT_CATCH_ALL_THRESHOLDS,
) -> bool:
    """Rewrite one technical CSV in-place with V2 columns appended.

    Returns True if the file existed and was rewritten; False otherwise.
    Uses an atomic rename (tmp file + os.replace) so a crash mid-write
    never leaves the CSV in a half-rewritten state.
    """
    # Local import breaks a cycle: explanation_v2 imports from this module
    # for the AdjustmentDecision type hint during static analysis only,
    # and this module imports explanation_v2 at call time.
    from .explanation_v2 import explain_row_with_history

    if not csv_path.is_file():
        return False

    fd, tmp_name = tempfile.mkstemp(
        prefix=csv_path.stem + ".", suffix=".tmp.csv", dir=str(csv_path.parent)
    )
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as fw, csv_path.open(
            encoding="utf-8", newline=""
        ) as fr:
            reader = csv.DictReader(fr)
            existing_fields = list(reader.fieldnames or [])
            fieldnames = existing_fields + [
                c for c in _NEW_COLUMNS if c not in existing_fields
            ]
            writer = csv.DictWriter(fw, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()

            for row in reader:
                domain = _pick_domain(row)
                record = pre_records.get(domain)
                score = _int(row.get("score"), default=0)
                hard_fail = _truthy(row.get("hard_fail"))
                reason = (row.get("final_output_reason") or "").strip()
                if reason:
                    bucket = bucket_from_output_reason(reason)
                    if bucket == UNKNOWN:
                        bucket = default_bucket
                else:
                    bucket = default_bucket

                decision = compute_row_adjustment(
                    score=score,
                    original_bucket=bucket,
                    hard_fail=hard_fail,
                    record=record,
                    config=config,
                    thresholds=thresholds,
                )

                # Phase 3: catch-all detection uses the final (post-flip)
                # bucket so a row promoted by Phase 2 to ready stops
                # corroborating the catch-all story.
                has_mx_now = _truthy(row.get("has_mx_record"))
                had_timeout_now = _dns_error_implies_timeout(row.get("dns_error"))
                catch_all = detect_catch_all_signals(
                    record,
                    current_row_in_review=(decision.final_bucket == REVIEW),
                    current_row_has_mx=has_mx_now,
                    thresholds=catch_all_thresholds,
                )
                review_subclass = classify_review_subclass(
                    final_bucket=decision.final_bucket,
                    catch_all=catch_all,
                    record=record,
                    current_row_had_timeout=had_timeout_now,
                    label_thresholds=thresholds,
                    catch_all_thresholds=catch_all_thresholds,
                )

                human = explain_row_with_history(decision, record, catch_all=catch_all)
                row.update(_decision_row_fields(decision, human))
                row["possible_catch_all"] = str(catch_all.is_possible_catch_all)
                row["catch_all_confidence"] = f"{catch_all.confidence:.3f}"
                row["catch_all_reason"] = catch_all.reason
                row["review_subclass"] = review_subclass
                writer.writerow(row)
                stats.record(
                    decision,
                    catch_all=catch_all,
                    review_subclass=review_subclass,
                )
        os.replace(tmp_path, csv_path)
        return True
    except Exception:
        # Clean up the tmp file if anything went wrong; caller wraps us.
        try:
            tmp_path.unlink()
        except OSError:
            pass
        raise


_CSVS_AND_DEFAULT_BUCKET: tuple[tuple[str, str], ...] = (
    ("clean_high_confidence.csv", READY),
    ("review_medium_confidence.csv", REVIEW),
    ("removed_invalid.csv", INVALID),
)


def enrich_run_outputs(
    run_dir: Path,
    pre_records: dict[str, DomainHistoryRecord],
    config: AdjustmentConfig,
    *,
    thresholds: LabelThresholds = DEFAULT_THRESHOLDS,
    catch_all_thresholds: CatchAllThresholds = DEFAULT_CATCH_ALL_THRESHOLDS,
    logger: logging.Logger | None = None,
) -> AdjustmentStats:
    """Enrich all three technical CSVs in-place with V2 columns."""
    log = logger or _LOGGER
    stats = AdjustmentStats()
    for csv_name, default_bucket in _CSVS_AND_DEFAULT_BUCKET:
        csv_path = run_dir / csv_name
        try:
            enrich_csv(
                csv_path,
                pre_records=pre_records,
                config=config,
                stats=stats,
                default_bucket=default_bucket,
                thresholds=thresholds,
                catch_all_thresholds=catch_all_thresholds,
            )
        except Exception as exc:  # pragma: no cover - defensive guard
            log.warning("adjustment: failed to enrich %s (%s)", csv_path.name, exc)
    return stats


# --------------------------------------------------------------------------- #
# Adjustment-summary report                                                   #
# --------------------------------------------------------------------------- #


def write_adjustment_summary(run_dir: Path, stats: AdjustmentStats) -> Path:
    """Write ``historical_adjustment_summary.csv`` in metric/value format."""
    run_dir = Path(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    out_path = run_dir / "historical_adjustment_summary.csv"

    rows: list[tuple[str, Any]] = [
        ("total_rows_scanned", stats.total_rows_scanned),
        ("rows_with_positive_adjustment", stats.rows_with_positive_adjustment),
        ("rows_with_negative_adjustment", stats.rows_with_negative_adjustment),
        ("rows_with_zero_adjustment", stats.rows_with_zero_adjustment),
        ("bucket_flips_review_to_ready", stats.bucket_flips_review_to_ready),
        ("bucket_flips_ready_to_review", stats.bucket_flips_ready_to_review),
        ("flips_blocked_hard_fail", stats.flips_blocked_hard_fail),
        ("flips_blocked_duplicate", stats.flips_blocked_duplicate),
        ("flips_blocked_cross_tier", stats.flips_blocked_cross_tier),
        ("flips_blocked_config", stats.flips_blocked_config),
        ("flips_blocked_insufficient_data", stats.flips_blocked_insufficient_data),
        ("rows_with_possible_catch_all", stats.rows_with_possible_catch_all),
    ]
    for label, count in sorted(stats.rows_by_label.items()):
        rows.append((f"rows_by_label:{label}", count))
    for subclass, count in sorted(stats.review_subclasses.items()):
        rows.append((f"review_subclass:{subclass}", count))

    with out_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(("metric", "value"))
        for label, value in rows:
            writer.writerow((label, value))

    return out_path


__all__ = [
    "AdjustmentConfig",
    "AdjustmentDecision",
    "AdjustmentStats",
    "DUPLICATE",
    "HARD_FAIL",
    "INVALID",
    "NEW_COLUMNS",
    "READY",
    "REVIEW",
    "UNKNOWN",
    "bucket_from_output_reason",
    "bucket_from_score",
    "compute_row_adjustment",
    "enrich_csv",
    "enrich_run_outputs",
    "write_adjustment_summary",
]
