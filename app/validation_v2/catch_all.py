"""V2 Phase 3 — catch-all heuristic + review-bucket subclassification.

This module is purely heuristic and deterministic. It never hits the
network and never mutates the pipeline's output; its job is to turn
the historical record plus a few current-row fields into two signals:

* :class:`CatchAllSignal` — fires when a domain looks like it may
  accept all inbound addresses (high review rate + MX present + low
  invalid rate + corroborating current-row evidence).
* A ``review_subclass`` string that refines the V1 "review" bucket
  into one of: ``review_catch_all``, ``review_timeout``,
  ``review_inconsistent``, ``review_low_confidence``, or
  ``not_review`` (for non-review rows).

Guardrails (hard rules, checked before scoring):
  * Domains with fewer observations than ``min_observations`` never
    fire — insufficient evidence.
  * Domains with ``mx_rate < min_mx_rate`` never fire — they don't even
    route mail, so "accept all" doesn't apply.
  * Domains with ``invalid_rate > max_invalid_rate`` never fire — these
    are just bad domains, not catch-alls.
  * Domains with ``review_rate < min_review_rate`` never fire — no
    ambiguity in their history.
"""

from __future__ import annotations

from dataclasses import dataclass

from .domain_memory import DEFAULT_THRESHOLDS, LabelThresholds, classify_domain
from .history_models import DomainHistoryRecord, HistoricalLabel


# --------------------------------------------------------------------------- #
# Vocabulary (module-level constants)                                         #
# --------------------------------------------------------------------------- #


# Review subclass labels.
REVIEW_CATCH_ALL: str = "review_catch_all"
REVIEW_TIMEOUT: str = "review_timeout"
REVIEW_INCONSISTENT: str = "review_inconsistent"
REVIEW_LOW_CONFIDENCE: str = "review_low_confidence"
NOT_REVIEW: str = "not_review"


# Catch-all reason codes.
REASON_INSUFFICIENT: str = "insufficient_history"
REASON_NO_MX: str = "no_mx_presence"
REASON_HIGH_INVALID: str = "historical_invalid_rate_too_high"
REASON_LOW_REVIEW: str = "historical_review_rate_too_low"
REASON_LOW_CONFIDENCE: str = "low_confidence"
REASON_CATCH_ALL_LIKELY: str = "historical_ambiguous_routing"


# --------------------------------------------------------------------------- #
# Dataclasses                                                                 #
# --------------------------------------------------------------------------- #


@dataclass(slots=True, frozen=True)
class CatchAllThresholds:
    """Tunable thresholds for catch-all detection."""

    # Hard gates (early-exit before scoring).
    min_observations: int = 10
    min_mx_rate: float = 0.30
    max_invalid_rate: float = 0.40
    min_review_rate: float = 0.25

    # Ramp endpoints for the scoring signals.
    review_rate_saturates_at: float = 0.80
    mx_rate_ramp_start: float = 0.50
    mx_rate_saturates_at: float = 0.90

    # Weights (must sum to 1.0 for interpretability).
    weight_review_rate: float = 0.40
    weight_mx_rate: float = 0.25
    weight_low_invalid: float = 0.20
    weight_current_review_with_mx: float = 0.15

    # Overall confidence floor to fire as a catch-all.
    min_confidence_to_fire: float = 0.35

    # Used by review-subclass decision for "current row showed timeout"
    # fallback when the row itself carries a timeout error.
    review_timeout_rate_floor: float = 0.20


DEFAULT_CATCH_ALL_THRESHOLDS: CatchAllThresholds = CatchAllThresholds()


@dataclass(slots=True, frozen=True)
class CatchAllSignal:
    """Output of :func:`detect_catch_all_signals`."""

    is_possible_catch_all: bool
    confidence: float
    reason: str


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #


def _ramp(value: float, lo: float, hi: float) -> float:
    """Linear 0→1 ramp clamped to [0, 1]."""
    if hi <= lo:
        return 1.0 if value >= hi else 0.0
    return max(0.0, min(1.0, (value - lo) / (hi - lo)))


# --------------------------------------------------------------------------- #
# Public API                                                                  #
# --------------------------------------------------------------------------- #


def detect_catch_all_signals(
    record: DomainHistoryRecord | None,
    *,
    current_row_in_review: bool = False,
    current_row_has_mx: bool = False,
    thresholds: CatchAllThresholds = DEFAULT_CATCH_ALL_THRESHOLDS,
) -> CatchAllSignal:
    """Return a CatchAllSignal for the given domain.

    Early exits cover insufficient evidence, non-routing domains,
    evidently-bad domains, and domains whose history shows no review
    ambiguity. When none of the early exits trip, confidence is a
    weighted sum of four [0, 1] signals, capped at 1.0.
    """
    if record is None or record.total_seen_count < thresholds.min_observations:
        return CatchAllSignal(False, 0.0, REASON_INSUFFICIENT)

    if record.mx_rate < thresholds.min_mx_rate:
        return CatchAllSignal(False, 0.0, REASON_NO_MX)

    if record.invalid_rate > thresholds.max_invalid_rate:
        return CatchAllSignal(False, 0.0, REASON_HIGH_INVALID)

    if record.review_rate < thresholds.min_review_rate:
        return CatchAllSignal(False, 0.0, REASON_LOW_REVIEW)

    # Signals — each clamped to [0, 1].
    s_review = _ramp(
        record.review_rate,
        thresholds.min_review_rate,
        thresholds.review_rate_saturates_at,
    )
    s_mx = _ramp(
        record.mx_rate,
        thresholds.mx_rate_ramp_start,
        thresholds.mx_rate_saturates_at,
    )
    if thresholds.max_invalid_rate <= 0:
        s_low_invalid = 0.0
    else:
        s_low_invalid = max(
            0.0,
            min(1.0, 1.0 - record.invalid_rate / thresholds.max_invalid_rate),
        )
    s_current = 1.0 if (current_row_in_review and current_row_has_mx) else 0.0

    confidence = (
        thresholds.weight_review_rate * s_review
        + thresholds.weight_mx_rate * s_mx
        + thresholds.weight_low_invalid * s_low_invalid
        + thresholds.weight_current_review_with_mx * s_current
    )
    confidence = round(max(0.0, min(1.0, confidence)), 3)

    if confidence < thresholds.min_confidence_to_fire:
        return CatchAllSignal(False, confidence, REASON_LOW_CONFIDENCE)

    return CatchAllSignal(True, confidence, REASON_CATCH_ALL_LIKELY)


def classify_review_subclass(
    *,
    final_bucket: str,
    catch_all: CatchAllSignal,
    record: DomainHistoryRecord | None,
    current_row_had_timeout: bool,
    label_thresholds: LabelThresholds = DEFAULT_THRESHOLDS,
    catch_all_thresholds: CatchAllThresholds = DEFAULT_CATCH_ALL_THRESHOLDS,
) -> str:
    """Refine the V1 "review" bucket into a more specific subclass.

    Non-review buckets return ``NOT_REVIEW``. The first matching rule
    wins — order matters:
      1. Catch-all fired → ``review_catch_all``.
      2. Current row or domain history shows elevated timeout → ``review_timeout``.
      3. Domain history is classified NEUTRAL (mixed signals) → ``review_inconsistent``.
      4. Anything else → ``review_low_confidence``.
    """
    if final_bucket != "review":
        return NOT_REVIEW

    if catch_all.is_possible_catch_all:
        return REVIEW_CATCH_ALL

    if current_row_had_timeout:
        return REVIEW_TIMEOUT

    if (
        record is not None
        and record.total_seen_count >= catch_all_thresholds.min_observations
    ):
        if record.timeout_rate > catch_all_thresholds.review_timeout_rate_floor:
            return REVIEW_TIMEOUT
        label = classify_domain(record, label_thresholds)
        if label == HistoricalLabel.NEUTRAL:
            return REVIEW_INCONSISTENT

    return REVIEW_LOW_CONFIDENCE


__all__ = [
    "CatchAllSignal",
    "CatchAllThresholds",
    "DEFAULT_CATCH_ALL_THRESHOLDS",
    "NOT_REVIEW",
    "REVIEW_CATCH_ALL",
    "REVIEW_INCONSISTENT",
    "REVIEW_LOW_CONFIDENCE",
    "REVIEW_TIMEOUT",
    "REASON_CATCH_ALL_LIKELY",
    "REASON_HIGH_INVALID",
    "REASON_INSUFFICIENT",
    "REASON_LOW_CONFIDENCE",
    "REASON_LOW_REVIEW",
    "REASON_NO_MX",
    "classify_review_subclass",
    "detect_catch_all_signals",
]
