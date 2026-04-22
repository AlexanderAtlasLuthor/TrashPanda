"""Business rules over :class:`DomainHistoryRecord`.

All deterministic, threshold-driven logic that turns a raw history
record into human-meaningful outputs lives here:

* :func:`classify_domain`     — picks a :class:`HistoricalLabel`.
* :func:`compute_adjustment`  — returns a small integer confidence
                                 adjustment, bounded by config.
* :class:`LabelThresholds`    — tunable thresholds; overridable from
                                 tests and future calibration.

Rules are intentionally simple and easy to audit. No ML, no external
services, no reliance on pipeline internals.
"""

from __future__ import annotations

from dataclasses import dataclass

from .history_models import DomainHistoryRecord, HistoricalLabel


# --------------------------------------------------------------------------- #
# Thresholds                                                                  #
# --------------------------------------------------------------------------- #


@dataclass(slots=True, frozen=True)
class LabelThresholds:
    """Thresholds applied in :func:`classify_domain`.

    Defaults are deliberately conservative. Override in tests or future
    config if you want different bands.
    """

    min_observations: int = 5

    # A domain is "unstable" when network/DNS signals are frequently bad.
    unstable_timeout_rate: float = 0.30
    unstable_dns_failure_rate: float = 0.30

    # A domain is "risky" when its historical decisions are mostly negative.
    risky_invalid_rate: float = 0.50
    risky_hard_fail_rate: float = 0.30

    # A domain is "reliable" when MX is present, ready-rate is high, and
    # historical negatives are low.
    reliable_mx_rate: float = 0.80
    reliable_ready_rate: float = 0.70
    reliable_invalid_rate_max: float = 0.20


DEFAULT_THRESHOLDS: LabelThresholds = LabelThresholds()


# --------------------------------------------------------------------------- #
# Classification                                                              #
# --------------------------------------------------------------------------- #


def classify_domain(
    record: DomainHistoryRecord | None,
    thresholds: LabelThresholds = DEFAULT_THRESHOLDS,
) -> str:
    """Return a :class:`HistoricalLabel` string for this record.

    Evaluation order (first match wins):
      1. ``INSUFFICIENT_DATA`` — too few observations to say anything.
      2. ``UNSTABLE``          — network/DNS is unreliable.
      3. ``RISKY``             — past runs mostly rejected this domain.
      4. ``RELIABLE``          — strong MX + high ready-rate + low invalid.
      5. ``NEUTRAL``           — none of the above.

    Unstable is checked before risky because "we can't tell" should
    beat "it's been bad" when the underlying signal is networking noise.
    """
    if record is None or record.total_seen_count < thresholds.min_observations:
        return HistoricalLabel.INSUFFICIENT_DATA

    if (
        record.timeout_rate >= thresholds.unstable_timeout_rate
        or record.dns_failure_rate >= thresholds.unstable_dns_failure_rate
    ):
        return HistoricalLabel.UNSTABLE

    if (
        record.invalid_rate >= thresholds.risky_invalid_rate
        or record.hard_fail_rate >= thresholds.risky_hard_fail_rate
    ):
        return HistoricalLabel.RISKY

    if (
        record.mx_rate >= thresholds.reliable_mx_rate
        and record.ready_rate >= thresholds.reliable_ready_rate
        and record.invalid_rate <= thresholds.reliable_invalid_rate_max
    ):
        return HistoricalLabel.RELIABLE

    return HistoricalLabel.NEUTRAL


# --------------------------------------------------------------------------- #
# Confidence adjustment                                                       #
# --------------------------------------------------------------------------- #


def compute_adjustment(
    record: DomainHistoryRecord | None,
    *,
    max_positive: int,
    max_negative: int,
    thresholds: LabelThresholds = DEFAULT_THRESHOLDS,
) -> int:
    """Compute a small, bounded confidence adjustment.

    Positive = nudge toward ready-to-send.
    Negative = nudge toward review / invalid.

    The value is clamped to ``[-max_negative, +max_positive]``. Callers
    are expected to *record* this adjustment in reports/explanations; in
    Phase 1 we do not feed it back into scoring.

    Non-negative bounds only — ``max_negative`` is a magnitude.
    """
    if record is None or record.total_seen_count < thresholds.min_observations:
        return 0

    label = classify_domain(record, thresholds)

    if label == HistoricalLabel.RELIABLE:
        # Scale by readiness strength — but cap conservatively.
        strength = min(1.0, record.ready_rate)
        return max(0, min(max_positive, round(max_positive * strength)))

    if label == HistoricalLabel.UNSTABLE:
        # Unstable hurts less than outright risky.
        strength = max(record.timeout_rate, record.dns_failure_rate)
        return -max(0, min(max_negative, round((max_negative - 1) * strength)))

    if label == HistoricalLabel.RISKY:
        strength = max(record.invalid_rate, record.hard_fail_rate)
        return -max(0, min(max_negative, round(max_negative * strength)))

    return 0


__all__ = [
    "DEFAULT_THRESHOLDS",
    "LabelThresholds",
    "classify_domain",
    "compute_adjustment",
]
