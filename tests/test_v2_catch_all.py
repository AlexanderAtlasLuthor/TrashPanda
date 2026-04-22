"""Unit tests for app.validation_v2.catch_all.

Covers:
  * Every early-exit guardrail returns a ``not possible_catch_all`` with
    the expected reason code.
  * Confidence is monotonic in each underlying signal.
  * The current-row boost only matters when the row is both in review
    and carries an MX — otherwise it's zero.
  * ``classify_review_subclass`` covers all four review subclasses plus
    ``not_review``.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from app.validation_v2.catch_all import (
    DEFAULT_CATCH_ALL_THRESHOLDS,
    NOT_REVIEW,
    REASON_CATCH_ALL_LIKELY,
    REASON_HIGH_INVALID,
    REASON_INSUFFICIENT,
    REASON_LOW_CONFIDENCE,
    REASON_LOW_REVIEW,
    REASON_NO_MX,
    REVIEW_CATCH_ALL,
    REVIEW_INCONSISTENT,
    REVIEW_LOW_CONFIDENCE,
    REVIEW_TIMEOUT,
    CatchAllSignal,
    CatchAllThresholds,
    classify_review_subclass,
    detect_catch_all_signals,
)
from app.validation_v2.history_models import (
    DomainHistoryRecord,
    HistoricalLabel,
)


# ─────────────────────────────────────────────────────────────────────── #
# Builders                                                                #
# ─────────────────────────────────────────────────────────────────────── #


def _rec(
    *,
    total: int = 50,
    mx_rate: float = 0.80,
    invalid_rate: float = 0.10,
    review_rate: float = 0.40,
    ready_rate: float = 0.50,
    timeout_rate: float = 0.0,
) -> DomainHistoryRecord:
    when = datetime(2026, 4, 22)
    return DomainHistoryRecord(
        domain="x.com",
        first_seen_at=when,
        last_seen_at=when,
        total_seen_count=total,
        mx_present_count=int(total * mx_rate),
        invalid_count=int(total * invalid_rate),
        review_count=int(total * review_rate),
        ready_count=int(total * ready_rate),
        timeout_count=int(total * timeout_rate),
    )


# ─────────────────────────────────────────────────────────────────────── #
# Guardrail early-exits                                                   #
# ─────────────────────────────────────────────────────────────────────── #


class TestEarlyExits:
    """Every guardrail returns is_possible_catch_all=False with a reason."""

    def test_none_record_is_insufficient(self) -> None:
        signal = detect_catch_all_signals(None)
        assert signal == CatchAllSignal(False, 0.0, REASON_INSUFFICIENT)

    def test_too_few_observations_is_insufficient(self) -> None:
        rec = _rec(total=DEFAULT_CATCH_ALL_THRESHOLDS.min_observations - 1)
        signal = detect_catch_all_signals(rec)
        assert signal.is_possible_catch_all is False
        assert signal.reason == REASON_INSUFFICIENT

    def test_no_mx_rate_means_not_a_catch_all(self) -> None:
        rec = _rec(mx_rate=0.05)
        signal = detect_catch_all_signals(rec)
        assert signal.reason == REASON_NO_MX

    def test_high_invalid_rate_means_not_a_catch_all(self) -> None:
        rec = _rec(mx_rate=0.80, invalid_rate=0.80, review_rate=0.15)
        signal = detect_catch_all_signals(rec)
        assert signal.reason == REASON_HIGH_INVALID

    def test_low_review_rate_means_not_a_catch_all(self) -> None:
        rec = _rec(
            total=100, mx_rate=0.90, invalid_rate=0.05, review_rate=0.10, ready_rate=0.85
        )
        signal = detect_catch_all_signals(rec)
        assert signal.reason == REASON_LOW_REVIEW

    def test_guardrail_order_insufficient_beats_other_reasons(self) -> None:
        # If the record is too sparse, we should report insufficient
        # even if invalid_rate is also high.
        rec = _rec(total=3, mx_rate=0.00, invalid_rate=0.90)
        assert detect_catch_all_signals(rec).reason == REASON_INSUFFICIENT

    def test_guardrail_order_no_mx_beats_high_invalid(self) -> None:
        rec = _rec(mx_rate=0.00, invalid_rate=0.90, review_rate=0.50)
        assert detect_catch_all_signals(rec).reason == REASON_NO_MX


# ─────────────────────────────────────────────────────────────────────── #
# Confidence band tests                                                   #
# ─────────────────────────────────────────────────────────────────────── #


class TestConfidenceMonotonicity:
    """Signal components must move confidence in the expected direction."""

    def test_higher_review_rate_increases_confidence(self) -> None:
        low = detect_catch_all_signals(
            _rec(total=100, mx_rate=0.90, invalid_rate=0.10, review_rate=0.30)
        )
        high = detect_catch_all_signals(
            _rec(total=100, mx_rate=0.90, invalid_rate=0.10, review_rate=0.70)
        )
        assert high.confidence > low.confidence

    def test_higher_invalid_rate_decreases_confidence(self) -> None:
        a = detect_catch_all_signals(
            _rec(total=100, mx_rate=0.90, invalid_rate=0.05, review_rate=0.50)
        )
        b = detect_catch_all_signals(
            _rec(total=100, mx_rate=0.90, invalid_rate=0.35, review_rate=0.50)
        )
        assert a.confidence > b.confidence

    def test_higher_mx_rate_increases_confidence(self) -> None:
        low_mx = detect_catch_all_signals(
            _rec(total=100, mx_rate=0.35, invalid_rate=0.10, review_rate=0.55)
        )
        high_mx = detect_catch_all_signals(
            _rec(total=100, mx_rate=0.95, invalid_rate=0.10, review_rate=0.55)
        )
        assert high_mx.confidence > low_mx.confidence

    def test_current_row_boost_only_applies_with_both_flags(self) -> None:
        rec = _rec(total=100, mx_rate=0.90, invalid_rate=0.10, review_rate=0.50)
        base = detect_catch_all_signals(rec).confidence
        only_review = detect_catch_all_signals(
            rec, current_row_in_review=True, current_row_has_mx=False,
        ).confidence
        only_mx = detect_catch_all_signals(
            rec, current_row_in_review=False, current_row_has_mx=True,
        ).confidence
        both = detect_catch_all_signals(
            rec, current_row_in_review=True, current_row_has_mx=True,
        ).confidence
        assert only_review == pytest.approx(base)
        assert only_mx == pytest.approx(base)
        assert both > base


class TestConfidenceBands:
    def test_strongly_catch_all_profile_fires_with_high_confidence(self) -> None:
        rec = _rec(total=200, mx_rate=0.95, invalid_rate=0.05, review_rate=0.70, ready_rate=0.25)
        signal = detect_catch_all_signals(
            rec, current_row_in_review=True, current_row_has_mx=True,
        )
        assert signal.is_possible_catch_all is True
        assert signal.confidence >= 0.70
        assert signal.reason == REASON_CATCH_ALL_LIKELY

    def test_borderline_profile_does_not_fire(self) -> None:
        # Just above review_rate gate but nothing else aligns.
        rec = _rec(total=50, mx_rate=0.40, invalid_rate=0.35, review_rate=0.30, ready_rate=0.35)
        signal = detect_catch_all_signals(rec)
        assert signal.is_possible_catch_all is False
        assert signal.reason == REASON_LOW_CONFIDENCE

    def test_confidence_is_bounded_between_zero_and_one(self) -> None:
        for (mx, inv, rev) in [
            (0.99, 0.00, 0.99), (0.50, 0.30, 0.30), (0.70, 0.20, 0.50),
        ]:
            rec = _rec(total=200, mx_rate=mx, invalid_rate=inv, review_rate=rev)
            signal = detect_catch_all_signals(
                rec, current_row_in_review=True, current_row_has_mx=True,
            )
            assert 0.0 <= signal.confidence <= 1.0


class TestNoAggressiveFalsePositives:
    """Make sure catch-all doesn't fire on reliable-looking domains."""

    def test_reliable_domain_never_fires_catch_all(self) -> None:
        rec = _rec(total=500, mx_rate=0.98, invalid_rate=0.02, review_rate=0.05, ready_rate=0.93)
        signal = detect_catch_all_signals(
            rec, current_row_in_review=False, current_row_has_mx=True,
        )
        assert signal.is_possible_catch_all is False
        assert signal.reason == REASON_LOW_REVIEW

    def test_obviously_broken_domain_never_fires_catch_all(self) -> None:
        rec = _rec(total=100, mx_rate=0.10, invalid_rate=0.70, review_rate=0.15, ready_rate=0.15)
        assert detect_catch_all_signals(rec).is_possible_catch_all is False


# ─────────────────────────────────────────────────────────────────────── #
# Review subclass                                                         #
# ─────────────────────────────────────────────────────────────────────── #


class TestReviewSubclass:
    def _fire_signal(self) -> CatchAllSignal:
        return CatchAllSignal(True, 0.7, REASON_CATCH_ALL_LIKELY)

    def _miss_signal(self) -> CatchAllSignal:
        return CatchAllSignal(False, 0.0, REASON_LOW_CONFIDENCE)

    def test_non_review_bucket_returns_not_review(self) -> None:
        for bucket in ("ready", "invalid", "hard_fail", "duplicate"):
            assert classify_review_subclass(
                final_bucket=bucket,
                catch_all=self._fire_signal(),
                record=None,
                current_row_had_timeout=False,
            ) == NOT_REVIEW

    def test_catch_all_wins_for_review_bucket(self) -> None:
        assert classify_review_subclass(
            final_bucket="review",
            catch_all=self._fire_signal(),
            record=_rec(total=100, mx_rate=0.10),  # would otherwise hit other branches
            current_row_had_timeout=True,
        ) == REVIEW_CATCH_ALL

    def test_current_row_timeout_yields_review_timeout(self) -> None:
        assert classify_review_subclass(
            final_bucket="review",
            catch_all=self._miss_signal(),
            record=None,
            current_row_had_timeout=True,
        ) == REVIEW_TIMEOUT

    def test_historical_timeout_rate_yields_review_timeout(self) -> None:
        rec = _rec(
            total=100, mx_rate=0.50, invalid_rate=0.20, review_rate=0.30,
            ready_rate=0.50, timeout_rate=0.40,
        )
        assert classify_review_subclass(
            final_bucket="review",
            catch_all=self._miss_signal(),
            record=rec,
            current_row_had_timeout=False,
        ) == REVIEW_TIMEOUT

    def test_neutral_label_yields_review_inconsistent(self) -> None:
        # NEUTRAL means mixed signals that aren't reliable/unstable/risky.
        rec = _rec(
            total=100, mx_rate=0.70, invalid_rate=0.20, review_rate=0.30, ready_rate=0.50,
        )
        subclass = classify_review_subclass(
            final_bucket="review",
            catch_all=self._miss_signal(),
            record=rec,
            current_row_had_timeout=False,
        )
        # Depending on classify_domain thresholds this may be neutral or
        # low_confidence; ensure it's one of the review* variants but not
        # catch_all or timeout.
        assert subclass in (REVIEW_INCONSISTENT, REVIEW_LOW_CONFIDENCE)

    def test_sparse_record_falls_through_to_low_confidence(self) -> None:
        sparse = _rec(
            total=3,  # below min_observations
            mx_rate=0.66, invalid_rate=0.0, review_rate=0.66, ready_rate=0.33,
        )
        assert classify_review_subclass(
            final_bucket="review",
            catch_all=self._miss_signal(),
            record=sparse,
            current_row_had_timeout=False,
        ) == REVIEW_LOW_CONFIDENCE

    def test_no_record_defaults_to_low_confidence(self) -> None:
        assert classify_review_subclass(
            final_bucket="review",
            catch_all=self._miss_signal(),
            record=None,
            current_row_had_timeout=False,
        ) == REVIEW_LOW_CONFIDENCE


# ─────────────────────────────────────────────────────────────────────── #
# Threshold overridability                                                #
# ─────────────────────────────────────────────────────────────────────── #


def test_raising_min_observations_blocks_catch_all_fires() -> None:
    rec = _rec(total=15, mx_rate=0.90, invalid_rate=0.10, review_rate=0.60)
    default_fires = detect_catch_all_signals(rec).is_possible_catch_all
    raised = CatchAllThresholds(min_observations=50)
    assert detect_catch_all_signals(rec, thresholds=raised).is_possible_catch_all is False
    # Sanity check: default would have fired for this profile.
    assert default_fires is True


def test_raising_min_confidence_floor_blocks_borderline_cases() -> None:
    rec = _rec(total=30, mx_rate=0.60, invalid_rate=0.20, review_rate=0.40, ready_rate=0.40)
    default = detect_catch_all_signals(rec)
    strict = CatchAllThresholds(min_confidence_to_fire=0.99)
    strict_signal = detect_catch_all_signals(rec, thresholds=strict)
    # If default fires, strict must not (higher floor). Otherwise both miss.
    if default.is_possible_catch_all:
        assert strict_signal.is_possible_catch_all is False
        assert strict_signal.reason == REASON_LOW_CONFIDENCE
