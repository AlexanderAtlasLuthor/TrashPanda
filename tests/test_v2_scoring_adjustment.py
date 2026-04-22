"""Unit tests for app.validation_v2.scoring_adjustment.

Covers the full truth table of guardrails:

  apply × hard_fail × bucket × label × allow_flip → AdjustmentDecision

Each guardrail branch (hard_fail, duplicate, config_disabled,
insufficient_data, flips_disabled, cross_tier, safe_flip) has at least
one dedicated test.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from app.validation_v2.history_models import (
    DomainHistoryRecord,
    HistoricalLabel,
)
from app.validation_v2.scoring_adjustment import (
    DUPLICATE,
    HARD_FAIL,
    INVALID,
    READY,
    REVIEW,
    AdjustmentConfig,
    AdjustmentDecision,
    AdjustmentStats,
    bucket_from_output_reason,
    bucket_from_score,
    compute_row_adjustment,
)


# ─────────────────────────────────────────────────────────────────────── #
# Builders                                                                #
# ─────────────────────────────────────────────────────────────────────── #


def _rec(**counts: int) -> DomainHistoryRecord:
    when = datetime(2026, 4, 22)
    defaults = dict(
        mx_present_count=0, a_fallback_count=0, dns_failure_count=0,
        timeout_count=0, typo_corrected_count=0, review_count=0,
        invalid_count=0, ready_count=0, hard_fail_count=0,
    )
    total = counts.pop("total_seen_count", 100)
    defaults.update(counts)
    return DomainHistoryRecord(
        domain="test.com", first_seen_at=when, last_seen_at=when,
        total_seen_count=total, **defaults,
    )


def _reliable(total: int = 100) -> DomainHistoryRecord:
    return _rec(
        total_seen_count=total, mx_present_count=total,
        ready_count=int(total * 0.85), review_count=int(total * 0.10),
        invalid_count=int(total * 0.05),
    )


def _risky(total: int = 100) -> DomainHistoryRecord:
    return _rec(
        total_seen_count=total, mx_present_count=total,
        invalid_count=int(total * 0.80),
    )


def _unstable(total: int = 100) -> DomainHistoryRecord:
    return _rec(total_seen_count=total, timeout_count=int(total * 0.7))


_CFG_OFF = AdjustmentConfig(apply=False)
_CFG_ON_NO_FLIPS = AdjustmentConfig(
    apply=True,
    max_positive_adjustment=3,
    max_negative_adjustment=5,
    min_observations_for_adjustment=5,
    allow_bucket_flip_from_history=False,
    high_confidence_threshold=70,
    review_threshold=40,
)
_CFG_ON_WITH_FLIPS = AdjustmentConfig(
    apply=True,
    max_positive_adjustment=3,
    max_negative_adjustment=5,
    min_observations_for_adjustment=5,
    allow_bucket_flip_from_history=True,
    high_confidence_threshold=70,
    review_threshold=40,
)


# ─────────────────────────────────────────────────────────────────────── #
# Pure helpers                                                            #
# ─────────────────────────────────────────────────────────────────────── #


@pytest.mark.parametrize(
    "reason,expected",
    [
        ("kept_high_confidence", READY),
        ("kept_review", REVIEW),
        ("removed_low_score", INVALID),
        ("removed_hard_fail", HARD_FAIL),
        ("removed_duplicate", DUPLICATE),
        ("", "unknown"),
        (None, "unknown"),
        ("nonsense", "unknown"),
    ],
)
def test_bucket_from_output_reason_maps_all_known_values(reason, expected) -> None:
    assert bucket_from_output_reason(reason) == expected


@pytest.mark.parametrize(
    "score,expected",
    [(0, INVALID), (39, INVALID), (40, REVIEW), (69, REVIEW), (70, READY), (100, READY)],
)
def test_bucket_from_score_uses_inclusive_lower_bounds(score, expected) -> None:
    assert bucket_from_score(score, 70, 40) == expected


# ─────────────────────────────────────────────────────────────────────── #
# Guardrails: no adjustment                                               #
# ─────────────────────────────────────────────────────────────────────── #


class TestGuardrailsPreventAdjustment:
    """When a guardrail trips, score_post == score_pre and adjustment == 0."""

    def test_hard_fail_flag_blocks_adjustment_even_for_reliable_domain(self) -> None:
        decision = compute_row_adjustment(
            score=20, original_bucket=READY, hard_fail=True,
            record=_reliable(), config=_CFG_ON_WITH_FLIPS,
        )
        assert decision.score_post_history == decision.score_pre_history == 20
        assert decision.confidence_adjustment_applied == 0
        assert decision.flip_blocked_reason == "hard_fail"
        assert decision.final_bucket == READY

    def test_hard_fail_bucket_blocks_adjustment(self) -> None:
        decision = compute_row_adjustment(
            score=10, original_bucket=HARD_FAIL, hard_fail=False,
            record=_reliable(), config=_CFG_ON_WITH_FLIPS,
        )
        assert decision.confidence_adjustment_applied == 0
        assert decision.flip_blocked_reason == "hard_fail"

    def test_duplicate_bucket_blocks_adjustment(self) -> None:
        decision = compute_row_adjustment(
            score=80, original_bucket=DUPLICATE, hard_fail=False,
            record=_reliable(), config=_CFG_ON_WITH_FLIPS,
        )
        assert decision.score_post_history == decision.score_pre_history == 80
        assert decision.confidence_adjustment_applied == 0
        assert decision.flip_blocked_reason == "duplicate"
        assert decision.final_bucket == DUPLICATE

    def test_config_off_produces_no_change(self) -> None:
        decision = compute_row_adjustment(
            score=55, original_bucket=REVIEW, hard_fail=False,
            record=_reliable(), config=_CFG_OFF,
        )
        assert decision.score_post_history == 55
        assert decision.confidence_adjustment_applied == 0
        assert decision.flip_blocked_reason == "config_disabled"
        assert decision.final_bucket == REVIEW

    def test_insufficient_data_blocks_adjustment(self) -> None:
        # total_seen_count (4) < min_observations_for_adjustment (5)
        sparse = _rec(total_seen_count=4, mx_present_count=4, ready_count=4)
        decision = compute_row_adjustment(
            score=50, original_bucket=REVIEW, hard_fail=False,
            record=sparse, config=_CFG_ON_WITH_FLIPS,
        )
        assert decision.confidence_adjustment_applied == 0
        assert decision.flip_blocked_reason == "insufficient_data"
        assert decision.historical_label == HistoricalLabel.INSUFFICIENT_DATA

    def test_no_record_treated_as_insufficient_data(self) -> None:
        decision = compute_row_adjustment(
            score=55, original_bucket=REVIEW, hard_fail=False,
            record=None, config=_CFG_ON_WITH_FLIPS,
        )
        assert decision.historical_label == HistoricalLabel.INSUFFICIENT_DATA
        assert decision.confidence_adjustment_applied == 0
        assert decision.flip_blocked_reason == "insufficient_data"


# ─────────────────────────────────────────────────────────────────────── #
# Adjustment magnitudes                                                   #
# ─────────────────────────────────────────────────────────────────────── #


class TestAdjustmentMagnitudes:
    """Adjustment must be bounded AND signed correctly per label."""

    def test_reliable_domain_produces_small_positive_adjustment(self) -> None:
        decision = compute_row_adjustment(
            score=60, original_bucket=REVIEW, hard_fail=False,
            record=_reliable(), config=_CFG_ON_NO_FLIPS,
        )
        assert 0 < decision.confidence_adjustment_applied <= 3
        assert decision.score_post_history > decision.score_pre_history

    def test_risky_domain_produces_negative_adjustment(self) -> None:
        decision = compute_row_adjustment(
            score=72, original_bucket=READY, hard_fail=False,
            record=_risky(), config=_CFG_ON_NO_FLIPS,
        )
        assert decision.confidence_adjustment_applied < 0
        assert decision.confidence_adjustment_applied >= -5

    def test_unstable_domain_produces_negative_adjustment(self) -> None:
        decision = compute_row_adjustment(
            score=55, original_bucket=REVIEW, hard_fail=False,
            record=_unstable(), config=_CFG_ON_NO_FLIPS,
        )
        assert decision.confidence_adjustment_applied < 0

    def test_adjustment_clamped_to_zero_to_hundred(self) -> None:
        decision = compute_row_adjustment(
            score=99, original_bucket=READY, hard_fail=False,
            record=_reliable(), config=_CFG_ON_NO_FLIPS,
        )
        assert 0 <= decision.score_post_history <= 100

    def test_score_never_goes_negative(self) -> None:
        decision = compute_row_adjustment(
            score=2, original_bucket=INVALID, hard_fail=False,
            record=_risky(), config=_CFG_ON_NO_FLIPS,
        )
        assert decision.score_post_history >= 0


# ─────────────────────────────────────────────────────────────────────── #
# Bucket flip logic                                                       #
# ─────────────────────────────────────────────────────────────────────── #


class TestBucketFlipLogic:
    """Flips only happen review↔ready, only when allow_bucket_flip=True."""

    def test_flips_disabled_records_score_but_keeps_bucket(self) -> None:
        decision = compute_row_adjustment(
            score=68, original_bucket=REVIEW, hard_fail=False,
            record=_reliable(), config=_CFG_ON_NO_FLIPS,
        )
        assert decision.final_bucket == REVIEW
        assert decision.score_post_history > decision.score_pre_history
        assert decision.flip_blocked_reason == "flips_disabled"
        assert not decision.historical_bucket_flipped

    def test_review_to_ready_flip_happens_when_score_crosses_threshold(self) -> None:
        # 68 + reliable bonus (+3) → 71, crosses high threshold (70).
        decision = compute_row_adjustment(
            score=68, original_bucket=REVIEW, hard_fail=False,
            record=_reliable(), config=_CFG_ON_WITH_FLIPS,
        )
        assert decision.historical_bucket_flipped is True
        assert decision.final_bucket == READY
        assert decision.flip_blocked_reason == ""

    def test_ready_to_review_flip_happens_for_risky_history(self) -> None:
        # 72 + risky penalty (−5) → 67, drops below high threshold (70).
        decision = compute_row_adjustment(
            score=72, original_bucket=READY, hard_fail=False,
            record=_risky(), config=_CFG_ON_WITH_FLIPS,
        )
        assert decision.historical_bucket_flipped is True
        assert decision.final_bucket == REVIEW

    def test_cross_tier_invalid_to_ready_is_blocked_even_with_flips_enabled(self) -> None:
        # Hypothetical: score=38 invalid + reliable (+3) → 41, would reach review.
        # But tier jumps (invalid→review or invalid→ready) are blocked.
        decision = compute_row_adjustment(
            score=38, original_bucket=INVALID, hard_fail=False,
            record=_reliable(), config=_CFG_ON_WITH_FLIPS,
        )
        # Adjustment is recorded but bucket stays.
        assert decision.final_bucket == INVALID
        assert decision.historical_bucket_flipped is False
        assert decision.flip_blocked_reason == "cross_tier"

    def test_cross_tier_ready_to_invalid_blocked(self) -> None:
        # Forcibly construct a near-boundary ready→invalid scenario.
        # Actually the bucket rules cap negative bucket moves per-tier:
        # with max_negative=5, ready score=72 can't reach invalid (score<40).
        # So simulate with a very low starting score that *could* be invalid
        # after adjustment if tier rules didn't block it.
        # We'll patch by using score=41 and a massive-penalty scenario.
        decision = compute_row_adjustment(
            score=41, original_bucket=READY, hard_fail=False,
            record=_risky(), config=_CFG_ON_WITH_FLIPS,
        )
        # ready is only valid above 70, but the original_bucket was READY
        # (per the CSV the row sits in). Adjustment of -5 gives score 36,
        # which would land in invalid by thresholds — but tier is blocked.
        assert decision.final_bucket in (READY, REVIEW)
        if decision.final_bucket == READY:
            assert decision.flip_blocked_reason == "cross_tier"
        else:
            # review was reached, which is the safe one-step flip.
            assert decision.flip_blocked_reason == ""

    def test_same_bucket_after_flip_calc_is_not_a_flip(self) -> None:
        # Score 90 + reliable (+3) → 93, still ready.
        decision = compute_row_adjustment(
            score=90, original_bucket=READY, hard_fail=False,
            record=_reliable(), config=_CFG_ON_WITH_FLIPS,
        )
        assert decision.final_bucket == READY
        assert decision.historical_bucket_flipped is False
        assert decision.flip_blocked_reason == ""


# ─────────────────────────────────────────────────────────────────────── #
# The "no rescue" rule                                                    #
# ─────────────────────────────────────────────────────────────────────── #


class TestReliableCannotRescueHardFails:
    """The crown-jewel guardrail: a reliable domain cannot un-hard-fail a row."""

    def test_hard_fail_with_reliable_history_stays_hard_fail(self) -> None:
        decision = compute_row_adjustment(
            score=10, original_bucket=HARD_FAIL, hard_fail=True,
            record=_reliable(total=500), config=_CFG_ON_WITH_FLIPS,
        )
        assert decision.final_bucket == HARD_FAIL
        assert decision.score_post_history == 10
        assert decision.confidence_adjustment_applied == 0
        assert decision.flip_blocked_reason == "hard_fail"

    def test_invalid_with_reliable_history_stays_invalid(self) -> None:
        decision = compute_row_adjustment(
            score=30, original_bucket=INVALID, hard_fail=False,
            record=_reliable(total=500), config=_CFG_ON_WITH_FLIPS,
        )
        assert decision.final_bucket == INVALID
        # A cross-tier rescue is explicitly blocked.
        assert decision.flip_blocked_reason in ("cross_tier", "")


# ─────────────────────────────────────────────────────────────────────── #
# AdjustmentStats                                                         #
# ─────────────────────────────────────────────────────────────────────── #


class TestAdjustmentStats:
    def _decision(
        self,
        *,
        adjustment: int = 0,
        label: str = HistoricalLabel.NEUTRAL,
        original: str = REVIEW,
        final: str = REVIEW,
        blocked: str = "",
    ) -> AdjustmentDecision:
        return AdjustmentDecision(
            score_pre_history=50, score_post_history=50 + adjustment,
            confidence_adjustment_applied=adjustment,
            historical_label=label, historical_total_seen_count=100,
            historical_ready_rate=0.0, historical_invalid_rate=0.0,
            historical_timeout_rate=0.0, original_bucket=original,
            final_bucket=final,
            historical_bucket_flipped=(original != final),
            flip_blocked_reason=blocked,
        )

    def test_counters_classify_adjustment_sign_correctly(self) -> None:
        stats = AdjustmentStats()
        stats.record(self._decision(adjustment=3))
        stats.record(self._decision(adjustment=-4))
        stats.record(self._decision(adjustment=0))
        assert stats.rows_with_positive_adjustment == 1
        assert stats.rows_with_negative_adjustment == 1
        assert stats.rows_with_zero_adjustment == 1
        assert stats.total_rows_scanned == 3

    def test_counters_track_flips_by_direction(self) -> None:
        stats = AdjustmentStats()
        stats.record(self._decision(original=REVIEW, final=READY))
        stats.record(self._decision(original=REVIEW, final=READY))
        stats.record(self._decision(original=READY, final=REVIEW))
        assert stats.bucket_flips_review_to_ready == 2
        assert stats.bucket_flips_ready_to_review == 1

    def test_counters_track_flip_block_reasons(self) -> None:
        stats = AdjustmentStats()
        stats.record(self._decision(blocked="hard_fail"))
        stats.record(self._decision(blocked="duplicate"))
        stats.record(self._decision(blocked="cross_tier"))
        stats.record(self._decision(blocked="config_disabled"))
        stats.record(self._decision(blocked="flips_disabled"))
        stats.record(self._decision(blocked="insufficient_data"))
        assert stats.flips_blocked_hard_fail == 1
        assert stats.flips_blocked_duplicate == 1
        assert stats.flips_blocked_cross_tier == 1
        assert stats.flips_blocked_config == 2  # config_disabled + flips_disabled
        assert stats.flips_blocked_insufficient_data == 1
