"""Unit tests for app.validation_v2.probability.row_model.

Verifies:
  * Hard guards (hard_fail, duplicate, no MX) short-circuit to 0.
  * Multipliers compose deterministically; order doesn't matter.
  * Result is always clamped to [0, 1].
  * Label thresholds produce the expected high/medium/low boundaries.
  * Each signal independently moves probability in the expected direction.
  * ``inputs_from_row`` gracefully handles missing columns.
"""

from __future__ import annotations

import pytest

from app.validation_v2.probability.row_model import (
    DEFAULT_PROBABILITY_THRESHOLDS,
    DeliverabilityInputs,
    ProbabilityThresholds,
    compute_deliverability_probability,
    inputs_from_row,
)


# ─────────────────────────────────────────────────────────────────────── #
# Builders                                                                #
# ─────────────────────────────────────────────────────────────────────── #


def _inputs(**kw: object) -> DeliverabilityInputs:
    defaults = {
        "score_post_history": 70,
        "historical_label": "neutral",
        "confidence_adjustment_applied": 0,
        "catch_all_confidence": 0.0,
        "smtp_result": "not_tested",
        "smtp_confidence": 0.0,
        "has_mx_record": True,
        "hard_fail": False,
        "v2_final_bucket": "review",
    }
    defaults.update(kw)
    return DeliverabilityInputs(**defaults)  # type: ignore[arg-type]


# ─────────────────────────────────────────────────────────────────────── #
# Hard guards                                                             #
# ─────────────────────────────────────────────────────────────────────── #


class TestHardGuards:
    """Guards short-circuit to probability=0 regardless of other signals."""

    def test_hard_fail_flag_overrides_everything(self) -> None:
        result = compute_deliverability_probability(
            _inputs(
                hard_fail=True, score_post_history=100,
                historical_label="historically_reliable",
                smtp_result="deliverable",
            )
        )
        assert result.probability == 0.0
        assert result.label == "low"
        assert result.override_reason == "hard_fail"

    def test_hard_fail_bucket_overrides_everything(self) -> None:
        result = compute_deliverability_probability(
            _inputs(v2_final_bucket="hard_fail", score_post_history=100)
        )
        assert result.probability == 0.0
        assert result.override_reason == "hard_fail"

    def test_duplicate_bucket_overrides(self) -> None:
        result = compute_deliverability_probability(
            _inputs(v2_final_bucket="duplicate", score_post_history=100)
        )
        assert result.probability == 0.0
        assert result.override_reason == "duplicate"

    def test_no_mx_record_overrides(self) -> None:
        result = compute_deliverability_probability(
            _inputs(has_mx_record=False, score_post_history=100)
        )
        assert result.probability == 0.0
        assert result.override_reason == "no_mx_record"

    def test_override_result_has_no_applied_factors(self) -> None:
        # Override factor is recorded, but the normal signal chain is skipped.
        result = compute_deliverability_probability(
            _inputs(hard_fail=True, smtp_result="deliverable",
                    historical_label="historically_reliable")
        )
        # Only the override factor is listed; SMTP and history factors absent.
        assert all(not f.name.startswith("smtp:") for f in result.factors)
        assert all(not f.name.startswith("history:") for f in result.factors)


# ─────────────────────────────────────────────────────────────────────── #
# Base + single-factor effects                                            #
# ─────────────────────────────────────────────────────────────────────── #


class TestBaseProbability:
    def test_base_is_score_over_100(self) -> None:
        assert compute_deliverability_probability(
            _inputs(score_post_history=60)
        ).base_probability == pytest.approx(0.60)

    def test_base_clamped_to_one_when_score_over_100(self) -> None:
        assert compute_deliverability_probability(
            _inputs(score_post_history=150)
        ).base_probability == 1.0

    def test_base_zero_when_score_is_zero(self) -> None:
        result = compute_deliverability_probability(_inputs(score_post_history=0))
        assert result.base_probability == 0.0
        assert result.probability == 0.0
        assert result.label == "low"


class TestSingleSignalMovements:
    """Each individual signal pushes probability in the expected direction."""

    def test_deliverable_smtp_raises_probability(self) -> None:
        neutral = compute_deliverability_probability(_inputs(score_post_history=60))
        boosted = compute_deliverability_probability(
            _inputs(score_post_history=60, smtp_result="deliverable")
        )
        assert boosted.probability > neutral.probability

    def test_undeliverable_smtp_crushes_probability(self) -> None:
        result = compute_deliverability_probability(
            _inputs(score_post_history=95, smtp_result="undeliverable")
        )
        # 0.95 * 0.20 = 0.19 → low band.
        assert result.probability < 0.20
        assert result.label == "low"

    def test_catch_all_smtp_moves_probability_down(self) -> None:
        result = compute_deliverability_probability(
            _inputs(score_post_history=80, smtp_result="catch_all")
        )
        # 0.80 * 0.60 = 0.48 → medium band.
        assert result.label == "medium"

    def test_inconclusive_smtp_only_slightly_reduces(self) -> None:
        result = compute_deliverability_probability(
            _inputs(score_post_history=80, smtp_result="inconclusive")
        )
        # 0.80 * 0.95 = 0.76, still high.
        assert result.label == "high"

    def test_reliable_history_raises_probability(self) -> None:
        result = compute_deliverability_probability(
            _inputs(score_post_history=60, historical_label="historically_reliable")
        )
        # 0.60 * 1.10 = 0.66
        assert 0.65 <= result.probability <= 0.67

    def test_risky_history_lowers_probability(self) -> None:
        result = compute_deliverability_probability(
            _inputs(score_post_history=75, historical_label="historically_risky")
        )
        # 0.75 * 0.60 = 0.45 → medium
        assert result.label == "medium"

    def test_strong_catch_all_halves_probability(self) -> None:
        result = compute_deliverability_probability(
            _inputs(score_post_history=80, catch_all_confidence=0.85)
        )
        # 0.80 * 0.50 = 0.40
        assert result.probability == pytest.approx(0.40)
        assert any(f.name == "catch_all:strong" for f in result.factors)

    def test_moderate_catch_all_applies_moderate_multiplier(self) -> None:
        result = compute_deliverability_probability(
            _inputs(score_post_history=80, catch_all_confidence=0.45)
        )
        # 0.80 * 0.75 = 0.60
        assert result.probability == pytest.approx(0.60)
        assert any(f.name == "catch_all:moderate" for f in result.factors)

    def test_catch_all_below_moderate_threshold_applies_no_multiplier(self) -> None:
        result = compute_deliverability_probability(
            _inputs(score_post_history=80, catch_all_confidence=0.10)
        )
        assert result.probability == pytest.approx(0.80)
        assert not any(f.name.startswith("catch_all:") for f in result.factors)


# ─────────────────────────────────────────────────────────────────────── #
# Composition / edge cases                                                #
# ─────────────────────────────────────────────────────────────────────── #


class TestComposition:
    def test_result_always_in_unit_interval(self) -> None:
        # Extreme-positive scenario that would overflow without clamping.
        result = compute_deliverability_probability(
            _inputs(
                score_post_history=100,
                historical_label="historically_reliable",
                smtp_result="deliverable",
            )
        )
        assert 0.0 <= result.probability <= 1.0

    def test_extreme_negative_reaches_zero_or_near(self) -> None:
        result = compute_deliverability_probability(
            _inputs(
                score_post_history=10,
                historical_label="historically_risky",
                smtp_result="undeliverable",
                catch_all_confidence=0.90,
            )
        )
        # 0.10 * 0.60 * 0.20 * 0.50 = 0.006
        assert result.probability < 0.05
        assert result.label == "low"

    def test_factors_recorded_in_order(self) -> None:
        result = compute_deliverability_probability(
            _inputs(
                score_post_history=70, smtp_result="deliverable",
                historical_label="historically_reliable",
                catch_all_confidence=0.75,
            )
        )
        names = [f.name for f in result.factors]
        assert names == [
            "smtp:deliverable",
            "history:historically_reliable",
            "catch_all:strong",
        ]

    def test_product_matches_analytical_expectation(self) -> None:
        """Model is a plain product — unit-test the arithmetic directly."""
        result = compute_deliverability_probability(
            _inputs(
                score_post_history=50,
                smtp_result="catch_all",               # ×0.60
                historical_label="historically_unstable",  # ×0.70
                catch_all_confidence=0.0,
            )
        )
        # 0.50 * 0.60 * 0.70 = 0.21
        assert result.probability == pytest.approx(0.21, abs=1e-9)


# ─────────────────────────────────────────────────────────────────────── #
# Label thresholds                                                        #
# ─────────────────────────────────────────────────────────────────────── #


class TestLabelThresholds:
    @pytest.mark.parametrize(
        "score, expected",
        [
            (0, "low"),
            (39, "low"),
            (40, "medium"),
            (69, "medium"),
            (70, "high"),
            (100, "high"),
        ],
    )
    def test_label_boundaries_with_default_thresholds(
        self, score: int, expected: str,
    ) -> None:
        result = compute_deliverability_probability(
            _inputs(score_post_history=score)
        )
        assert result.label == expected

    def test_custom_thresholds_change_label_boundaries(self) -> None:
        strict = ProbabilityThresholds(high_threshold=0.90, medium_threshold=0.60)
        # 0.70 would be "high" by default but only "medium" with stricter thresholds.
        result = compute_deliverability_probability(
            _inputs(score_post_history=70), thresholds=strict,
        )
        assert result.label == "medium"


# ─────────────────────────────────────────────────────────────────────── #
# inputs_from_row                                                         #
# ─────────────────────────────────────────────────────────────────────── #


class TestInputsFromRow:
    def test_all_columns_present_is_parsed_cleanly(self) -> None:
        row = {
            "score_post_history": "85",
            "historical_label": "historically_reliable",
            "confidence_adjustment_applied": "2",
            "catch_all_confidence": "0.15",
            "smtp_result": "deliverable",
            "smtp_confidence": "0.9",
            "has_mx_record": "True",
            "hard_fail": "False",
            "v2_final_bucket": "ready",
        }
        inputs = inputs_from_row(row)
        assert inputs.score_post_history == 85
        assert inputs.historical_label == "historically_reliable"
        assert inputs.catch_all_confidence == pytest.approx(0.15)
        assert inputs.smtp_result == "deliverable"
        assert inputs.has_mx_record is True

    def test_missing_score_post_history_falls_back_to_score(self) -> None:
        row = {"score": "55", "has_mx_record": "True"}
        assert inputs_from_row(row).score_post_history == 55

    def test_missing_smtp_result_defaults_to_not_tested(self) -> None:
        row = {"has_mx_record": "True"}
        assert inputs_from_row(row).smtp_result == "not_tested"

    def test_missing_historical_label_defaults_to_neutral(self) -> None:
        row = {"has_mx_record": "True"}
        assert inputs_from_row(row).historical_label == "neutral"

    def test_missing_v2_final_bucket_defaults_to_unknown(self) -> None:
        row = {"has_mx_record": "True"}
        assert inputs_from_row(row).v2_final_bucket == "unknown"

    def test_malformed_floats_fall_back_to_zero(self) -> None:
        row = {"catch_all_confidence": "not-a-number", "has_mx_record": "True"}
        assert inputs_from_row(row).catch_all_confidence == 0.0


# ─────────────────────────────────────────────────────────────────────── #
# Consistency                                                             #
# ─────────────────────────────────────────────────────────────────────── #


class TestConsistency:
    def test_same_inputs_produce_same_output(self) -> None:
        inputs = _inputs(
            score_post_history=72, smtp_result="catch_all",
            historical_label="historically_risky", catch_all_confidence=0.5,
        )
        r1 = compute_deliverability_probability(inputs)
        r2 = compute_deliverability_probability(inputs)
        assert r1 == r2

    def test_identity_signals_produce_base_probability(self) -> None:
        """All-neutral inputs → probability equals base."""
        inputs = _inputs(
            score_post_history=55,
            historical_label="neutral",
            smtp_result="not_tested",
            catch_all_confidence=0.0,
        )
        result = compute_deliverability_probability(inputs)
        assert result.probability == pytest.approx(result.base_probability)
        assert result.factors == ()


# ─────────────────────────────────────────────────────────────────────── #
# Defaults sanity                                                         #
# ─────────────────────────────────────────────────────────────────────── #


def test_default_thresholds_are_consistent() -> None:
    t = DEFAULT_PROBABILITY_THRESHOLDS
    assert 0.0 < t.medium_threshold < t.high_threshold < 1.0
    assert t.smtp_deliverable_multiplier > 1.0
    assert t.smtp_undeliverable_multiplier < 1.0
    assert t.smtp_catch_all_multiplier < 1.0
    assert t.historical_reliable_multiplier > 1.0
    assert t.historical_risky_multiplier < 1.0
    assert t.catch_all_strong_multiplier < t.catch_all_moderate_multiplier < 1.0
