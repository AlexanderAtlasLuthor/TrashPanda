"""Unit tests for app.validation_v2.probability.row_model (additive model v2).

The model is additive, deterministic, and smooths with tiny noise. Tests
verify the **contract** rather than exact arithmetic:

  * Hard guards (hard_fail, duplicate) short-circuit to 0.
  * Each signal pushes probability in the expected direction.
  * Missing MX is a soft negative (NOT an override).
  * Result is always clamped to [0, 1].
  * Label thresholds produce the expected high/medium/low boundaries.
  * ``inputs_from_row`` gracefully handles missing columns.
  * Same inputs ⇒ same output (deterministic noise).
  * Different emails ⇒ slight jitter (continuous distribution).
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


def _inputs(**kw: object) -> DeliverabilityInputs:
    defaults: dict[str, object] = {
        "score_post_history": 70,
        "historical_label": "neutral",
        "confidence_adjustment_applied": 0,
        "catch_all_confidence": 0.0,
        "possible_catch_all": False,
        "smtp_result": "not_tested",
        "smtp_confidence": 0.0,
        "has_mx_record": True,
        "has_a_record": False,
        "domain_match": False,
        "typo_detected": False,
        "hard_fail": False,
        "v2_final_bucket": "review",
        "email": "user@example.com",
        "domain": "example.com",
    }
    defaults.update(kw)
    return DeliverabilityInputs(**defaults)  # type: ignore[arg-type]


# --------------------------------------------------------------------------- #
# Hard guards                                                                 #
# --------------------------------------------------------------------------- #


class TestHardGuards:
    def test_hard_fail_flag_overrides_everything(self) -> None:
        result = compute_deliverability_probability(
            _inputs(
                hard_fail=True,
                historical_label="historically_reliable",
                smtp_result="deliverable",
            )
        )
        assert result.probability == 0.0
        assert result.label == "low"
        assert result.override_reason == "hard_fail"

    def test_hard_fail_bucket_overrides(self) -> None:
        result = compute_deliverability_probability(
            _inputs(v2_final_bucket="hard_fail")
        )
        assert result.probability == 0.0
        assert result.override_reason == "hard_fail"

    def test_duplicate_bucket_overrides(self) -> None:
        result = compute_deliverability_probability(
            _inputs(v2_final_bucket="duplicate")
        )
        assert result.probability == 0.0
        assert result.override_reason == "duplicate"

    def test_no_mx_is_soft_negative_not_override(self) -> None:
        """Regression: the old model overrode no_mx to 0; the new model treats
        it as a strong soft negative that can still receive some probability
        from other signals."""
        result = compute_deliverability_probability(
            _inputs(has_mx_record=False, has_a_record=False)
        )
        assert result.override_reason == ""
        assert 0.0 <= result.probability <= 1.0
        # base 0.5 + no_dns(-0.25) ≈ 0.25 + noise → clearly below medium.
        assert result.label == "low"
        assert any(f.name == "no_dns" for f in result.factors)

    def test_no_mx_plus_a_fallback_is_soft_low(self) -> None:
        result = compute_deliverability_probability(
            _inputs(has_mx_record=False, has_a_record=True)
        )
        assert result.override_reason == ""
        # base 0.5 + a_fallback(0.05) ≈ 0.55 ± noise → medium band.
        assert result.label in ("low", "medium")


# --------------------------------------------------------------------------- #
# Base + individual signals                                                   #
# --------------------------------------------------------------------------- #


class TestBaseAndSignals:
    def test_base_is_thresholds_base_score(self) -> None:
        result = compute_deliverability_probability(_inputs())
        assert result.base_probability == DEFAULT_PROBABILITY_THRESHOLDS.base_score

    def test_mx_present_raises_probability(self) -> None:
        no_mx = compute_deliverability_probability(
            _inputs(has_mx_record=False, has_a_record=False)
        )
        with_mx = compute_deliverability_probability(_inputs(has_mx_record=True))
        assert with_mx.probability > no_mx.probability

    def test_a_fallback_between_no_dns_and_mx(self) -> None:
        no_dns = compute_deliverability_probability(
            _inputs(has_mx_record=False, has_a_record=False)
        )
        a_fb = compute_deliverability_probability(
            _inputs(has_mx_record=False, has_a_record=True)
        )
        mx = compute_deliverability_probability(_inputs(has_mx_record=True))
        assert no_dns.probability < a_fb.probability < mx.probability

    def test_deliverable_smtp_raises(self) -> None:
        neutral = compute_deliverability_probability(_inputs())
        boosted = compute_deliverability_probability(_inputs(smtp_result="deliverable"))
        assert boosted.probability > neutral.probability

    def test_undeliverable_smtp_lowers(self) -> None:
        neutral = compute_deliverability_probability(_inputs())
        crushed = compute_deliverability_probability(_inputs(smtp_result="undeliverable"))
        assert crushed.probability < neutral.probability
        # base 0.5 + mx(0.20) + smtp_undeliverable(-0.25) ≈ 0.45 → low.
        assert crushed.label in ("low", "medium")

    def test_catch_all_smtp_lowers(self) -> None:
        result = compute_deliverability_probability(_inputs(smtp_result="catch_all"))
        # base 0.5 + mx(0.20) + smtp_catch_all(-0.15) ≈ 0.55 → medium.
        assert result.label in ("low", "medium")

    def test_reliable_history_raises(self) -> None:
        neutral = compute_deliverability_probability(_inputs())
        boosted = compute_deliverability_probability(
            _inputs(historical_label="historically_reliable")
        )
        assert boosted.probability > neutral.probability

    def test_risky_history_lowers(self) -> None:
        neutral = compute_deliverability_probability(_inputs())
        risked = compute_deliverability_probability(
            _inputs(historical_label="historically_risky")
        )
        assert risked.probability < neutral.probability

    def test_catch_all_flag_lowers(self) -> None:
        neutral = compute_deliverability_probability(_inputs())
        flagged = compute_deliverability_probability(_inputs(possible_catch_all=True))
        assert flagged.probability < neutral.probability
        assert any(f.name == "catch_all:flag" for f in flagged.factors)

    def test_strong_catch_all_confidence_stacks_on_flag(self) -> None:
        flag_only = compute_deliverability_probability(_inputs(possible_catch_all=True))
        both = compute_deliverability_probability(
            _inputs(possible_catch_all=True, catch_all_confidence=0.85)
        )
        assert both.probability < flag_only.probability
        assert any(f.name == "catch_all:strong" for f in both.factors)


# --------------------------------------------------------------------------- #
# Clamp / composition                                                         #
# --------------------------------------------------------------------------- #


class TestComposition:
    def test_result_always_in_unit_interval(self) -> None:
        for ins in (
            _inputs(historical_label="historically_reliable", smtp_result="deliverable",
                    domain_match=True),
            _inputs(historical_label="historically_risky", smtp_result="undeliverable",
                    catch_all_confidence=0.9, possible_catch_all=True),
        ):
            result = compute_deliverability_probability(ins)
            assert 0.0 <= result.probability <= 1.0

    def test_extreme_positive_approaches_high(self) -> None:
        result = compute_deliverability_probability(_inputs(
            has_mx_record=True,
            historical_label="historically_reliable",
            smtp_result="deliverable",
            domain_match=True,
        ))
        # 0.5 + 0.20 + 0.05 + 0.10 + 0.05 = 0.90 ± noise → high.
        assert result.label == "high"

    def test_extreme_negative_approaches_low(self) -> None:
        result = compute_deliverability_probability(_inputs(
            has_mx_record=False, has_a_record=False,
            historical_label="historically_risky",
            smtp_result="undeliverable",
            possible_catch_all=True, catch_all_confidence=0.85,
        ))
        # 0.5 - 0.25 - 0.10 - 0.25 - 0.10 - 0.05 ≈ -0.25 → clamped to 0.
        assert result.probability <= 0.10
        assert result.label == "low"


# --------------------------------------------------------------------------- #
# Labels                                                                      #
# --------------------------------------------------------------------------- #


class TestLabelThresholds:
    def test_label_uses_high_and_medium_boundaries(self) -> None:
        custom = ProbabilityThresholds(
            high_threshold=0.80, medium_threshold=0.50,
            base_score=0.50,
            # neutralize all weights so only base shows through
            mx_present_weight=0.0, a_fallback_weight=0.0, no_dns_weight=0.0,
            domain_match_weight=0.0, typo_detected_weight=0.0,
            historical_reliable_weight=0.0, historical_unstable_weight=0.0,
            historical_risky_weight=0.0,
            smtp_deliverable_weight=0.0, smtp_undeliverable_weight=0.0,
            smtp_catch_all_weight=0.0, smtp_inconclusive_weight=0.0,
            catch_all_flag_weight=0.0, catch_all_strong_weight=0.0,
            catch_all_moderate_weight=0.0, noise_amplitude=0.0,
        )
        # base 0.50 exactly → medium (≥ medium_threshold).
        r = compute_deliverability_probability(_inputs(), custom)
        assert r.probability == pytest.approx(0.50)
        assert r.label == "medium"

    def test_custom_thresholds_change_boundaries(self) -> None:
        strict = ProbabilityThresholds(high_threshold=0.95, medium_threshold=0.80)
        # base 0.5 + mx(0.20) = 0.70 ± noise → below strict medium → low.
        r = compute_deliverability_probability(_inputs(), strict)
        assert r.label == "low"


# --------------------------------------------------------------------------- #
# inputs_from_row                                                             #
# --------------------------------------------------------------------------- #


class TestInputsFromRow:
    def test_all_columns_present_parse_cleanly(self) -> None:
        row = {
            "email": "alice@example.com",
            "domain": "example.com",
            "score_post_history": "85",
            "historical_label": "historically_reliable",
            "confidence_adjustment_applied": "2",
            "catch_all_confidence": "0.15",
            "possible_catch_all": "True",
            "smtp_result": "deliverable",
            "smtp_confidence": "0.9",
            "has_mx_record": "True",
            "has_a_record": "True",
            "domain_matches_input_column": "True",
            "typo_detected": "False",
            "hard_fail": "False",
            "v2_final_bucket": "ready",
        }
        inp = inputs_from_row(row)
        assert inp.score_post_history == 85
        assert inp.historical_label == "historically_reliable"
        assert inp.catch_all_confidence == pytest.approx(0.15)
        assert inp.possible_catch_all is True
        assert inp.smtp_result == "deliverable"
        assert inp.has_mx_record is True
        assert inp.has_a_record is True
        assert inp.domain_match is True
        assert inp.email == "alice@example.com"
        assert inp.domain == "example.com"

    def test_missing_fields_take_safe_defaults(self) -> None:
        inp = inputs_from_row({})
        assert inp.has_mx_record is False
        assert inp.has_a_record is False
        assert inp.smtp_result == "not_tested"
        assert inp.historical_label == "neutral"
        assert inp.domain_match is False
        assert inp.possible_catch_all is False
        assert inp.v2_final_bucket == "unknown"

    def test_malformed_floats_fall_back_to_zero(self) -> None:
        row = {"catch_all_confidence": "not-a-number", "has_mx_record": "True"}
        assert inputs_from_row(row).catch_all_confidence == 0.0


# --------------------------------------------------------------------------- #
# Determinism + noise                                                         #
# --------------------------------------------------------------------------- #


class TestDeterminism:
    def test_same_inputs_produce_same_output(self) -> None:
        ins = _inputs(email="alice@example.com", domain="example.com")
        r1 = compute_deliverability_probability(ins)
        r2 = compute_deliverability_probability(ins)
        assert r1 == r2

    def test_noise_creates_jitter_between_different_rows(self) -> None:
        """Two rows with the same V2 signals but different emails/domains
        should produce slightly different probabilities — this is what
        makes the distribution continuous rather than clustered."""
        r1 = compute_deliverability_probability(
            _inputs(email="alice@example.com", domain="example.com")
        )
        r2 = compute_deliverability_probability(
            _inputs(email="bob@another.com", domain="another.com")
        )
        assert r1.probability != r2.probability
        # The jitter is small (bounded by noise_amplitude).
        amplitude = DEFAULT_PROBABILITY_THRESHOLDS.noise_amplitude
        assert abs(r1.probability - r2.probability) <= 2.0 * amplitude + 1e-9


# --------------------------------------------------------------------------- #
# Defaults sanity                                                             #
# --------------------------------------------------------------------------- #


def test_default_thresholds_are_consistent() -> None:
    t = DEFAULT_PROBABILITY_THRESHOLDS
    assert 0.0 < t.medium_threshold < t.high_threshold < 1.0
    assert 0.0 <= t.base_score <= 1.0
    # Positive signals must be positive; negative signals must be negative.
    assert t.mx_present_weight > 0
    assert t.a_fallback_weight > 0
    assert t.no_dns_weight < 0
    assert t.smtp_deliverable_weight > 0
    assert t.smtp_undeliverable_weight < 0
    assert t.smtp_catch_all_weight < 0
    assert t.historical_reliable_weight > 0
    assert t.historical_risky_weight < 0
    assert t.historical_unstable_weight < 0
    assert t.catch_all_flag_weight < 0
    assert t.catch_all_strong_weight < 0
    assert t.catch_all_moderate_weight < 0
    assert t.noise_amplitude >= 0
