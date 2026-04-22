"""Unit tests for app.validation_v2.decision.

Verifies:
  * threshold boundaries produce the expected action;
  * hard guards (hard_fail, duplicate) always auto-reject;
  * bucket override is opt-in and never fires for guarded rows;
  * policy construction rejects invalid thresholds;
  * engine is deterministic.
"""

from __future__ import annotations

import pytest

from app.validation_v2.decision import (
    DEFAULT_DECISION_POLICY,
    DecisionInputs,
    DecisionPolicy,
    DecisionReason,
    FinalAction,
    apply_decision_policy,
    explain_decision,
    inputs_from_row,
)


# ─────────────────────────────────────────────────────────────────────── #
# Builders                                                                #
# ─────────────────────────────────────────────────────────────────────── #


def _inputs(
    probability: float = 0.75,
    *,
    bucket: str = "review",
    hard_fail: bool = False,
    smtp_result: str = "not_tested",
) -> DecisionInputs:
    return DecisionInputs(
        deliverability_probability=probability,
        v2_final_bucket=bucket,
        hard_fail=hard_fail,
        smtp_result=smtp_result,
    )


_OVERRIDE_ON = DecisionPolicy(
    approve_threshold=0.80, review_threshold=0.50, enable_bucket_override=True,
)


# ─────────────────────────────────────────────────────────────────────── #
# Policy validation                                                       #
# ─────────────────────────────────────────────────────────────────────── #


class TestPolicyValidation:
    def test_default_policy_is_consistent(self) -> None:
        assert DEFAULT_DECISION_POLICY.approve_threshold == 0.80
        assert DEFAULT_DECISION_POLICY.review_threshold == 0.50
        assert DEFAULT_DECISION_POLICY.enable_bucket_override is False

    def test_inverted_thresholds_raise(self) -> None:
        with pytest.raises(ValueError):
            DecisionPolicy(approve_threshold=0.4, review_threshold=0.7)

    def test_equal_thresholds_raise(self) -> None:
        with pytest.raises(ValueError):
            DecisionPolicy(approve_threshold=0.5, review_threshold=0.5)

    def test_negative_threshold_raises(self) -> None:
        with pytest.raises(ValueError):
            DecisionPolicy(approve_threshold=0.8, review_threshold=-0.1)

    def test_threshold_above_one_raises(self) -> None:
        with pytest.raises(ValueError):
            DecisionPolicy(approve_threshold=1.2, review_threshold=0.5)


# ─────────────────────────────────────────────────────────────────────── #
# Threshold boundaries                                                    #
# ─────────────────────────────────────────────────────────────────────── #


class TestThresholdBoundaries:
    """Verify that 0.80 and 0.50 are inclusive lower bounds."""

    @pytest.mark.parametrize(
        "probability,expected_action",
        [
            (0.0, FinalAction.AUTO_REJECT),
            (0.49, FinalAction.AUTO_REJECT),
            (0.50, FinalAction.MANUAL_REVIEW),
            (0.79, FinalAction.MANUAL_REVIEW),
            (0.80, FinalAction.AUTO_APPROVE),
            (1.00, FinalAction.AUTO_APPROVE),
        ],
    )
    def test_action_matches_probability_band(
        self, probability: float, expected_action: str,
    ) -> None:
        r = apply_decision_policy(_inputs(probability=probability))
        assert r.final_action == expected_action

    def test_confidence_is_clamped_below_zero(self) -> None:
        r = apply_decision_policy(_inputs(probability=-5.0))
        assert r.decision_confidence == 0.0
        assert r.final_action == FinalAction.AUTO_REJECT

    def test_confidence_is_clamped_above_one(self) -> None:
        r = apply_decision_policy(_inputs(probability=5.0))
        assert r.decision_confidence == 1.0
        assert r.final_action == FinalAction.AUTO_APPROVE


class TestCustomThresholds:
    def test_custom_policy_shifts_boundaries(self) -> None:
        strict = DecisionPolicy(approve_threshold=0.90, review_threshold=0.70)
        # 0.80 would auto_approve by default, but only manual_review under strict.
        r = apply_decision_policy(_inputs(probability=0.80), strict)
        assert r.final_action == FinalAction.MANUAL_REVIEW

    def test_reason_codes_match_action(self) -> None:
        for probability, expected_reason in (
            (0.95, DecisionReason.HIGH_PROBABILITY),
            (0.65, DecisionReason.MEDIUM_PROBABILITY),
            (0.20, DecisionReason.LOW_PROBABILITY),
        ):
            r = apply_decision_policy(_inputs(probability=probability))
            assert r.decision_reason == expected_reason


# ─────────────────────────────────────────────────────────────────────── #
# Hard guards                                                             #
# ─────────────────────────────────────────────────────────────────────── #


class TestHardGuards:
    def test_hard_fail_flag_forces_auto_reject(self) -> None:
        r = apply_decision_policy(_inputs(probability=0.99, hard_fail=True))
        assert r.final_action == FinalAction.AUTO_REJECT
        assert r.decision_reason == DecisionReason.HARD_FAIL
        assert r.decision_confidence == 0.0

    def test_hard_fail_bucket_forces_auto_reject(self) -> None:
        r = apply_decision_policy(_inputs(probability=0.99, bucket="hard_fail"))
        assert r.final_action == FinalAction.AUTO_REJECT
        assert r.decision_reason == DecisionReason.HARD_FAIL

    def test_duplicate_bucket_forces_auto_reject(self) -> None:
        r = apply_decision_policy(_inputs(probability=0.99, bucket="duplicate"))
        assert r.final_action == FinalAction.AUTO_REJECT
        assert r.decision_reason == DecisionReason.DUPLICATE

    def test_hard_guards_never_trigger_bucket_override(self) -> None:
        # Even with override ON, hard-fail/duplicate preserve their bucket.
        hard = apply_decision_policy(
            _inputs(probability=0.99, hard_fail=True), _OVERRIDE_ON,
        )
        dup = apply_decision_policy(
            _inputs(probability=0.99, bucket="duplicate"), _OVERRIDE_ON,
        )
        assert hard.overridden_bucket == ""
        assert dup.overridden_bucket == ""


# ─────────────────────────────────────────────────────────────────────── #
# Bucket override (opt-in)                                                #
# ─────────────────────────────────────────────────────────────────────── #


class TestBucketOverride:
    def test_override_off_never_sets_overridden_bucket(self) -> None:
        for probability, bucket in (
            (0.95, "review"), (0.10, "ready"), (0.60, "review"),
        ):
            r = apply_decision_policy(_inputs(probability=probability, bucket=bucket))
            assert r.overridden_bucket == ""

    def test_override_on_auto_approve_review_row_targets_ready(self) -> None:
        r = apply_decision_policy(
            _inputs(probability=0.95, bucket="review"), _OVERRIDE_ON,
        )
        assert r.final_action == FinalAction.AUTO_APPROVE
        assert r.overridden_bucket == "ready"

    def test_override_on_auto_approve_already_ready_is_noop(self) -> None:
        r = apply_decision_policy(
            _inputs(probability=0.95, bucket="ready"), _OVERRIDE_ON,
        )
        assert r.overridden_bucket == ""

    def test_override_on_auto_reject_review_row_targets_invalid(self) -> None:
        r = apply_decision_policy(
            _inputs(probability=0.10, bucket="review"), _OVERRIDE_ON,
        )
        assert r.final_action == FinalAction.AUTO_REJECT
        assert r.overridden_bucket == "invalid"

    def test_override_on_auto_reject_ready_row_targets_invalid(self) -> None:
        """Cross-tier reject is allowed under override-on; it's explicit."""
        r = apply_decision_policy(
            _inputs(probability=0.10, bucket="ready"), _OVERRIDE_ON,
        )
        assert r.overridden_bucket == "invalid"

    def test_manual_review_never_sets_overridden_bucket(self) -> None:
        r = apply_decision_policy(
            _inputs(probability=0.60, bucket="ready"), _OVERRIDE_ON,
        )
        assert r.final_action == FinalAction.MANUAL_REVIEW
        assert r.overridden_bucket == ""


# ─────────────────────────────────────────────────────────────────────── #
# Determinism                                                             #
# ─────────────────────────────────────────────────────────────────────── #


def test_same_inputs_produce_identical_output() -> None:
    a = _inputs(probability=0.73, bucket="review")
    r1 = apply_decision_policy(a)
    r2 = apply_decision_policy(a)
    assert r1 == r2


# ─────────────────────────────────────────────────────────────────────── #
# inputs_from_row                                                         #
# ─────────────────────────────────────────────────────────────────────── #


class TestInputsFromRow:
    def test_valid_row_is_parsed(self) -> None:
        inp = inputs_from_row({
            "deliverability_probability": "0.85",
            "v2_final_bucket": "review",
            "hard_fail": "False",
            "smtp_result": "deliverable",
        })
        assert inp.deliverability_probability == pytest.approx(0.85)
        assert inp.v2_final_bucket == "review"
        assert inp.hard_fail is False
        assert inp.smtp_result == "deliverable"

    def test_missing_columns_get_safe_defaults(self) -> None:
        inp = inputs_from_row({})
        assert inp.deliverability_probability == 0.0
        assert inp.v2_final_bucket == "unknown"
        assert inp.hard_fail is False
        assert inp.smtp_result == "not_tested"

    def test_malformed_probability_falls_back_to_zero(self) -> None:
        inp = inputs_from_row({"deliverability_probability": "not-a-number"})
        assert inp.deliverability_probability == 0.0

    def test_truthy_hard_fail_strings_are_recognised(self) -> None:
        for raw in ("True", "TRUE", "1", "yes", "t"):
            assert inputs_from_row({"hard_fail": raw}).hard_fail is True
        for raw in ("False", "0", "no", ""):
            assert inputs_from_row({"hard_fail": raw}).hard_fail is False


# ─────────────────────────────────────────────────────────────────────── #
# Explanations                                                            #
# ─────────────────────────────────────────────────────────────────────── #


class TestExplain:
    def test_hard_fail_explanation(self) -> None:
        r = apply_decision_policy(_inputs(probability=0.99, hard_fail=True))
        text = explain_decision(r, _inputs(probability=0.99, hard_fail=True))
        assert "hard-failed" in text.lower()

    def test_duplicate_explanation(self) -> None:
        r = apply_decision_policy(_inputs(probability=0.99, bucket="duplicate"))
        text = explain_decision(r, _inputs(probability=0.99, bucket="duplicate"))
        assert "duplicate" in text.lower()

    def test_auto_approve_mentions_probability(self) -> None:
        r = apply_decision_policy(_inputs(probability=0.92))
        text = explain_decision(r)
        assert "0.92" in text
        assert "auto-approved" in text.lower()

    def test_auto_approve_with_smtp_deliverable_mentions_it(self) -> None:
        inp = _inputs(probability=0.92, smtp_result="deliverable")
        r = apply_decision_policy(inp)
        text = explain_decision(r, inp)
        assert "smtp delivery" in text.lower()

    def test_auto_reject_with_smtp_undeliverable_mentions_it(self) -> None:
        inp = _inputs(probability=0.10, smtp_result="undeliverable")
        r = apply_decision_policy(inp)
        text = explain_decision(r, inp)
        assert "smtp rejection" in text.lower()

    def test_manual_review_phrasing(self) -> None:
        r = apply_decision_policy(_inputs(probability=0.65))
        text = explain_decision(r)
        assert "manual review" in text.lower()
        assert "0.65" in text
