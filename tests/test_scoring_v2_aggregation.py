"""Tests for ScoringEngineV2 aggregation, hard-stop, and bucket logic.

Exercises the aggregation subphase:
  * positive / negative totals
  * raw_score (pre-clamp)
  * final_score (normalized to [0.0, 1.0])
  * signal-strength-weighted confidence
  * hard-stop handling (preserves totals / confidence / signals)
  * bucket assignment from (final_score, confidence, hard_stop)
  * reason_codes ordering
  * explanation synthesis (+ hard-stop tail)
  * breakdown_dict JSON serializability
  * end-to-end engine using real evaluators
"""

from __future__ import annotations

import copy
import json

import pytest

from app.scoring_v2 import (
    DnsSignalEvaluator,
    DomainMatchSignalEvaluator,
    DomainPresenceSignalEvaluator,
    ScoreBreakdown,
    ScoringEngineV2,
    ScoringProfile,
    ScoringSignal,
    SignalEvaluator,
    SyntaxSignalEvaluator,
    TypoCorrectionSignalEvaluator,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _StaticEvaluator(SignalEvaluator):
    """Emit a preconfigured list of signals verbatim."""

    def __init__(
        self, signals: list[ScoringSignal], name: str = "static"
    ) -> None:
        self.name = name
        self._signals = signals

    def evaluate(self, row: dict) -> list[ScoringSignal]:
        return list(self._signals)


def _mk(
    *,
    reason_code: str,
    direction: str = "positive",
    value: float = 1.0,
    weight: float = 10.0,
    confidence: float = 1.0,
    name: str | None = None,
    explanation: str = "",
) -> ScoringSignal:
    return ScoringSignal(
        name=name or reason_code,
        direction=direction,  # type: ignore[arg-type]
        value=value,
        weight=weight,
        confidence=confidence,
        reason_code=reason_code,
        explanation=explanation,
    )


def _engine_with(
    signals: list[ScoringSignal],
    *,
    profile: ScoringProfile | None = None,
) -> ScoringEngineV2:
    return ScoringEngineV2(
        evaluators=[_StaticEvaluator(signals)],
        profile=profile or ScoringProfile(),
    )


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------


class TestAggregationTotals:
    def test_positive_only_signals(self):
        signals = [
            _mk(reason_code="a", direction="positive", weight=10.0),
            _mk(reason_code="b", direction="positive", weight=5.0),
        ]
        out = _engine_with(signals).evaluate_row({})
        assert out.positive_total == 15.0
        assert out.negative_total == 0.0
        assert out.raw_score == 15.0
        # max_positive = 15, raw=15 → final_score = 1.0
        assert out.final_score == 1.0

    def test_positive_and_negative_mixed(self):
        signals = [
            _mk(reason_code="pos", direction="positive", weight=10.0),
            _mk(reason_code="neg", direction="negative", weight=4.0),
        ]
        out = _engine_with(signals).evaluate_row({})
        assert out.positive_total == 10.0
        assert out.negative_total == 4.0
        assert out.raw_score == 6.0
        # max_positive = 10, final = 6/10 = 0.6
        assert out.final_score == pytest.approx(0.6)

    def test_negative_exceeds_positive_clamps_to_zero(self):
        signals = [
            _mk(reason_code="pos", direction="positive", weight=5.0),
            _mk(reason_code="neg", direction="negative", weight=20.0),
        ]
        out = _engine_with(signals).evaluate_row({})
        assert out.positive_total == 5.0
        assert out.negative_total == 20.0
        # raw_score is NOT clamped
        assert out.raw_score == -15.0
        # final_score is clamped
        assert out.final_score == 0.0

    def test_no_signals_yields_zero(self):
        engine = ScoringEngineV2(evaluators=[], profile=ScoringProfile())
        out = engine.evaluate_row({})
        assert out.positive_total == 0.0
        assert out.negative_total == 0.0
        assert out.raw_score == 0.0
        assert out.final_score == 0.0
        assert out.confidence == 0.0
        assert out.reason_codes == []
        assert out.signals == []

    def test_neutral_signals_do_not_contribute_to_totals(self):
        signals = [
            _mk(reason_code="neu", direction="neutral", weight=99.0),
            _mk(reason_code="pos", direction="positive", weight=10.0),
        ]
        out = _engine_with(signals).evaluate_row({})
        assert out.positive_total == 10.0
        assert out.negative_total == 0.0
        assert out.raw_score == 10.0

    def test_value_fraction_scales_contribution(self):
        """contribution = value * weight, not weight alone."""
        signals = [
            _mk(reason_code="p", direction="positive", value=0.5, weight=10.0),
        ]
        out = _engine_with(signals).evaluate_row({})
        assert out.positive_total == 5.0
        # max_positive uses weight (10.0), so final = 5/10 = 0.5
        assert out.final_score == pytest.approx(0.5)

    def test_final_score_clamped_at_one(self):
        """value<1 on a negative cannot push final_score above 1.0 either."""
        signals = [
            _mk(reason_code="p", direction="positive", weight=10.0),
            # A "negative" with value 0 that somehow shifts — ensure clamp
            # path is safe even in degenerate cases.
            _mk(reason_code="n", direction="negative", value=0.0, weight=1.0),
        ]
        out = _engine_with(signals).evaluate_row({})
        assert out.final_score == 1.0


# ---------------------------------------------------------------------------
# Confidence
# ---------------------------------------------------------------------------


class TestConfidenceAggregation:
    def test_single_signal_confidence_is_its_own(self):
        signals = [_mk(reason_code="a", confidence=0.7, weight=10.0)]
        out = _engine_with(signals).evaluate_row({})
        assert out.confidence == pytest.approx(0.7)

    def test_weighted_average_confidence(self):
        signals = [
            _mk(reason_code="a", confidence=1.0, weight=10.0),
            _mk(reason_code="b", confidence=0.0, weight=10.0),
        ]
        out = _engine_with(signals).evaluate_row({})
        # Equal strength → simple mean
        assert out.confidence == pytest.approx(0.5)

    def test_stronger_weight_signal_dominates_confidence(self):
        signals = [
            _mk(reason_code="strong", confidence=1.0, weight=100.0),
            _mk(reason_code="weak", confidence=0.0, weight=1.0),
        ]
        out = _engine_with(signals).evaluate_row({})
        # (1.0*100 + 0*1) / (100 + 1) = 100/101 ≈ 0.9901
        assert out.confidence == pytest.approx(100.0 / 101.0)
        assert out.confidence > 0.95

    def test_negatives_also_contribute_to_confidence_weighting(self):
        """Confidence aggregates strength across all emitted signals,
        regardless of direction."""
        signals = [
            _mk(reason_code="p", direction="positive", confidence=0.5, weight=10.0),
            _mk(reason_code="n", direction="negative", confidence=1.0, weight=10.0),
        ]
        out = _engine_with(signals).evaluate_row({})
        assert out.confidence == pytest.approx(0.75)

    def test_no_signals_confidence_is_zero(self):
        engine = ScoringEngineV2(evaluators=[], profile=ScoringProfile())
        out = engine.evaluate_row({})
        assert out.confidence == 0.0

    def test_zero_strength_signals_yield_zero_confidence(self):
        """All-zero-weight emitted signals produce confidence 0.0."""
        signals = [_mk(reason_code="a", confidence=1.0, weight=0.0)]
        out = _engine_with(signals).evaluate_row({})
        assert out.confidence == 0.0


# ---------------------------------------------------------------------------
# Hard stop
# ---------------------------------------------------------------------------


class TestHardStop:
    def test_no_policy_means_no_hard_stop(self):
        signals = [_mk(reason_code="nxdomain", direction="negative", weight=50.0)]
        out = _engine_with(signals, profile=ScoringProfile()).evaluate_row({})
        assert out.hard_stop is False
        assert out.hard_stop_reason is None

    def test_hard_stop_triggers_on_matching_reason_code(self):
        profile = ScoringProfile(hard_stop_policy=["syntax_invalid", "nxdomain"])
        signals = [
            _mk(reason_code="pos", direction="positive", weight=10.0),
            _mk(reason_code="nxdomain", direction="negative", weight=50.0),
        ]
        out = _engine_with(signals, profile=profile).evaluate_row({})
        assert out.hard_stop is True
        assert out.hard_stop_reason == "nxdomain"

    def test_hard_stop_preserves_totals_and_confidence(self):
        """Hard stop must NOT erase signals / totals / confidence."""
        profile = ScoringProfile(hard_stop_policy=["nxdomain"])
        signals = [
            _mk(reason_code="pos", direction="positive", weight=10.0, confidence=1.0),
            _mk(
                reason_code="nxdomain",
                direction="negative",
                weight=50.0,
                confidence=0.95,
            ),
        ]
        out = _engine_with(signals, profile=profile).evaluate_row({})
        assert out.hard_stop is True
        # Totals are still computed as if hard-stop had not fired
        assert out.positive_total == 10.0
        assert out.negative_total == 50.0
        assert out.raw_score == -40.0
        assert len(out.signals) == 2
        # Confidence is the strength-weighted average: (1.0*10 + 0.95*50) / 60
        expected_conf = (1.0 * 10 + 0.95 * 50) / 60
        assert out.confidence == pytest.approx(expected_conf)

    def test_first_matching_hard_stop_reason_wins(self):
        """When multiple signals match the policy, the first (in emission
        order) is recorded."""
        profile = ScoringProfile(
            hard_stop_policy=["syntax_invalid", "nxdomain", "no_domain"]
        )
        signals = [
            _mk(reason_code="pos", direction="positive", weight=10.0),
            _mk(reason_code="nxdomain", direction="negative", weight=50.0),
            _mk(reason_code="syntax_invalid", direction="negative", weight=50.0),
        ]
        out = _engine_with(signals, profile=profile).evaluate_row({})
        assert out.hard_stop is True
        assert out.hard_stop_reason == "nxdomain"

    def test_non_matching_reason_code_does_not_trigger(self):
        profile = ScoringProfile(hard_stop_policy=["syntax_invalid"])
        signals = [
            _mk(reason_code="something_else", direction="negative", weight=10.0)
        ]
        out = _engine_with(signals, profile=profile).evaluate_row({})
        assert out.hard_stop is False
        assert out.hard_stop_reason is None


# ---------------------------------------------------------------------------
# Bucketing
# ---------------------------------------------------------------------------


class TestBucketing:
    def test_hard_stop_is_always_invalid(self):
        profile = ScoringProfile(
            hard_stop_policy=["nxdomain"],
            high_confidence_threshold=0.80,
            review_threshold=0.45,
        )
        # Positive-dominant signals, but hard stop fires → invalid
        signals = [
            _mk(reason_code="pos", direction="positive", weight=100.0, confidence=1.0),
            _mk(
                reason_code="nxdomain",
                direction="negative",
                weight=0.0,  # zero weight so final_score still ~= 1.0
                confidence=0.95,
            ),
        ]
        out = _engine_with(signals, profile=profile).evaluate_row({})
        assert out.hard_stop is True
        assert out.final_score == pytest.approx(1.0)
        assert out.bucket == "invalid"

    def test_high_score_and_high_confidence_goes_high_confidence(self):
        profile = ScoringProfile(
            high_confidence_threshold=0.80,
            review_threshold=0.45,
        )
        signals = [
            _mk(reason_code="a", direction="positive", weight=10.0, confidence=0.95),
            _mk(reason_code="b", direction="positive", weight=10.0, confidence=0.90),
        ]
        out = _engine_with(signals, profile=profile).evaluate_row({})
        assert out.final_score == 1.0
        assert out.confidence >= 0.80
        assert out.bucket == "high_confidence"

    def test_high_score_low_confidence_falls_to_review(self):
        profile = ScoringProfile(
            high_confidence_threshold=0.80,
            review_threshold=0.45,
        )
        # final_score = 1.0 but average confidence is only 0.50 → review
        signals = [
            _mk(reason_code="a", direction="positive", weight=10.0, confidence=0.50),
        ]
        out = _engine_with(signals, profile=profile).evaluate_row({})
        assert out.final_score == 1.0
        assert out.confidence == pytest.approx(0.50)
        assert out.bucket == "review"

    def test_mid_score_goes_to_review(self):
        profile = ScoringProfile(
            high_confidence_threshold=0.80,
            review_threshold=0.45,
        )
        signals = [
            _mk(reason_code="p", direction="positive", weight=10.0, confidence=1.0),
            _mk(reason_code="n", direction="negative", weight=5.0, confidence=1.0),
        ]
        out = _engine_with(signals, profile=profile).evaluate_row({})
        assert out.final_score == pytest.approx(0.5)
        assert out.bucket == "review"

    def test_low_score_is_invalid(self):
        profile = ScoringProfile(
            high_confidence_threshold=0.80,
            review_threshold=0.45,
        )
        signals = [
            _mk(reason_code="p", direction="positive", weight=10.0, confidence=1.0),
            _mk(reason_code="n", direction="negative", weight=9.0, confidence=1.0),
        ]
        out = _engine_with(signals, profile=profile).evaluate_row({})
        # final = (10 - 9) / 10 = 0.10 < review_threshold
        assert out.final_score == pytest.approx(0.1)
        assert out.bucket == "invalid"

    def test_threshold_is_inclusive(self):
        """A score exactly on the review threshold lands in review."""
        profile = ScoringProfile(
            high_confidence_threshold=0.80,
            review_threshold=0.50,
        )
        signals = [
            _mk(reason_code="p", direction="positive", weight=10.0, confidence=1.0),
            _mk(reason_code="n", direction="negative", weight=5.0, confidence=1.0),
        ]
        out = _engine_with(signals, profile=profile).evaluate_row({})
        assert out.final_score == pytest.approx(0.5)
        assert out.bucket == "review"


# ---------------------------------------------------------------------------
# reason_codes + explanation + breakdown_dict
# ---------------------------------------------------------------------------


class TestReasonCodesAndExplanation:
    def test_reason_codes_preserve_evaluator_emission_order(self):
        ev1 = _StaticEvaluator(
            [_mk(reason_code="a"), _mk(reason_code="b")], name="ev1"
        )
        ev2 = _StaticEvaluator([_mk(reason_code="c")], name="ev2")
        engine = ScoringEngineV2(evaluators=[ev1, ev2], profile=ScoringProfile())
        out = engine.evaluate_row({})
        assert out.reason_codes == ["a", "b", "c"]

    def test_reason_codes_are_not_deduplicated(self):
        """Per the subphase spec, dedupe is explicitly out of scope."""
        signals = [
            _mk(reason_code="dup"),
            _mk(reason_code="dup"),
        ]
        out = _engine_with(signals).evaluate_row({})
        assert out.reason_codes == ["dup", "dup"]

    def test_explanation_joins_signal_explanations_in_order(self):
        signals = [
            _mk(reason_code="a", explanation="First reason."),
            _mk(reason_code="b", explanation="Second reason."),
        ]
        out = _engine_with(signals).evaluate_row({})
        assert out.explanation == "First reason.; Second reason."

    def test_explanation_empty_when_no_signals(self):
        engine = ScoringEngineV2(evaluators=[], profile=ScoringProfile())
        out = engine.evaluate_row({})
        assert out.explanation == "No scoring signals were emitted."

    def test_hard_stop_tail_is_appended_to_explanation(self):
        profile = ScoringProfile(hard_stop_policy=["nxdomain"])
        signals = [
            _mk(reason_code="pos", direction="positive", explanation="MX present."),
            _mk(
                reason_code="nxdomain",
                direction="negative",
                explanation="NXDOMAIN.",
            ),
        ]
        out = _engine_with(signals, profile=profile).evaluate_row({})
        assert out.explanation.startswith("MX present.; NXDOMAIN.")
        assert out.explanation.endswith("Hard stop triggered: nxdomain.")


class TestBreakdownDict:
    def test_breakdown_dict_has_required_keys(self):
        signals = [
            _mk(reason_code="p", direction="positive", weight=10.0),
            _mk(reason_code="n", direction="negative", weight=5.0),
        ]
        out = _engine_with(signals).evaluate_row({})
        required = {
            "signals",
            "positive_total",
            "negative_total",
            "raw_score",
            "final_score",
            "confidence",
            "hard_stop",
            "hard_stop_reason",
            "bucket",
            "reason_codes",
        }
        assert required.issubset(out.breakdown_dict.keys())

    def test_breakdown_dict_signal_entries_are_complete(self):
        signals = [
            _mk(
                reason_code="p",
                direction="positive",
                weight=10.0,
                value=1.0,
                confidence=0.9,
                explanation="Because.",
            )
        ]
        out = _engine_with(signals).evaluate_row({})
        entry = out.breakdown_dict["signals"][0]
        assert entry == {
            "name": "p",
            "direction": "positive",
            "value": 1.0,
            "weight": 10.0,
            "confidence": 0.9,
            "reason_code": "p",
            "explanation": "Because.",
        }

    def test_breakdown_dict_is_json_serializable(self):
        profile = ScoringProfile(hard_stop_policy=["nxdomain"])
        signals = [
            _mk(reason_code="p", direction="positive", weight=10.0),
            _mk(reason_code="nxdomain", direction="negative", weight=50.0),
        ]
        out = _engine_with(signals, profile=profile).evaluate_row({})
        # Must round-trip through json without custom encoders.
        encoded = json.dumps(out.breakdown_dict)
        roundtrip = json.loads(encoded)
        assert roundtrip["hard_stop"] is True
        assert roundtrip["hard_stop_reason"] == "nxdomain"
        assert roundtrip["reason_codes"] == ["p", "nxdomain"]
        assert len(roundtrip["signals"]) == 2

    def test_breakdown_dict_mirrors_top_level_fields(self):
        profile = ScoringProfile(hard_stop_policy=["nxdomain"])
        signals = [
            _mk(reason_code="p", direction="positive", weight=10.0, confidence=0.9),
            _mk(
                reason_code="nxdomain",
                direction="negative",
                weight=50.0,
                confidence=0.95,
            ),
        ]
        out = _engine_with(signals, profile=profile).evaluate_row({})
        bd = out.breakdown_dict
        assert bd["positive_total"] == out.positive_total
        assert bd["negative_total"] == out.negative_total
        assert bd["raw_score"] == out.raw_score
        assert bd["final_score"] == out.final_score
        assert bd["confidence"] == out.confidence
        assert bd["hard_stop"] == out.hard_stop
        assert bd["hard_stop_reason"] == out.hard_stop_reason
        assert bd["bucket"] == out.bucket
        assert bd["reason_codes"] == out.reason_codes


# ---------------------------------------------------------------------------
# End-to-end engine with real evaluators
# ---------------------------------------------------------------------------


def _real_engine(profile: ScoringProfile | None = None) -> ScoringEngineV2:
    return ScoringEngineV2(
        evaluators=[
            SyntaxSignalEvaluator(),
            DomainPresenceSignalEvaluator(),
            TypoCorrectionSignalEvaluator(),
            DomainMatchSignalEvaluator(),
            DnsSignalEvaluator(),
        ],
        profile=profile
        or ScoringProfile(
            hard_stop_policy=["syntax_invalid", "no_domain", "nxdomain"]
        ),
    )


class TestEndToEnd:
    def test_clean_row_produces_fully_populated_breakdown(self):
        row = {
            "syntax_valid": True,
            "corrected_domain": "gmail.com",
            "typo_corrected": False,
            "domain_matches_input_column": True,
            "has_mx_record": True,
            "has_a_record": False,
            "domain_exists": True,
            "dns_error": None,
        }
        engine = _real_engine()
        out = engine.evaluate_row(row)

        assert isinstance(out, ScoreBreakdown)
        assert out.signals, "expected at least one signal from real evaluators"
        assert out.reason_codes == [s.reason_code for s in out.signals]
        assert out.positive_total > 0.0
        assert out.negative_total == 0.0
        assert out.final_score == pytest.approx(1.0)
        assert out.confidence > 0.0
        assert out.hard_stop is False
        assert out.hard_stop_reason is None
        assert out.bucket in {"high_confidence", "review"}
        assert out.explanation  # non-empty
        # breakdown_dict must serialize
        json.dumps(out.breakdown_dict)

    def test_row_with_nxdomain_triggers_hard_stop(self):
        row = {
            "syntax_valid": True,
            "corrected_domain": "no-such.example",
            "typo_corrected": False,
            "domain_matches_input_column": True,
            "has_mx_record": False,
            "has_a_record": False,
            "domain_exists": False,
            "dns_error": "nxdomain",
        }
        engine = _real_engine()
        out = engine.evaluate_row(row)
        assert out.hard_stop is True
        assert out.hard_stop_reason == "nxdomain"
        assert out.bucket == "invalid"
        # Signals and totals preserved even on hard stop
        assert out.signals
        assert out.positive_total > 0.0  # syntax + domain_present + domain_match
        assert out.negative_total > 0.0  # nxdomain
        assert "Hard stop triggered: nxdomain." in out.explanation

    def test_row_with_invalid_syntax_triggers_hard_stop(self):
        row = {
            "syntax_valid": False,
            "corrected_domain": "gmail.com",
            "has_mx_record": True,
            "domain_exists": True,
        }
        engine = _real_engine()
        out = engine.evaluate_row(row)
        assert out.hard_stop is True
        assert out.hard_stop_reason == "syntax_invalid"
        assert out.bucket == "invalid"

    def test_engine_does_not_mutate_input_row(self):
        row = {
            "syntax_valid": True,
            "corrected_domain": "gmail.com",
            "typo_corrected": False,
            "domain_matches_input_column": True,
            "has_mx_record": True,
            "has_a_record": False,
            "domain_exists": True,
            "dns_error": None,
        }
        snapshot = copy.deepcopy(row)
        engine = _real_engine()
        engine.evaluate_row(row)
        assert row == snapshot

    def test_empty_row_produces_deterministic_invalid_breakdown(self):
        """An empty dict still runs — evaluators emit their "missing data"
        negatives, hard-stop fires, bucket is invalid."""
        engine = _real_engine()
        out = engine.evaluate_row({})
        assert out.hard_stop is True
        assert out.hard_stop_reason in {"syntax_invalid", "no_domain"}
        assert out.bucket == "invalid"
        # reason_codes preserve emission order from the configured
        # evaluator list; syntax is first, domain_presence second.
        assert out.reason_codes[0] == "syntax_invalid"
