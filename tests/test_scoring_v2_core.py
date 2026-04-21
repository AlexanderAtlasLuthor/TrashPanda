"""Foundational tests for scoring_v2 primitives.

Scope:
  * ScoringSignal validation (bounds on value / confidence / weight,
    direction enum enforcement).
  * SignalEvaluator abstract behavior.
  * ScoringEngineV2 collects signals correctly.

Out of scope for this step (per the subphase spec):
  * Scoring math (totals, final_score, bucket selection).
  * Hard-stop policy application.
  * Profile weight lookup.
"""

from __future__ import annotations

import json

import pytest

from app.scoring_v2 import (
    ScoreBreakdown,
    ScoringEngineV2,
    ScoringProfile,
    ScoringSignal,
    SignalEvaluator,
)


# ---------------------------------------------------------------------------
# ScoringSignal
# ---------------------------------------------------------------------------

class TestScoringSignal:
    def test_valid_instance_constructs(self):
        sig = ScoringSignal(
            name="mx",
            direction="positive",
            value=1.0,
            weight=50.0,
            confidence=0.95,
            reason_code="mx_present",
            explanation="MX record resolvable",
        )
        assert sig.name == "mx"
        assert sig.direction == "positive"
        assert sig.value == 1.0
        assert sig.weight == 50.0
        assert sig.confidence == 0.95
        assert sig.reason_code == "mx_present"
        assert sig.explanation == "MX record resolvable"

    def test_minimum_bounds_are_inclusive(self):
        ScoringSignal(
            name="x",
            direction="neutral",
            value=0.0,
            weight=0.0,
            confidence=0.0,
            reason_code="x",
        )

    def test_maximum_bounds_are_inclusive(self):
        ScoringSignal(
            name="x",
            direction="negative",
            value=1.0,
            weight=1_000.0,
            confidence=1.0,
            reason_code="x",
        )

    def test_value_above_1_raises(self):
        with pytest.raises(ValueError, match="value must be in"):
            ScoringSignal(
                name="x",
                direction="positive",
                value=1.1,
                weight=1.0,
                confidence=0.5,
                reason_code="x",
            )

    def test_value_below_0_raises(self):
        with pytest.raises(ValueError, match="value must be in"):
            ScoringSignal(
                name="x",
                direction="positive",
                value=-0.1,
                weight=1.0,
                confidence=0.5,
                reason_code="x",
            )

    def test_confidence_above_1_raises(self):
        with pytest.raises(ValueError, match="confidence must be in"):
            ScoringSignal(
                name="x",
                direction="positive",
                value=0.5,
                weight=1.0,
                confidence=1.5,
                reason_code="x",
            )

    def test_confidence_below_0_raises(self):
        with pytest.raises(ValueError, match="confidence must be in"):
            ScoringSignal(
                name="x",
                direction="positive",
                value=0.5,
                weight=1.0,
                confidence=-0.01,
                reason_code="x",
            )

    def test_negative_weight_raises(self):
        with pytest.raises(ValueError, match="weight must be >= 0"):
            ScoringSignal(
                name="x",
                direction="positive",
                value=0.5,
                weight=-1.0,
                confidence=0.5,
                reason_code="x",
            )

    def test_invalid_direction_raises(self):
        with pytest.raises(ValueError, match="direction must be one of"):
            ScoringSignal(
                name="x",
                direction="sideways",  # type: ignore[arg-type]
                value=0.5,
                weight=1.0,
                confidence=0.5,
                reason_code="x",
            )

    def test_is_frozen(self):
        sig = ScoringSignal(
            name="x",
            direction="positive",
            value=0.5,
            weight=1.0,
            confidence=0.5,
            reason_code="x",
        )
        with pytest.raises(Exception):
            sig.value = 0.9  # type: ignore[misc]


# ---------------------------------------------------------------------------
# SignalEvaluator
# ---------------------------------------------------------------------------

class TestSignalEvaluator:
    def test_cannot_instantiate_abstract_base(self):
        with pytest.raises(TypeError):
            SignalEvaluator()  # type: ignore[abstract]

    def test_subclass_without_evaluate_cannot_instantiate(self):
        class Incomplete(SignalEvaluator):
            name = "incomplete"

        with pytest.raises(TypeError):
            Incomplete()  # type: ignore[abstract]

    def test_concrete_subclass_runs_and_returns_signals(self):
        class Echo(SignalEvaluator):
            name = "echo"

            def evaluate(self, row: dict) -> list[ScoringSignal]:
                return [
                    ScoringSignal(
                        name="echo",
                        direction="neutral",
                        value=0.5,
                        weight=1.0,
                        confidence=1.0,
                        reason_code="echoed",
                        explanation=f"row={row!r}",
                    )
                ]

        out = Echo().evaluate({"email": "a@b.com"})
        assert len(out) == 1
        assert out[0].reason_code == "echoed"
        assert "a@b.com" in out[0].explanation

    def test_evaluator_must_not_mutate_row(self):
        """Evaluators are contractually required not to mutate their
        input. This test spot-checks that a well-behaved subclass
        leaves the input dict untouched."""

        class NoMutate(SignalEvaluator):
            name = "no_mutate"

            def evaluate(self, row: dict) -> list[ScoringSignal]:
                return []

        original = {"email": "x@y.com", "score": 75}
        snapshot = dict(original)
        NoMutate().evaluate(original)
        assert original == snapshot


# ---------------------------------------------------------------------------
# ScoringEngineV2 — signal collection contract
# ---------------------------------------------------------------------------

class _StaticEvaluator(SignalEvaluator):
    """Helper evaluator: emits a preconfigured list of signals verbatim."""

    def __init__(self, signals: list[ScoringSignal], name: str = "static") -> None:
        self.name = name
        self._signals = signals

    def evaluate(self, row: dict) -> list[ScoringSignal]:
        return list(self._signals)


class _EmptyEvaluator(SignalEvaluator):
    """Helper evaluator: returns an empty list for every row."""

    name = "empty"

    def evaluate(self, row: dict) -> list[ScoringSignal]:
        return []


def _signal(reason_code: str, direction: str = "positive") -> ScoringSignal:
    return ScoringSignal(
        name=reason_code,
        direction=direction,  # type: ignore[arg-type]
        value=1.0,
        weight=1.0,
        confidence=1.0,
        reason_code=reason_code,
    )


class TestScoringEngineV2:
    def test_constructor_stores_evaluators_and_profile(self):
        profile = ScoringProfile()
        ev1 = _StaticEvaluator([_signal("a")], name="ev1")
        ev2 = _StaticEvaluator([_signal("b")], name="ev2")

        engine = ScoringEngineV2(evaluators=[ev1, ev2], profile=profile)

        # evaluators property returns a copy in order
        assert [e.name for e in engine.evaluators] == ["ev1", "ev2"]
        assert engine.profile is profile

    def test_evaluators_property_is_a_copy(self):
        engine = ScoringEngineV2(evaluators=[_EmptyEvaluator()], profile=ScoringProfile())
        exposed = engine.evaluators
        exposed.clear()
        # Engine's internal list is unaffected.
        assert len(engine.evaluators) == 1

    def test_evaluate_row_collects_signals_in_evaluator_order(self):
        ev1 = _StaticEvaluator([_signal("a"), _signal("b")], name="ev1")
        ev2 = _StaticEvaluator([_signal("c")], name="ev2")
        engine = ScoringEngineV2(evaluators=[ev1, ev2], profile=ScoringProfile())

        out = engine.evaluate_row({"email": "x@y.com"})

        assert [s.reason_code for s in out.signals] == ["a", "b", "c"]
        assert out.reason_codes == ["a", "b", "c"]

    def test_evaluate_row_with_no_evaluators_returns_empty_breakdown(self):
        engine = ScoringEngineV2(evaluators=[], profile=ScoringProfile())
        out = engine.evaluate_row({"email": "x@y.com"})
        assert out.signals == []
        assert out.reason_codes == []

    def test_empty_evaluator_contributes_nothing(self):
        ev1 = _StaticEvaluator([_signal("a")], name="ev1")
        empty = _EmptyEvaluator()
        engine = ScoringEngineV2(evaluators=[ev1, empty], profile=ScoringProfile())

        out = engine.evaluate_row({"email": "x@y.com"})
        assert [s.reason_code for s in out.signals] == ["a"]

    def test_empty_profile_produces_zero_denominator_zero_score(self):
        """A profile with no ``max_positive_possible`` configured and no
        contributors cannot normalize. The engine must degrade
        gracefully (``final_score = 0.0``) rather than raise
        ZeroDivisionError — downstream code must be able to trust the
        numeric output regardless of profile completeness."""
        engine = ScoringEngineV2(
            evaluators=[_StaticEvaluator([_signal("a")])],
            profile=ScoringProfile(),
        )
        out = engine.evaluate_row({})
        assert out.final_score == 0.0
        assert out.hard_stop is False
        assert out.hard_stop_reason is None
        # A bucket is always assigned, never the placeholder "unknown".
        assert out.bucket in {"high_confidence", "review", "invalid"}

    def test_engine_does_not_mutate_input_row(self):
        ev = _StaticEvaluator([_signal("a")])
        engine = ScoringEngineV2(evaluators=[ev], profile=ScoringProfile())
        row = {"email": "a@b.com", "n": 42}
        snapshot = dict(row)
        engine.evaluate_row(row)
        assert row == snapshot


# ---------------------------------------------------------------------------
# ScoreBreakdown — basic construction + serialization surface
# ---------------------------------------------------------------------------

class TestScoreBreakdown:
    def test_default_construction(self):
        b = ScoreBreakdown()
        assert b.signals == []
        assert b.positive_total == 0.0
        assert b.bucket == "unknown"
        assert b.hard_stop is False
        assert b.hard_stop_reason is None
        assert b.breakdown_dict == {}

    def test_to_dict_is_json_serializable(self):
        b = ScoreBreakdown(
            signals=[_signal("a"), _signal("b", direction="negative")],
            positive_total=75.0,
            negative_total=10.0,
            raw_score=65.0,
            final_score=65.0,
            confidence=0.9,
            bucket="review",
            reason_codes=["a", "b"],
            explanation="placeholder",
        )
        d = b.to_dict()
        # Must round-trip through json.dumps without custom encoders.
        encoded = json.dumps(d)
        roundtrip = json.loads(encoded)
        assert roundtrip["final_score"] == 65.0
        assert roundtrip["bucket"] == "review"
        assert len(roundtrip["signals"]) == 2
        assert roundtrip["signals"][0]["reason_code"] == "a"
        assert roundtrip["signals"][1]["direction"] == "negative"


# ---------------------------------------------------------------------------
# ScoringProfile — structural sanity (no logic yet)
# ---------------------------------------------------------------------------

class TestScoringProfile:
    def test_default_values(self):
        p = ScoringProfile()
        assert p.weights == {}
        assert p.signal_weights == {}
        assert p.confidence_weights == {}
        # Thresholds are in [0.0, 1.0] because ``final_score`` is a
        # normalized fraction. These are the *bare* ScoringProfile()
        # defaults — the calibrated default lives in
        # ``build_default_profile`` and may differ.
        assert p.high_confidence_threshold == 0.75
        assert p.review_threshold == 0.40
        assert p.high_confidence_min_confidence == 0.80
        assert p.hard_stop_policy == []
        assert p.bucket_policy == {}
        assert p.strong_evidence_reason_codes == set()
        assert p.max_positive_contributors == set()

    def test_custom_values_round_trip(self):
        p = ScoringProfile(
            signal_weights={"mx_present": 50.0, "typo_corrected": 3.0},
            confidence_weights={"syntax_valid": 0.2},
            high_confidence_threshold=0.8,
            review_threshold=0.5,
            hard_stop_policy=["syntax_invalid", "nxdomain"],
            strong_evidence_reason_codes={"mx_present"},
            max_positive_contributors={"mx_present"},
            bucket_policy={"mode": "strict"},
        )
        assert p.signal_weights["mx_present"] == 50.0
        assert p.hard_stop_policy == ["syntax_invalid", "nxdomain"]
        assert p.bucket_policy["mode"] == "strict"
        assert p.strong_evidence_reason_codes == {"mx_present"}

    def test_legacy_weights_kwarg_populates_signal_weights(self):
        """Old call sites pass ``weights=``; the profile merges that
        into ``signal_weights`` so the engine sees a single source of
        truth."""
        p = ScoringProfile(weights={"mx_present": 50.0})
        assert p.signal_weights["mx_present"] == 50.0

    def test_effective_weight_falls_back_to_intrinsic(self):
        p = ScoringProfile(signal_weights={"mx_present": 50.0})
        # Configured — profile value wins.
        assert p.effective_weight("mx_present", 99.0) == 50.0
        # Not configured — intrinsic value is used.
        assert p.effective_weight("unknown_code", 99.0) == 99.0

    def test_effective_confidence_weight_defaults_to_one(self):
        p = ScoringProfile(confidence_weights={"syntax_valid": 0.2})
        assert p.effective_confidence_weight("syntax_valid") == 0.2
        # Missing keys default to 1.0.
        assert p.effective_confidence_weight("mx_present") == 1.0

    def test_derived_max_positive_possible_sums_configured_weights(self):
        p = ScoringProfile(
            signal_weights={
                "syntax_valid": 25.0,
                "domain_present": 10.0,
                "mx_present": 50.0,
                "a_fallback": 20.0,
                "domain_match": 5.0,
            },
            max_positive_contributors={
                "syntax_valid",
                "domain_present",
                "mx_present",
                "domain_match",
            },
        )
        # a_fallback is NOT in contributors (mutually exclusive with
        # mx_present); the theoretical maximum should exclude it.
        assert p.derived_max_positive_possible() == 90.0

    def test_derived_max_positive_possible_falls_back_to_literal(self):
        p = ScoringProfile(max_positive_possible=120.0)
        assert p.derived_max_positive_possible() == 120.0
