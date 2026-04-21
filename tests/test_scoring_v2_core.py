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

    def test_engine_populates_numeric_fields(self):
        """The aggregation engine fills in totals and the bucket label;
        placeholders are no longer returned."""
        engine = ScoringEngineV2(
            evaluators=[_StaticEvaluator([_signal("a")])],
            profile=ScoringProfile(),
        )
        out = engine.evaluate_row({})
        assert out.positive_total == 1.0
        assert out.negative_total == 0.0
        assert out.raw_score == 1.0
        assert out.final_score == 1.0
        assert out.confidence == 1.0
        assert out.hard_stop is False
        assert out.hard_stop_reason is None
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
        # V2 thresholds live in normalized [0.0, 1.0] space.
        assert p.high_confidence_threshold == 0.80
        assert p.review_threshold == 0.45
        assert p.review_threshold <= p.high_confidence_threshold
        assert p.hard_stop_policy == []
        assert p.bucket_policy == {}

    def test_custom_values_round_trip(self):
        p = ScoringProfile(
            weights={"mx_present": 50.0, "typo_corrected": -3.0},
            high_confidence_threshold=0.85,
            review_threshold=0.50,
            hard_stop_policy=["syntax_invalid", "nxdomain"],
            bucket_policy={"mode": "strict"},
        )
        assert p.weights["mx_present"] == 50.0
        assert p.high_confidence_threshold == 0.85
        assert p.review_threshold == 0.50
        assert p.hard_stop_policy == ["syntax_invalid", "nxdomain"]
        assert p.bucket_policy["mode"] == "strict"
