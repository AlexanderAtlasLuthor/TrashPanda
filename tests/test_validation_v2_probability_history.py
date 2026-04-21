"""Tests for historical intelligence in the deliverability probability layer."""

from __future__ import annotations

from typing import Any

import pytest

from app.validation_v2 import ValidationEngineV2, ValidationPolicy, ValidationRequest
from app.validation_v2.history import HistoricalIntelligence
from app.validation_v2.probability import (
    DeliverabilityAggregator,
    DeliverabilitySignal,
    ExplanationBuilder,
    MAX_HISTORY_ADJUSTMENT,
    ValidationDecisionPolicy,
)


def _signals() -> list[DeliverabilitySignal]:
    return [
        DeliverabilitySignal("syntax_valid", 1.0, 1.0, "structural"),
        DeliverabilitySignal("domain_present", 1.0, 0.8, "dns"),
        DeliverabilitySignal("smtp_result", 0.5, 2.0, "smtp"),
    ]


def _history(**overrides: Any) -> dict[str, Any]:
    defaults: dict[str, Any] = dict(
        domain="example.com",
        provider_key="gmail",
        history_cache_hit=True,
        historical_domain_reputation=0.9,
        historical_domain_reputation_confidence=0.8,
        historical_smtp_valid_rate=0.85,
        historical_smtp_invalid_rate=0.05,
        historical_smtp_uncertain_rate=0.05,
        historical_timeout_rate=0.05,
        historical_catch_all_risk=0.05,
        domain_observation_count=40,
        domain_last_seen_at=100.0,
        historical_provider_reputation=0.85,
        historical_provider_reputation_confidence=0.75,
        provider_observation_count=50,
        provider_last_seen_at=100.0,
        domain_history_stale=False,
        provider_history_stale=False,
    )
    defaults.update(overrides)
    return defaults


def _request() -> ValidationRequest:
    return ValidationRequest(
        email="alice@example.com",
        domain="example.com",
        corrected_domain=None,
        syntax_valid=True,
        domain_present=True,
        score_v2=0.6,
        confidence_v2=0.7,
        bucket_v2="review",
        reason_codes_v2=("mx_present",),
    )


class _HistoricalService:
    def __init__(self, historical: HistoricalIntelligence) -> None:
        self.historical = historical

    def fetch(
        self, domain: str, provider_key: str | None = None, now: float | None = None
    ) -> HistoricalIntelligence:
        return self.historical


def _historical_intelligence(**overrides: Any) -> HistoricalIntelligence:
    return HistoricalIntelligence(**_history(**overrides))


class TestProbabilityHistory:
    def test_no_history_unchanged_behavior(self) -> None:
        aggregator = DeliverabilityAggregator()
        base = aggregator.compute(_signals())
        no_history = aggregator.compute(
            _signals(),
            historical={"history_cache_hit": False},
        )

        assert no_history.probability == pytest.approx(base.probability)
        assert no_history.confidence == pytest.approx(base.confidence)
        assert no_history.historical_influence["applied"] is False

    def test_strong_positive_history_increases_probability(self) -> None:
        base = DeliverabilityAggregator().compute(_signals())
        adjusted = DeliverabilityAggregator().compute(_signals(), historical=_history())

        assert adjusted.probability > base.probability
        assert adjusted.historical_influence["adjustment"] > 0
        assert "high_domain_reputation" in adjusted.historical_influence["factors"]
        assert "low_catch_all_risk" in adjusted.historical_influence["factors"]

    def test_strong_negative_history_decreases_probability(self) -> None:
        base = DeliverabilityAggregator().compute(_signals())
        adjusted = DeliverabilityAggregator().compute(
            _signals(),
            historical=_history(
                historical_domain_reputation=0.2,
                historical_provider_reputation=0.2,
                historical_smtp_valid_rate=0.1,
                historical_smtp_invalid_rate=0.7,
                historical_timeout_rate=0.6,
                historical_catch_all_risk=0.9,
            ),
        )

        assert adjusted.probability < base.probability
        assert adjusted.historical_influence["adjustment"] < 0
        assert "high_historical_smtp_invalid_rate" in (
            adjusted.historical_influence["factors"]
        )

    def test_low_confidence_history_has_minimal_effect(self) -> None:
        adjusted = DeliverabilityAggregator().compute(
            _signals(),
            historical=_history(
                historical_domain_reputation_confidence=0.05,
                historical_provider_reputation_confidence=0.05,
            ),
        )

        assert abs(adjusted.historical_influence["adjustment"]) < 0.01

    def test_stale_history_reduces_effect(self) -> None:
        fresh = DeliverabilityAggregator().compute(_signals(), historical=_history())
        stale = DeliverabilityAggregator().compute(
            _signals(),
            historical=_history(domain_history_stale=True),
        )

        assert abs(stale.historical_influence["adjustment"]) < abs(
            fresh.historical_influence["adjustment"]
        )

    def test_adjustment_is_bounded(self) -> None:
        adjusted = DeliverabilityAggregator().compute(
            _signals(),
            historical=_history(
                historical_domain_reputation=0.0,
                historical_provider_reputation=0.0,
                historical_smtp_invalid_rate=1.0,
                historical_timeout_rate=1.0,
                historical_catch_all_risk=1.0,
                historical_domain_reputation_confidence=1.0,
                historical_provider_reputation_confidence=1.0,
            ),
        )

        assert -MAX_HISTORY_ADJUSTMENT <= adjusted.historical_influence[
            "adjustment"
        ] <= MAX_HISTORY_ADJUSTMENT


class TestConfidenceHistory:
    def test_high_observations_increase_confidence(self) -> None:
        base = DeliverabilityAggregator().compute(_signals())
        adjusted = DeliverabilityAggregator().compute(_signals(), historical=_history())

        assert adjusted.confidence > base.confidence

    def test_stale_reduces_confidence(self) -> None:
        fresh = DeliverabilityAggregator().compute(_signals(), historical=_history())
        stale = DeliverabilityAggregator().compute(
            _signals(),
            historical=_history(domain_history_stale=True),
        )

        assert stale.confidence < fresh.confidence

    def test_contradictions_reduce_confidence(self) -> None:
        adjusted = DeliverabilityAggregator().compute(
            _signals(),
            historical=_history(
                historical_smtp_valid_rate=0.5,
                historical_smtp_invalid_rate=0.45,
            ),
        )

        assert "contradictory_history" in adjusted.historical_influence["factors"]
        assert adjusted.historical_influence["confidence_delta"] < 0.1


class TestExplanationAndEngineTrace:
    def test_explanation_includes_historical_influence(self) -> None:
        result = DeliverabilityAggregator().compute(_signals(), historical=_history())
        decision = ValidationDecisionPolicy().decide(result.probability)

        explanation = ExplanationBuilder().build(result.signals, result, decision)

        assert explanation["historical_influence"]["adjustment"] == pytest.approx(
            result.historical_influence["adjustment"]
        )
        assert explanation["historical_influence"]["factors"]
        assert "historical reputation signals" in explanation["explanation_text"]

    def test_engine_trace_contains_history_adjustment_stage(self) -> None:
        engine = ValidationEngineV2(ValidationPolicy())
        engine.historical_intelligence_service = _HistoricalService(
            _historical_intelligence()
        )  # type: ignore[assignment]

        result = engine.validate(_request())

        steps = result.decision_trace["steps"]
        history_steps = [
            step
            for step in steps
            if step["stage"] == "probability_history_adjustment"
        ]
        assert len(history_steps) == 1
        assert history_steps[0]["decision"] == "applied"
        assert history_steps[0]["inputs"]["adjustment"] > 0
        assert result.validation_breakdown["historical_influence"][
            "adjustment"
        ] > 0
        assert result.metadata["historical_probability_influence"]["applied"] is True

    def test_engine_trace_skips_when_history_missing(self) -> None:
        result = ValidationEngineV2(ValidationPolicy()).validate(_request())

        step = [
            s
            for s in result.decision_trace["steps"]
            if s["stage"] == "probability_history_adjustment"
        ][0]
        assert step["decision"] == "skipped"
        assert step["reason"] == "no_history_or_low_confidence"

    def test_no_smtp_or_probe_behavior_changes(self) -> None:
        engine = ValidationEngineV2(ValidationPolicy())
        engine.historical_intelligence_service = _HistoricalService(
            _historical_intelligence()
        )  # type: ignore[assignment]

        result = engine.validate(_request())

        assert result.smtp_status == "not_attempted"
        assert result.retry_attempted is False
