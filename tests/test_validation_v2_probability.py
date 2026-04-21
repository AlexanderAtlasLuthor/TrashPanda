"""Tests for Subphase 6 deliverability probability layer."""

from __future__ import annotations

from typing import Any

import pytest

from app.validation_v2 import SMTPProbeClient, ValidationEngineV2, ValidationPolicy
from app.validation_v2.control import NetworkExecutionPolicy
from app.validation_v2.control.decision_trace import STAGE_PROBABILITY
from app.validation_v2.network import (
    CatchAllAnalyzer,
    IntelligentRetryStrategy,
    SMTPProbeResult,
)
from app.validation_v2.probability import (
    DeliverabilityAggregator,
    DeliverabilitySignal,
    ExplanationBuilder,
    ValidationDecisionPolicy,
)
from app.validation_v2.request import ValidationRequest
from app.validation_v2.services import (
    DefaultExclusionService,
    DefaultValidationCandidateSelector,
    DomainCacheStore,
    SimpleDomainIntelligenceService,
    SimpleProviderReputationService,
)


def _make_request(**overrides: Any) -> ValidationRequest:
    defaults: dict[str, Any] = dict(
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
    defaults.update(overrides)
    return ValidationRequest(**defaults)


class _SMTP(SMTPProbeClient):
    def __init__(self, result: SMTPProbeResult) -> None:
        self.calls = 0
        self._result = result

    def probe(self, request: ValidationRequest) -> SMTPProbeResult:
        self.calls += 1
        return self._result


def _wire_engine(
    *,
    smtp_client: SMTPProbeClient | None = None,
    catch_all: bool = False,
    cache: DomainCacheStore | None = None,
) -> ValidationEngineV2:
    cache = cache if cache is not None else DomainCacheStore()
    engine = ValidationEngineV2(
        ValidationPolicy(enable_smtp_probing=smtp_client is not None),
        domain_intel=SimpleDomainIntelligenceService(cache=cache),
        provider_reputation=SimpleProviderReputationService(cache=cache),
        exclusion_service=DefaultExclusionService(),
        candidate_selector=DefaultValidationCandidateSelector(),
        smtp_client=smtp_client,
        catch_all_analyzer=CatchAllAnalyzer() if catch_all else None,
        retry_strategy=IntelligentRetryStrategy(),
    )
    engine.execution_policy = NetworkExecutionPolicy(
        allow_network=True,
        allow_smtp=True,
        allow_catch_all=True,
    )
    return engine


class TestAggregator:
    def test_weighted_average(self) -> None:
        result = DeliverabilityAggregator().compute(
            [
                DeliverabilitySignal("a", 1.0, 2.0, "smtp"),
                DeliverabilitySignal("b", 0.0, 1.0, "dns"),
            ]
        )

        assert result.probability == pytest.approx(2 / 3)
        assert result.confidence > 0.0

    def test_empty_signals(self) -> None:
        result = DeliverabilityAggregator().compute([])

        assert result.probability == 0.0
        assert result.confidence == 0.0
        assert result.signals == []

    def test_clamping(self) -> None:
        result = DeliverabilityAggregator().compute(
            [DeliverabilitySignal("too_high", 3.0, 1.0, "smtp")]
        )

        assert result.probability == 1.0


class TestDecision:
    @pytest.mark.parametrize(
        "probability,status,action",
        [
            (0.85, "valid", "send"),
            (0.60, "likely_valid", "send_with_monitoring"),
            (0.40, "uncertain", "review"),
            (0.20, "risky", "verify"),
            (0.19, "invalid", "block"),
        ],
    )
    def test_thresholds(self, probability: float, status: str, action: str) -> None:
        decision = ValidationDecisionPolicy().decide(probability)

        assert decision.status == status
        assert decision.action == action


class TestExplanation:
    def test_includes_key_signals_and_is_deterministic(self) -> None:
        signals = [
            DeliverabilitySignal("smtp_result", 1.0, 2.0, "smtp"),
            DeliverabilitySignal("domain_pattern", 0.3, 1.0, "dns"),
            DeliverabilitySignal("syntax_valid", 1.0, 1.0, "structural"),
        ]
        result = DeliverabilityAggregator().compute(signals)
        decision = ValidationDecisionPolicy().decide(result.probability)
        builder = ExplanationBuilder()

        a = builder.build(signals, result, decision)
        b = builder.build(signals, result, decision)

        assert a == b
        assert a["top_positive_signals"][0]["name"] == "smtp_result"
        assert a["top_negative_signals"][0]["name"] == "domain_pattern"
        assert "recommended action" in a["explanation_text"]


class TestEngineProbabilityIntegration:
    def test_no_smtp_still_populates_probability_fields(self) -> None:
        engine = _wire_engine()

        result = engine.validate(_make_request())

        assert 0.0 <= result.deliverability_probability <= 1.0
        assert result.deliverability_confidence > 0.0
        assert result.validation_status in {
            "valid",
            "likely_valid",
            "uncertain",
            "risky",
            "invalid",
        }
        assert result.action_recommendation in {
            "send",
            "send_with_monitoring",
            "review",
            "verify",
            "block",
        }
        assert result.validation_breakdown["decision"]["action"] == (
            result.action_recommendation
        )
        assert _has_probability_step(result)

    def test_smtp_mapping_valid_invalid_uncertain(self) -> None:
        cases = [
            (SMTPProbeResult(True, 250, "ok", 1.0, None), 1.0),
            (SMTPProbeResult(True, 550, "no", 1.0, None), 0.0),
            (SMTPProbeResult(False, None, "timeout", 1.0, "timeout"), 0.5),
        ]
        for smtp_result, expected_smtp_value in cases:
            engine = _wire_engine(smtp_client=_SMTP(smtp_result))
            result = engine.validate(_make_request())
            signal = _signal(result, "smtp_result")
            assert signal["value"] == pytest.approx(expected_smtp_value)

    def test_catch_all_mapping(self) -> None:
        engine = _wire_engine(
            smtp_client=_SMTP(SMTPProbeResult(True, 550, "no", 1.0, None)),
            catch_all=True,
        )

        result = engine.validate(_make_request())

        assert result.catch_all_status == "unlikely"
        assert _signal(result, "catch_all")["value"] == pytest.approx(0.8)

    def test_reputation_mapping(self) -> None:
        cache = DomainCacheStore()
        cache.record_domain(
            "example.com",
            provider_type="custom",
            reputation_score=0.42,
        )
        engine = _wire_engine(cache=cache)

        result = engine.validate(_make_request())

        assert _signal(result, "provider_reputation")["value"] == pytest.approx(0.42)

    def test_full_signals_explanation_is_attached(self) -> None:
        engine = _wire_engine(
            smtp_client=_SMTP(SMTPProbeResult(True, 250, "ok", 1.0, None)),
            catch_all=True,
        )

        result = engine.validate(_make_request())

        explanation = result.validation_breakdown["explanation"]
        assert explanation["contributing_factors"]
        assert explanation["explanation_text"] == result.validation_explanation
        assert result.validation_breakdown["probability"] == pytest.approx(
            result.deliverability_probability
        )


def _signal(result, name: str) -> dict[str, object]:
    for signal in result.validation_breakdown["signals"]:
        if signal["name"] == name:
            return signal
    raise AssertionError(f"missing signal {name}")


def _has_probability_step(result) -> bool:
    return any(
        step["stage"] == STAGE_PROBABILITY
        and step["decision"] == "computed"
        and step["reason"] == "aggregation_complete"
        for step in result.decision_trace["steps"]
    )
