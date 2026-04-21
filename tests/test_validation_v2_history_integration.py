"""Tests for history-informed catch-all and retry refinement."""

from __future__ import annotations

from typing import Any

import pytest

from app.validation_v2 import SMTPProbeClient, ValidationEngineV2, ValidationPolicy
from app.validation_v2.history import HistoricalIntelligence
from app.validation_v2.network import (
    CatchAllAnalyzer,
    IntelligentRetryStrategy,
    SMTPProbeResult,
    SMTPResultClassifier,
)
from app.validation_v2.request import ValidationRequest
from app.validation_v2.control import NetworkExecutionPolicy
from app.validation_v2.control.decision_trace import STAGE_CATCH_ALL, STAGE_SMTP_RETRY


def _request(**overrides: Any) -> ValidationRequest:
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


def _history(**overrides: Any) -> dict[str, Any]:
    defaults: dict[str, Any] = dict(
        domain="example.com",
        provider_key="gmail",
        history_cache_hit=True,
        historical_domain_reputation=0.8,
        historical_domain_reputation_confidence=0.7,
        historical_smtp_valid_rate=0.65,
        historical_smtp_invalid_rate=0.1,
        historical_smtp_uncertain_rate=0.1,
        historical_timeout_rate=0.2,
        historical_catch_all_risk=0.8,
        domain_observation_count=12,
        domain_last_seen_at=100.0,
        historical_provider_reputation=0.75,
        historical_provider_reputation_confidence=0.65,
        provider_observation_count=20,
        provider_last_seen_at=100.0,
        domain_history_stale=False,
        provider_history_stale=False,
    )
    defaults.update(overrides)
    return defaults


def _historical_intelligence(**overrides: Any) -> HistoricalIntelligence:
    return HistoricalIntelligence(**_history(**overrides))


class _HistoricalService:
    def __init__(self, historical: HistoricalIntelligence) -> None:
        self.historical = historical
        self.calls: list[tuple[str, str | None]] = []

    def fetch(
        self, domain: str, provider_key: str | None = None, now: float | None = None
    ) -> HistoricalIntelligence:
        self.calls.append((domain, provider_key))
        return self.historical


class _SequenceSMTP(SMTPProbeClient):
    def __init__(self, results: list[SMTPProbeResult]) -> None:
        self.results = list(results)
        self.calls = 0

    def probe(self, request: ValidationRequest) -> SMTPProbeResult:
        self.calls += 1
        if self.results:
            return self.results.pop(0)
        return SMTPProbeResult(False, None, "extra", 1.0, "protocol_error")


def _wire_engine(
    *,
    smtp_client: _SequenceSMTP,
    historical: HistoricalIntelligence | None = None,
) -> ValidationEngineV2:
    engine = ValidationEngineV2(
        ValidationPolicy(enable_smtp_probing=True),
        smtp_client=smtp_client,
        catch_all_analyzer=CatchAllAnalyzer(),
        retry_strategy=IntelligentRetryStrategy(),
    )
    engine.execution_policy = NetworkExecutionPolicy(
        allow_network=True,
        allow_smtp=True,
        allow_catch_all=True,
    )
    if historical is not None:
        engine.historical_intelligence_service = _HistoricalService(historical)  # type: ignore[assignment]
    return engine


def _stage(result, stage: str) -> dict[str, Any]:
    for step in result.decision_trace["steps"]:
        if step["stage"] == stage:
            return step
    raise AssertionError(f"missing {stage}")


class TestCatchAllHistoricalRefinement:
    def test_no_history_same_behavior_as_before(self) -> None:
        smtp = SMTPProbeResult(True, 250, "ok", 1.0, None)

        assessment = CatchAllAnalyzer().assess(
            "example.com", smtp, SMTPResultClassifier().classify(smtp)
        )

        assert assessment.classification == "likely"
        assert assessment.confidence == pytest.approx(0.65)

    def test_strong_history_promotes_likely_to_confirmed(self) -> None:
        smtp = SMTPProbeResult(True, 250, "ok", 1.0, None)
        classification = SMTPResultClassifier().classify(smtp)
        historical = _history()
        from app.validation_v2.services import DomainCacheStore

        cache = DomainCacheStore()
        cache.record_domain("example.com", provider_type="unknown")

        assessment = CatchAllAnalyzer().assess(
            "example.com", smtp, classification, cache, historical
        )

        assert assessment.classification == "confirmed"
        assert assessment.confidence == pytest.approx(0.82)
        assert assessment.signals["reason"] == "historical_risk_supports_confirmed"

    def test_weak_stale_history_does_not_promote_unknown(self) -> None:
        smtp = SMTPProbeResult(True, 250, "ok", 1.0, None)
        historical = _history(
            domain_observation_count=2,
            historical_domain_reputation_confidence=0.1,
            domain_history_stale=True,
        )

        assessment = CatchAllAnalyzer().assess(
            "example.com",
            smtp,
            SMTPResultClassifier().classify(smtp),
            None,
            historical,
        )

        assert assessment.classification == "likely"

    def test_contradictory_history_reduces_certainty(self) -> None:
        smtp = SMTPProbeResult(True, 451, "try later", 1.0, None)
        historical = _history(historical_catch_all_risk=0.05)
        from app.validation_v2.services import DomainCacheStore

        cache = DomainCacheStore()
        record = cache.record_domain("example.com", provider_type="unknown")
        record.counters["catch_all_random_accepts"] = 2
        cache.set("example.com", record)

        assessment = CatchAllAnalyzer().assess(
            "example.com",
            smtp,
            SMTPResultClassifier().classify(smtp),
            cache,
            historical,
        )

        assert assessment.classification == "likely"
        assert assessment.signals["reason"] == (
            "history_conflicts_with_current_catch_all_signal"
        )

    def test_history_can_promote_unknown_to_likely(self) -> None:
        smtp = SMTPProbeResult(True, 451, "try later", 1.0, None)

        assessment = CatchAllAnalyzer().assess(
            "example.com",
            smtp,
            SMTPResultClassifier().classify(smtp),
            None,
            _history(),
        )

        assert assessment.classification == "likely"
        assert assessment.signals["reason"] == "historical_risk_supports_likely"


class TestRetryHistoricalRefinement:
    def test_no_history_same_behavior_as_before(self) -> None:
        result = SMTPProbeResult(False, None, "timeout", 1.0, "timeout")

        decision = IntelligentRetryStrategy().evaluate(result)

        assert decision.should_retry is True
        assert decision.reason == "timeout"

    def test_history_supporting_retry_keeps_transient_retry_enabled(self) -> None:
        result = SMTPProbeResult(False, None, "timeout", 1.0, "timeout")

        decision = IntelligentRetryStrategy().evaluate(result, _history())

        assert decision.should_retry is True
        assert decision.reason == "transient_error_and_history_supports_retry"

    def test_poor_history_suppresses_retry_conservatively(self) -> None:
        result = SMTPProbeResult(False, None, "timeout", 1.0, "timeout")
        historical = _history(
            historical_smtp_valid_rate=0.05,
            historical_smtp_invalid_rate=0.7,
            historical_smtp_uncertain_rate=0.1,
            historical_timeout_rate=0.2,
        )

        decision = IntelligentRetryStrategy().evaluate(result, historical)

        assert decision.should_retry is False
        assert decision.reason == "history_suggests_retry_unhelpful"

    @pytest.mark.parametrize(
        "smtp,reason",
        [
            (SMTPProbeResult(True, 250, "ok", 1.0, None), "smtp_250_final"),
            (SMTPProbeResult(True, 550, "hard", 1.0, None), "smtp_550_hard_fail"),
        ],
    )
    def test_history_never_retries_final_250_or_550(
        self, smtp: SMTPProbeResult, reason: str
    ) -> None:
        decision = IntelligentRetryStrategy().evaluate(smtp, _history())

        assert decision.should_retry is False
        assert decision.reason == reason


class TestEngineHistoryIntegration:
    def test_history_is_passed_to_retry_and_catch_all(self) -> None:
        smtp = _SequenceSMTP(
            [
                SMTPProbeResult(False, None, "timeout", 1.0, "timeout"),
                SMTPProbeResult(True, 250, "ok", 1.0, None),
            ]
        )
        engine = _wire_engine(smtp_client=smtp, historical=_historical_intelligence())

        result = engine.validate(_request())

        assert smtp.calls == 2
        assert _stage(result, STAGE_SMTP_RETRY)["reason"] == (
            "transient_error_and_history_supports_retry"
        )
        assert _stage(result, STAGE_CATCH_ALL)["reason"] in {
            "historical_risk_supports_confirmed",
            "historical_risk_supports_likely",
        }
        assert "historical_retry_influence" in result.metadata
        assert "historical_catch_all_influence" in result.metadata

    def test_engine_safe_when_history_missing(self) -> None:
        smtp = _SequenceSMTP([SMTPProbeResult(True, 250, "ok", 1.0, None)])
        engine = _wire_engine(smtp_client=smtp)

        result = engine.validate(_request())

        assert smtp.calls == 1
        assert result.metadata["historical_intelligence"]["history_cache_hit"] is False

    def test_poor_history_suppresses_retry_and_does_not_add_extra_probe(self) -> None:
        smtp = _SequenceSMTP(
            [
                SMTPProbeResult(False, None, "timeout", 1.0, "timeout"),
                SMTPProbeResult(True, 250, "should not happen", 1.0, None),
            ]
        )
        engine = _wire_engine(
            smtp_client=smtp,
            historical=_historical_intelligence(
                historical_smtp_valid_rate=0.05,
                historical_smtp_invalid_rate=0.7,
                historical_timeout_rate=0.2,
            ),
        )

        result = engine.validate(_request())

        assert smtp.calls == 1
        assert result.retry_attempted is False
        assert _stage(result, STAGE_SMTP_RETRY)["reason"] == (
            "history_suggests_retry_unhelpful"
        )

    def test_probability_behavior_is_now_integrated_conservatively(self) -> None:
        base_smtp = _SequenceSMTP([SMTPProbeResult(True, 250, "ok", 1.0, None)])
        hist_smtp = _SequenceSMTP([SMTPProbeResult(True, 250, "ok", 1.0, None)])
        baseline = _wire_engine(smtp_client=base_smtp).validate(_request())
        with_history = _wire_engine(
            smtp_client=hist_smtp,
            historical=_historical_intelligence(),
        ).validate(_request())

        delta = with_history.deliverability_probability - baseline.deliverability_probability
        assert abs(delta) <= 0.15
        assert with_history.metadata["historical_probability_influence"]["applied"] is True
