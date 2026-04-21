"""Tests for Subphase 5 catch-all detection and intelligent retries."""

from __future__ import annotations

from typing import Any

import pytest

from app.validation_v2 import SMTPProbeClient, ValidationEngineV2, ValidationPolicy
from app.validation_v2.control import (
    EVENT_CATCH_ALL_CLASSIFIED,
    EVENT_SMTP_PROBE_COMPLETED,
    EVENT_SMTP_PROBE_FAILED,
    EVENT_SMTP_PROBE_STARTED,
    EVENT_SMTP_RETRY_ATTEMPTED,
    EVENT_SMTP_RETRY_SKIPPED,
    EVENT_VALIDATION_ALLOWED,
    EVENT_VALIDATION_STARTED,
    InMemoryTelemetrySink,
    NetworkExecutionPolicy,
)
from app.validation_v2.control.decision_trace import (
    STAGE_CATCH_ALL,
    STAGE_SMTP_RETRY,
)
from app.validation_v2.network import (
    CatchAllAnalyzer as NetworkCatchAllAnalyzer,
    IntelligentRetryStrategy,
    SMTPProbeResult,
    SMTPResultClassifier,
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


class _SequenceSMTPClient(SMTPProbeClient):
    def __init__(self, results: list[SMTPProbeResult]) -> None:
        self.results = list(results)
        self.calls: list[ValidationRequest] = []

    def probe(self, request: ValidationRequest) -> SMTPProbeResult:
        self.calls.append(request)
        if self.results:
            return self.results.pop(0)
        return SMTPProbeResult(False, None, "extra call", 1.0, "protocol_error")


class _ExplodingCatchAll:
    def assess(self, *args, **kwargs):
        raise RuntimeError("catch-all failed")


def _wire_engine(
    *,
    smtp_client: SMTPProbeClient,
    cache: DomainCacheStore | None = None,
    catch_all_analyzer: object | None = None,
    retry_strategy: object | None = None,
    telemetry_sink: InMemoryTelemetrySink | None = None,
) -> ValidationEngineV2:
    cache = cache if cache is not None else DomainCacheStore()
    engine = ValidationEngineV2(
        ValidationPolicy(enable_smtp_probing=True),
        domain_intel=SimpleDomainIntelligenceService(cache=cache),
        provider_reputation=SimpleProviderReputationService(cache=cache),
        exclusion_service=DefaultExclusionService(),
        candidate_selector=DefaultValidationCandidateSelector(),
        smtp_client=smtp_client,
        catch_all_analyzer=catch_all_analyzer,
        retry_strategy=retry_strategy,
    )
    engine.execution_policy = NetworkExecutionPolicy(
        allow_network=True,
        allow_smtp=True,
        allow_catch_all=True,
    )
    engine.telemetry_sink = telemetry_sink
    return engine


def _event_types(sink: InMemoryTelemetrySink) -> list[str]:
    return [event.event_type for event in sink.events]


def _steps(result, stage: str) -> list[dict[str, Any]]:
    return [
        step
        for step in result.decision_trace["steps"]
        if step["stage"] == stage
    ]


class TestCatchAllAnalyzer:
    def test_confirmed_classification_from_historical_random_accepts(self) -> None:
        cache = DomainCacheStore()
        record = cache.record_domain("example.com", provider_type="unknown")
        record.counters["catch_all_random_accepts"] = 2
        cache.set("example.com", record)
        smtp = SMTPProbeResult(True, 250, "ok", 1.0, None)
        classification = SMTPResultClassifier().classify(smtp)

        assessment = NetworkCatchAllAnalyzer().assess(
            "example.com", smtp, classification, cache
        )

        assert assessment.classification == "confirmed"
        assert assessment.confidence == pytest.approx(0.95)
        assert assessment.signals["random_invalid_accept_signals"] == 2

    def test_likely_classification_for_250_unknown_domain(self) -> None:
        cache = DomainCacheStore()
        cache.record_domain("example.com", provider_type="unknown")
        smtp = SMTPProbeResult(True, 250, "ok", 1.0, None)

        assessment = NetworkCatchAllAnalyzer().assess(
            "example.com",
            smtp,
            SMTPResultClassifier().classify(smtp),
            cache,
        )

        assert assessment.classification == "likely"
        assert assessment.confidence == pytest.approx(0.65)

    def test_unlikely_classification_for_550(self) -> None:
        smtp = SMTPProbeResult(True, 550, "no mailbox", 1.0, None)

        assessment = NetworkCatchAllAnalyzer().assess(
            "example.com",
            smtp,
            SMTPResultClassifier().classify(smtp),
        )

        assert assessment.classification == "unlikely"
        assert assessment.confidence == pytest.approx(0.75)

    def test_unknown_fallback_for_insufficient_signal(self) -> None:
        cache = DomainCacheStore()
        cache.record_domain("example.com", provider_type="tier_1")
        smtp = SMTPProbeResult(True, 250, "ok", 1.0, None)

        assessment = NetworkCatchAllAnalyzer().assess(
            "example.com",
            smtp,
            SMTPResultClassifier().classify(smtp),
            cache,
        )

        assert assessment.classification == "unknown"
        assert assessment.confidence == pytest.approx(0.0)

    def test_cache_records_last_assessment_and_signal_count(self) -> None:
        cache = DomainCacheStore()
        smtp = SMTPProbeResult(True, 550, "no", 1.0, None)

        NetworkCatchAllAnalyzer().assess(
            "example.com",
            smtp,
            SMTPResultClassifier().classify(smtp),
            cache,
        )

        record = cache.get("example.com")
        assert record is not None
        assert record.catch_all_classification == "unlikely"
        assert record.catch_all_confidence == pytest.approx(0.75)
        assert record.counters["catch_all_signal_count"] == 1


class TestIntelligentRetryStrategy:
    @pytest.mark.parametrize(
        "result,should_retry,reason",
        [
            (SMTPProbeResult(False, None, "timeout", 1.0, "timeout"), True, "timeout"),
            (
                SMTPProbeResult(True, 451, "try later", 1.0, None),
                True,
                "temporary_4xx",
            ),
            (SMTPProbeResult(True, 250, "ok", 1.0, None), False, "smtp_250_final"),
            (
                SMTPProbeResult(True, 550, "hard fail", 1.0, None),
                False,
                "smtp_550_hard_fail",
            ),
        ],
    )
    def test_retry_rules(
        self,
        result: SMTPProbeResult,
        should_retry: bool,
        reason: str,
    ) -> None:
        decision = IntelligentRetryStrategy(delay_ms=250).evaluate(result)

        assert decision.should_retry is should_retry
        assert decision.reason == reason
        assert decision.delay_ms == (250 if should_retry else None)


class TestEngineCatchAllRetryIntegration:
    def test_retry_executed_once_and_final_result_used(self) -> None:
        sink = InMemoryTelemetrySink()
        smtp = _SequenceSMTPClient(
            [
                SMTPProbeResult(False, None, "timeout", 10.0, "timeout"),
                SMTPProbeResult(True, 250, "ok", 12.0, None),
            ]
        )
        engine = _wire_engine(
            smtp_client=smtp,
            catch_all_analyzer=NetworkCatchAllAnalyzer(),
            retry_strategy=IntelligentRetryStrategy(),
            telemetry_sink=sink,
        )

        result = engine.validate(_make_request())

        assert len(smtp.calls) == 2
        assert result.retry_attempted is True
        assert result.retry_outcome == "success"
        assert result.smtp_status == "valid"
        assert result.smtp_code == 250
        assert _steps(result, STAGE_SMTP_RETRY)[0]["decision"] == "executed"
        assert _steps(result, STAGE_CATCH_ALL)[0]["decision"] == "classified"
        assert EVENT_SMTP_RETRY_ATTEMPTED in _event_types(sink)
        assert EVENT_CATCH_ALL_CLASSIFIED in _event_types(sink)

    def test_retry_skipped_when_result_is_final(self) -> None:
        sink = InMemoryTelemetrySink()
        smtp = _SequenceSMTPClient([SMTPProbeResult(True, 250, "ok", 1.0, None)])
        engine = _wire_engine(
            smtp_client=smtp,
            catch_all_analyzer=NetworkCatchAllAnalyzer(),
            retry_strategy=IntelligentRetryStrategy(),
            telemetry_sink=sink,
        )

        result = engine.validate(_make_request())

        assert len(smtp.calls) == 1
        assert result.retry_attempted is False
        assert result.retry_outcome == "none"
        assert _steps(result, STAGE_SMTP_RETRY)[0]["decision"] == "skipped"
        assert _steps(result, STAGE_SMTP_RETRY)[0]["reason"] == "smtp_250_final"
        assert EVENT_SMTP_RETRY_SKIPPED in _event_types(sink)

    def test_catch_all_computed_after_smtp(self) -> None:
        smtp = _SequenceSMTPClient([SMTPProbeResult(True, 550, "no", 1.0, None)])
        engine = _wire_engine(
            smtp_client=smtp,
            catch_all_analyzer=NetworkCatchAllAnalyzer(),
            retry_strategy=IntelligentRetryStrategy(),
        )

        result = engine.validate(_make_request())

        assert result.catch_all_status == "unlikely"
        assert result.catch_all_confidence == pytest.approx(0.75)
        assert result.metadata["catch_all"]["classification"] == "unlikely"

    def test_no_multiple_retries_or_infinite_loop(self) -> None:
        smtp = _SequenceSMTPClient(
            [
                SMTPProbeResult(False, None, "timeout", 1.0, "timeout"),
                SMTPProbeResult(False, None, "timeout", 1.0, "timeout"),
                SMTPProbeResult(True, 250, "should not be used", 1.0, None),
            ]
        )
        engine = _wire_engine(
            smtp_client=smtp,
            catch_all_analyzer=NetworkCatchAllAnalyzer(),
            retry_strategy=IntelligentRetryStrategy(),
        )

        result = engine.validate(_make_request())

        assert len(smtp.calls) == 2
        assert result.retry_attempted is True
        assert result.retry_outcome == "fail"
        assert result.smtp_status == "uncertain"
        assert len(_steps(result, STAGE_SMTP_RETRY)) == 1

    def test_engine_survives_catch_all_failure(self) -> None:
        sink = InMemoryTelemetrySink()
        smtp = _SequenceSMTPClient([SMTPProbeResult(True, 250, "ok", 1.0, None)])
        engine = _wire_engine(
            smtp_client=smtp,
            catch_all_analyzer=_ExplodingCatchAll(),
            retry_strategy=IntelligentRetryStrategy(),
            telemetry_sink=sink,
        )

        result = engine.validate(_make_request())

        assert result.catch_all_status == "unknown"
        assert result.catch_all_confidence == pytest.approx(0.0)
        assert result.metadata["catch_all"]["signals"]["reason"] == (
            "catch_all_analyzer_error"
        )

    def test_expected_telemetry_sequence_for_successful_retry_flow(self) -> None:
        sink = InMemoryTelemetrySink()
        smtp = _SequenceSMTPClient(
            [
                SMTPProbeResult(True, 451, "try later", 1.0, None),
                SMTPProbeResult(True, 250, "ok", 1.0, None),
            ]
        )
        engine = _wire_engine(
            smtp_client=smtp,
            catch_all_analyzer=NetworkCatchAllAnalyzer(),
            retry_strategy=IntelligentRetryStrategy(),
            telemetry_sink=sink,
        )

        engine.validate(_make_request())

        assert _event_types(sink) == [
            EVENT_VALIDATION_STARTED,
            EVENT_VALIDATION_ALLOWED,
            EVENT_SMTP_PROBE_STARTED,
            EVENT_SMTP_PROBE_COMPLETED,
            EVENT_SMTP_RETRY_ATTEMPTED,
            EVENT_CATCH_ALL_CLASSIFIED,
        ]
