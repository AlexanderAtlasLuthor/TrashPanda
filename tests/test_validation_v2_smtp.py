"""Tests for Subphase 4 controlled SMTP sampler.

No real DNS resolution or SMTP servers are used. SMTP client tests inject
fake factories; engine tests inject fake SMTPProbeClient implementations.
"""

from __future__ import annotations

import socket
from typing import Any

import pytest

from app.validation_v2 import (
    SMTPProbeClient,
    ValidationEngineV2,
    ValidationPolicy,
    ValidationRequest,
)
from app.validation_v2.control import (
    EVENT_CANDIDATE_SKIPPED,
    EVENT_EXCLUDED,
    EVENT_SMTP_PROBE_COMPLETED,
    EVENT_SMTP_PROBE_FAILED,
    EVENT_SMTP_PROBE_STARTED,
    EVENT_VALIDATION_ALLOWED,
    EVENT_VALIDATION_BLOCKED_BY_POLICY,
    EVENT_VALIDATION_STARTED,
    EXECUTION_REASON_ALLOWED,
    EXECUTION_REASON_NETWORK_DISABLED,
    EXECUTION_REASON_RATE_LIMITED,
    InMemoryRateLimiter,
    InMemoryTelemetrySink,
    NetworkExecutionPolicy,
    RateLimitPolicy,
)
from app.validation_v2.control.decision_trace import STAGE_SMTP_PROBE
from app.validation_v2.network import (
    SMTPProbeResult,
    SMTPResultClassifier,
    SafeSMTPProbeClient,
)
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
        metadata={"mx_host": "mx.example.test"},
    )
    defaults.update(overrides)
    return ValidationRequest(**defaults)


class _FakeSMTP:
    calls: list[tuple[str, tuple[Any, ...]]] = []

    def __init__(self, *, host: str, port: int, timeout: float) -> None:
        type(self).calls.append(("connect", (host, port, timeout)))

    def helo(self, host: str):
        type(self).calls.append(("helo", (host,)))
        return 250, b"hello"

    def mail(self, sender: str):
        type(self).calls.append(("mail", (sender,)))
        return 250, b"sender ok"

    def rcpt(self, recipient: str):
        type(self).calls.append(("rcpt", (recipient,)))
        return 250, b"recipient ok"

    def quit(self):
        type(self).calls.append(("quit", ()))
        return 221, b"bye"


class _CountingSMTPClient(SMTPProbeClient):
    def __init__(
        self,
        result: SMTPProbeResult | None = None,
        *,
        exc: BaseException | None = None,
    ) -> None:
        self.calls: list[ValidationRequest] = []
        self._result = result or SMTPProbeResult(
            success=True,
            code=250,
            message="ok",
            latency_ms=12.5,
            error_type=None,
        )
        self._exc = exc

    def probe(self, request: ValidationRequest) -> SMTPProbeResult:
        self.calls.append(request)
        if self._exc is not None:
            raise self._exc
        return self._result


def _wire_engine(
    *,
    policy: ValidationPolicy | None = None,
    smtp_client: SMTPProbeClient | None = None,
    telemetry_sink: InMemoryTelemetrySink | None = None,
    rate_limiter: InMemoryRateLimiter | None = None,
    execution_policy: NetworkExecutionPolicy | None = None,
) -> ValidationEngineV2:
    cache = DomainCacheStore()
    engine = ValidationEngineV2(
        policy or ValidationPolicy(enable_smtp_probing=True),
        domain_intel=SimpleDomainIntelligenceService(cache=cache),
        provider_reputation=SimpleProviderReputationService(cache=cache),
        exclusion_service=DefaultExclusionService(),
        candidate_selector=DefaultValidationCandidateSelector(),
        smtp_client=smtp_client,
    )
    engine.telemetry_sink = telemetry_sink
    engine.rate_limiter = rate_limiter
    engine.execution_policy = execution_policy or NetworkExecutionPolicy(
        allow_network=True,
        allow_smtp=True,
        allow_catch_all=False,
    )
    return engine


def _event_types(sink: InMemoryTelemetrySink) -> list[str]:
    return [event.event_type for event in sink.events]


def _smtp_steps(result) -> list[dict[str, Any]]:
    return [
        step
        for step in result.decision_trace["steps"]
        if step["stage"] == STAGE_SMTP_PROBE
    ]


class TestSafeSMTPProbeClient:
    def test_success_case_stops_after_rcpt_to(self) -> None:
        _FakeSMTP.calls = []
        factory_calls = 0

        def factory(**kwargs):
            nonlocal factory_calls
            factory_calls += 1
            return _FakeSMTP(**kwargs)

        client = SafeSMTPProbeClient(
            smtp_factory=factory,
            timeout_seconds=2.5,
            mail_from="probe@example.invalid",
        )

        result = client.probe(_make_request())

        assert result.success is True
        assert result.code == 250
        assert result.message == "recipient ok"
        assert result.error_type is None
        assert result.latency_ms is not None
        assert factory_calls == 1
        assert [call[0] for call in _FakeSMTP.calls] == [
            "connect",
            "helo",
            "mail",
            "rcpt",
            "quit",
        ]

    def test_timeout_case_returns_safe_error(self) -> None:
        calls = 0

        def factory(**kwargs):
            nonlocal calls
            calls += 1
            raise socket.timeout("timed out")

        client = SafeSMTPProbeClient(smtp_factory=factory)

        result = client.probe(_make_request())

        assert calls == 1
        assert result.success is False
        assert result.code is None
        assert result.error_type == "timeout"

    def test_connection_error_returns_safe_error(self) -> None:
        calls = 0

        def factory(**kwargs):
            nonlocal calls
            calls += 1
            raise OSError("connection refused")

        client = SafeSMTPProbeClient(smtp_factory=factory)

        result = client.probe(_make_request())

        assert calls == 1
        assert result.success is False
        assert result.code is None
        assert result.error_type == "connection_error"


class TestSMTPResultClassifier:
    @pytest.mark.parametrize(
        "result,expected",
        [
            (
                SMTPProbeResult(True, 250, "ok", 1.0, None),
                {
                    "smtp_valid": True,
                    "smtp_invalid": False,
                    "smtp_uncertain": False,
                    "smtp_status": "valid",
                    "classification_reason": "smtp_250_accepted",
                },
            ),
            (
                SMTPProbeResult(True, 550, "no", 1.0, None),
                {
                    "smtp_valid": False,
                    "smtp_invalid": True,
                    "smtp_uncertain": False,
                    "smtp_status": "invalid",
                    "classification_reason": "smtp_550_rejected",
                },
            ),
            (
                SMTPProbeResult(True, 451, "try later", 1.0, None),
                {
                    "smtp_valid": False,
                    "smtp_invalid": False,
                    "smtp_uncertain": True,
                    "smtp_status": "uncertain",
                    "classification_reason": "smtp_4xx_temporary_response",
                },
            ),
            (
                SMTPProbeResult(False, None, "timed out", 1.0, "timeout"),
                {
                    "smtp_valid": False,
                    "smtp_invalid": False,
                    "smtp_uncertain": True,
                    "smtp_status": "uncertain",
                    "classification_reason": "smtp_error:timeout",
                },
            ),
        ],
    )
    def test_classifier_rules(
        self,
        result: SMTPProbeResult,
        expected: dict[str, object],
    ) -> None:
        assert SMTPResultClassifier().classify(result) == expected


class TestEngineSMTPIntegration:
    def test_smtp_runs_only_when_allowed(self) -> None:
        sink = InMemoryTelemetrySink()
        smtp = _CountingSMTPClient()
        engine = _wire_engine(smtp_client=smtp, telemetry_sink=sink)

        result = engine.validate(_make_request())

        assert len(smtp.calls) == 1
        assert result.execution_decision == {
            "allowed": True,
            "reason": EXECUTION_REASON_ALLOWED,
        }
        assert result.smtp_status == "valid"
        assert result.smtp_code == 250
        assert result.smtp_latency == pytest.approx(12.5)
        assert result.smtp_error_type is None
        assert result.metadata["smtp_classification"]["smtp_valid"] is True
        assert _smtp_steps(result) == [
            {
                "stage": STAGE_SMTP_PROBE,
                "decision": "executed",
                "reason": "allowed_by_policy",
                "inputs": {
                    "email": "alice@example.com",
                    "domain": "example.com",
                    "allow_smtp": True,
                },
            }
        ]
        assert _event_types(sink) == [
            EVENT_VALIDATION_STARTED,
            EVENT_VALIDATION_ALLOWED,
            EVENT_SMTP_PROBE_STARTED,
            EVENT_SMTP_PROBE_COMPLETED,
            "smtp_retry_skipped",
        ]

    @pytest.mark.parametrize(
        "engine_kwargs,probe_request,expected_events,expected_reason",
        [
            (
                {"policy": ValidationPolicy(
                    enable_smtp_probing=True,
                    excluded_domains={"blocked.test"},
                )},
                _make_request(domain="blocked.test"),
                [EVENT_VALIDATION_STARTED, EVENT_EXCLUDED],
                "excluded",
            ),
            (
                {},
                _make_request(bucket_v2="invalid"),
                [EVENT_VALIDATION_STARTED, EVENT_CANDIDATE_SKIPPED],
                "not_candidate",
            ),
            (
                {"execution_policy": NetworkExecutionPolicy.disabled()},
                _make_request(),
                [
                    EVENT_VALIDATION_STARTED,
                    EVENT_VALIDATION_BLOCKED_BY_POLICY,
                ],
                "network_blocked",
            ),
            (
                {"rate_limiter": InMemoryRateLimiter(
                    RateLimitPolicy(
                        max_per_domain_per_minute=0,
                        max_global_per_minute=99,
                    )
                )},
                _make_request(),
                [
                    EVENT_VALIDATION_STARTED,
                    EVENT_VALIDATION_BLOCKED_BY_POLICY,
                ],
                "network_blocked",
            ),
        ],
    )
    def test_smtp_skipped_when_gates_block(
        self,
        engine_kwargs: dict[str, Any],
        probe_request: ValidationRequest,
        expected_events: list[str],
        expected_reason: str,
    ) -> None:
        sink = InMemoryTelemetrySink()
        smtp = _CountingSMTPClient()
        engine = _wire_engine(
            smtp_client=smtp,
            telemetry_sink=sink,
            **engine_kwargs,
        )

        result = engine.validate(probe_request)

        assert smtp.calls == []
        assert result.smtp_status == "not_attempted"
        assert result.smtp_code is None
        assert result.smtp_latency is None
        assert result.smtp_error_type is None
        assert _smtp_steps(result)[-1]["decision"] == "skipped"
        assert _smtp_steps(result)[-1]["reason"] == expected_reason
        assert _event_types(sink) == expected_events

    def test_rate_limited_decision_is_preserved(self) -> None:
        smtp = _CountingSMTPClient()
        engine = _wire_engine(
            smtp_client=smtp,
            rate_limiter=InMemoryRateLimiter(
                RateLimitPolicy(max_per_domain_per_minute=0, max_global_per_minute=99)
            ),
        )

        result = engine.validate(_make_request())

        assert result.execution_decision == {
            "allowed": False,
            "reason": EXECUTION_REASON_RATE_LIMITED,
        }
        assert smtp.calls == []

    def test_network_disabled_decision_is_preserved(self) -> None:
        smtp = _CountingSMTPClient()
        engine = _wire_engine(
            smtp_client=smtp,
            execution_policy=NetworkExecutionPolicy.disabled(),
        )

        result = engine.validate(_make_request())

        assert result.execution_decision == {
            "allowed": False,
            "reason": EXECUTION_REASON_NETWORK_DISABLED,
        }
        assert smtp.calls == []

    def test_smtp_disabled_by_validation_policy_skips_probe(self) -> None:
        sink = InMemoryTelemetrySink()
        smtp = _CountingSMTPClient()
        engine = _wire_engine(
            policy=ValidationPolicy(enable_smtp_probing=False),
            smtp_client=smtp,
            telemetry_sink=sink,
        )

        result = engine.validate(_make_request())

        assert smtp.calls == []
        assert _smtp_steps(result)[-1]["reason"] == "smtp_disabled_by_policy"
        assert _event_types(sink) == [
            EVENT_VALIDATION_STARTED,
            EVENT_VALIDATION_ALLOWED,
        ]

    def test_no_retries_or_multiple_calls_per_request(self) -> None:
        smtp = _CountingSMTPClient(
            SMTPProbeResult(
                success=False,
                code=None,
                message="timeout",
                latency_ms=2.0,
                error_type="timeout",
            )
        )
        engine = _wire_engine(smtp_client=smtp)

        result = engine.validate(_make_request())

        assert len(smtp.calls) == 1
        assert result.smtp_status == "uncertain"
        assert result.smtp_error_type == "timeout"

    def test_engine_survives_smtp_client_exception(self) -> None:
        sink = InMemoryTelemetrySink()
        smtp = _CountingSMTPClient(exc=RuntimeError("boom"))
        engine = _wire_engine(smtp_client=smtp, telemetry_sink=sink)

        result = engine.validate(_make_request())

        assert len(smtp.calls) == 1
        assert result.smtp_status == "uncertain"
        assert result.smtp_code is None
        assert result.smtp_error_type == "protocol_error"
        assert _event_types(sink) == [
            EVENT_VALIDATION_STARTED,
            EVENT_VALIDATION_ALLOWED,
            EVENT_SMTP_PROBE_STARTED,
            EVENT_SMTP_PROBE_FAILED,
            "smtp_retry_skipped",
        ]
