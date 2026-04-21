"""Tests for Validation Engine V2 control-plane behavior.

Scope:
  * Structured telemetry events and sink resilience.
  * Per-domain and global rate limiting with deterministic time.
  * Execution-policy branch coverage.
  * Decision trace shape, order, and terminal decisions.
  * Engine integration across excluded, skipped, allowed, and
    rate-limited paths.

No SMTP, catch-all, retry, or network behavior is allowed in this
subphase; exploding fakes are wired anywhere those interfaces exist.
"""

from __future__ import annotations

from typing import Any

import pytest

from app.validation_v2 import (
    CatchAllAnalyzer,
    RetryStrategy,
    SMTPProbeClient,
    ValidationEngineV2,
    ValidationPolicy,
    ValidationRequest,
    ValidationStatus,
)
from app.validation_v2.control import (
    EVENT_CANDIDATE_SKIPPED,
    EVENT_EXCLUDED,
    EVENT_VALIDATION_ALLOWED,
    EVENT_VALIDATION_BLOCKED_BY_POLICY,
    EVENT_VALIDATION_STARTED,
    EXECUTION_REASON_ALLOWED,
    EXECUTION_REASON_EXCLUDED,
    EXECUTION_REASON_NETWORK_DISABLED,
    EXECUTION_REASON_NOT_CANDIDATE,
    EXECUTION_REASON_RATE_LIMITED,
    InMemoryRateLimiter,
    InMemoryTelemetrySink,
    NetworkExecutionPolicy,
    RateLimitPolicy,
    TelemetryEvent,
    TelemetrySink,
    is_validation_allowed,
)
from app.validation_v2.control.decision_trace import (
    STAGE_DOMAIN_INTELLIGENCE,
    STAGE_EXCLUSION,
    STAGE_EXECUTION_POLICY,
    STAGE_HISTORICAL_INTELLIGENCE,
    STAGE_PROVIDER_REPUTATION,
    STAGE_PROBABILITY,
    STAGE_RATE_LIMIT,
    STAGE_SMTP_PROBE,
)
from app.validation_v2.services import (
    DefaultExclusionService,
    DefaultValidationCandidateSelector,
    DomainCacheStore,
    SimpleDomainIntelligenceService,
    SimpleProviderReputationService,
)


STAGE_CANDIDATE_SELECTION = "candidate_selection"
STAGE_HISTORICAL_WRITE = "historical_write"
STAGE_PROBABILITY_HISTORY_ADJUSTMENT = "probability_history_adjustment"


class _ManualClock:
    def __init__(self, start: float = 0.0) -> None:
        self.now = float(start)

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += float(seconds)


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


class _ExplodingSmtp(SMTPProbeClient):
    def probe(self, request):  # pragma: no cover - must never be called
        raise AssertionError("SMTP probe must not be called in Subphase 3")


class _ExplodingCatchAll(CatchAllAnalyzer):
    def assess(self, domain, probe_result):  # pragma: no cover
        raise AssertionError("catch-all must not be called in Subphase 3")


class _ExplodingRetry(RetryStrategy):
    def decide(self, probe_result):  # pragma: no cover
        raise AssertionError("retry must not be called in Subphase 3")


class _FailingTelemetrySink(TelemetrySink):
    def emit(self, event: TelemetryEvent) -> None:
        raise RuntimeError("telemetry sink exploded")


def _wire_engine(
    *,
    policy: ValidationPolicy | None = None,
    execution_policy: NetworkExecutionPolicy | None = None,
    telemetry_sink: TelemetrySink | None = None,
    rate_limiter: InMemoryRateLimiter | None = None,
) -> ValidationEngineV2:
    cache = DomainCacheStore()
    engine = ValidationEngineV2(
        policy or ValidationPolicy(),
        domain_intel=SimpleDomainIntelligenceService(cache=cache),
        provider_reputation=SimpleProviderReputationService(cache=cache),
        exclusion_service=DefaultExclusionService(),
        candidate_selector=DefaultValidationCandidateSelector(),
        smtp_client=_ExplodingSmtp(),
        catch_all_analyzer=_ExplodingCatchAll(),
        retry_strategy=_ExplodingRetry(),
    )
    engine.execution_policy = execution_policy
    engine.telemetry_sink = telemetry_sink
    engine.rate_limiter = rate_limiter
    return engine


def _event_types(sink: InMemoryTelemetrySink) -> list[str]:
    return [event.event_type for event in sink.events]


def _trace_steps(result) -> list[dict[str, Any]]:
    trace = result.decision_trace
    assert isinstance(trace, dict)
    assert isinstance(trace.get("steps"), list)
    return trace["steps"]


def _assert_trace_step_shape(result) -> None:
    steps = _trace_steps(result)
    assert steps
    for step in steps:
        assert set(step) == {"stage", "decision", "reason", "inputs"}
        assert isinstance(step["stage"], str)
        assert isinstance(step["decision"], str)
        assert isinstance(step["reason"], str)
        assert isinstance(step["inputs"], dict)


def _stages(result) -> list[str]:
    return [step["stage"] for step in _trace_steps(result)]


class TestTelemetry:
    def test_sink_collects_events_in_order_and_returns_copy(self) -> None:
        sink = InMemoryTelemetrySink()
        first = TelemetryEvent("first", 1.0, "a.test", {"n": 1})
        second = TelemetryEvent("second", 2.0, "b.test", {"n": 2})

        sink.emit(first)
        sink.emit(second)
        copy = sink.events
        copy.clear()

        assert len(sink) == 2
        assert sink.event_types() == ["first", "second"]
        assert sink.events == [first, second]

    def test_engine_emits_started_and_excluded_events(self) -> None:
        sink = InMemoryTelemetrySink()
        engine = _wire_engine(
            policy=ValidationPolicy(excluded_domains={"blocked.test"}),
            telemetry_sink=sink,
        )

        engine.validate(_make_request(domain="blocked.test"))

        assert _event_types(sink) == [
            EVENT_VALIDATION_STARTED,
            EVENT_EXCLUDED,
        ]
        assert sink.events[-1].domain == "blocked.test"
        assert sink.events[-1].metadata["reason"] == "excluded_domain"

    def test_engine_emits_started_and_candidate_skipped_events(self) -> None:
        sink = InMemoryTelemetrySink()
        engine = _wire_engine(telemetry_sink=sink)

        engine.validate(_make_request(bucket_v2="invalid"))

        assert _event_types(sink) == [
            EVENT_VALIDATION_STARTED,
            EVENT_CANDIDATE_SKIPPED,
        ]
        assert sink.events[-1].metadata["reason"] == "bucket_not_allowed"

    def test_engine_emits_validation_allowed_event(self) -> None:
        sink = InMemoryTelemetrySink()
        engine = _wire_engine(
            telemetry_sink=sink,
            execution_policy=NetworkExecutionPolicy.network_only(),
        )

        engine.validate(_make_request())

        assert _event_types(sink) == [
            EVENT_VALIDATION_STARTED,
            EVENT_VALIDATION_ALLOWED,
        ]
        assert sink.events[-1].metadata["reason"] == EXECUTION_REASON_ALLOWED

    def test_engine_emits_validation_blocked_by_policy_event(self) -> None:
        sink = InMemoryTelemetrySink()
        engine = _wire_engine(
            telemetry_sink=sink,
            execution_policy=NetworkExecutionPolicy.disabled(),
        )

        engine.validate(_make_request())

        assert _event_types(sink) == [
            EVENT_VALIDATION_STARTED,
            EVENT_VALIDATION_BLOCKED_BY_POLICY,
        ]
        assert sink.events[-1].metadata["reason"] == (
            EXECUTION_REASON_NETWORK_DISABLED
        )

    def test_engine_does_not_crash_if_sink_emit_raises(self) -> None:
        engine = _wire_engine(
            telemetry_sink=_FailingTelemetrySink(),
            execution_policy=NetworkExecutionPolicy.network_only(),
        )

        result = engine.validate(_make_request())

        assert result.execution_decision == {
            "allowed": True,
            "reason": EXECUTION_REASON_ALLOWED,
        }


class TestRateLimiter:
    def test_per_domain_allows_first_n_and_blocks_afterwards(self) -> None:
        limiter = InMemoryRateLimiter(
            RateLimitPolicy(max_per_domain_per_minute=2, max_global_per_minute=99)
        )

        assert limiter.allow("example.com") is True
        assert limiter.allow("example.com") is True
        assert limiter.allow("example.com") is False
        assert limiter.allow("other.com") is True

    def test_global_limit_allows_until_cap_then_blocks_all_domains(self) -> None:
        limiter = InMemoryRateLimiter(
            RateLimitPolicy(max_per_domain_per_minute=99, max_global_per_minute=2)
        )

        assert limiter.allow("a.test") is True
        assert limiter.allow("b.test") is True
        assert limiter.allow("c.test") is False
        assert limiter.allow("a.test") is False

    def test_requests_allowed_again_after_window_passes(self) -> None:
        clock = _ManualClock()
        limiter = InMemoryRateLimiter(
            RateLimitPolicy(max_per_domain_per_minute=1, max_global_per_minute=1),
            time_source=clock,
        )

        assert limiter.allow("example.com") is True
        assert limiter.allow("example.com") is False

        clock.advance(60.1)

        assert limiter.allow("example.com") is True
        assert limiter.domain_count("example.com") == 1
        assert limiter.global_count() == 1


class TestExecutionPolicy:
    @pytest.mark.parametrize(
        "policy,candidate,exclusion,expected_allowed,expected_reason",
        [
            (
                NetworkExecutionPolicy.network_only(),
                True,
                "excluded_domain",
                False,
                EXECUTION_REASON_EXCLUDED,
            ),
            (
                NetworkExecutionPolicy.network_only(),
                False,
                None,
                False,
                EXECUTION_REASON_NOT_CANDIDATE,
            ),
            (
                NetworkExecutionPolicy.disabled(),
                True,
                None,
                False,
                EXECUTION_REASON_NETWORK_DISABLED,
            ),
            (
                NetworkExecutionPolicy.network_only(),
                True,
                None,
                True,
                EXECUTION_REASON_ALLOWED,
            ),
        ],
    )
    def test_all_execution_policy_branches(
        self,
        policy: NetworkExecutionPolicy,
        candidate: bool,
        exclusion: str | None,
        expected_allowed: bool,
        expected_reason: str,
    ) -> None:
        allowed, reason = is_validation_allowed(policy, candidate, exclusion)

        assert allowed is expected_allowed
        assert reason == expected_reason


class TestDecisionTrace:
    def test_trace_exists_on_all_terminal_paths(self) -> None:
        cases = [
            _wire_engine(
                policy=ValidationPolicy(excluded_domains={"blocked.test"}),
                execution_policy=NetworkExecutionPolicy.network_only(),
            ).validate(_make_request(domain="blocked.test")),
            _wire_engine(
                execution_policy=NetworkExecutionPolicy.network_only(),
            ).validate(_make_request(bucket_v2="invalid")),
            _wire_engine(
                execution_policy=NetworkExecutionPolicy.network_only(),
            ).validate(_make_request()),
        ]

        for result in cases:
            assert result.decision_trace
            _assert_trace_step_shape(result)

    def test_allowed_trace_has_complete_ordered_control_stages(self) -> None:
        limiter = InMemoryRateLimiter(
            RateLimitPolicy(max_per_domain_per_minute=5, max_global_per_minute=5)
        )
        engine = _wire_engine(
            rate_limiter=limiter,
            execution_policy=NetworkExecutionPolicy.network_only(),
        )

        result = engine.validate(_make_request())

        assert _stages(result) == [
            STAGE_DOMAIN_INTELLIGENCE,
            STAGE_PROVIDER_REPUTATION,
            STAGE_HISTORICAL_INTELLIGENCE,
            STAGE_EXCLUSION,
            STAGE_CANDIDATE_SELECTION,
            STAGE_RATE_LIMIT,
            STAGE_EXECUTION_POLICY,
            STAGE_SMTP_PROBE,
            STAGE_PROBABILITY_HISTORY_ADJUSTMENT,
            STAGE_PROBABILITY,
            STAGE_HISTORICAL_WRITE,
        ]
        _assert_trace_step_shape(result)

    def test_excluded_trace_short_circuits_after_exclusion(self) -> None:
        engine = _wire_engine(
            policy=ValidationPolicy(excluded_domains={"blocked.test"}),
            execution_policy=NetworkExecutionPolicy.network_only(),
        )

        result = engine.validate(_make_request(domain="blocked.test"))

        assert _stages(result) == [
            STAGE_DOMAIN_INTELLIGENCE,
            STAGE_PROVIDER_REPUTATION,
            STAGE_HISTORICAL_INTELLIGENCE,
            STAGE_EXCLUSION,
            STAGE_SMTP_PROBE,
            STAGE_HISTORICAL_WRITE,
        ]
        steps = _trace_steps(result)
        stages = _stages(result)
        assert steps[stages.index(STAGE_EXCLUSION)]["decision"] == "excluded"
        assert steps[stages.index(STAGE_SMTP_PROBE)]["decision"] == "skipped"

    def test_skipped_trace_short_circuits_after_candidate_selection(self) -> None:
        engine = _wire_engine(
            execution_policy=NetworkExecutionPolicy.network_only(),
        )

        result = engine.validate(_make_request(bucket_v2="invalid"))

        assert _stages(result) == [
            STAGE_DOMAIN_INTELLIGENCE,
            STAGE_PROVIDER_REPUTATION,
            STAGE_HISTORICAL_INTELLIGENCE,
            STAGE_EXCLUSION,
            STAGE_CANDIDATE_SELECTION,
            STAGE_SMTP_PROBE,
            STAGE_HISTORICAL_WRITE,
        ]
        steps = _trace_steps(result)
        stages = _stages(result)
        assert steps[stages.index(STAGE_CANDIDATE_SELECTION)]["decision"] == "rejected"
        assert steps[stages.index(STAGE_SMTP_PROBE)]["decision"] == "skipped"


class TestEngineIntegration:
    def test_telemetry_and_trace_populated_for_excluded_skipped_and_allowed(
        self,
    ) -> None:
        cases = [
            (
                _wire_engine(
                    policy=ValidationPolicy(excluded_domains={"blocked.test"}),
                    telemetry_sink=InMemoryTelemetrySink(),
                    execution_policy=NetworkExecutionPolicy.network_only(),
                ),
                _make_request(domain="blocked.test"),
                [EVENT_VALIDATION_STARTED, EVENT_EXCLUDED],
                False,
                "excluded",
            ),
            (
                _wire_engine(
                    telemetry_sink=InMemoryTelemetrySink(),
                    execution_policy=NetworkExecutionPolicy.network_only(),
                ),
                _make_request(bucket_v2="invalid"),
                [EVENT_VALIDATION_STARTED, EVENT_CANDIDATE_SKIPPED],
                False,
                "not_candidate",
            ),
            (
                _wire_engine(
                    telemetry_sink=InMemoryTelemetrySink(),
                    execution_policy=NetworkExecutionPolicy.network_only(),
                ),
                _make_request(),
                [EVENT_VALIDATION_STARTED, EVENT_VALIDATION_ALLOWED],
                True,
                EXECUTION_REASON_ALLOWED,
            ),
        ]

        for engine, request, expected_events, expected_allowed, reason in cases:
            result = engine.validate(request)
            sink = engine.telemetry_sink
            assert isinstance(sink, InMemoryTelemetrySink)
            assert _event_types(sink) == expected_events
            assert result.decision_trace
            assert result.execution_decision == {
                "allowed": expected_allowed,
                "reason": reason,
            }

    def test_rate_limiter_blocks_execution_and_emits_policy_block(self) -> None:
        sink = InMemoryTelemetrySink()
        limiter = InMemoryRateLimiter(
            RateLimitPolicy(max_per_domain_per_minute=1, max_global_per_minute=99)
        )
        engine = _wire_engine(
            telemetry_sink=sink,
            rate_limiter=limiter,
            execution_policy=NetworkExecutionPolicy.network_only(),
        )

        first = engine.validate(_make_request())
        second = engine.validate(_make_request())

        assert first.execution_decision == {
            "allowed": True,
            "reason": EXECUTION_REASON_ALLOWED,
        }
        assert second.execution_decision == {
            "allowed": False,
            "reason": EXECUTION_REASON_RATE_LIMITED,
        }
        assert _event_types(sink) == [
            EVENT_VALIDATION_STARTED,
            EVENT_VALIDATION_ALLOWED,
            EVENT_VALIDATION_STARTED,
            EVENT_VALIDATION_BLOCKED_BY_POLICY,
        ]
        stages = _stages(second)
        assert stages[-1] == STAGE_HISTORICAL_WRITE
        rate_step = _trace_steps(second)[stages.index(STAGE_RATE_LIMIT)]
        smtp_step = _trace_steps(second)[stages.index(STAGE_SMTP_PROBE)]
        assert rate_step["decision"] == "blocked"
        assert smtp_step["decision"] == "skipped"
        assert STAGE_PROBABILITY in stages

    def test_execution_decision_matches_network_policy(self) -> None:
        engine = _wire_engine(
            execution_policy=NetworkExecutionPolicy.disabled(),
        )

        result = engine.validate(_make_request())

        assert result.execution_decision == {
            "allowed": False,
            "reason": EXECUTION_REASON_NETWORK_DISABLED,
        }
        assert result.validation_status in {
            "valid",
            "likely_valid",
            "uncertain",
            "risky",
            "invalid",
        }

    def test_no_smtp_or_network_interfaces_are_called_on_any_path(self) -> None:
        excluded = _wire_engine(
            policy=ValidationPolicy(excluded_domains={"blocked.test"}),
            execution_policy=NetworkExecutionPolicy.network_only(),
        )
        skipped = _wire_engine(
            execution_policy=NetworkExecutionPolicy.network_only(),
        )
        allowed = _wire_engine(
            execution_policy=NetworkExecutionPolicy.network_only(),
        )
        limited = _wire_engine(
            rate_limiter=InMemoryRateLimiter(
                RateLimitPolicy(
                    max_per_domain_per_minute=0,
                    max_global_per_minute=99,
                )
            ),
            execution_policy=NetworkExecutionPolicy.network_only(),
        )

        excluded.validate(_make_request(domain="blocked.test"))
        skipped.validate(_make_request(bucket_v2="invalid"))
        allowed.validate(_make_request())
        limited.validate(_make_request())
