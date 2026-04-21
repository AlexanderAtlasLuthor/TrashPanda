"""Foundational tests for validation_v2 primitives.

Scope:
  * ValidationRequest — construction, type enforcement, immutability.
  * ValidationResult — defaults, to_dict() JSON-serializability.
  * ValidationPolicy — defaults, override ergonomics, immutability.
  * Interfaces — abstract classes cannot be instantiated; concrete
    subclasses must implement every method.
  * ValidationEngineV2 — excluded path, candidate-skip path, missing
    services degrade gracefully, engine does not mutate the request,
    output structure is consistent across paths.

Out of scope (per subphase spec):
  * SMTP probing. No network calls. No real validation logic.
"""

from __future__ import annotations

import dataclasses
import json
from typing import Any

import pytest

from app.validation_v2 import (
    CatchAllAnalyzer,
    CatchAllStatus,
    DomainIntelligenceService,
    ExclusionService,
    ProviderReputationService,
    ReasonCode,
    RetryStrategy,
    SMTPProbeClient,
    SmtpProbeStatus,
    TelemetrySink,
    ValidationCandidateSelector,
    ValidationEngineV2,
    ValidationPolicy,
    ValidationRequest,
    ValidationResult,
    ValidationStatus,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


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


class _ExcludeAll(ExclusionService):
    def is_excluded(self, request, policy):
        return True


class _ExcludeNone(ExclusionService):
    def is_excluded(self, request, policy):
        return False


class _CandidateNo(ValidationCandidateSelector):
    def should_validate(self, request, policy):
        return False


class _CandidateYes(ValidationCandidateSelector):
    def should_validate(self, request, policy):
        return True


class _Intel(DomainIntelligenceService):
    def __init__(self) -> None:
        self.calls: list[str] = []

    def analyze(self, domain: str) -> dict[str, Any]:
        self.calls.append(domain)
        return {"mx": True, "domain": domain}


class _Reputation(ProviderReputationService):
    def classify(self, domain: str) -> dict[str, Any]:
        return {"provider": "acme", "score": 0.9, "domain": domain}


class _RecordingTelemetry(TelemetrySink):
    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    def emit(self, event: dict[str, Any]) -> None:
        self.events.append(event)


class _ExplodingTelemetry(TelemetrySink):
    def emit(self, event: dict[str, Any]) -> None:
        raise RuntimeError("boom")


# ---------------------------------------------------------------------------
# ValidationRequest
# ---------------------------------------------------------------------------


class TestValidationRequest:
    def test_valid_creation(self) -> None:
        r = _make_request()
        assert r.email == "alice@example.com"
        assert r.domain == "example.com"
        assert r.corrected_domain is None
        assert r.syntax_valid is True
        assert r.domain_present is True
        assert r.score_v2 == pytest.approx(0.6)
        assert r.confidence_v2 == pytest.approx(0.7)
        assert r.bucket_v2 == "review"
        assert r.reason_codes_v2 == ("mx_present",)

    def test_metadata_defaults_empty_and_readonly(self) -> None:
        r = _make_request()
        assert dict(r.metadata) == {}
        # Read-only view: setitem must fail.
        with pytest.raises(TypeError):
            r.metadata["key"] = "value"  # type: ignore[index]

    def test_metadata_wraps_incoming_dict(self) -> None:
        source = {"dns_cached": True}
        r = _make_request(metadata=source)
        assert r.metadata["dns_cached"] is True
        # Mutating the source must not mutate the request's view.
        source["dns_cached"] = False
        assert r.metadata["dns_cached"] is True

    @pytest.mark.parametrize(
        "field,bad_value",
        [
            ("email", 123),
            ("domain", None),
            ("syntax_valid", "yes"),
            ("domain_present", 1),  # int, not bool
            ("score_v2", "0.5"),
            ("score_v2", True),  # bool leaks through int checks
            ("confidence_v2", None),
            ("bucket_v2", 42),
            ("reason_codes_v2", ["mx_present"]),  # list, not tuple
            ("reason_codes_v2", ("ok", 5)),
            ("corrected_domain", 3),
        ],
    )
    def test_invalid_types_fail(self, field: str, bad_value: Any) -> None:
        with pytest.raises(TypeError):
            _make_request(**{field: bad_value})

    def test_immutability(self) -> None:
        r = _make_request()
        with pytest.raises(dataclasses.FrozenInstanceError):
            r.email = "other@example.com"  # type: ignore[misc]
        with pytest.raises(dataclasses.FrozenInstanceError):
            r.score_v2 = 0.0  # type: ignore[misc]

    def test_reason_codes_must_be_tuple_of_str(self) -> None:
        with pytest.raises(TypeError):
            _make_request(reason_codes_v2=("ok", 5))


# ---------------------------------------------------------------------------
# ValidationResult
# ---------------------------------------------------------------------------


class TestValidationResult:
    def test_defaults_valid(self) -> None:
        r = ValidationResult()
        assert r.validation_status == ValidationStatus.DELIVERABLE_UNCERTAIN.value
        assert r.deliverability_probability == 0.0
        assert r.smtp_probe_status is None
        assert r.catch_all_status is None
        assert r.provider_reputation is None
        assert r.retry_recommended is False
        assert r.validation_reason_codes == ()
        assert r.validation_explanation == ""
        assert r.breakdown == {}
        assert r.metadata == {}

    def test_to_dict_structure(self) -> None:
        r = ValidationResult(
            validation_status=ValidationStatus.DELIVERABLE_LIKELY.value,
            deliverability_probability=0.42,
            smtp_probe_status=SmtpProbeStatus.NOT_ATTEMPTED.value,
            catch_all_status=CatchAllStatus.UNKNOWN.value,
            provider_reputation="acme",
            retry_recommended=True,
            validation_reason_codes=("a", "b"),
            validation_explanation="why",
            breakdown={"k": 1},
            metadata={"email": "x@y.com"},
        )
        d = r.to_dict()
        assert d["validation_status"] == "deliverable_likely"
        assert d["deliverability_probability"] == pytest.approx(0.42)
        assert d["smtp_probe_status"] == "not_attempted"
        assert d["catch_all_status"] == "unknown"
        assert d["provider_reputation"] == "acme"
        assert d["retry_recommended"] is True
        assert d["validation_reason_codes"] == ["a", "b"]
        assert d["validation_explanation"] == "why"
        assert d["breakdown"] == {"k": 1}
        assert d["metadata"] == {"email": "x@y.com"}

    def test_to_dict_json_serializable(self) -> None:
        r = ValidationResult(
            validation_reason_codes=("a",),
            breakdown={"nested": {"n": 1}},
            metadata={"x": [1, 2, 3]},
        )
        # json.dumps will raise if any non-serializable value leaked
        # through. No default= fallback is used — the result must
        # be natively serializable.
        payload = json.dumps(r.to_dict())
        reloaded = json.loads(payload)
        assert reloaded["breakdown"]["nested"]["n"] == 1
        assert reloaded["validation_reason_codes"] == ["a"]

    def test_to_dict_copies_breakdown_and_metadata(self) -> None:
        r = ValidationResult(breakdown={"k": 1}, metadata={"m": 2})
        d = r.to_dict()
        d["breakdown"]["k"] = 999
        d["metadata"]["m"] = 999
        assert r.breakdown["k"] == 1
        assert r.metadata["m"] == 2

    def test_to_dict_coerces_enum_members(self) -> None:
        # Callers may assign raw Enum members; to_dict() must still
        # produce plain strings in the output.
        r = ValidationResult(
            validation_status=ValidationStatus.RISKY_CATCH_ALL,  # type: ignore[arg-type]
            smtp_probe_status=SmtpProbeStatus.SKIPPED,  # type: ignore[arg-type]
            catch_all_status=CatchAllStatus.LIKELY,  # type: ignore[arg-type]
        )
        d = r.to_dict()
        assert d["validation_status"] == "risky_catch_all"
        assert d["smtp_probe_status"] == "skipped"
        assert d["catch_all_status"] == "likely"


# ---------------------------------------------------------------------------
# ValidationPolicy
# ---------------------------------------------------------------------------


class TestValidationPolicy:
    def test_defaults_valid(self) -> None:
        p = ValidationPolicy()
        assert p.enable_smtp_probing is False
        assert p.max_probes_per_domain >= 0
        assert p.max_probes_per_run >= 0
        assert p.retry_enabled is True
        assert p.max_retries >= 0
        assert p.cache_ttl_seconds >= 0
        assert isinstance(p.excluded_domains, frozenset)
        assert "review" in p.allow_validation_for_buckets

    def test_override_works_via_constructor(self) -> None:
        p = ValidationPolicy(
            enable_smtp_probing=True,
            max_probes_per_domain=10,
            excluded_domains={"a.com", "b.com"},
        )
        assert p.enable_smtp_probing is True
        assert p.max_probes_per_domain == 10
        assert p.excluded_domains == frozenset({"a.com", "b.com"})

    def test_override_via_with_overrides(self) -> None:
        p = ValidationPolicy()
        p2 = p.with_overrides(max_retries=5, retry_enabled=False)
        assert p.max_retries == 2  # unchanged
        assert p2.max_retries == 5
        assert p2.retry_enabled is False

    def test_immutability(self) -> None:
        p = ValidationPolicy()
        with pytest.raises(dataclasses.FrozenInstanceError):
            p.max_retries = 99  # type: ignore[misc]

    def test_set_inputs_are_frozen(self) -> None:
        raw = {"a.com"}
        p = ValidationPolicy(excluded_domains=raw)
        assert isinstance(p.excluded_domains, frozenset)
        # Mutating the original set must not affect the policy.
        raw.add("b.com")
        assert "b.com" not in p.excluded_domains

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"max_probes_per_domain": -1},
            {"max_probes_per_run": -5},
            {"max_retries": -1},
            {"cache_ttl_seconds": -1},
            {"strong_candidate_min_score": 1.5},
            {"strong_candidate_min_confidence": -0.1},
        ],
    )
    def test_invalid_values_rejected(self, kwargs: dict[str, Any]) -> None:
        with pytest.raises(ValueError):
            ValidationPolicy(**kwargs)


# ---------------------------------------------------------------------------
# Interfaces
# ---------------------------------------------------------------------------


class TestInterfaces:
    @pytest.mark.parametrize(
        "cls",
        [
            DomainIntelligenceService,
            ProviderReputationService,
            ExclusionService,
            ValidationCandidateSelector,
            SMTPProbeClient,
            CatchAllAnalyzer,
            RetryStrategy,
            TelemetrySink,
        ],
    )
    def test_abstract_classes_cannot_be_instantiated(self, cls: type) -> None:
        with pytest.raises(TypeError):
            cls()  # type: ignore[call-arg,abstract]

    def test_subclass_missing_method_cannot_be_instantiated(self) -> None:
        class HalfExclusion(ExclusionService):
            pass

        with pytest.raises(TypeError):
            HalfExclusion()  # type: ignore[abstract]

    def test_concrete_subclass_works(self) -> None:
        class OkExclusion(ExclusionService):
            def is_excluded(self, request, policy):
                return False

        inst = OkExclusion()
        assert inst.is_excluded(_make_request(), ValidationPolicy()) is False


# ---------------------------------------------------------------------------
# ValidationEngineV2
# ---------------------------------------------------------------------------


class TestValidationEngineV2:
    def test_excluded_path(self) -> None:
        engine = ValidationEngineV2(
            ValidationPolicy(),
            exclusion_service=_ExcludeAll(),
        )
        result = engine.validate(_make_request())
        assert result.validation_status == ValidationStatus.EXCLUDED_BY_POLICY.value
        assert result.deliverability_probability == 0.0
        assert result.smtp_probe_status == SmtpProbeStatus.SKIPPED.value
        assert ReasonCode.EXCLUDED_BY_POLICY.value in result.validation_reason_codes
        assert "excluded" in result.validation_explanation.lower()

    def test_candidate_skip_path(self) -> None:
        engine = ValidationEngineV2(
            ValidationPolicy(),
            candidate_selector=_CandidateNo(),
        )
        result = engine.validate(_make_request())
        assert (
            result.validation_status == ValidationStatus.DELIVERABLE_UNCERTAIN.value
        )
        assert result.smtp_probe_status == SmtpProbeStatus.SKIPPED.value
        assert ReasonCode.NOT_A_CANDIDATE.value in result.validation_reason_codes
        assert ReasonCode.VALIDATION_SKIPPED.value in result.validation_reason_codes

    def test_excluded_short_circuits_before_candidate(self) -> None:
        # If exclusion fires, candidate_selector must not be asked.
        class ExplodingSelector(ValidationCandidateSelector):
            def should_validate(self, request, policy):
                raise AssertionError("selector should not be called when excluded")

        engine = ValidationEngineV2(
            ValidationPolicy(),
            exclusion_service=_ExcludeAll(),
            candidate_selector=ExplodingSelector(),
        )
        # Should not raise.
        result = engine.validate(_make_request())
        assert result.validation_status == ValidationStatus.EXCLUDED_BY_POLICY.value

    def test_engine_handles_missing_services(self) -> None:
        # No services provided at all.
        engine = ValidationEngineV2(ValidationPolicy())
        result = engine.validate(_make_request())
        assert (
            result.validation_status == ValidationStatus.DELIVERABLE_UNCERTAIN.value
        )
        assert result.smtp_probe_status == SmtpProbeStatus.NOT_ATTEMPTED.value
        assert ReasonCode.VALIDATION_SKIPPED.value in result.validation_reason_codes
        assert ReasonCode.NO_ACTIVE_PROBE.value in result.validation_reason_codes
        assert result.provider_reputation is None
        assert result.breakdown == {}

    def test_intel_and_reputation_collected_into_breakdown(self) -> None:
        intel = _Intel()
        engine = ValidationEngineV2(
            ValidationPolicy(),
            domain_intel=intel,
            provider_reputation=_Reputation(),
            candidate_selector=_CandidateYes(),
            exclusion_service=_ExcludeNone(),
        )
        request = _make_request(domain="foo.example")
        result = engine.validate(request)
        assert intel.calls == ["foo.example"]
        assert result.breakdown["domain_intelligence"] == {
            "mx": True,
            "domain": "foo.example",
        }
        assert result.breakdown["provider_reputation"]["provider"] == "acme"
        assert result.provider_reputation == "acme"

    def test_does_not_mutate_request(self) -> None:
        request = _make_request(metadata={"k": "v"})
        snapshot = {
            "email": request.email,
            "domain": request.domain,
            "corrected_domain": request.corrected_domain,
            "syntax_valid": request.syntax_valid,
            "domain_present": request.domain_present,
            "score_v2": request.score_v2,
            "confidence_v2": request.confidence_v2,
            "bucket_v2": request.bucket_v2,
            "reason_codes_v2": request.reason_codes_v2,
            "metadata": dict(request.metadata),
        }
        engine = ValidationEngineV2(
            ValidationPolicy(),
            domain_intel=_Intel(),
            provider_reputation=_Reputation(),
        )
        engine.validate(request)
        assert request.email == snapshot["email"]
        assert request.domain == snapshot["domain"]
        assert request.corrected_domain == snapshot["corrected_domain"]
        assert request.syntax_valid == snapshot["syntax_valid"]
        assert request.domain_present == snapshot["domain_present"]
        assert request.score_v2 == snapshot["score_v2"]
        assert request.confidence_v2 == snapshot["confidence_v2"]
        assert request.bucket_v2 == snapshot["bucket_v2"]
        assert request.reason_codes_v2 == snapshot["reason_codes_v2"]
        assert dict(request.metadata) == snapshot["metadata"]

    def test_output_structure_consistent_across_paths(self) -> None:
        # Same set of top-level fields must be present regardless of
        # which path the engine took.
        expected_keys = {
            "validation_status",
            "deliverability_probability",
            "smtp_probe_status",
            "catch_all_status",
            "provider_reputation",
            "retry_recommended",
            "validation_reason_codes",
            "validation_explanation",
            "breakdown",
            "metadata",
        }
        policy = ValidationPolicy()
        paths = [
            ValidationEngineV2(policy, exclusion_service=_ExcludeAll()),
            ValidationEngineV2(policy, candidate_selector=_CandidateNo()),
            ValidationEngineV2(policy),
            ValidationEngineV2(
                policy,
                domain_intel=_Intel(),
                provider_reputation=_Reputation(),
            ),
        ]
        for engine in paths:
            d = engine.validate(_make_request()).to_dict()
            assert set(d.keys()) == expected_keys

    def test_engine_is_deterministic(self) -> None:
        engine = ValidationEngineV2(
            ValidationPolicy(),
            domain_intel=_Intel(),
            provider_reputation=_Reputation(),
        )
        req = _make_request()
        a = engine.validate(req).to_dict()
        b = engine.validate(req).to_dict()
        assert a == b

    def test_smtp_client_never_called_in_skeleton(self) -> None:
        class ExplodingSmtp(SMTPProbeClient):
            def probe(self, request):
                raise AssertionError("SMTP probe must not be called in skeleton")

        class ExplodingCatchAll(CatchAllAnalyzer):
            def assess(self, domain, probe_result):
                raise AssertionError("catch-all must not be called in skeleton")

        class ExplodingRetry(RetryStrategy):
            def decide(self, probe_result):
                raise AssertionError("retry must not be called in skeleton")

        engine = ValidationEngineV2(
            ValidationPolicy(enable_smtp_probing=True),
            smtp_client=ExplodingSmtp(),
            catch_all_analyzer=ExplodingCatchAll(),
            retry_strategy=ExplodingRetry(),
        )
        # Must not raise.
        result = engine.validate(_make_request())
        assert result.smtp_probe_status == SmtpProbeStatus.NOT_ATTEMPTED.value

    def test_telemetry_emitted_and_failures_swallowed(self) -> None:
        sink = _RecordingTelemetry()
        engine = ValidationEngineV2(ValidationPolicy(), telemetry=sink)
        engine.validate(_make_request())
        assert len(sink.events) == 1
        assert sink.events[0]["event"] == "skeleton_completed"

        # Exploding sink must not break validation.
        engine2 = ValidationEngineV2(
            ValidationPolicy(), telemetry=_ExplodingTelemetry()
        )
        result = engine2.validate(_make_request())
        assert (
            result.validation_status == ValidationStatus.DELIVERABLE_UNCERTAIN.value
        )

    def test_policy_accessor(self) -> None:
        policy = ValidationPolicy(max_retries=7)
        engine = ValidationEngineV2(policy)
        assert engine.policy is policy
