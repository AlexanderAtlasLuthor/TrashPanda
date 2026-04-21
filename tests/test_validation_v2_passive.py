"""Tests for the Validation Engine V2 passive intelligence layer.

Scope:
  * DomainCacheStore / PatternCacheStore (set/get, TTL expiration,
    record_domain helper, counter increments).
  * SimpleDomainIntelligenceService (common-provider detection,
    suspicious-pattern detection, cache behaviour).
  * SimpleProviderReputationService (classification, score ranges,
    cache hit behaviour).
  * DefaultExclusionService (policy list, invalid shape, syntax).
  * DefaultValidationCandidateSelector (bucket / score / confidence
    gates, explain() parity with should_validate()).
  * ValidationEngineV2 integration with the concrete services
    (services are called, metadata is populated, short-circuits
    still work, SMTP is never invoked).

No network, no DNS, no real validation logic.
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
from app.validation_v2.engine import (
    REASON_EXCLUDED_DOMAIN as ENGINE_REASON_EXCLUDED_DOMAIN,
    REASON_LOW_PRIORITY_CANDIDATE,
)
from app.validation_v2.services import (
    COMMON_PROVIDERS,
    CandidateDecision,
    DefaultExclusionService,
    DefaultValidationCandidateSelector,
    DomainCacheStore,
    DomainRecord,
    PatternCacheStore,
    REASON_EXCLUDED_DOMAIN,
    REASON_INVALID_DOMAIN,
    REASON_SYNTAX_INVALID,
    REPUTATION_SCORES,
    SimpleDomainIntelligenceService,
    SimpleProviderReputationService,
    TRUST_LEVELS,
)
from app.validation_v2.services.candidate_selector import (
    REASON_ACCEPTED,
    REASON_BUCKET_NOT_ALLOWED,
    REASON_CONFIDENCE_BELOW_THRESHOLD,
    REASON_SCORE_BELOW_THRESHOLD,
    REASON_SYNTAX_INVALID as CANDIDATE_REASON_SYNTAX_INVALID,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _ManualClock:
    """Monotonic-ish clock whose time is controlled by the test.

    Using an explicit clock instead of ``time.sleep`` keeps the
    TTL tests fast and reproducible.
    """

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


# ---------------------------------------------------------------------------
# Stores
# ---------------------------------------------------------------------------


class TestDomainCacheStore:
    def test_set_and_get(self) -> None:
        store = DomainCacheStore(default_ttl_seconds=60.0)
        record = DomainRecord(
            domain="foo.com",
            last_seen=0.0,
            provider_type="tier_1",
            reputation_score=0.9,
        )
        store.set("foo.com", record)
        fetched = store.get("foo.com")
        assert fetched is not None
        assert fetched.provider_type == "tier_1"
        assert fetched.reputation_score == pytest.approx(0.9)

    def test_get_missing_returns_none(self) -> None:
        store = DomainCacheStore()
        assert store.get("nope.example") is None

    def test_ttl_expiration(self) -> None:
        clock = _ManualClock(100.0)
        store = DomainCacheStore(default_ttl_seconds=10.0, time_source=clock)
        store.record_domain("foo.com", provider_type="tier_1")
        assert "foo.com" in store

        clock.advance(5.0)
        assert store.get("foo.com") is not None

        clock.advance(10.0)  # total elapsed = 15s, TTL = 10s
        assert store.get("foo.com") is None
        assert "foo.com" not in store

    def test_custom_ttl_overrides_default(self) -> None:
        clock = _ManualClock(100.0)
        store = DomainCacheStore(default_ttl_seconds=10.0, time_source=clock)
        store.record_domain("foo.com", provider_type="x", ttl_seconds=60.0)
        clock.advance(30.0)
        assert store.get("foo.com") is not None

    def test_ttl_zero_is_sticky(self) -> None:
        clock = _ManualClock(0.0)
        store = DomainCacheStore(default_ttl_seconds=10.0, time_source=clock)
        store.record_domain("sticky.com", provider_type="x", ttl_seconds=0.0)
        clock.advance(10_000.0)
        assert store.get("sticky.com") is not None

    def test_record_domain_preserves_counters(self) -> None:
        store = DomainCacheStore()
        store.increment("foo.com", "seen")
        store.record_domain("foo.com", provider_type="tier_1")
        assert store.get("foo.com").counters["seen"] == 1

    def test_increment_creates_record(self) -> None:
        store = DomainCacheStore()
        assert store.increment("new.com", "seen") == 1
        assert store.increment("new.com", "seen") == 2
        assert store.get("new.com").counters["seen"] == 2

    def test_record_domain_does_not_overwrite_existing_provider_with_none(
        self,
    ) -> None:
        store = DomainCacheStore()
        store.record_domain("foo.com", provider_type="tier_1")
        store.record_domain("foo.com", provider_type=None)
        assert store.get("foo.com").provider_type == "tier_1"

    def test_negative_ttl_rejected(self) -> None:
        with pytest.raises(ValueError):
            DomainCacheStore(default_ttl_seconds=-1.0)

    def test_accepts_dict_serialization(self) -> None:
        store = DomainCacheStore()
        # Simulate external state being seeded into the cache as
        # a plain dict (e.g. from a JSON restore).
        store._cache.set(  # type: ignore[attr-defined]
            "foo.com",
            {
                "domain": "foo.com",
                "last_seen": 1.0,
                "provider_type": "tier_1",
                "reputation_score": 0.9,
                "counters": {"seen": 3},
            },
        )
        record = store.get("foo.com")
        assert record is not None
        assert record.provider_type == "tier_1"
        assert record.counters == {"seen": 3}


class TestPatternCacheStore:
    def test_set_and_get(self) -> None:
        store = PatternCacheStore(default_ttl_seconds=60.0)
        store.set("suspicious/long", {"count": 5})
        assert store.get("suspicious/long") == {"count": 5}

    def test_ttl_expiration(self) -> None:
        clock = _ManualClock(0.0)
        store = PatternCacheStore(default_ttl_seconds=5.0, time_source=clock)
        store.set("k", "v")
        clock.advance(4.0)
        assert store.get("k") == "v"
        clock.advance(2.0)
        assert store.get("k") is None

    def test_delete(self) -> None:
        store = PatternCacheStore()
        store.set("k", "v")
        store.delete("k")
        assert store.get("k") is None

    def test_clear(self) -> None:
        store = PatternCacheStore()
        store.set("a", 1)
        store.set("b", 2)
        store.clear()
        assert len(store) == 0


# ---------------------------------------------------------------------------
# SimpleDomainIntelligenceService
# ---------------------------------------------------------------------------


class TestSimpleDomainIntelligenceService:
    def test_common_provider_detected(self) -> None:
        svc = SimpleDomainIntelligenceService()
        result = svc.analyze("gmail.com")
        assert result["is_common_provider"] is True
        assert result["provider_hint"] == "gmail"
        assert result["is_suspicious_pattern"] is False
        assert result["suspicious_reasons"] == []

    def test_common_provider_normalization(self) -> None:
        svc = SimpleDomainIntelligenceService()
        result = svc.analyze("Gmail.COM")
        assert result["domain"] == "gmail.com"
        assert result["is_common_provider"] is True

    @pytest.mark.parametrize(
        "domain,expected_reason",
        [
            ("a" * 50 + ".com", "very_long_domain"),
            ("a-b-c-d-e.com", "many_hyphens"),
            # digit ratio >= 0.5 over alnum chars: 7 digits / 10
            # alnum = 0.7, clearly above the default threshold.
            ("1234567.com", "numeric_heavy"),
        ],
    )
    def test_suspicious_patterns(
        self, domain: str, expected_reason: str
    ) -> None:
        svc = SimpleDomainIntelligenceService()
        result = svc.analyze(domain)
        assert result["is_suspicious_pattern"] is True
        assert expected_reason in result["suspicious_reasons"]

    def test_non_common_non_suspicious(self) -> None:
        svc = SimpleDomainIntelligenceService()
        result = svc.analyze("acme.io")
        assert result["is_common_provider"] is False
        assert result["is_suspicious_pattern"] is False
        assert result["provider_hint"] == "other"

    def test_cache_hit_reported(self) -> None:
        cache = DomainCacheStore()
        svc = SimpleDomainIntelligenceService(cache=cache)
        first = svc.analyze("gmail.com")
        assert first["cache_hit"] is False

        # Seed the cache with a reputation score and re-analyze.
        cache.record_domain(
            "gmail.com", provider_type="tier_1", reputation_score=0.9
        )
        second = svc.analyze("gmail.com")
        assert second["cache_hit"] is True
        assert second["historical_score"] == pytest.approx(0.9)

    def test_cache_not_required(self) -> None:
        svc = SimpleDomainIntelligenceService()
        result = svc.analyze("gmail.com")
        assert result["cache_hit"] is False
        assert result["historical_score"] is None

    def test_empty_domain_suspicious(self) -> None:
        svc = SimpleDomainIntelligenceService()
        result = svc.analyze("")
        assert result["is_suspicious_pattern"] is True
        assert "empty_domain" in result["suspicious_reasons"]

    def test_known_common_providers_enumerated(self) -> None:
        # Sanity check — renaming the set should force a test
        # update, which is the point.
        assert "gmail.com" in COMMON_PROVIDERS
        assert "outlook.com" in COMMON_PROVIDERS
        assert "icloud.com" in COMMON_PROVIDERS


# ---------------------------------------------------------------------------
# SimpleProviderReputationService
# ---------------------------------------------------------------------------


class TestSimpleProviderReputationService:
    @pytest.mark.parametrize(
        "domain,expected_type,expected_trust",
        [
            ("gmail.com", "tier_1", "high"),
            ("outlook.com", "tier_1", "high"),
            ("icloud.com", "tier_1", "high"),
            ("acme.io", "enterprise", "medium"),
            ("example.com", "enterprise", "medium"),
            ("random.xyz", "unknown", "medium"),
            ("a-b-c-d-e.com", "suspicious", "low"),
            ("", "suspicious", "low"),
        ],
    )
    def test_classification(
        self, domain: str, expected_type: str, expected_trust: str
    ) -> None:
        svc = SimpleProviderReputationService()
        result = svc.classify(domain)
        assert result["provider_type"] == expected_type
        assert result["trust_level"] == expected_trust

    def test_score_range_valid_for_all_types(self) -> None:
        for provider_type, score in REPUTATION_SCORES.items():
            assert 0.0 <= score <= 1.0, provider_type

    def test_trust_level_set_matches_reputation_scores(self) -> None:
        assert set(TRUST_LEVELS.keys()) == set(REPUTATION_SCORES.keys())

    def test_tier_1_scores_higher_than_unknown(self) -> None:
        assert REPUTATION_SCORES["tier_1"] > REPUTATION_SCORES["unknown"]
        assert REPUTATION_SCORES["unknown"] > REPUTATION_SCORES["suspicious"]
        assert REPUTATION_SCORES["enterprise"] > REPUTATION_SCORES["unknown"]

    def test_cache_hit(self) -> None:
        cache = DomainCacheStore()
        svc = SimpleProviderReputationService(cache=cache)
        first = svc.classify("gmail.com")
        assert first["cache_hit"] is False
        second = svc.classify("gmail.com")
        assert second["cache_hit"] is True
        assert second["provider_type"] == "tier_1"
        assert second["reputation_score"] == pytest.approx(
            REPUTATION_SCORES["tier_1"]
        )


# ---------------------------------------------------------------------------
# DefaultExclusionService
# ---------------------------------------------------------------------------


class TestDefaultExclusionService:
    def test_excluded_domain_rule(self) -> None:
        svc = DefaultExclusionService()
        policy = ValidationPolicy(excluded_domains={"bad.com"})
        assert svc.is_excluded(_make_request(domain="bad.com"), policy) is True
        assert svc.check(_make_request(domain="bad.com"), policy) == (
            REASON_EXCLUDED_DOMAIN
        )

    def test_valid_domain_passes(self) -> None:
        svc = DefaultExclusionService()
        policy = ValidationPolicy()
        assert svc.is_excluded(_make_request(), policy) is False
        assert svc.check(_make_request(), policy) is None

    @pytest.mark.parametrize(
        "domain,domain_present",
        [
            ("", False),
            ("nodot", True),
            (" space.com", True),
            (".leading.com", True),
            ("trailing.", True),
            ("-leading-hyphen.com", True),
            ("trailing-hyphen-.com", True),
        ],
    )
    def test_invalid_domain_shape(
        self, domain: str, domain_present: bool
    ) -> None:
        svc = DefaultExclusionService()
        policy = ValidationPolicy()
        req = _make_request(domain=domain, domain_present=domain_present)
        assert svc.check(req, policy) == REASON_INVALID_DOMAIN

    def test_syntax_invalid_rule(self) -> None:
        svc = DefaultExclusionService()
        policy = ValidationPolicy()
        req = _make_request(syntax_valid=False)
        assert svc.check(req, policy) == REASON_SYNTAX_INVALID

    def test_rule_order(self) -> None:
        # An excluded-domain match wins over a syntax-invalid flag.
        svc = DefaultExclusionService()
        policy = ValidationPolicy(excluded_domains={"example.com"})
        req = _make_request(syntax_valid=False)
        assert svc.check(req, policy) == REASON_EXCLUDED_DOMAIN


# ---------------------------------------------------------------------------
# DefaultValidationCandidateSelector
# ---------------------------------------------------------------------------


class TestDefaultValidationCandidateSelector:
    def test_review_rows_accepted(self) -> None:
        svc = DefaultValidationCandidateSelector()
        policy = ValidationPolicy()
        req = _make_request(bucket_v2="review", score_v2=0.6, confidence_v2=0.7)
        decision = svc.explain(req, policy)
        assert decision == CandidateDecision(True, REASON_ACCEPTED)
        assert svc.should_validate(req, policy) is True

    def test_bucket_not_allowed(self) -> None:
        svc = DefaultValidationCandidateSelector()
        policy = ValidationPolicy()
        req = _make_request(bucket_v2="invalid")
        decision = svc.explain(req, policy)
        assert decision.accepted is False
        assert decision.reason == REASON_BUCKET_NOT_ALLOWED

    def test_syntax_invalid_rejected(self) -> None:
        svc = DefaultValidationCandidateSelector()
        policy = ValidationPolicy()
        req = _make_request(syntax_valid=False)
        decision = svc.explain(req, policy)
        assert decision.accepted is False
        assert decision.reason == CANDIDATE_REASON_SYNTAX_INVALID

    def test_score_below_threshold(self) -> None:
        svc = DefaultValidationCandidateSelector()
        policy = ValidationPolicy(strong_candidate_min_score=0.5)
        req = _make_request(score_v2=0.1)
        decision = svc.explain(req, policy)
        assert decision.reason == REASON_SCORE_BELOW_THRESHOLD

    def test_confidence_below_threshold(self) -> None:
        svc = DefaultValidationCandidateSelector()
        policy = ValidationPolicy(
            strong_candidate_min_score=0.0,
            strong_candidate_min_confidence=0.8,
        )
        req = _make_request(score_v2=0.6, confidence_v2=0.1)
        decision = svc.explain(req, policy)
        assert decision.reason == REASON_CONFIDENCE_BELOW_THRESHOLD

    def test_explain_matches_should_validate(self) -> None:
        svc = DefaultValidationCandidateSelector()
        policy = ValidationPolicy(
            strong_candidate_min_score=0.5,
            strong_candidate_min_confidence=0.5,
        )
        cases = [
            _make_request(),
            _make_request(bucket_v2="invalid"),
            _make_request(syntax_valid=False),
            _make_request(score_v2=0.0),
            _make_request(confidence_v2=0.0),
        ]
        for req in cases:
            assert (
                svc.explain(req, policy).accepted
                == svc.should_validate(req, policy)
            )


# ---------------------------------------------------------------------------
# Engine integration with concrete services
# ---------------------------------------------------------------------------


class _ExplodingSmtp(SMTPProbeClient):
    def probe(self, request):  # pragma: no cover - should never be called
        raise AssertionError("SMTP probe must not be called in passive subphase")


class _ExplodingCatchAll(CatchAllAnalyzer):
    def assess(self, domain, probe_result):  # pragma: no cover
        raise AssertionError("catch-all must not be called in passive subphase")


class _ExplodingRetry(RetryStrategy):
    def decide(self, probe_result):  # pragma: no cover
        raise AssertionError("retry must not be called in passive subphase")


def _wire_full_passive_engine(
    *,
    policy: ValidationPolicy | None = None,
    cache: DomainCacheStore | None = None,
) -> ValidationEngineV2:
    cache = cache if cache is not None else DomainCacheStore()
    return ValidationEngineV2(
        policy or ValidationPolicy(),
        domain_intel=SimpleDomainIntelligenceService(cache=cache),
        provider_reputation=SimpleProviderReputationService(cache=cache),
        exclusion_service=DefaultExclusionService(),
        candidate_selector=DefaultValidationCandidateSelector(),
        smtp_client=_ExplodingSmtp(),
        catch_all_analyzer=_ExplodingCatchAll(),
        retry_strategy=_ExplodingRetry(),
    )


class TestEngineIntegration:
    def test_services_are_called_and_metadata_populated(self) -> None:
        engine = _wire_full_passive_engine()
        result = engine.validate(_make_request(domain="gmail.com"))

        assert result.metadata["provider_type"] == "tier_1"
        assert result.metadata["reputation_score"] == pytest.approx(
            REPUTATION_SCORES["tier_1"]
        )
        assert result.metadata["provider_hint"] == "gmail"
        assert result.metadata["is_common_provider"] is True
        assert result.metadata["is_suspicious_pattern"] is False
        assert result.metadata["candidate_decision"]["accepted"] is True
        assert result.metadata["candidate_decision"]["reason"] == REASON_ACCEPTED
        assert "domain_intelligence" in result.breakdown
        assert "provider_reputation" in result.breakdown

    def test_cache_hit_flows_into_metadata_after_second_call(self) -> None:
        cache = DomainCacheStore()
        engine = _wire_full_passive_engine(cache=cache)
        first = engine.validate(_make_request(domain="gmail.com"))
        second = engine.validate(_make_request(domain="gmail.com"))
        assert first.metadata["cache_hit"] is False
        assert second.metadata["cache_hit"] is True

    def test_exclusion_short_circuits_with_reason(self) -> None:
        policy = ValidationPolicy(excluded_domains={"bad.com"})
        engine = _wire_full_passive_engine(policy=policy)
        result = engine.validate(_make_request(domain="bad.com"))

        assert result.validation_status == ValidationStatus.EXCLUDED_BY_POLICY.value
        assert REASON_EXCLUDED_DOMAIN in result.validation_reason_codes
        assert result.metadata["exclusion_reason"] == REASON_EXCLUDED_DOMAIN
        # Matches the engine's own token.
        assert ENGINE_REASON_EXCLUDED_DOMAIN == REASON_EXCLUDED_DOMAIN

    def test_candidate_skip_short_circuits_with_reason(self) -> None:
        engine = _wire_full_passive_engine()
        # bucket=invalid is not in the allow-list → selector skips.
        result = engine.validate(_make_request(bucket_v2="invalid"))

        assert (
            result.validation_status == ValidationStatus.DELIVERABLE_UNCERTAIN.value
        )
        assert REASON_LOW_PRIORITY_CANDIDATE in result.validation_reason_codes
        assert result.metadata["candidate_decision"]["accepted"] is False
        assert (
            result.metadata["candidate_decision"]["reason"]
            == REASON_BUCKET_NOT_ALLOWED
        )

    def test_smtp_never_invoked_even_when_wired(self) -> None:
        # ExplodingSmtp is wired; the test would fail noisily if
        # the engine called it. Running every path is enough
        # coverage — we already raise in the class body.
        engine = _wire_full_passive_engine()
        engine.validate(_make_request(domain="gmail.com"))
        engine.validate(_make_request(bucket_v2="invalid"))
        engine.validate(
            _make_request(domain="bad.com"),
        )  # exclusion path via default (no match) — still no SMTP
        engine.validate(_make_request(syntax_valid=False))

    def test_exclusion_path_still_collects_intel(self) -> None:
        policy = ValidationPolicy(excluded_domains={"bad.com"})
        engine = _wire_full_passive_engine(policy=policy)
        result = engine.validate(_make_request(domain="bad.com"))
        # Intel is observational and runs before exclusion.
        assert "domain_intelligence" in result.breakdown
        assert "provider_reputation" in result.breakdown

    def test_engine_does_not_mutate_request(self) -> None:
        engine = _wire_full_passive_engine()
        req = _make_request(metadata={"seed": True})
        snapshot_email = req.email
        snapshot_metadata = dict(req.metadata)
        engine.validate(req)
        assert req.email == snapshot_email
        assert dict(req.metadata) == snapshot_metadata

    def test_determinism_across_repeated_calls(self) -> None:
        engine = _wire_full_passive_engine()
        req = _make_request(domain="acme.io")
        a = engine.validate(req).to_dict()
        b = engine.validate(req).to_dict()
        # Two fields legitimately flip once the cache warms:
        # ``cache_hit`` (False → True) and ``historical_score``
        # (None → cached reputation). Normalize both before
        # comparing the rest of the payload.
        for blob in (a, b):
            blob["metadata"]["cache_hit"] = True
            blob["metadata"].pop("historical_score", None)
            intel = blob["breakdown"]["domain_intelligence"]
            intel["cache_hit"] = True
            intel["historical_score"] = None
            rep = blob["breakdown"]["provider_reputation"]
            rep["cache_hit"] = True
        assert a == b

    def test_output_structure_consistent(self) -> None:
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
            # Subphase 3 control-plane surface.
            "decision_trace",
            "execution_decision",
            # Subphase 4 controlled SMTP sampler surface.
            "smtp_status",
            "smtp_code",
            "smtp_latency",
            "smtp_error_type",
            # Subphase 5 catch-all / retry surface.
            "catch_all_confidence",
            "retry_attempted",
            "retry_outcome",
            # Subphase 6 probability surface.
            "deliverability_confidence",
            "action_recommendation",
            "validation_breakdown",
        }
        engine = _wire_full_passive_engine(
            policy=ValidationPolicy(excluded_domains={"bad.com"}),
        )
        paths = [
            _make_request(domain="gmail.com"),
            _make_request(bucket_v2="invalid"),
            _make_request(domain="bad.com"),
            _make_request(syntax_valid=False),
        ]
        for req in paths:
            d = engine.validate(req).to_dict()
            assert set(d.keys()) == expected_keys
