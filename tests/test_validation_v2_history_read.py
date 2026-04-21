"""Tests for read-only historical intelligence integration."""

from __future__ import annotations

from typing import Any

import pytest

from app.validation_v2 import (
    ProviderReputationService,
    ValidationEngineV2,
    ValidationPolicy,
    ValidationRequest,
)
from app.validation_v2.control.decision_trace import STAGE_HISTORICAL_INTELLIGENCE
from app.validation_v2.history import (
    DomainHistoryRecord,
    HistoricalIntelligence,
    HistoricalIntelligenceService,
    ProviderHistoryRecord,
)
from app.validation_v2.history.read_service import _is_stale, _safe_rate


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


def _domain_record(
    *,
    domain: str = "example.com",
    smtp_attempt_count: int = 10,
    last_seen_at: float = 100.0,
) -> DomainHistoryRecord:
    return DomainHistoryRecord(
        domain=domain,
        provider_type="consumer_mailbox",
        provider_hint="gmail",
        first_seen_at=1.0,
        last_seen_at=last_seen_at,
        ttl_expires_at=None,
        total_observations=12,
        smtp_attempt_count=smtp_attempt_count,
        smtp_valid_count=6,
        smtp_invalid_count=3,
        smtp_uncertain_count=1,
        timeout_count=2,
        retry_count=1,
        catch_all_confirmed_count=1,
        catch_all_likely_count=2,
        catch_all_unlikely_count=3,
        last_smtp_status="valid",
        last_catch_all_status="likely",
        last_deliverability_probability=0.9,
        last_validation_status="deliverable",
        domain_reputation_score=0.82,
        domain_reputation_confidence=0.72,
    )


def _provider_record(
    *,
    provider_key: str = "gmail",
    last_seen_at: float = 100.0,
) -> ProviderHistoryRecord:
    return ProviderHistoryRecord(
        provider_key=provider_key,
        provider_type="consumer_mailbox",
        first_seen_at=1.0,
        last_seen_at=last_seen_at,
        ttl_expires_at=None,
        total_domains_seen=4,
        total_observations=20,
        smtp_valid_count=12,
        smtp_invalid_count=5,
        smtp_uncertain_count=3,
        timeout_count=1,
        catch_all_confirmed_count=2,
        catch_all_likely_count=3,
        catch_all_unlikely_count=5,
        provider_reputation_score=0.77,
        provider_reputation_confidence=0.66,
    )


class _ReadOnlyDomainStore:
    def __init__(self, record: DomainHistoryRecord | None = None) -> None:
        self.record = record
        self.get_calls: list[str] = []

    def get(self, domain: str) -> DomainHistoryRecord | None:
        self.get_calls.append(domain)
        if self.record is not None and self.record.domain == domain:
            return self.record
        return None

    def upsert(self, record):  # pragma: no cover - must never be called
        raise AssertionError("domain store mutated")

    def delete_expired(self, now=None):  # pragma: no cover
        raise AssertionError("domain store mutated")


class _ReadOnlyProviderStore:
    def __init__(self, record: ProviderHistoryRecord | None = None) -> None:
        self.record = record
        self.get_calls: list[str] = []

    def get(self, provider_key: str) -> ProviderHistoryRecord | None:
        self.get_calls.append(provider_key)
        if self.record is not None and self.record.provider_key == provider_key:
            return self.record
        return None

    def upsert(self, record):  # pragma: no cover - must never be called
        raise AssertionError("provider store mutated")

    def delete_expired(self, now=None):  # pragma: no cover
        raise AssertionError("provider store mutated")


class _Reputation(ProviderReputationService):
    def classify(self, domain: str) -> dict[str, Any]:
        return {"provider": "gmail", "provider_key": "gmail", "domain": domain}


def _history_step(result) -> dict[str, Any]:
    for step in result.decision_trace["steps"]:
        if step["stage"] == STAGE_HISTORICAL_INTELLIGENCE:
            return step
    raise AssertionError("missing historical intelligence trace step")


class TestHistoricalIntelligenceService:
    def test_no_stores_configured_returns_safe_empty_result(self) -> None:
        result = HistoricalIntelligenceService().fetch(
            "example.com",
            "gmail",
            now=100.0,
        )

        assert result.to_dict() == {
            "domain": "example.com",
            "provider_key": "gmail",
            "history_cache_hit": False,
            "historical_domain_reputation": None,
            "historical_domain_reputation_confidence": None,
            "historical_smtp_valid_rate": None,
            "historical_smtp_invalid_rate": None,
            "historical_smtp_uncertain_rate": None,
            "historical_timeout_rate": None,
            "historical_catch_all_risk": None,
            "domain_observation_count": 0,
            "domain_last_seen_at": None,
            "historical_provider_reputation": None,
            "historical_provider_reputation_confidence": None,
            "provider_observation_count": 0,
            "provider_last_seen_at": None,
            "domain_history_stale": False,
            "provider_history_stale": False,
        }

    def test_missing_domain_and_provider_records_are_safe_empty(self) -> None:
        service = HistoricalIntelligenceService(
            domain_store=_ReadOnlyDomainStore(),
            provider_store=_ReadOnlyProviderStore(),
        )

        result = service.fetch("example.com", "gmail", now=100.0)

        assert result.history_cache_hit is False
        assert result.domain_observation_count == 0
        assert result.provider_observation_count == 0

    def test_domain_record_only_populates_domain_signals(self) -> None:
        service = HistoricalIntelligenceService(
            domain_store=_ReadOnlyDomainStore(_domain_record()),
        )

        result = service.fetch("example.com", "gmail", now=200.0)

        assert result.history_cache_hit is True
        assert result.historical_domain_reputation == pytest.approx(0.82)
        assert result.historical_domain_reputation_confidence == pytest.approx(0.72)
        assert result.historical_smtp_valid_rate == pytest.approx(0.6)
        assert result.historical_smtp_invalid_rate == pytest.approx(0.3)
        assert result.historical_smtp_uncertain_rate == pytest.approx(0.1)
        assert result.historical_timeout_rate == pytest.approx(0.2)
        assert result.historical_catch_all_risk == pytest.approx(0.5)
        assert result.domain_observation_count == 12
        assert result.provider_observation_count == 0
        assert result.historical_provider_reputation is None

    def test_provider_record_only_populates_provider_signals(self) -> None:
        service = HistoricalIntelligenceService(
            provider_store=_ReadOnlyProviderStore(_provider_record()),
        )

        result = service.fetch("example.com", "gmail", now=200.0)

        assert result.history_cache_hit is True
        assert result.historical_provider_reputation == pytest.approx(0.77)
        assert result.historical_provider_reputation_confidence == pytest.approx(0.66)
        assert result.provider_observation_count == 20
        assert result.domain_observation_count == 0
        assert result.historical_domain_reputation is None

    def test_both_records_are_merged(self) -> None:
        service = HistoricalIntelligenceService(
            domain_store=_ReadOnlyDomainStore(_domain_record()),
            provider_store=_ReadOnlyProviderStore(_provider_record()),
        )

        result = service.fetch("example.com", "gmail", now=200.0)

        assert result.history_cache_hit is True
        assert result.domain_observation_count == 12
        assert result.provider_observation_count == 20
        assert result.domain_last_seen_at == 100.0
        assert result.provider_last_seen_at == 100.0

    def test_stale_detection_works(self) -> None:
        service = HistoricalIntelligenceService(
            domain_store=_ReadOnlyDomainStore(
                _domain_record(last_seen_at=10.0)
            ),
            provider_store=_ReadOnlyProviderStore(
                _provider_record(last_seen_at=95.0)
            ),
            stale_after_seconds=50,
        )

        result = service.fetch("example.com", "gmail", now=100.0)

        assert result.domain_history_stale is True
        assert result.provider_history_stale is False

    def test_rate_helpers_and_divide_by_zero_are_safe(self) -> None:
        assert _safe_rate(1, 4) == pytest.approx(0.25)
        assert _safe_rate(1, 0) is None
        assert _is_stale(None, 10, 100.0) is False
        assert _is_stale(80.0, 10, 100.0) is True

        service = HistoricalIntelligenceService(
            domain_store=_ReadOnlyDomainStore(
                _domain_record(smtp_attempt_count=0)
            ),
        )
        result = service.fetch("example.com", now=100.0)

        assert result.historical_smtp_valid_rate is None
        assert result.historical_timeout_rate is None

    def test_stores_are_never_mutated(self) -> None:
        domain_store = _ReadOnlyDomainStore(_domain_record())
        provider_store = _ReadOnlyProviderStore(_provider_record())
        service = HistoricalIntelligenceService(domain_store, provider_store)

        service.fetch("example.com", "gmail", now=100.0)

        assert domain_store.get_calls == ["example.com"]
        assert provider_store.get_calls == ["gmail"]


class TestEngineIntegration:
    def test_engine_attaches_historical_intelligence(self) -> None:
        service = HistoricalIntelligenceService(
            domain_store=_ReadOnlyDomainStore(_domain_record()),
            provider_store=_ReadOnlyProviderStore(_provider_record()),
        )
        engine = ValidationEngineV2(
            ValidationPolicy(),
            provider_reputation=_Reputation(),
        )
        engine.historical_intelligence_service = service

        result = engine.validate(_make_request())

        historical = result.metadata["historical_intelligence"]
        assert historical["history_cache_hit"] is True
        assert historical["domain_observation_count"] == 12
        assert result.breakdown["historical_intelligence"]["history_cache_hit"] is True
        assert _history_step(result) == {
            "stage": STAGE_HISTORICAL_INTELLIGENCE,
            "decision": "fetched",
            "reason": "cache_hit",
            "inputs": {"domain": "example.com", "provider_key": "gmail"},
        }

    def test_engine_no_service_configured_path_is_safe(self) -> None:
        engine = ValidationEngineV2(ValidationPolicy())

        result = engine.validate(_make_request())

        assert result.metadata["historical_intelligence"]["history_cache_hit"] is False
        assert result.breakdown["historical_intelligence"]["history_cache_hit"] is False
        assert _history_step(result)["decision"] == "missing"
        assert _history_step(result)["reason"] == "service_unavailable"

    def test_engine_service_available_but_no_history_path_is_safe(self) -> None:
        engine = ValidationEngineV2(ValidationPolicy())
        engine.historical_intelligence_service = HistoricalIntelligenceService(
            domain_store=_ReadOnlyDomainStore(),
            provider_store=_ReadOnlyProviderStore(),
        )

        result = engine.validate(_make_request())

        assert result.metadata["historical_intelligence"]["history_cache_hit"] is False
        assert result.breakdown["historical_intelligence"]["domain_observation_count"] == 0
        assert _history_step(result)["reason"] == "no_history"

    def test_engine_does_not_use_history_to_change_probability(self) -> None:
        no_history_engine = ValidationEngineV2(ValidationPolicy())
        history_engine = ValidationEngineV2(ValidationPolicy())
        history_engine.historical_intelligence_service = HistoricalIntelligenceService(
            domain_store=_ReadOnlyDomainStore(_domain_record()),
        )
        request = _make_request()

        no_history = no_history_engine.validate(request)
        with_history = history_engine.validate(request)

        assert with_history.deliverability_probability == (
            no_history.deliverability_probability
        )
        assert with_history.validation_status == no_history.validation_status
