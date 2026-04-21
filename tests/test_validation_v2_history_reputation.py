"""Tests for deterministic historical reputation scoring."""

from __future__ import annotations

from typing import Any

import pytest

from app.validation_v2 import ValidationEngineV2, ValidationPolicy, ValidationRequest
from app.validation_v2.history import (
    DomainHistoryRecord,
    DomainHistoryStore,
    HistoricalIntelligenceService,
    ProviderHistoryRecord,
    ProviderHistoryStore,
    ReputationLearningService,
    SQLiteHistoryDB,
    compute_domain_reputation_confidence,
    compute_domain_reputation_score,
    compute_provider_reputation_confidence,
    compute_provider_reputation_score,
)
from app.validation_v2.result import ValidationResult


def _db(tmp_path) -> SQLiteHistoryDB:
    return SQLiteHistoryDB(tmp_path / "history.sqlite")


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


def _result(**overrides: Any) -> ValidationResult:
    defaults: dict[str, Any] = dict(
        validation_status="deliverable",
        deliverability_probability=0.9,
        smtp_status="valid",
        smtp_code=250,
        smtp_error_type=None,
        catch_all_status="unlikely",
        retry_attempted=False,
        retry_outcome="none",
        provider_reputation="gmail",
        metadata={
            "provider_type": "consumer_mailbox",
            "provider_hint": "gmail",
            "historical_intelligence": {"provider_key": "gmail"},
        },
    )
    defaults.update(overrides)
    return ValidationResult(**defaults)


def _domain_record(**overrides: Any) -> DomainHistoryRecord:
    defaults: dict[str, Any] = dict(
        domain="example.com",
        provider_type="consumer_mailbox",
        provider_hint="gmail",
        first_seen_at=1.0,
        last_seen_at=100.0,
        ttl_expires_at=None,
        total_observations=20,
        smtp_attempt_count=20,
        smtp_valid_count=18,
        smtp_invalid_count=1,
        smtp_uncertain_count=1,
        timeout_count=0,
        retry_count=0,
        catch_all_confirmed_count=0,
        catch_all_likely_count=0,
        catch_all_unlikely_count=10,
        last_smtp_status="valid",
        last_catch_all_status="unlikely",
        last_deliverability_probability=0.9,
        last_validation_status="deliverable",
        domain_reputation_score=None,
        domain_reputation_confidence=None,
    )
    defaults.update(overrides)
    return DomainHistoryRecord(**defaults)


def _provider_record(**overrides: Any) -> ProviderHistoryRecord:
    defaults: dict[str, Any] = dict(
        provider_key="gmail",
        provider_type="consumer_mailbox",
        first_seen_at=1.0,
        last_seen_at=100.0,
        ttl_expires_at=None,
        total_domains_seen=5,
        total_observations=20,
        smtp_valid_count=18,
        smtp_invalid_count=1,
        smtp_uncertain_count=1,
        timeout_count=0,
        catch_all_confirmed_count=0,
        catch_all_likely_count=0,
        catch_all_unlikely_count=10,
        provider_reputation_score=None,
        provider_reputation_confidence=None,
    )
    defaults.update(overrides)
    return ProviderHistoryRecord(**defaults)


class TestDomainReputation:
    def test_high_valid_low_risk_history_has_high_score(self) -> None:
        assert compute_domain_reputation_score(_domain_record()) > 0.85

    def test_high_invalid_high_risk_history_has_low_score(self) -> None:
        record = _domain_record(
            smtp_valid_count=1,
            smtp_invalid_count=16,
            smtp_uncertain_count=2,
            timeout_count=1,
            catch_all_confirmed_count=8,
            catch_all_likely_count=2,
            catch_all_unlikely_count=0,
        )

        assert compute_domain_reputation_score(record) < 0.35

    def test_mixed_history_has_middle_score(self) -> None:
        record = _domain_record(
            smtp_valid_count=10,
            smtp_invalid_count=8,
            smtp_uncertain_count=2,
            timeout_count=1,
            catch_all_confirmed_count=2,
            catch_all_likely_count=2,
            catch_all_unlikely_count=6,
        )

        score = compute_domain_reputation_score(record)
        assert score is not None
        assert 0.45 < score < 0.75

    def test_no_smtp_attempts_returns_none_score(self) -> None:
        record = _domain_record(
            smtp_attempt_count=0,
            smtp_valid_count=0,
            smtp_invalid_count=0,
            smtp_uncertain_count=0,
            timeout_count=0,
        )

        assert compute_domain_reputation_score(record) is None

    def test_confidence_reflects_observations_staleness_and_contradiction(self) -> None:
        few = _domain_record(total_observations=2)
        many = _domain_record(total_observations=20)
        stale = _domain_record(total_observations=20, ttl_expires_at=50.0)
        contradictory = _domain_record(
            total_observations=20,
            smtp_valid_count=8,
            smtp_invalid_count=8,
            smtp_uncertain_count=4,
        )

        assert compute_domain_reputation_confidence(few) == pytest.approx(0.1)
        assert compute_domain_reputation_confidence(many) == pytest.approx(1.0)
        assert compute_domain_reputation_confidence(stale) == pytest.approx(0.6)
        assert compute_domain_reputation_confidence(contradictory) == pytest.approx(0.7)


class TestProviderReputation:
    def test_high_valid_low_risk_provider_has_high_score(self) -> None:
        assert compute_provider_reputation_score(_provider_record()) > 0.85

    def test_high_invalid_high_risk_provider_has_low_score(self) -> None:
        record = _provider_record(
            smtp_valid_count=1,
            smtp_invalid_count=16,
            smtp_uncertain_count=2,
            timeout_count=1,
            catch_all_confirmed_count=8,
            catch_all_likely_count=2,
            catch_all_unlikely_count=0,
        )

        assert compute_provider_reputation_score(record) < 0.35

    def test_mixed_provider_has_middle_score(self) -> None:
        record = _provider_record(
            smtp_valid_count=10,
            smtp_invalid_count=8,
            smtp_uncertain_count=2,
            timeout_count=1,
            catch_all_confirmed_count=2,
            catch_all_likely_count=2,
            catch_all_unlikely_count=6,
        )

        score = compute_provider_reputation_score(record)
        assert score is not None
        assert 0.45 < score < 0.75

    def test_no_provider_attempts_returns_none_score(self) -> None:
        record = _provider_record(
            smtp_valid_count=0,
            smtp_invalid_count=0,
            smtp_uncertain_count=0,
            timeout_count=0,
        )

        assert compute_provider_reputation_score(record) is None

    def test_provider_confidence_cases(self) -> None:
        few = _provider_record(total_observations=2)
        many = _provider_record(total_observations=20)
        stale = _provider_record(total_observations=20, ttl_expires_at=50.0)
        contradictory = _provider_record(
            total_observations=20,
            smtp_valid_count=8,
            smtp_invalid_count=8,
            smtp_uncertain_count=4,
        )

        assert compute_provider_reputation_confidence(few) == pytest.approx(0.1)
        assert compute_provider_reputation_confidence(many) == pytest.approx(1.0)
        assert compute_provider_reputation_confidence(stale) == pytest.approx(0.6)
        assert compute_provider_reputation_confidence(contradictory) == pytest.approx(0.7)


class TestReadPathIntegration:
    def test_read_service_prefers_stored_scores(self, tmp_path) -> None:
        db = _db(tmp_path)
        DomainHistoryStore(db).upsert(
            _domain_record(
                domain_reputation_score=0.42,
                domain_reputation_confidence=0.31,
            )
        )

        result = HistoricalIntelligenceService(
            domain_store=DomainHistoryStore(db)
        ).fetch("example.com", now=100.0)

        assert result.historical_domain_reputation == pytest.approx(0.42)
        assert result.historical_domain_reputation_confidence == pytest.approx(0.31)

    def test_read_service_computes_missing_scores(self, tmp_path) -> None:
        db = _db(tmp_path)
        DomainHistoryStore(db).upsert(_domain_record())
        ProviderHistoryStore(db).upsert(_provider_record())

        result = HistoricalIntelligenceService(
            domain_store=DomainHistoryStore(db),
            provider_store=ProviderHistoryStore(db),
        ).fetch("example.com", "gmail", now=100.0)

        assert result.historical_domain_reputation == pytest.approx(
            compute_domain_reputation_score(_domain_record())
        )
        assert result.historical_provider_reputation == pytest.approx(
            compute_provider_reputation_score(_provider_record())
        )
        payload = result.to_dict()
        assert payload["historical_domain_reputation"] is not None
        assert payload["historical_provider_reputation_confidence"] is not None


class TestWritePathIntegration:
    def test_learning_persists_domain_and_provider_reputation(self, tmp_path) -> None:
        db = _db(tmp_path)
        domain_store = DomainHistoryStore(db)
        provider_store = ProviderHistoryStore(db)
        service = ReputationLearningService(
            domain_store=domain_store,
            provider_store=provider_store,
            time_fn=lambda: 100.0,
        )

        service.record_validation(_request(), _result())

        domain = domain_store.get("example.com")
        provider = provider_store.get("gmail")
        assert domain is not None
        assert provider is not None
        assert domain.domain_reputation_score is not None
        assert domain.domain_reputation_confidence is not None
        assert provider.provider_reputation_score is not None
        assert provider.provider_reputation_confidence is not None
        assert 0.0 <= domain.domain_reputation_score <= 1.0
        assert 0.0 <= provider.provider_reputation_score <= 1.0


def test_scores_and_confidences_are_clamped() -> None:
    domain = _domain_record(
        smtp_valid_count=1000,
        smtp_invalid_count=0,
        smtp_uncertain_count=0,
        timeout_count=0,
        total_observations=1000,
    )
    provider = _provider_record(
        smtp_valid_count=1000,
        smtp_invalid_count=0,
        smtp_uncertain_count=0,
        timeout_count=0,
        total_observations=1000,
    )

    values = [
        compute_domain_reputation_score(domain),
        compute_domain_reputation_confidence(domain),
        compute_provider_reputation_score(provider),
        compute_provider_reputation_confidence(provider),
    ]
    assert all(value is not None and 0.0 <= value <= 1.0 for value in values)


def test_reputation_does_not_change_validation_behavior() -> None:
    baseline_engine = ValidationEngineV2(ValidationPolicy())
    history_engine = ValidationEngineV2(ValidationPolicy())
    request = _request()

    baseline = baseline_engine.validate(request)
    with_history = history_engine.validate(request)

    assert with_history.validation_status == baseline.validation_status
    assert with_history.deliverability_probability == baseline.deliverability_probability
    assert with_history.action_recommendation == baseline.action_recommendation
