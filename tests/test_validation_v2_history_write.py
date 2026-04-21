"""Tests for post-validation reputation learning writes."""

from __future__ import annotations

from typing import Any

import pytest

from app.validation_v2 import ValidationEngineV2, ValidationPolicy, ValidationRequest
from app.validation_v2.history import (
    DomainHistoryStore,
    ProbeEventStore,
    ProviderHistoryStore,
    ReputationLearningService,
    SQLiteHistoryDB,
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
            "historical_intelligence": {
                "provider_key": "gmail",
            },
        },
    )
    defaults.update(overrides)
    return ValidationResult(**defaults)


class _ExplodingLearningService:
    def record_validation(self, **kwargs) -> None:
        raise RuntimeError("learning write failed")


class _RecordingLearningService:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def record_validation(self, **kwargs) -> None:
        result = kwargs["result"]
        assert isinstance(result, ValidationResult)
        assert result.validation_status
        self.calls.append(kwargs)


class TestReputationLearningService:
    def test_missing_stores_do_not_crash(self) -> None:
        service = ReputationLearningService(time_fn=lambda: 100.0)

        service.record_validation(_request(), _result())

    def test_domain_record_created_on_first_write_with_ttl(self, tmp_path) -> None:
        domain_store = DomainHistoryStore(_db(tmp_path))
        service = ReputationLearningService(
            domain_store=domain_store,
            ttl_seconds=60,
            time_fn=lambda: 100.0,
        )

        service.record_validation(_request(), _result())

        record = domain_store.get("example.com")
        assert record is not None
        assert record.first_seen_at == 100.0
        assert record.last_seen_at == 100.0
        assert record.ttl_expires_at == 160.0
        assert record.total_observations == 1
        assert record.smtp_attempt_count == 1
        assert record.smtp_valid_count == 1
        assert record.catch_all_unlikely_count == 1
        assert record.last_smtp_status == "valid"
        assert record.last_catch_all_status == "unlikely"
        assert record.last_deliverability_probability == pytest.approx(0.9)
        assert record.last_validation_status == "deliverable"
        assert record.provider_type == "consumer_mailbox"
        assert record.provider_hint == "gmail"

    def test_domain_counters_increment_correctly(self, tmp_path) -> None:
        domain_store = DomainHistoryStore(_db(tmp_path))
        service = ReputationLearningService(
            domain_store=domain_store,
            ttl_seconds=None,
            time_fn=lambda: 200.0,
        )

        service.record_validation(
            _request(),
            _result(
                smtp_status="invalid",
                catch_all_status="confirmed",
                retry_attempted=True,
                retry_outcome="success",
            ),
        )
        service.record_validation(
            _request(),
            _result(
                smtp_status="uncertain",
                smtp_code=None,
                smtp_error_type="connection_timeout",
                catch_all_status="likely",
            ),
        )

        record = domain_store.get("example.com")
        assert record is not None
        assert record.ttl_expires_at is None
        assert record.total_observations == 2
        assert record.smtp_attempt_count == 2
        assert record.smtp_valid_count == 0
        assert record.smtp_invalid_count == 1
        assert record.smtp_uncertain_count == 1
        assert record.timeout_count == 1
        assert record.retry_count == 1
        assert record.catch_all_confirmed_count == 1
        assert record.catch_all_likely_count == 1

    def test_provider_record_created_and_updated_for_same_domain(self, tmp_path) -> None:
        db = _db(tmp_path)
        domain_store = DomainHistoryStore(db)
        provider_store = ProviderHistoryStore(db)
        service = ReputationLearningService(
            domain_store=domain_store,
            provider_store=provider_store,
            time_fn=lambda: 100.0,
        )

        service.record_validation(_request(), _result())
        service.record_validation(_request(), _result(smtp_status="invalid"))

        provider = provider_store.get("gmail")
        assert provider is not None
        assert provider.provider_type == "consumer_mailbox"
        assert provider.total_observations == 2
        assert provider.total_domains_seen == 1
        assert provider.smtp_valid_count == 1
        assert provider.smtp_invalid_count == 1
        assert provider.catch_all_unlikely_count == 2

    def test_probe_event_appended(self, tmp_path) -> None:
        event_store = ProbeEventStore(_db(tmp_path))
        service = ReputationLearningService(
            probe_event_store=event_store,
            time_fn=lambda: 123.456,
        )

        service.record_validation(_request(), _result())

        events = event_store.list_by_domain("example.com")
        assert len(events) == 1
        event = events[0]
        assert event.event_id == "example.com:123.456000:1"
        assert event.timestamp == 123.456
        assert event.provider_key == "gmail"
        assert event.smtp_status == "valid"
        assert event.smtp_code == 250
        assert event.catch_all_status == "unlikely"
        assert event.retry_attempted is False
        assert event.deliverability_probability == pytest.approx(0.9)
        assert event.validation_status == "deliverable"

    def test_timeout_retry_and_catch_all_counters_increment(self, tmp_path) -> None:
        db = _db(tmp_path)
        domain_store = DomainHistoryStore(db)
        provider_store = ProviderHistoryStore(db)
        event_store = ProbeEventStore(db)
        service = ReputationLearningService(
            domain_store=domain_store,
            provider_store=provider_store,
            probe_event_store=event_store,
            time_fn=lambda: 100.0,
        )

        service.record_validation(
            _request(),
            _result(
                smtp_status="uncertain",
                smtp_code=None,
                smtp_error_type="socket_timeout",
                catch_all_status="likely",
                retry_attempted=True,
                retry_outcome="fail",
            ),
        )

        domain = domain_store.get("example.com")
        provider = provider_store.get("gmail")
        event = event_store.list_recent()[0]
        assert domain is not None
        assert provider is not None
        assert domain.timeout_count == 1
        assert domain.retry_count == 1
        assert domain.catch_all_likely_count == 1
        assert provider.timeout_count == 1
        assert provider.catch_all_likely_count == 1
        assert event.smtp_error_type == "socket_timeout"
        assert event.retry_attempted is True
        assert event.retry_outcome == "fail"


class TestEngineLearningIntegration:
    def test_engine_calls_learning_service_after_result_creation(self) -> None:
        learning = _RecordingLearningService()
        engine = ValidationEngineV2(ValidationPolicy())
        engine.reputation_learning_service = learning  # type: ignore[assignment]

        result = engine.validate(_request())

        assert len(learning.calls) == 1
        assert learning.calls[0]["result"] is result
        assert result.metadata["historical_write_recorded"] is True
        assert result.decision_trace["steps"][-1] == {
            "stage": "historical_write",
            "decision": "recorded",
            "reason": "learning_recorded",
            "inputs": {"domain": "example.com"},
        }

    def test_learning_failure_does_not_crash_engine(self) -> None:
        engine = ValidationEngineV2(ValidationPolicy())
        engine.reputation_learning_service = _ExplodingLearningService()  # type: ignore[assignment]

        result = engine.validate(_request())

        assert result.metadata["historical_write_recorded"] is False
        assert result.decision_trace["steps"][-1]["stage"] == "historical_write"
        assert result.decision_trace["steps"][-1]["decision"] == "failed"

    def test_missing_learning_service_records_skipped(self) -> None:
        engine = ValidationEngineV2(ValidationPolicy())

        result = engine.validate(_request())

        assert result.metadata["historical_write_recorded"] is False
        assert result.decision_trace["steps"][-1] == {
            "stage": "historical_write",
            "decision": "skipped",
            "reason": "service_unavailable",
            "inputs": {"domain": "example.com"},
        }

    def test_learning_does_not_change_behavior_fields(self) -> None:
        baseline_engine = ValidationEngineV2(ValidationPolicy())
        learning_engine = ValidationEngineV2(ValidationPolicy())
        learning_engine.reputation_learning_service = _RecordingLearningService()  # type: ignore[assignment]
        request = _request()

        baseline = baseline_engine.validate(request)
        learned = learning_engine.validate(request)

        assert learned.validation_status == baseline.validation_status
        assert learned.deliverability_probability == baseline.deliverability_probability
        assert learned.action_recommendation == baseline.action_recommendation
        assert learned.smtp_status == baseline.smtp_status

    def test_stores_are_updated_only_when_service_present(self, tmp_path) -> None:
        db = _db(tmp_path)
        domain_store = DomainHistoryStore(db)
        engine = ValidationEngineV2(ValidationPolicy())

        engine.validate(_request())
        assert domain_store.get("example.com") is None

        engine.reputation_learning_service = ReputationLearningService(
            domain_store=domain_store,
            time_fn=lambda: 100.0,
        )
        engine.validate(_request())
        assert domain_store.get("example.com") is not None
