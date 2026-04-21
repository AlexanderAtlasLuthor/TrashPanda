"""Tests for Validation Engine V2 persistent history storage."""

from __future__ import annotations

import sqlite3

import pytest

from app.validation_v2.history import (
    DomainHistoryRecord,
    DomainHistoryStore,
    ProbeEventRecord,
    ProbeEventStore,
    ProviderHistoryRecord,
    ProviderHistoryStore,
    SQLiteHistoryDB,
    compute_ttl_expiry,
    is_expired,
)


def _db(tmp_path) -> SQLiteHistoryDB:
    return SQLiteHistoryDB(tmp_path / "history.sqlite")


def _domain_record(
    domain: str = "example.com",
    *,
    last_seen_at: float = 100.0,
    ttl_expires_at: float | None = None,
    total_observations: int = 1,
    provider_hint: str | None = "gmail",
) -> DomainHistoryRecord:
    return DomainHistoryRecord(
        domain=domain,
        provider_type="consumer_mailbox",
        provider_hint=provider_hint,
        first_seen_at=1.0,
        last_seen_at=last_seen_at,
        ttl_expires_at=ttl_expires_at,
        total_observations=total_observations,
        smtp_attempt_count=1,
        smtp_valid_count=1,
        smtp_invalid_count=0,
        smtp_uncertain_count=0,
        timeout_count=0,
        retry_count=0,
        catch_all_confirmed_count=0,
        catch_all_likely_count=0,
        catch_all_unlikely_count=1,
        last_smtp_status="valid",
        last_catch_all_status="unlikely",
        last_deliverability_probability=0.95,
        last_validation_status="deliverable",
        domain_reputation_score=0.8,
        domain_reputation_confidence=0.7,
    )


def _provider_record(
    provider_key: str = "gmail",
    *,
    last_seen_at: float = 100.0,
    ttl_expires_at: float | None = None,
    total_observations: int = 1,
) -> ProviderHistoryRecord:
    return ProviderHistoryRecord(
        provider_key=provider_key,
        provider_type="consumer_mailbox",
        first_seen_at=1.0,
        last_seen_at=last_seen_at,
        ttl_expires_at=ttl_expires_at,
        total_domains_seen=1,
        total_observations=total_observations,
        smtp_valid_count=1,
        smtp_invalid_count=0,
        smtp_uncertain_count=0,
        timeout_count=0,
        catch_all_confirmed_count=0,
        catch_all_likely_count=0,
        catch_all_unlikely_count=1,
        provider_reputation_score=0.8,
        provider_reputation_confidence=0.7,
    )


def _probe_event(
    event_id: str = "evt-1",
    *,
    domain: str = "example.com",
    timestamp: float = 100.0,
) -> ProbeEventRecord:
    return ProbeEventRecord(
        event_id=event_id,
        timestamp=timestamp,
        domain=domain,
        provider_key="gmail",
        smtp_status="valid",
        smtp_code=250,
        smtp_error_type=None,
        catch_all_status="unlikely",
        retry_attempted=False,
        retry_outcome=None,
        deliverability_probability=0.95,
        validation_status="deliverable",
    )


def test_models_valid_construction_and_to_dict() -> None:
    domain = _domain_record()
    provider = _provider_record()
    event = _probe_event()

    assert domain.to_dict()["domain"] == "example.com"
    assert provider.to_dict()["provider_key"] == "gmail"
    assert event.to_dict()["retry_attempted"] is False


def test_models_reject_negative_counts() -> None:
    with pytest.raises(ValueError, match="smtp_valid_count"):
        DomainHistoryRecord(**{**_domain_record().to_dict(), "smtp_valid_count": -1})

    with pytest.raises(ValueError, match="total_domains_seen"):
        ProviderHistoryRecord(
            **{**_provider_record().to_dict(), "total_domains_seen": -1}
        )


def test_models_reject_invalid_probabilities() -> None:
    with pytest.raises(ValueError, match="last_deliverability_probability"):
        DomainHistoryRecord(
            **{**_domain_record().to_dict(), "last_deliverability_probability": 1.1}
        )

    with pytest.raises(ValueError, match="provider_reputation_score"):
        ProviderHistoryRecord(
            **{**_provider_record().to_dict(), "provider_reputation_score": -0.1}
        )

    with pytest.raises(ValueError, match="deliverability_probability"):
        ProbeEventRecord(
            **{**_probe_event().to_dict(), "deliverability_probability": 2.0}
        )


def test_sqlite_init_creates_tables_and_indexes(tmp_path) -> None:
    db = _db(tmp_path)

    with sqlite3.connect(db.db_path) as connection:
        objects = {
            row[0]: row[1]
            for row in connection.execute(
                """
                SELECT name, type
                FROM sqlite_master
                WHERE type IN ('table', 'index')
                """
            )
        }

    assert objects["domain_history"] == "table"
    assert objects["provider_history"] == "table"
    assert objects["probe_events"] == "table"
    assert objects["idx_probe_events_domain"] == "index"
    assert objects["idx_probe_events_timestamp"] == "index"
    assert objects["idx_provider_history_provider_key"] == "index"


def test_domain_history_store_get_upsert_update_and_list_recent(tmp_path) -> None:
    store = DomainHistoryStore(_db(tmp_path))

    assert store.get("missing.example") is None

    store.upsert(_domain_record("old.example", last_seen_at=10.0))
    store.upsert(_domain_record("new.example", last_seen_at=20.0))

    updated = _domain_record(
        "old.example",
        last_seen_at=30.0,
        total_observations=5,
        provider_hint="updated",
    )
    store.upsert(updated)

    loaded = store.get("old.example")
    assert loaded == updated
    assert loaded is not None
    assert loaded.total_observations == 5

    assert [record.domain for record in store.list_recent()] == [
        "old.example",
        "new.example",
    ]


def test_domain_history_store_delete_expired(tmp_path) -> None:
    store = DomainHistoryStore(_db(tmp_path))
    store.upsert(_domain_record("expired.example", ttl_expires_at=50.0))
    store.upsert(_domain_record("fresh.example", ttl_expires_at=150.0))
    store.upsert(_domain_record("forever.example", ttl_expires_at=None))

    assert store.delete_expired(now=100.0) == 1
    assert store.get("expired.example") is None
    assert store.get("fresh.example") is not None
    assert store.get("forever.example") is not None


def test_provider_history_store_get_upsert_update_and_list_recent(tmp_path) -> None:
    store = ProviderHistoryStore(_db(tmp_path))

    assert store.get("missing") is None

    store.upsert(_provider_record("old", last_seen_at=10.0))
    store.upsert(_provider_record("new", last_seen_at=20.0))

    updated = _provider_record("old", last_seen_at=30.0, total_observations=5)
    store.upsert(updated)

    loaded = store.get("old")
    assert loaded == updated
    assert loaded is not None
    assert loaded.total_observations == 5

    assert [record.provider_key for record in store.list_recent()] == ["old", "new"]


def test_provider_history_store_delete_expired(tmp_path) -> None:
    store = ProviderHistoryStore(_db(tmp_path))
    store.upsert(_provider_record("expired", ttl_expires_at=50.0))
    store.upsert(_provider_record("fresh", ttl_expires_at=150.0))
    store.upsert(_provider_record("forever", ttl_expires_at=None))

    assert store.delete_expired(now=100.0) == 1
    assert store.get("expired") is None
    assert store.get("fresh") is not None
    assert store.get("forever") is not None


def test_probe_event_store_append_and_list_by_domain(tmp_path) -> None:
    store = ProbeEventStore(_db(tmp_path))
    store.append(_probe_event("evt-1", domain="example.com", timestamp=10.0))
    store.append(_probe_event("evt-2", domain="other.com", timestamp=20.0))
    store.append(_probe_event("evt-3", domain="example.com", timestamp=30.0))

    assert [event.event_id for event in store.list_by_domain("example.com")] == [
        "evt-3",
        "evt-1",
    ]


def test_probe_event_store_list_recent_ordering_and_delete_older_than(tmp_path) -> None:
    store = ProbeEventStore(_db(tmp_path))
    store.append(_probe_event("evt-old", timestamp=10.0))
    store.append(_probe_event("evt-mid", timestamp=20.0))
    store.append(_probe_event("evt-new", timestamp=30.0))

    assert [event.event_id for event in store.list_recent(limit=2)] == [
        "evt-new",
        "evt-mid",
    ]

    assert store.delete_older_than(20.0) == 1
    assert [event.event_id for event in store.list_recent()] == ["evt-new", "evt-mid"]


def test_probe_event_store_append_only_rejects_duplicate_event_id(tmp_path) -> None:
    store = ProbeEventStore(_db(tmp_path))
    event = _probe_event("evt-1")
    store.append(event)

    with pytest.raises(sqlite3.IntegrityError):
        store.append(event)


def test_ttl_helpers() -> None:
    assert compute_ttl_expiry(100.0, None) is None
    assert compute_ttl_expiry(100.0, 60) == 160.0

    with pytest.raises(ValueError, match="ttl_seconds"):
        compute_ttl_expiry(100.0, -1)

    assert is_expired(None, 100.0) is False
    assert is_expired(100.0, 100.0) is True
    assert is_expired(101.0, 100.0) is False
