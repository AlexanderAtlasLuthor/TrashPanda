"""Tests for app.validation_v2.history_store — SQLite persistence."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from app.validation_v2.history_models import (
    DomainHistoryRecord,
    DomainObservation,
    FinalDecision,
)
from app.validation_v2.history_store import DomainHistoryStore


# ─────────────────────────────────────────────────────────────────────── #
# Helpers                                                                 #
# ─────────────────────────────────────────────────────────────────────── #


def _now() -> datetime:
    return datetime(2026, 4, 22, 12, 0, 0)


def _ready(domain: str, had_mx: bool = True) -> DomainObservation:
    return DomainObservation(
        domain=domain, had_mx=had_mx, final_decision=FinalDecision.READY
    )


def _invalid(domain: str) -> DomainObservation:
    return DomainObservation(domain=domain, final_decision=FinalDecision.INVALID)


def _timeout(domain: str) -> DomainObservation:
    return DomainObservation(
        domain=domain, had_timeout=True, had_dns_failure=True,
        final_decision=FinalDecision.INVALID,
    )


# ─────────────────────────────────────────────────────────────────────── #
# Basic CRUD                                                              #
# ─────────────────────────────────────────────────────────────────────── #


def test_empty_store_reports_no_record() -> None:
    with DomainHistoryStore(":memory:") as store:
        assert store.get("nobody.com") is None
        assert store.exists("nobody.com") is False
        assert store.count() == 0


def test_first_observation_creates_new_record() -> None:
    with DomainHistoryStore(":memory:") as store:
        record = store.update_from_observation(_ready("alpha.com"), now=_now())
        assert record.domain == "alpha.com"
        assert record.total_seen_count == 1
        assert record.mx_present_count == 1
        assert record.ready_count == 1
        assert record.first_seen_at == _now()
        assert record.last_seen_at == _now()
        assert store.exists("alpha.com")
        assert store.count() == 1


def test_repeated_observations_accumulate_monotonically() -> None:
    with DomainHistoryStore(":memory:") as store:
        for _ in range(5):
            store.update_from_observation(_ready("bravo.com"))
        rec = store.get("bravo.com")
        assert rec is not None
        assert rec.total_seen_count == 5
        assert rec.ready_count == 5
        assert rec.mx_present_count == 5


def test_bulk_update_aggregates_by_domain() -> None:
    with DomainHistoryStore(":memory:") as store:
        observations = (
            [_ready("a.com") for _ in range(3)]
            + [_invalid("b.com") for _ in range(2)]
            + [_timeout("c.com")]
        )
        updated = store.bulk_update(observations, now=_now())
        assert set(updated.keys()) == {"a.com", "b.com", "c.com"}
        assert updated["a.com"].ready_count == 3
        assert updated["b.com"].invalid_count == 2
        assert updated["c.com"].timeout_count == 1
        assert updated["c.com"].dns_failure_count == 1


def test_upsert_overwrites_counters_verbatim() -> None:
    with DomainHistoryStore(":memory:") as store:
        # Seed with one observation.
        store.update_from_observation(_ready("delta.com"))
        # Upsert a hand-crafted snapshot.
        snapshot = DomainHistoryRecord(
            domain="delta.com",
            first_seen_at=_now(),
            last_seen_at=_now(),
            total_seen_count=99,
            ready_count=99,
        )
        store.upsert(snapshot)
        persisted = store.get("delta.com")
        assert persisted is not None
        assert persisted.total_seen_count == 99
        assert persisted.ready_count == 99


def test_delete_removes_domain() -> None:
    with DomainHistoryStore(":memory:") as store:
        store.update_from_observation(_ready("echo.com"))
        assert store.exists("echo.com")
        assert store.delete("echo.com") is True
        assert store.exists("echo.com") is False
        assert store.delete("echo.com") is False  # idempotent


def test_get_many_returns_only_known_domains() -> None:
    with DomainHistoryStore(":memory:") as store:
        store.update_from_observation(_ready("foo.com"))
        store.update_from_observation(_ready("bar.com"))
        result = store.get_many(["foo.com", "missing.com", "bar.com"])
        assert set(result.keys()) == {"foo.com", "bar.com"}


def test_iter_all_yields_records_sorted_by_last_seen_desc() -> None:
    old = datetime(2026, 1, 1)
    new = datetime(2026, 4, 22)
    with DomainHistoryStore(":memory:") as store:
        store.update_from_observation(_ready("old.com"), now=old)
        store.update_from_observation(_ready("new.com"), now=new)
        domains = [r.domain for r in store.iter_all()]
        assert domains[0] == "new.com"
        assert domains[-1] == "old.com"


# ─────────────────────────────────────────────────────────────────────── #
# Cross-run persistence                                                   #
# ─────────────────────────────────────────────────────────────────────── #


def test_sqlite_persists_between_store_instances(tmp_path: Path) -> None:
    db_path = tmp_path / "history.sqlite"

    store_a = DomainHistoryStore(db_path)
    store_a.update_from_observation(_ready("persistent.com"))
    store_a.update_from_observation(_invalid("persistent.com"))
    store_a.close()

    store_b = DomainHistoryStore(db_path)
    try:
        rec = store_b.get("persistent.com")
        assert rec is not None
        assert rec.total_seen_count == 2
        assert rec.ready_count == 1
        assert rec.invalid_count == 1
    finally:
        store_b.close()


def test_sqlite_creates_parent_directory_on_demand(tmp_path: Path) -> None:
    nested_path = tmp_path / "deeply" / "nested" / "history.sqlite"
    assert not nested_path.parent.exists()
    with DomainHistoryStore(nested_path) as store:
        store.update_from_observation(_ready("z.com"))
    assert nested_path.is_file()


# ─────────────────────────────────────────────────────────────────────── #
# Robustness                                                              #
# ─────────────────────────────────────────────────────────────────────── #


def test_bulk_update_with_empty_iterable_returns_empty_dict() -> None:
    with DomainHistoryStore(":memory:") as store:
        assert store.bulk_update([]) == {}
        assert store.count() == 0


def test_observations_with_blank_domain_are_skipped() -> None:
    with DomainHistoryStore(":memory:") as store:
        updated = store.bulk_update([DomainObservation(domain=""), _ready("keeper.com")])
        assert "keeper.com" in updated
        assert "" not in updated
        assert store.count() == 1


def test_domain_lookup_is_case_insensitive_via_observation() -> None:
    with DomainHistoryStore(":memory:") as store:
        store.update_from_observation(_ready("CaseTest.Com"))
        assert store.get("casetest.com") is not None
        assert store.get("CASETEST.COM") is not None
