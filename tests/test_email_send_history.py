"""Tests for app.validation_v2.email_send_history.

Covers the persistent SQLite store on its own (no pipeline). The
companion file ``test_email_send_history_integration.py`` pins the
behaviour through the SMTPVerificationStage.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from app.validation_v2.email_send_history import (
    EmailSendHistoryStore,
    EmailSendRecord,
)


# ─────────────────────────────────────────────────────────────────────── #
# Helpers                                                                 #
# ─────────────────────────────────────────────────────────────────────── #


def _now() -> datetime:
    return datetime(2026, 5, 1, 12, 0, 0)


def _record_kwargs(**overrides):
    base = dict(
        email_normalized="alice@gmail.com",
        domain="gmail.com",
        status="valid",
        smtp_result="deliverable",
        response_code=250,
        response_message="2.1.5 Recipient OK",
        was_success=True,
        is_catch_all=False,
        inconclusive=False,
    )
    base.update(overrides)
    return base


# ─────────────────────────────────────────────────────────────────────── #
# Basic CRUD                                                              #
# ─────────────────────────────────────────────────────────────────────── #


def test_empty_store_returns_none() -> None:
    with EmailSendHistoryStore(":memory:") as store:
        assert store.lookup("alice@gmail.com") is None
        assert store.lookup_fresh("alice@gmail.com", ttl_days=30) is None
        assert store.count() == 0


def test_first_record_persists_canonical_fields() -> None:
    with EmailSendHistoryStore(":memory:") as store:
        rec = store.record(now=_now(), **_record_kwargs())
        assert isinstance(rec, EmailSendRecord)
        assert rec.email_normalized == "alice@gmail.com"
        assert rec.domain == "gmail.com"
        assert rec.send_count == 1
        assert rec.first_sent_at == _now()
        assert rec.last_sent_at == _now()
        assert rec.last_status == "valid"
        assert rec.last_smtp_result == "deliverable"
        assert rec.last_response_code == 250
        assert rec.last_was_success is True
        assert rec.last_is_catch_all is False
        assert rec.last_inconclusive is False
        assert store.count() == 1


def test_lookup_after_record_returns_same_record() -> None:
    with EmailSendHistoryStore(":memory:") as store:
        store.record(now=_now(), **_record_kwargs())
        rec = store.lookup("alice@gmail.com")
        assert rec is not None
        assert rec.send_count == 1
        assert rec.last_status == "valid"


def test_email_is_normalized_on_read_and_write() -> None:
    with EmailSendHistoryStore(":memory:") as store:
        store.record(
            now=_now(),
            **_record_kwargs(email_normalized="  Alice@GMAIL.com  "),
        )
        # Lookup is case-insensitive and trim-tolerant.
        assert store.lookup("alice@gmail.com") is not None
        assert store.lookup("ALICE@gmail.com") is not None
        assert store.lookup("\talice@gmail.com\n") is not None
        assert store.count() == 1


def test_repeated_record_for_same_email_increments_count_and_preserves_first_seen():
    later = _now() + timedelta(hours=1)
    with EmailSendHistoryStore(":memory:") as store:
        store.record(now=_now(), **_record_kwargs())
        rec = store.record(
            now=later,
            **_record_kwargs(
                status="invalid",
                smtp_result="undeliverable",
                response_code=550,
                response_message="5.1.1 User unknown",
                was_success=False,
                is_catch_all=False,
                inconclusive=False,
            ),
        )
        assert rec.send_count == 2
        assert rec.first_sent_at == _now()
        assert rec.last_sent_at == later
        # Last result fields reflect the *new* outcome, not the old one.
        assert rec.last_status == "invalid"
        assert rec.last_response_code == 550
        assert rec.last_was_success is False


def test_empty_email_rejected_on_write() -> None:
    with EmailSendHistoryStore(":memory:") as store:
        with pytest.raises(ValueError):
            store.record(now=_now(), **_record_kwargs(email_normalized="   "))


def test_blank_email_returns_none_on_lookup() -> None:
    with EmailSendHistoryStore(":memory:") as store:
        assert store.lookup("") is None
        assert store.lookup("   ") is None


def test_delete_removes_row() -> None:
    with EmailSendHistoryStore(":memory:") as store:
        store.record(now=_now(), **_record_kwargs())
        assert store.delete("alice@gmail.com") is True
        assert store.lookup("alice@gmail.com") is None
        assert store.delete("alice@gmail.com") is False  # idempotent


# ─────────────────────────────────────────────────────────────────────── #
# Freshness / TTL                                                         #
# ─────────────────────────────────────────────────────────────────────── #


def test_record_within_ttl_is_fresh() -> None:
    with EmailSendHistoryStore(":memory:") as store:
        store.record(now=_now(), **_record_kwargs())
        rec = store.lookup_fresh(
            "alice@gmail.com",
            ttl_days=30,
            now=_now() + timedelta(days=10),
        )
        assert rec is not None


def test_record_past_ttl_is_stale() -> None:
    with EmailSendHistoryStore(":memory:") as store:
        store.record(now=_now(), **_record_kwargs())
        assert (
            store.lookup_fresh(
                "alice@gmail.com",
                ttl_days=30,
                now=_now() + timedelta(days=31),
            )
            is None
        )


@pytest.mark.parametrize("ttl", [0, None, -1])
def test_zero_or_negative_ttl_means_never_expire(ttl) -> None:
    with EmailSendHistoryStore(":memory:") as store:
        store.record(now=_now(), **_record_kwargs())
        # Even a year later the record is still considered fresh.
        assert (
            store.lookup_fresh(
                "alice@gmail.com",
                ttl_days=ttl,
                now=_now() + timedelta(days=365),
            )
            is not None
        )


# ─────────────────────────────────────────────────────────────────────── #
# Persistence across connections                                          #
# ─────────────────────────────────────────────────────────────────────── #


def test_records_persist_across_store_reopen(tmp_path) -> None:
    db_path = tmp_path / "send_history.sqlite"
    with EmailSendHistoryStore(db_path) as store:
        store.record(now=_now(), **_record_kwargs())
    with EmailSendHistoryStore(db_path) as store:
        rec = store.lookup("alice@gmail.com")
        assert rec is not None
        assert rec.send_count == 1
        assert rec.last_status == "valid"


# ─────────────────────────────────────────────────────────────────────── #
# Housekeeping                                                            #
# ─────────────────────────────────────────────────────────────────────── #


def test_purge_expired_removes_only_old_rows() -> None:
    old = _now() - timedelta(days=90)
    with EmailSendHistoryStore(":memory:") as store:
        store.record(now=old, **_record_kwargs(email_normalized="old@example.com"))
        store.record(now=_now(), **_record_kwargs(email_normalized="new@example.com"))
        removed = store.purge_expired(ttl_days=30, now=_now())
        assert removed == 1
        assert store.lookup("old@example.com") is None
        assert store.lookup("new@example.com") is not None


def test_purge_with_zero_ttl_is_noop() -> None:
    with EmailSendHistoryStore(":memory:") as store:
        store.record(now=_now(), **_record_kwargs())
        assert store.purge_expired(ttl_days=0, now=_now()) == 0
        assert store.count() == 1


def test_iter_all_yields_in_recency_order() -> None:
    earlier = _now()
    later = _now() + timedelta(hours=1)
    with EmailSendHistoryStore(":memory:") as store:
        store.record(now=earlier, **_record_kwargs(email_normalized="a@x.com"))
        store.record(now=later, **_record_kwargs(email_normalized="b@x.com"))
        emails = [r.email_normalized for r in store.iter_all()]
        assert emails == ["b@x.com", "a@x.com"]
