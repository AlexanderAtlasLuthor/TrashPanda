"""V2.10.12 — unit tests for ``app.db.pilot_send_tracker``."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.db.pilot_send_tracker import (
    DEFAULT_EXPIRY_HOURS,
    DEFAULT_WAIT_WINDOW_HOURS,
    DELIVERY_VERIFIED_VERDICTS,
    DO_NOT_SEND_VERDICTS,
    PilotSendTracker,
    STATE_EXPIRED,
    STATE_PENDING_SEND,
    STATE_SENT,
    STATE_VERDICT_READY,
    VERDICT_BLOCKED,
    VERDICT_COMPLAINT,
    VERDICT_DELIVERED,
    VERDICT_HARD_BOUNCE,
    VERDICT_SOFT_BOUNCE,
    VERDICT_UNKNOWN,
    open_for_run,
)


@pytest.fixture
def tmp_tracker(tmp_path: Path):
    with open_for_run(tmp_path) as tracker:
        yield tracker


def _add(tracker: PilotSendTracker, **overrides) -> bool:
    base = dict(
        job_id="job-1",
        batch_id="batch-1",
        source_row=1,
        email="alice@example-corp.com",
        domain="example-corp.com",
        provider_family="corporate_unknown",
        verp_token="tok-aaa-001",
    )
    base.update(overrides)
    return tracker.add_candidate(**base)


# --------------------------------------------------------------------- #
# Add candidate + uniqueness
# --------------------------------------------------------------------- #


class TestAddCandidate:
    def test_first_add_returns_true(self, tmp_tracker):
        assert _add(tmp_tracker) is True
        snap = tmp_tracker.snapshot()
        assert len(snap) == 1
        assert snap[0].state == STATE_PENDING_SEND

    def test_duplicate_email_in_same_batch_rejected(self, tmp_tracker):
        assert _add(tmp_tracker, verp_token="t1") is True
        assert _add(tmp_tracker, verp_token="t2") is False  # same (job, batch, email)

    def test_duplicate_token_rejected(self, tmp_tracker):
        assert _add(tmp_tracker, email="a@x.com", verp_token="dup") is True
        assert _add(tmp_tracker, email="b@y.com", verp_token="dup") is False

    def test_empty_email_rejected(self, tmp_tracker):
        assert _add(tmp_tracker, email="") is False

    def test_empty_token_rejected(self, tmp_tracker):
        assert _add(tmp_tracker, verp_token="") is False

    def test_different_batch_same_email_allowed(self, tmp_tracker):
        assert _add(tmp_tracker, batch_id="b1", verp_token="t1") is True
        assert _add(tmp_tracker, batch_id="b2", verp_token="t2") is True


# --------------------------------------------------------------------- #
# Mark sent
# --------------------------------------------------------------------- #


class TestMarkSent:
    def test_pending_to_sent(self, tmp_tracker):
        _add(tmp_tracker)
        row = tmp_tracker.snapshot()[0]
        ts = datetime.now(tz=timezone.utc)
        tmp_tracker.mark_sent(row.id, message_id="<msg@x>", now=ts)
        snap = tmp_tracker.snapshot()
        assert snap[0].state == STATE_SENT
        assert snap[0].message_id == "<msg@x>"
        assert snap[0].sent_at is not None


# --------------------------------------------------------------------- #
# DSN recording
# --------------------------------------------------------------------- #


class TestRecordDSN:
    def test_dsn_only_applies_to_sent_state(self, tmp_tracker):
        _add(tmp_tracker)
        applied = tmp_tracker.record_dsn(
            "tok-aaa-001",
            dsn_status=VERDICT_HARD_BOUNCE,
        )
        # Row is in pending_send, record_dsn requires sent → no rows.
        assert applied is False
        assert tmp_tracker.snapshot()[0].state == STATE_PENDING_SEND

    def test_dsn_transitions_sent_to_verdict_ready(self, tmp_tracker):
        _add(tmp_tracker)
        row = tmp_tracker.snapshot()[0]
        tmp_tracker.mark_sent(row.id, message_id=None)
        applied = tmp_tracker.record_dsn(
            "tok-aaa-001",
            dsn_status=VERDICT_HARD_BOUNCE,
            dsn_diagnostic="550 No such user",
            dsn_smtp_code="5.1.1",
        )
        assert applied is True
        snap = tmp_tracker.snapshot()
        assert snap[0].state == STATE_VERDICT_READY
        assert snap[0].dsn_status == VERDICT_HARD_BOUNCE
        assert snap[0].dsn_smtp_code == "5.1.1"

    def test_dsn_idempotent_second_call_noop(self, tmp_tracker):
        _add(tmp_tracker)
        row = tmp_tracker.snapshot()[0]
        tmp_tracker.mark_sent(row.id, message_id=None)
        tmp_tracker.record_dsn("tok-aaa-001", dsn_status=VERDICT_HARD_BOUNCE)
        # Second call hits state=verdict_ready filter → no update.
        applied = tmp_tracker.record_dsn(
            "tok-aaa-001",
            dsn_status=VERDICT_BLOCKED,
        )
        assert applied is False
        assert tmp_tracker.snapshot()[0].dsn_status == VERDICT_HARD_BOUNCE


# --------------------------------------------------------------------- #
# Wait-window + expiry transitions
# --------------------------------------------------------------------- #


class TestWaitWindowAndExpiry:
    def _setup_sent(self, tracker, *, sent_at: datetime):
        _add(tracker)
        row = tracker.snapshot()[0]
        tracker.mark_sent(row.id, message_id=None, now=sent_at)
        return row.id

    def test_mark_delivered_after_wait_flips_old_sent(self, tmp_tracker):
        sent_at = datetime.now(tz=timezone.utc)
        self._setup_sent(tmp_tracker, sent_at=sent_at)
        future = sent_at + timedelta(hours=DEFAULT_WAIT_WINDOW_HOURS + 1)
        moved = tmp_tracker.mark_delivered_after_wait(
            wait_window_hours=DEFAULT_WAIT_WINDOW_HOURS, now=future,
        )
        assert moved == 1
        snap = tmp_tracker.snapshot()
        assert snap[0].state == STATE_VERDICT_READY
        assert snap[0].dsn_status == VERDICT_DELIVERED

    def test_mark_delivered_skips_recent(self, tmp_tracker):
        sent_at = datetime.now(tz=timezone.utc)
        self._setup_sent(tmp_tracker, sent_at=sent_at)
        moved = tmp_tracker.mark_delivered_after_wait(
            wait_window_hours=48,
            now=sent_at + timedelta(hours=2),
        )
        assert moved == 0

    def test_mark_expired_flips_long_sent(self, tmp_tracker):
        sent_at = datetime.now(tz=timezone.utc)
        self._setup_sent(tmp_tracker, sent_at=sent_at)
        moved = tmp_tracker.mark_expired(
            expiry_hours=DEFAULT_EXPIRY_HOURS,
            now=sent_at + timedelta(hours=DEFAULT_EXPIRY_HOURS + 1),
        )
        assert moved == 1
        snap = tmp_tracker.snapshot()
        assert snap[0].state == STATE_EXPIRED
        assert snap[0].dsn_status == VERDICT_UNKNOWN


# --------------------------------------------------------------------- #
# Counts + hard_bounce_rate
# --------------------------------------------------------------------- #


class TestCounts:
    def test_counts_aggregator(self, tmp_tracker):
        # 3 rows: 1 delivered, 1 hard_bounce, 1 still pending_send.
        _add(tmp_tracker, source_row=1, email="a@x.com", verp_token="t1")
        _add(tmp_tracker, source_row=2, email="b@x.com", verp_token="t2")
        _add(tmp_tracker, source_row=3, email="c@x.com", verp_token="t3")
        ts = datetime.now(tz=timezone.utc)
        for row in tmp_tracker.snapshot()[:2]:
            tmp_tracker.mark_sent(row.id, message_id=None, now=ts)
        tmp_tracker.record_dsn("t1", dsn_status=VERDICT_DELIVERED)
        tmp_tracker.record_dsn("t2", dsn_status=VERDICT_HARD_BOUNCE)

        counts = tmp_tracker.counts()
        assert counts.pending_send == 1
        assert counts.verdict_ready == 2
        assert counts.delivered == 1
        assert counts.hard_bounce == 1
        assert counts.total == 3
        # 1 hard / (1 hard + 1 delivered) = 0.5
        assert counts.hard_bounce_rate == 0.5

    def test_counts_scoped_by_batch(self, tmp_tracker):
        _add(tmp_tracker, batch_id="b1", verp_token="t1")
        _add(tmp_tracker, batch_id="b2", verp_token="t2")
        b1 = tmp_tracker.counts(batch_id="b1")
        b2 = tmp_tracker.counts(batch_id="b2")
        assert b1.total == 1
        assert b2.total == 1


# --------------------------------------------------------------------- #
# Lookup helpers
# --------------------------------------------------------------------- #


class TestLookup:
    def test_by_token_round_trip(self, tmp_tracker):
        _add(tmp_tracker, verp_token="lookup-tok")
        row = tmp_tracker.by_token("lookup-tok")
        assert row is not None
        assert row.verp_token == "lookup-tok"

    def test_by_token_missing(self, tmp_tracker):
        assert tmp_tracker.by_token("nonexistent") is None

    def test_snapshot_filters(self, tmp_tracker):
        _add(tmp_tracker, source_row=1, email="a@x.com", verp_token="t1")
        _add(tmp_tracker, source_row=2, email="b@x.com", verp_token="t2")
        ts = datetime.now(tz=timezone.utc)
        for row in tmp_tracker.snapshot():
            tmp_tracker.mark_sent(row.id, message_id=None, now=ts)
        tmp_tracker.record_dsn("t1", dsn_status=VERDICT_DELIVERED)
        delivered = tmp_tracker.snapshot(verdicts=[VERDICT_DELIVERED])
        assert len(delivered) == 1
        assert delivered[0].verp_token == "t1"


class TestVerdictSets:
    def test_do_not_send_set(self):
        assert VERDICT_HARD_BOUNCE in DO_NOT_SEND_VERDICTS
        assert VERDICT_BLOCKED in DO_NOT_SEND_VERDICTS
        assert VERDICT_COMPLAINT in DO_NOT_SEND_VERDICTS
        assert VERDICT_DELIVERED not in DO_NOT_SEND_VERDICTS
        assert VERDICT_SOFT_BOUNCE not in DO_NOT_SEND_VERDICTS

    def test_delivery_verified_set(self):
        assert VERDICT_DELIVERED in DELIVERY_VERIFIED_VERDICTS
        assert VERDICT_HARD_BOUNCE not in DELIVERY_VERIFIED_VERDICTS
