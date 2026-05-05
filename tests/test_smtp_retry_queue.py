"""V2.10.11 — unit tests for ``app.db.smtp_retry_queue``."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.db.smtp_retry_queue import (
    DEFAULT_RETRY_SCHEDULE_MINUTES,
    RetryQueueCounts,
    SMTPRetryQueue,
    STATE_EXHAUSTED,
    STATE_EXPIRED,
    STATE_PENDING,
    STATE_RUNNING,
    STATE_SUCCEEDED,
    open_for_run,
)


@pytest.fixture
def tmp_queue(tmp_path: Path) -> SMTPRetryQueue:
    queue = open_for_run(tmp_path)
    yield queue
    queue.close()


def _enqueue(queue: SMTPRetryQueue, **overrides):
    base = dict(
        job_id="job-1",
        source_row=1,
        email="alice@example-corp.com",
        domain="example-corp.com",
        provider_family="corporate_unknown",
        last_status="temp_fail",
        last_response_code=421,
        last_response_message="4.7.0 Greylisted",
    )
    base.update(overrides)
    return queue.enqueue(**base)


# --------------------------------------------------------------------- #
# Schema + enqueue
# --------------------------------------------------------------------- #


class TestEnqueue:
    def test_creates_db_file(self, tmp_path: Path):
        queue = open_for_run(tmp_path)
        try:
            assert queue.path.is_file()
        finally:
            queue.close()

    def test_first_enqueue_returns_true(self, tmp_queue: SMTPRetryQueue):
        assert _enqueue(tmp_queue) is True

    def test_duplicate_returns_false(self, tmp_queue: SMTPRetryQueue):
        assert _enqueue(tmp_queue) is True
        assert _enqueue(tmp_queue) is False  # same (job_id, source_row, email)

    def test_empty_email_rejected(self, tmp_queue: SMTPRetryQueue):
        assert _enqueue(tmp_queue, email="") is False

    def test_initial_state_is_pending(self, tmp_queue: SMTPRetryQueue):
        _enqueue(tmp_queue)
        rows = tmp_queue.snapshot()
        assert len(rows) == 1
        assert rows[0].state == STATE_PENDING
        assert rows[0].attempt == 0

    def test_empty_schedule_marks_exhausted_immediately(
        self, tmp_queue: SMTPRetryQueue
    ):
        ok = tmp_queue.enqueue(
            job_id="j",
            source_row=2,
            email="b@example.com",
            domain="example.com",
            provider_family="corporate_unknown",
            last_status="temp_fail",
            last_response_code=421,
            last_response_message="x",
            schedule_minutes=(),
        )
        assert ok is True
        rows = tmp_queue.snapshot()
        assert rows[0].state == STATE_EXHAUSTED


# --------------------------------------------------------------------- #
# claim_pending atomicity + scheduling
# --------------------------------------------------------------------- #


class TestClaimPending:
    def test_claim_only_due_rows(self, tmp_queue: SMTPRetryQueue):
        now = datetime.now(tz=timezone.utc)
        _enqueue(tmp_queue, source_row=1, email="a@x.com")
        # Manually push next_retry_at into the future for row 2 by
        # claiming + rescheduling; simpler approach is just to claim
        # with a `now` before the schedule's first slot.
        rows = tmp_queue.claim_pending(
            now=now,  # right at enqueue time, before 15-min wait
            limit=10,
        )
        assert rows == []  # not yet due
        rows = tmp_queue.claim_pending(
            now=now + timedelta(minutes=20),
            limit=10,
        )
        assert len(rows) == 1
        assert rows[0].state == STATE_RUNNING

    def test_claim_marks_running(self, tmp_queue: SMTPRetryQueue):
        _enqueue(tmp_queue)
        future = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        rows = tmp_queue.claim_pending(now=future, limit=10)
        assert rows[0].state == STATE_RUNNING
        snapshot = tmp_queue.snapshot()
        assert snapshot[0].state == STATE_RUNNING

    def test_already_running_is_not_re_claimed(
        self, tmp_queue: SMTPRetryQueue
    ):
        _enqueue(tmp_queue)
        future = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        first = tmp_queue.claim_pending(now=future)
        assert len(first) == 1
        second = tmp_queue.claim_pending(now=future)
        assert second == []


# --------------------------------------------------------------------- #
# Reschedule + state transitions
# --------------------------------------------------------------------- #


class TestStateTransitions:
    def test_mark_succeeded(self, tmp_queue: SMTPRetryQueue):
        _enqueue(tmp_queue)
        future = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        rows = tmp_queue.claim_pending(now=future)
        tmp_queue.mark_succeeded(
            rows[0].id,
            last_status="valid",
            last_response_code=250,
            last_response_message="OK",
        )
        snapshot = tmp_queue.snapshot()
        assert snapshot[0].state == STATE_SUCCEEDED
        assert snapshot[0].last_status == "valid"
        assert snapshot[0].last_response_code == 250

    def test_reschedule_within_budget(self, tmp_queue: SMTPRetryQueue):
        _enqueue(tmp_queue)
        future = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        rows = tmp_queue.claim_pending(now=future)
        new_state = tmp_queue.reschedule(
            rows[0].id,
            schedule_minutes=DEFAULT_RETRY_SCHEDULE_MINUTES,
            last_status="temp_fail",
            last_response_code=421,
            last_response_message="still greylisted",
            now=future,
        )
        assert new_state == STATE_PENDING
        snap = tmp_queue.snapshot()[0]
        assert snap.attempt == 1
        # next_retry_at should be in the future (now + schedule[1] = 30min)
        assert snap.next_retry_at > future

    def test_reschedule_exhausts_after_max(self, tmp_queue: SMTPRetryQueue):
        _enqueue(tmp_queue)
        clock = datetime.now(tz=timezone.utc)
        # Walk through every retry slot. Advance the clock past each
        # successive next_retry_at by jumping a full day per pass —
        # enough to clear every slot in the schedule.
        new_state = STATE_PENDING
        for _ in range(len(DEFAULT_RETRY_SCHEDULE_MINUTES)):
            clock += timedelta(days=1)
            rows = tmp_queue.claim_pending(now=clock)
            assert len(rows) == 1, "row should be due each iteration"
            new_state = tmp_queue.reschedule(
                rows[0].id,
                schedule_minutes=DEFAULT_RETRY_SCHEDULE_MINUTES,
                last_status="temp_fail",
                now=clock,
            )
        assert new_state == STATE_EXHAUSTED
        snap = tmp_queue.snapshot()[0]
        assert snap.state == STATE_EXHAUSTED


class TestExpire:
    def test_old_pending_rows_expire(self, tmp_queue: SMTPRetryQueue):
        _enqueue(tmp_queue)
        # 25 hours later, pending row should expire under 24h TTL.
        far_future = datetime.now(tz=timezone.utc) + timedelta(hours=25)
        moved = tmp_queue.expire_old(
            ttl=timedelta(hours=24),
            now=far_future,
        )
        assert moved == 1
        assert tmp_queue.snapshot()[0].state == STATE_EXPIRED

    def test_recent_pending_does_not_expire(
        self, tmp_queue: SMTPRetryQueue
    ):
        _enqueue(tmp_queue)
        not_so_far = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        moved = tmp_queue.expire_old(
            ttl=timedelta(hours=24),
            now=not_so_far,
        )
        assert moved == 0
        assert tmp_queue.snapshot()[0].state == STATE_PENDING


class TestCounts:
    def test_counts_track_states(self, tmp_queue: SMTPRetryQueue):
        # 1 pending, 1 succeeded, 1 exhausted
        _enqueue(tmp_queue, source_row=1, email="a@x.com")
        _enqueue(tmp_queue, source_row=2, email="b@x.com")
        _enqueue(tmp_queue, source_row=3, email="c@x.com")
        future = datetime.now(tz=timezone.utc) + timedelta(hours=1)

        rows = tmp_queue.claim_pending(now=future, limit=2)
        tmp_queue.mark_succeeded(rows[0].id, last_status="valid")
        tmp_queue.reschedule(
            rows[1].id,
            schedule_minutes=(),  # empty → exhausts on next call... but reschedule
            last_status="temp_fail",
            now=future,
        )
        # Reschedule with an empty schedule means max_retries=0, so it
        # exhausts immediately.
        counts = tmp_queue.counts()
        assert counts.succeeded == 1
        assert counts.exhausted == 1
        assert counts.pending == 1
        assert counts.total == 3
