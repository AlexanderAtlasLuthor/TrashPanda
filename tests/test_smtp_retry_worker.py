"""V2.10.11 — unit tests for ``app.smtp_retry_worker``."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.db.smtp_retry_queue import (
    DEFAULT_RETRY_SCHEDULE_MINUTES,
    SMTPRetryQueue,
    STATE_EXHAUSTED,
    STATE_PENDING,
    STATE_SUCCEEDED,
    open_for_run,
)
from app.smtp_retry_worker import (
    drain_all,
    drain_run_queue,
    iter_run_dirs,
    read_retry_config,
    write_retry_config,
)
from app.validation_v2.smtp_probe import SMTPResult


# --------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------- #


@pytest.fixture
def run_dir_with_pending(tmp_path: Path):
    """A run directory with one pending row enqueued + auto_retry on."""
    write_retry_config(tmp_path, auto_retry_enabled=True)
    queue = open_for_run(tmp_path)
    queue.enqueue(
        job_id="job-1",
        source_row=1,
        email="alice@some-corp.com",
        domain="some-corp.com",
        provider_family="corporate_unknown",
        last_status="temp_fail",
        last_response_code=421,
        last_response_message="4.7.0 Greylisted",
    )
    queue.close()
    return tmp_path


def _result_valid() -> SMTPResult:
    return SMTPResult(
        success=True,
        response_code=250,
        response_message="2.1.5 OK",
        is_catch_all_like=False,
        inconclusive=False,
    )


def _result_temp_fail() -> SMTPResult:
    return SMTPResult(
        success=False,
        response_code=421,
        response_message="4.7.0 Try again later",
        is_catch_all_like=False,
        inconclusive=True,
    )


# --------------------------------------------------------------------- #
# Config persistence
# --------------------------------------------------------------------- #


class TestRetryConfig:
    def test_missing_returns_default_off(self, tmp_path: Path):
        cfg = read_retry_config(tmp_path)
        assert cfg["auto_retry_enabled"] is False

    def test_round_trip(self, tmp_path: Path):
        write_retry_config(tmp_path, auto_retry_enabled=True)
        assert read_retry_config(tmp_path)["auto_retry_enabled"] is True

    def test_corrupted_json_falls_back_to_off(self, tmp_path: Path):
        from app.db.smtp_retry_queue import SMTP_RETRY_CONFIG_FILENAME

        (tmp_path / SMTP_RETRY_CONFIG_FILENAME).write_text("not json")
        assert read_retry_config(tmp_path)["auto_retry_enabled"] is False


# --------------------------------------------------------------------- #
# drain_run_queue contract
# --------------------------------------------------------------------- #


class TestDrainRunQueue:
    def test_no_queue_file_is_a_noop(self, tmp_path: Path):
        result = drain_run_queue(tmp_path, probe_fn=lambda *a, **kw: _result_valid())
        assert result.probed == 0
        assert result.succeeded == 0

    def test_auto_retry_off_skips_drain(self, run_dir_with_pending: Path):
        write_retry_config(run_dir_with_pending, auto_retry_enabled=False)
        future = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        result = drain_run_queue(
            run_dir_with_pending,
            probe_fn=lambda *a, **kw: _result_valid(),
            now=future,
        )
        assert result.probed == 0
        # Queue still has a pending row.
        with open_for_run(run_dir_with_pending) as queue:
            counts = queue.counts()
        assert counts.pending == 1

    def test_require_auto_retry_false_drains_anyway(
        self, run_dir_with_pending: Path
    ):
        """The on-demand HTTP endpoint passes require_auto_retry=False
        to drain regardless of the operator flag."""
        write_retry_config(run_dir_with_pending, auto_retry_enabled=False)
        future = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        result = drain_run_queue(
            run_dir_with_pending,
            probe_fn=lambda *a, **kw: _result_valid(),
            require_auto_retry=False,
            now=future,
        )
        assert result.probed == 1
        assert result.succeeded == 1

    def test_valid_response_marks_succeeded(
        self, run_dir_with_pending: Path
    ):
        future = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        result = drain_run_queue(
            run_dir_with_pending,
            probe_fn=lambda *a, **kw: _result_valid(),
            now=future,
        )
        assert result.probed == 1
        assert result.succeeded == 1
        assert result.exhausted == 0
        with open_for_run(run_dir_with_pending) as queue:
            snap = queue.snapshot()
        assert snap[0].state == STATE_SUCCEEDED
        assert snap[0].last_status == "valid"

    def test_temp_fail_reschedules(self, run_dir_with_pending: Path):
        future = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        result = drain_run_queue(
            run_dir_with_pending,
            probe_fn=lambda *a, **kw: _result_temp_fail(),
            now=future,
        )
        assert result.probed == 1
        assert result.succeeded == 0
        assert result.rescheduled == 1
        with open_for_run(run_dir_with_pending) as queue:
            snap = queue.snapshot()
        assert snap[0].state == STATE_PENDING
        assert snap[0].attempt == 1

    def test_repeated_temp_fail_eventually_exhausts(
        self, run_dir_with_pending: Path
    ):
        # Advance the clock just past each scheduled next_retry_at so
        # the TTL (default 24h) doesn't expire the row before the
        # retry budget exhausts. The total elapsed time stays within
        # 24h: 16min + 31min + 61min ≈ 108min.
        clock = datetime.now(tz=timezone.utc)
        for slot_minutes in DEFAULT_RETRY_SCHEDULE_MINUTES:
            clock += timedelta(minutes=slot_minutes + 1)
            drain_run_queue(
                run_dir_with_pending,
                probe_fn=lambda *a, **kw: _result_temp_fail(),
                now=clock,
            )
        with open_for_run(run_dir_with_pending) as queue:
            snap = queue.snapshot()
        assert snap[0].state == STATE_EXHAUSTED


# --------------------------------------------------------------------- #
# iter_run_dirs / drain_all
# --------------------------------------------------------------------- #


class TestDrainAll:
    def test_iterates_jobs_with_queue_files(self, tmp_path: Path):
        # Create runtime/jobs/<job_id>/<run_id>/ layout.
        runtime = tmp_path / "runtime" / "jobs"
        run_a = runtime / "job_a" / "run_001"
        run_b = runtime / "job_b" / "run_002"
        empty = runtime / "job_c" / "run_003"
        for d in (run_a, run_b, empty):
            d.mkdir(parents=True)
        # Only run_a + run_b have queue files.
        for d in (run_a, run_b):
            with open_for_run(d) as queue:
                queue.enqueue(
                    job_id=d.parent.name,
                    source_row=1,
                    email="x@example.com",
                    domain="example.com",
                    provider_family="corporate_unknown",
                    last_status="temp_fail",
                    last_response_code=421,
                    last_response_message="x",
                )
            write_retry_config(d, auto_retry_enabled=True)

        found = list(iter_run_dirs(runtime))
        assert {d.name for d in found} == {"run_001", "run_002"}

    def test_drain_all_returns_per_run_results(self, tmp_path: Path):
        runtime = tmp_path / "runtime" / "jobs"
        run_a = runtime / "job_a" / "run_001"
        run_a.mkdir(parents=True)
        with open_for_run(run_a) as queue:
            queue.enqueue(
                job_id="job_a",
                source_row=1,
                email="a@example.com",
                domain="example.com",
                provider_family="corporate_unknown",
                last_status="temp_fail",
                last_response_code=421,
                last_response_message="x",
            )
        write_retry_config(run_a, auto_retry_enabled=True)

        future = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        results = drain_all(
            runtime,
            probe_fn=lambda *a, **kw: _result_valid(),
            now=future,
        )
        assert len(results) == 1
        assert results[0].succeeded == 1


# --------------------------------------------------------------------- #
# Defensive guards
# --------------------------------------------------------------------- #


class TestDrainResilience:
    def test_probe_exception_reschedules(self, run_dir_with_pending: Path):
        def _boom(*_a, **_kw):
            raise RuntimeError("network unreachable")

        future = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        result = drain_run_queue(
            run_dir_with_pending,
            probe_fn=_boom,
            now=future,
        )
        assert result.probed == 1
        assert result.succeeded == 0
        assert result.rescheduled == 1
