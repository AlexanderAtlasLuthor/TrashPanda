"""Tests for the wall-clock watchdog in app.server._run_job."""

from __future__ import annotations

import threading
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from app import cancellation, server


def _drain_threads(deadline_s: float = 1.0) -> None:
    """Best-effort wait for daemon Timer threads spawned by the watchdog."""
    end = time.monotonic() + deadline_s
    while time.monotonic() < end:
        if not any(t.is_alive() and t.daemon for t in threading.enumerate()
                   if isinstance(t, threading.Timer)):
            return
        time.sleep(0.01)


class TestWatchdog:
    def setup_method(self) -> None:
        cancellation.reset_all()

    def teardown_method(self) -> None:
        cancellation.reset_all()

    def test_disabled_watchdog_returns_none(self) -> None:
        with patch.object(server, "MAX_JOB_WALL_CLOCK_SECONDS", 0):
            assert server._arm_wall_clock_watchdog("job-1") is None

    def test_watchdog_trips_after_deadline(self) -> None:
        # 50ms deadline + 200ms grace lets us assert the flag flips
        # without making the suite slow.
        with patch.object(server, "MAX_JOB_WALL_CLOCK_SECONDS", 0.05):
            timer = server._arm_wall_clock_watchdog("job-watchdog")
        assert timer is not None
        time.sleep(0.2)
        assert cancellation.is_cancelled("job-watchdog") is True

    def test_watchdog_can_be_cancelled_before_deadline(self) -> None:
        # Long deadline; cancel the timer immediately. The cancel flag
        # must not get set.
        with patch.object(server, "MAX_JOB_WALL_CLOCK_SECONDS", 60):
            timer = server._arm_wall_clock_watchdog("job-clean")
        assert timer is not None
        timer.cancel()
        time.sleep(0.05)
        assert cancellation.is_cancelled("job-clean") is False

    def test_watchdog_does_not_clobber_independent_jobs(self) -> None:
        with patch.object(server, "MAX_JOB_WALL_CLOCK_SECONDS", 0.05):
            t1 = server._arm_wall_clock_watchdog("job-a")
            t2 = server._arm_wall_clock_watchdog("job-b")
        time.sleep(0.2)
        # Both flagged.
        assert cancellation.is_cancelled("job-a") is True
        assert cancellation.is_cancelled("job-b") is True
        # An unrelated job_id is unaffected.
        assert cancellation.is_cancelled("job-c") is False
        # Timers should already be done.
        assert t1 is not None and t2 is not None
