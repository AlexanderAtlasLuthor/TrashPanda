"""Tests for the post-mortem SMTP probe hardening.

Covers:
  * per-command socket timeouts are applied,
  * total budget enforcement,
  * cancellation propagation,
  * opaque-provider skip-list,
  * catch-all fake-RCPT is gated on remaining budget,
  * exponential backoff helper.
"""

from __future__ import annotations

import socket
from unittest.mock import MagicMock, patch

import pytest

from app import smtp_runtime
from app.cancellation import (
    JobCancelled,
    cancel,
    is_cancelled,
    make_cancel_check,
    reset_all,
)
from app.validation_v2.smtp_probe import (
    DEFAULT_OPAQUE_PROVIDERS,
    SMTPResult,
    probe_email_smtplib,
)


# --------------------------------------------------------------------------- #
# Cancellation registry                                                       #
# --------------------------------------------------------------------------- #


class TestCancellationRegistry:
    def setup_method(self) -> None:
        reset_all()

    def test_cancel_then_check_returns_true(self) -> None:
        assert cancel("job-A") is True
        assert is_cancelled("job-A") is True

    def test_cancel_is_idempotent(self) -> None:
        assert cancel("job-A") is True
        assert cancel("job-A") is False

    def test_unrelated_jobs_unaffected(self) -> None:
        cancel("job-A")
        assert is_cancelled("job-B") is False

    def test_make_cancel_check_returns_callable(self) -> None:
        check = make_cancel_check("job-X")
        assert check() is False
        cancel("job-X")
        assert check() is True

    def test_make_cancel_check_handles_none_id(self) -> None:
        check = make_cancel_check(None)
        assert check() is False

    def test_job_cancelled_exception_carries_id(self) -> None:
        with pytest.raises(JobCancelled) as exc:
            raise JobCancelled("job-Y")
        assert exc.value.job_id == "job-Y"


# --------------------------------------------------------------------------- #
# Provider skip-list                                                          #
# --------------------------------------------------------------------------- #


class TestProviderSkipList:
    def test_yahoo_is_skipped_without_network(self) -> None:
        with patch(
            "app.validation_v2.smtp_probe._dns_mx_lookup"
        ) as mx, patch("smtplib.SMTP") as smtp_cls:
            r = probe_email_smtplib("anyone@yahoo.com", sender="me@me.com")
            mx.assert_not_called()
            smtp_cls.assert_not_called()
        assert r.inconclusive is True
        assert "skipped" in r.response_message
        assert r.verdict == "inconclusive"

    def test_aol_is_skipped(self) -> None:
        with patch("app.validation_v2.smtp_probe._dns_mx_lookup") as mx:
            r = probe_email_smtplib(
                "x@aol.com", sender="me@me.com",
            )
            mx.assert_not_called()
        assert r.inconclusive is True

    def test_skip_list_can_be_overridden_to_empty(self) -> None:
        # An empty iterable lets even Yahoo through to the network path.
        with patch(
            "app.validation_v2.smtp_probe._dns_mx_lookup", return_value=None
        ):
            r = probe_email_smtplib(
                "x@yahoo.com",
                sender="me@me.com",
                skip_providers=[],
            )
        # No MX → still inconclusive but for a different reason.
        assert r.response_message == "no mx record"

    def test_default_set_includes_known_opaque_providers(self) -> None:
        for d in ("yahoo.com", "aol.com", "ymail.com", "sbcglobal.net"):
            assert d in DEFAULT_OPAQUE_PROVIDERS


# --------------------------------------------------------------------------- #
# Per-command timeouts and total budget                                       #
# --------------------------------------------------------------------------- #


def _fake_smtp_with_socket(
    *, on_rcpt: callable | None = None,
    on_helo: callable | None = None,
    on_mail: callable | None = None,
):
    """Build a MagicMock that mimics smtplib.SMTP with a settable .sock."""

    smtp = MagicMock()
    sock = MagicMock()
    smtp.sock = sock

    def _helo(*_a, **_kw):
        if on_helo is not None:
            on_helo()
        return (250, b"ok")

    def _mail(*_a, **_kw):
        if on_mail is not None:
            on_mail()
        return (250, b"ok")

    def _rcpt(*_a, **_kw):
        if on_rcpt is not None:
            return on_rcpt()
        return (250, b"ok")

    smtp.helo.side_effect = _helo
    smtp.mail.side_effect = _mail
    smtp.rcpt.side_effect = _rcpt
    smtp.__enter__ = lambda s: s
    smtp.__exit__ = lambda s, *a: False
    return smtp, sock


class TestPerCommandTimeouts:
    def test_per_command_settimeout_applied_before_each_command(self) -> None:
        smtp, sock = _fake_smtp_with_socket()
        with patch(
            "app.validation_v2.smtp_probe._dns_mx_lookup",
            return_value="mx.example-corp.com",
        ), patch("smtplib.SMTP", return_value=smtp):
            r = probe_email_smtplib(
                "alice@example-corp.com",
                sender="me@me.com",
                timeout=4.0,
                per_command_timeout=2.0,
            )
        # settimeout called: HELO, MAIL, RCPT, plus the catch-all RCPT.
        assert sock.settimeout.call_count >= 3
        for call in sock.settimeout.call_args_list:
            (value,) = call.args
            assert value <= 2.0 + 1e-9
            assert value >= 0.5
        assert r.success is True

    def test_total_budget_blocks_probe_when_exhausted(self) -> None:
        # The probe should bail out before opening a connection if its
        # total budget is already exhausted.
        with patch(
            "app.validation_v2.smtp_probe._dns_mx_lookup",
            return_value="mx.example-corp.com",
        ), patch("smtplib.SMTP") as smtp_cls:
            r = probe_email_smtplib(
                "alice@example-corp.com",
                sender="me@me.com",
                timeout=4.0,
                total_budget_seconds=0.0,
            )
            smtp_cls.assert_not_called()
        assert r.inconclusive is True
        assert "timeout" in r.response_message.lower() or \
               "budget" in r.response_message.lower()


class TestCatchAllBudgetGating:
    def test_catch_all_skipped_when_real_rcpt_eats_the_budget(self) -> None:
        # Drive the monotonic clock so it stays constant until the
        # real RCPT completes, then jumps near the deadline. The
        # post-RCPT budget guard must trip and the fake-RCPT must not
        # execute.
        smtp, _sock = _fake_smtp_with_socket()
        # Each invocation pops one value; the last value is sticky.
        # Layout: many "100.0" readings cover deadline-set + every
        # pre-command _budget_left/_too_little_budget check; the
        # final "101.95" simulates the budget being consumed *during*
        # the real RCPT call.
        readings = [100.0] * 7 + [101.95] * 20

        index = {"i": 0}

        def _clock() -> float:
            i = min(index["i"], len(readings) - 1)
            index["i"] += 1
            return readings[i]

        with patch(
            "app.validation_v2.smtp_probe._dns_mx_lookup",
            return_value="mx.example-corp.com",
        ), patch("smtplib.SMTP", return_value=smtp), patch(
            "app.validation_v2.smtp_probe.time.monotonic", side_effect=_clock
        ):
            r = probe_email_smtplib(
                "alice@example-corp.com",
                sender="me@me.com",
                timeout=4.0,
                total_budget_seconds=2.0,
                per_command_timeout=1.0,
            )
        # Real RCPT happened exactly once; fake RCPT was gated.
        assert smtp.rcpt.call_count == 1
        assert r.success is True
        assert r.is_catch_all_like is False

    def test_catch_all_can_be_disabled_explicitly(self) -> None:
        smtp, _sock = _fake_smtp_with_socket()
        with patch(
            "app.validation_v2.smtp_probe._dns_mx_lookup",
            return_value="mx.example-corp.com",
        ), patch("smtplib.SMTP", return_value=smtp):
            r = probe_email_smtplib(
                "alice@example-corp.com",
                sender="me@me.com",
                enable_catch_all_check=False,
            )
        assert smtp.rcpt.call_count == 1
        assert r.is_catch_all_like is False


class TestCancelPropagation:
    def setup_method(self) -> None:
        reset_all()

    def test_probe_returns_inconclusive_when_pre_cancelled(self) -> None:
        check = make_cancel_check("job-Z")
        cancel("job-Z")
        with patch(
            "app.validation_v2.smtp_probe._dns_mx_lookup"
        ) as mx, patch("smtplib.SMTP") as smtp_cls:
            r = probe_email_smtplib(
                "alice@example-corp.com",
                sender="me@me.com",
                cancel_check=check,
            )
            mx.assert_not_called()
            smtp_cls.assert_not_called()
        assert r.inconclusive is True
        assert r.response_message == "cancelled"


class TestSocketTimeoutTranslated:
    def test_socket_timeout_during_rcpt_is_inconclusive(self) -> None:
        smtp, _sock = _fake_smtp_with_socket(
            on_rcpt=lambda: (_ for _ in ()).throw(socket.timeout("read timed out")),
        )
        with patch(
            "app.validation_v2.smtp_probe._dns_mx_lookup",
            return_value="mx.example-corp.com",
        ), patch("smtplib.SMTP", return_value=smtp):
            r = probe_email_smtplib(
                "alice@example-corp.com",
                sender="me@me.com",
            )
        assert r.success is False
        assert r.inconclusive is True
        assert "timeout" in r.response_message.lower()


# --------------------------------------------------------------------------- #
# Backoff helper                                                              #
# --------------------------------------------------------------------------- #


class TestBackoff:
    def test_backoff_grows_then_caps(self) -> None:
        delays = [smtp_runtime.compute_retry_backoff_seconds(a) for a in range(1, 7)]
        # Strictly non-decreasing.
        for a, b in zip(delays, delays[1:]):
            assert a <= b
        # Capped.
        assert delays[-1] <= smtp_runtime.SMTP_RETRY_MAX_BACKOFF_SECONDS

    def test_attempt_zero_returns_zero(self) -> None:
        assert smtp_runtime.compute_retry_backoff_seconds(0) == 0.0
        assert smtp_runtime.compute_retry_backoff_seconds(-1) == 0.0

    def test_retry_execution_enabled_flag(self) -> None:
        assert smtp_runtime.SMTP_RETRY_EXECUTION_ENABLED is True


# --------------------------------------------------------------------------- #
# SMTPResult sanity (regression)                                              #
# --------------------------------------------------------------------------- #


class TestResultRegression:
    def test_invalid_email_inconclusive(self) -> None:
        r = probe_email_smtplib("not-an-email", sender="me@me.com")
        assert isinstance(r, SMTPResult)
        assert r.inconclusive is True
        assert r.success is False
