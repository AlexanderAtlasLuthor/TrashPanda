"""Unit tests for app.validation_v2.smtp_probe.

Covers the SMTPResult verdict truth table, the dry-run primitive, and
the error-path of the real smtplib probe (mocked network). We never
open a real socket — the ``smtplib.SMTP`` class is monkey-patched so
tests stay hermetic.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from app.validation_v2.smtp_probe import (
    SMTPResult,
    probe_email_dry_run,
    probe_email_smtplib,
)


# ─────────────────────────────────────────────────────────────────────── #
# SMTPResult.verdict                                                      #
# ─────────────────────────────────────────────────────────────────────── #


class TestVerdict:
    def test_catch_all_wins_over_everything_else(self) -> None:
        r = SMTPResult(True, 250, "ok", is_catch_all_like=True, inconclusive=False)
        assert r.verdict == "catch_all"

    def test_inconclusive_when_not_catch_all_and_not_success(self) -> None:
        r = SMTPResult(False, 451, "temp fail", False, True)
        assert r.verdict == "inconclusive"

    def test_deliverable_for_plain_success(self) -> None:
        r = SMTPResult(True, 250, "ok", False, False)
        assert r.verdict == "deliverable"

    def test_undeliverable_for_plain_rejection(self) -> None:
        r = SMTPResult(False, 550, "no such user", False, False)
        assert r.verdict == "undeliverable"

    def test_inconclusive_wins_over_undeliverable_when_both_flagged(self) -> None:
        r = SMTPResult(False, 451, "busy", False, True)
        assert r.verdict == "inconclusive"


# ─────────────────────────────────────────────────────────────────────── #
# Dry-run primitive                                                       #
# ─────────────────────────────────────────────────────────────────────── #


class TestDryRun:
    def test_dry_run_returns_inconclusive_result(self) -> None:
        r = probe_email_dry_run("anyone@example.com")
        assert r.success is False
        assert r.inconclusive is True
        assert r.is_catch_all_like is False
        assert r.response_message == "dry_run"

    def test_dry_run_accepts_and_ignores_kwargs(self) -> None:
        r = probe_email_dry_run(
            "x@y.com", sender="me@me.com", timeout=99.0, local_hostname="host",
        )
        assert r.verdict == "inconclusive"


# ─────────────────────────────────────────────────────────────────────── #
# probe_email_smtplib — every error path is caught                        #
# ─────────────────────────────────────────────────────────────────────── #


class TestSMTPLibErrorHandling:
    """Every failure mode must collapse into inconclusive, never raise."""

    def test_invalid_email_returns_inconclusive(self) -> None:
        r = probe_email_smtplib("not-an-email", sender="me@me.com")
        assert r.inconclusive is True
        assert r.success is False
        assert "invalid" in r.response_message.lower()

    def test_empty_email_returns_inconclusive(self) -> None:
        r = probe_email_smtplib("", sender="me@me.com")
        assert r.inconclusive is True

    def test_no_mx_record_returns_inconclusive(self) -> None:
        with patch("app.validation_v2.smtp_probe._dns_mx_lookup", return_value=None):
            r = probe_email_smtplib("foo@unresolvable.tld", sender="me@me.com")
        assert r.inconclusive is True
        assert "mx" in r.response_message.lower()

    def test_network_error_is_caught(self) -> None:
        """If smtplib raises OSError, we normalise it."""
        mock_smtp = MagicMock(side_effect=OSError("connection refused"))
        with patch("app.validation_v2.smtp_probe._dns_mx_lookup", return_value="mx.example"):
            with patch("smtplib.SMTP", mock_smtp):
                r = probe_email_smtplib("foo@example.com", sender="me@me.com")
        assert r.inconclusive is True
        assert r.success is False

    def test_smtplib_exception_is_caught(self) -> None:
        import smtplib
        mock_smtp = MagicMock(side_effect=smtplib.SMTPException("boom"))
        with patch("app.validation_v2.smtp_probe._dns_mx_lookup", return_value="mx.example"):
            with patch("smtplib.SMTP", mock_smtp):
                r = probe_email_smtplib("foo@example.com", sender="me@me.com")
        assert r.inconclusive is True
        assert "boom" in r.response_message.lower()


class TestSMTPLibSuccessPath:
    """Happy paths through smtplib — with a fully mocked SMTP object."""

    def _mock_smtp(
        self, *, mail_code: int = 250, rcpt_code: int = 250, fake_code: int = 550,
    ) -> MagicMock:
        """Build a mocked ``smtplib.SMTP`` context manager."""
        smtp_instance = MagicMock()
        smtp_instance.__enter__.return_value = smtp_instance
        smtp_instance.__exit__.return_value = False
        smtp_instance.helo = MagicMock()
        smtp_instance.mail = MagicMock(return_value=(mail_code, b"ok"))
        smtp_instance.rcpt = MagicMock(side_effect=[
            (rcpt_code, b"ok"),
            (fake_code, b"unknown user"),
        ])
        mock_factory = MagicMock(return_value=smtp_instance)
        return mock_factory

    def test_real_address_accepted_fake_rejected_is_deliverable(self) -> None:
        mock_factory = self._mock_smtp(mail_code=250, rcpt_code=250, fake_code=550)
        with patch("app.validation_v2.smtp_probe._dns_mx_lookup", return_value="mx.example"):
            with patch("smtplib.SMTP", mock_factory):
                r = probe_email_smtplib("real@example.com", sender="me@me.com")
        assert r.verdict == "deliverable"
        assert r.success is True
        assert r.is_catch_all_like is False

    def test_both_real_and_fake_accepted_is_catch_all(self) -> None:
        mock_factory = self._mock_smtp(mail_code=250, rcpt_code=250, fake_code=250)
        with patch("app.validation_v2.smtp_probe._dns_mx_lookup", return_value="mx.example"):
            with patch("smtplib.SMTP", mock_factory):
                r = probe_email_smtplib("real@example.com", sender="me@me.com")
        assert r.verdict == "catch_all"
        assert r.is_catch_all_like is True

    def test_rcpt_hard_reject_is_undeliverable(self) -> None:
        mock_factory = self._mock_smtp(mail_code=250, rcpt_code=550, fake_code=550)
        with patch("app.validation_v2.smtp_probe._dns_mx_lookup", return_value="mx.example"):
            with patch("smtplib.SMTP", mock_factory):
                r = probe_email_smtplib("gone@example.com", sender="me@me.com")
        assert r.verdict == "undeliverable"
        assert r.success is False
        assert r.inconclusive is False

    def test_rcpt_temporary_reject_is_inconclusive(self) -> None:
        mock_factory = self._mock_smtp(mail_code=250, rcpt_code=451, fake_code=550)
        with patch("app.validation_v2.smtp_probe._dns_mx_lookup", return_value="mx.example"):
            with patch("smtplib.SMTP", mock_factory):
                r = probe_email_smtplib("x@example.com", sender="me@me.com")
        assert r.verdict == "inconclusive"

    def test_mail_from_rejected_is_inconclusive(self) -> None:
        mock_factory = self._mock_smtp(mail_code=530, rcpt_code=250, fake_code=550)
        with patch("app.validation_v2.smtp_probe._dns_mx_lookup", return_value="mx.example"):
            with patch("smtplib.SMTP", mock_factory):
                r = probe_email_smtplib("x@example.com", sender="me@me.com")
        # MAIL FROM failure → inconclusive, without even calling RCPT.
        assert r.inconclusive is True


# ─────────────────────────────────────────────────────────────────────── #
# Response truncation                                                     #
# ─────────────────────────────────────────────────────────────────────── #


def test_response_message_is_truncated_to_200_chars() -> None:
    long_msg = b"A" * 5000
    smtp_instance = MagicMock()
    smtp_instance.__enter__.return_value = smtp_instance
    smtp_instance.__exit__.return_value = False
    smtp_instance.helo = MagicMock()
    smtp_instance.mail = MagicMock(return_value=(250, b"ok"))
    smtp_instance.rcpt = MagicMock(side_effect=[(250, long_msg), (550, b"no")])
    with patch("app.validation_v2.smtp_probe._dns_mx_lookup", return_value="mx.example"):
        with patch("smtplib.SMTP", MagicMock(return_value=smtp_instance)):
            r = probe_email_smtplib("x@example.com", sender="me@me.com")
    assert len(r.response_message) <= 200
