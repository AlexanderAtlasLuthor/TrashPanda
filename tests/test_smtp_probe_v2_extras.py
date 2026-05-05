"""Tests for the V2-extras post-mortem fixes:

  - configs/skip_smtp_providers.txt loader
  - catch-all probe runs on a *second* SMTP connection
  - rejected tier separated from suppress in extra_strict_clean
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from app.validation_v2.smtp_probe import (
    DEFAULT_OPAQUE_PROVIDERS,
    load_skip_providers_from_file,
    probe_email_smtplib,
)


# --------------------------------------------------------------------------- #
# Skip-list loader                                                            #
# --------------------------------------------------------------------------- #


class TestSkipProvidersLoader:
    def test_missing_file_returns_defaults(self, tmp_path: Path) -> None:
        result = load_skip_providers_from_file(tmp_path / "no-such-file.txt")
        assert result == DEFAULT_OPAQUE_PROVIDERS

    def test_loaded_file_extends_defaults(self, tmp_path: Path) -> None:
        path = tmp_path / "skip.txt"
        path.write_text(
            "\n".join(
                [
                    "# Custom additions",
                    "Custom-Bad.Example.com",
                    "another-bad.test",
                    "  # leading whitespace + comment",
                    "",
                    "yahoo.com  # already a default",
                ]
            ),
            encoding="utf-8",
        )

        result = load_skip_providers_from_file(path)
        # Defaults still present
        for d in ("yahoo.com", "aol.com", "sbcglobal.net"):
            assert d in result
        # Custom additions normalised to lowercase, comments stripped.
        assert "custom-bad.example.com" in result
        assert "another-bad.test" in result

    def test_default_path_used_when_no_argument(self) -> None:
        # Smoke check: the canonical configs/ file ships the well-known
        # opaque providers. The pipeline depends on this — if it ever
        # becomes empty we want the test suite to scream.
        loaded = load_skip_providers_from_file()
        assert "yahoo.com" in loaded
        assert "aol.com" in loaded


# --------------------------------------------------------------------------- #
# Catch-all on a second SMTP connection                                       #
# --------------------------------------------------------------------------- #


def _make_smtp_factory_recording_calls():
    """Return (factory, list_of_smtp_instances).

    Each call to ``smtplib.SMTP(host, port, timeout=...)`` returns a
    fresh MagicMock that mimics the smtplib.SMTP API and records its
    own command call counts. Tests inspect the list afterwards to
    assert how many connections were opened and what each one did.
    """

    instances: list[MagicMock] = []

    def factory(*_args, **_kwargs):
        smtp = MagicMock()
        sock = MagicMock()
        smtp.sock = sock
        smtp.helo.return_value = (250, b"ok")
        smtp.mail.return_value = (250, b"ok")
        smtp.rcpt.return_value = (250, b"ok")
        smtp.__enter__ = lambda s: s
        smtp.__exit__ = lambda s, *a: False
        instances.append(smtp)
        return smtp

    return factory, instances


class TestCatchAllSecondConnection:
    def test_second_connection_used_for_catch_all_probe_by_default(self) -> None:
        factory, instances = _make_smtp_factory_recording_calls()
        with patch(
            "app.validation_v2.smtp_probe._dns_mx_lookup",
            return_value="mx.example-corp.com",
        ), patch("smtplib.SMTP", side_effect=factory):
            r = probe_email_smtplib(
                "alice@example-corp.com",
                sender="me@me.com",
                # Override skip-list so example-corp.com never gets
                # caught by yahoo-class skip rules.
                skip_providers=[],
            )

        # Two distinct SMTP() calls = two physical connections.
        assert len(instances) == 2
        # Each connection issued exactly one RCPT.
        for smtp in instances:
            assert smtp.rcpt.call_count == 1
        assert r.success is True

    def test_legacy_same_connection_mode_uses_one_socket(self) -> None:
        factory, instances = _make_smtp_factory_recording_calls()
        with patch(
            "app.validation_v2.smtp_probe._dns_mx_lookup",
            return_value="mx.example-corp.com",
        ), patch("smtplib.SMTP", side_effect=factory):
            probe_email_smtplib(
                "alice@example-corp.com",
                sender="me@me.com",
                skip_providers=[],
                catch_all_separate_connection=False,
            )

        # Legacy path: one SMTP() call, two RCPTs on the same socket.
        assert len(instances) == 1
        assert instances[0].rcpt.call_count == 2

    def test_disabled_catch_all_only_opens_one_connection(self) -> None:
        factory, instances = _make_smtp_factory_recording_calls()
        with patch(
            "app.validation_v2.smtp_probe._dns_mx_lookup",
            return_value="mx.example-corp.com",
        ), patch("smtplib.SMTP", side_effect=factory):
            probe_email_smtplib(
                "alice@example-corp.com",
                sender="me@me.com",
                skip_providers=[],
                enable_catch_all_check=False,
            )

        assert len(instances) == 1
        assert instances[0].rcpt.call_count == 1

    def test_catch_all_failure_leaves_real_result_authoritative(self) -> None:
        # Real RCPT succeeds; the catch-all probe raises a network
        # error. The probe must *not* claim catch-all in that case.
        instances: list[MagicMock] = []
        call_idx = {"i": 0}

        def factory(*_args, **_kwargs):
            smtp = MagicMock()
            smtp.sock = MagicMock()
            smtp.helo.return_value = (250, b"ok")
            smtp.mail.return_value = (250, b"ok")
            if call_idx["i"] == 0:
                smtp.rcpt.return_value = (250, b"ok")
            else:
                # Second connection: rcpt raises.
                import smtplib as _smtplib

                smtp.rcpt.side_effect = _smtplib.SMTPException("upstream timeout")
            smtp.__enter__ = lambda s: s
            smtp.__exit__ = lambda s, *a: False
            call_idx["i"] += 1
            instances.append(smtp)
            return smtp

        with patch(
            "app.validation_v2.smtp_probe._dns_mx_lookup",
            return_value="mx.example-corp.com",
        ), patch("smtplib.SMTP", side_effect=factory):
            r = probe_email_smtplib(
                "alice@example-corp.com",
                sender="me@me.com",
                skip_providers=[],
            )

        assert len(instances) == 2
        assert r.success is True
        assert r.is_catch_all_like is False
