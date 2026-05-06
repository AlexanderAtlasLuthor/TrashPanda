"""V2.10.12 — pilot SMTP sender tests.

Use a fake SMTP transport so no real socket is opened. The sender
uses ``dns.resolver`` for MX lookup; we monkeypatch
``app.pilot_send.sender._resolve_mx`` to avoid real DNS.
"""

from __future__ import annotations

import smtplib
from unittest.mock import MagicMock

import pytest

from app.pilot_send.sender import (
    PilotSendOutcome,
    SMTPSender,
)


@pytest.fixture(autouse=True)
def _mock_mx(monkeypatch):
    """Force MX resolution to return a fixed host so tests don't hit DNS."""
    from app.pilot_send import sender

    monkeypatch.setattr(
        sender, "_resolve_mx",
        lambda domain, *, timeout: [f"mx.{domain}"],
    )


class _FakeTransport:
    """Stand-in for ``smtplib.SMTP``. Configurable per-call behaviour."""

    def __init__(self, *, behaviour: str = "ok", code: int = 250):
        self._behaviour = behaviour
        self._code = code
        self.sendmail_calls: list[tuple] = []
        self.quit_called = False

    def sendmail(self, from_addr, to_addrs, msg):
        self.sendmail_calls.append((from_addr, to_addrs, msg))
        if self._behaviour == "ok":
            return {}
        if self._behaviour == "refused_dict":
            recipient = to_addrs if isinstance(to_addrs, str) else to_addrs[0]
            return {recipient: (self._code, b"refused")}
        if self._behaviour == "raise_recipients_refused":
            recipient = to_addrs if isinstance(to_addrs, str) else to_addrs[0]
            raise smtplib.SMTPRecipientsRefused(
                {recipient: (self._code, b"550 No such user")},
            )
        if self._behaviour == "raise_smtp_error":
            raise smtplib.SMTPException("connection lost")
        raise RuntimeError(f"unhandled behaviour {self._behaviour}")

    def quit(self):
        self.quit_called = True
        return (221, b"bye")


def _factory(transport: _FakeTransport):
    def make(*, host, port, timeout):
        return transport
    return make


class TestSendOne:
    def _send(self, transport: _FakeTransport):
        sender = SMTPSender(
            smtp_factory=_factory(transport),
            sleep_fn=lambda _s: None,
            clock_fn=lambda: 0.0,
            per_recipient_delay_seconds=0.0,
        )
        return sender.send_one(
            recipient="alice@example.com",
            verp_token="tok-1",
            return_path="bounce+tok-1@bounces.acme.com",
            sender_name="Acme",
            sender_address="sender@acme.com",
            subject="Hello",
            body_text="Hi there",
        )

    def test_happy_path_returns_sent(self):
        transport = _FakeTransport(behaviour="ok")
        outcome = self._send(transport)
        assert outcome.sent is True
        assert outcome.smtp_response_code == 250
        assert outcome.message_id is not None
        assert transport.quit_called is True

    def test_refused_dict_records_code_and_message(self):
        transport = _FakeTransport(behaviour="refused_dict", code=550)
        outcome = self._send(transport)
        assert outcome.sent is False
        assert outcome.smtp_response_code == 550
        assert outcome.error == "rcpt_refused"

    def test_recipients_refused_exception_captured(self):
        transport = _FakeTransport(
            behaviour="raise_recipients_refused", code=550,
        )
        outcome = self._send(transport)
        assert outcome.sent is False
        assert outcome.smtp_response_code == 550
        assert outcome.error == "rcpt_refused"

    def test_smtp_exception_captured(self):
        transport = _FakeTransport(behaviour="raise_smtp_error")
        outcome = self._send(transport)
        assert outcome.sent is False
        assert outcome.error.startswith("smtp_error:") or outcome.error == "all_mx_failed"

    def test_invalid_recipient(self):
        sender = SMTPSender(
            smtp_factory=_factory(_FakeTransport()),
            sleep_fn=lambda _s: None,
            clock_fn=lambda: 0.0,
            per_recipient_delay_seconds=0.0,
        )
        outcome = sender.send_one(
            recipient="",
            verp_token="t",
            return_path="bounce+t@bounces.acme.com",
            sender_name="Acme",
            sender_address="sender@acme.com",
            subject="x",
            body_text="y",
        )
        assert outcome.sent is False


class TestConnectFailureFallthrough:
    def test_connect_refused_recorded(self):
        def boom(*, host, port, timeout):
            raise ConnectionRefusedError("no port 25 here")

        sender = SMTPSender(
            smtp_factory=boom,
            sleep_fn=lambda _s: None,
            clock_fn=lambda: 0.0,
            per_recipient_delay_seconds=0.0,
        )
        outcome = sender.send_one(
            recipient="alice@example.com",
            verp_token="tok-1",
            return_path="bounce+tok-1@bounces.acme.com",
            sender_name="Acme",
            sender_address="sender@acme.com",
            subject="Hello",
            body_text="Hi",
        )
        assert outcome.sent is False
        assert "connect_failed" in (outcome.error or "")


class TestSendBatch:
    def test_batch_iterates_recipients(self):
        transport = _FakeTransport(behaviour="ok")
        sender = SMTPSender(
            smtp_factory=_factory(transport),
            sleep_fn=lambda _s: None,
            clock_fn=lambda: 0.0,
            per_recipient_delay_seconds=0.0,
        )
        outcomes = sender.send_batch(
            recipients=[
                ("a@x.com", "t1"),
                ("b@x.com", "t2"),
            ],
            return_path_domain="bounces.acme.com",
            verp_local_part="bounce",
            sender_name="Acme",
            sender_address="sender@acme.com",
            subject="x",
            body_text="y",
        )
        assert len(outcomes) == 2
        assert all(o.sent for o in outcomes)
        assert len(transport.sendmail_calls) == 2
