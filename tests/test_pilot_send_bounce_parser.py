"""V2.10.12 — DSN parser tests."""

from __future__ import annotations

import textwrap

from app.pilot_send.bounce_parser import parse_dsn_message


def _multipart_dsn(
    *,
    action: str = "failed",
    status: str = "5.1.1",
    diagnostic: str = "smtp; 550 5.1.1 No such user",
    final_recipient: str = "rfc822; recipient@example.com",
    original_recipient: str | None = None,
    feedback_type: str | None = None,
    body_text: str = "Bounced.",
    to_header: str = "postmaster@bounces.acme.com",
) -> str:
    """Build a synthetic multipart/report DSN."""
    headers = (
        "From: postmaster@destination.example.com\r\n"
        f"To: {to_header}\r\n"
        "Subject: Mail delivery failed\r\n"
        "MIME-Version: 1.0\r\n"
    )
    if feedback_type:
        headers += f"Feedback-Type: {feedback_type}\r\n"
    headers += (
        "Content-Type: multipart/report; report-type=delivery-status; "
        'boundary="bdy"\r\n'
        "\r\n"
    )

    text_part = (
        "--bdy\r\n"
        "Content-Type: text/plain\r\n\r\n"
        f"{body_text}\r\n"
    )

    status_lines = []
    if action:
        status_lines.append(f"Action: {action}")
    if status:
        status_lines.append(f"Status: {status}")
    if diagnostic:
        status_lines.append(f"Diagnostic-Code: {diagnostic}")
    if final_recipient:
        status_lines.append(f"Final-Recipient: {final_recipient}")
    if original_recipient:
        status_lines.append(f"Original-Recipient: {original_recipient}")
    status_blob = "\r\n".join(status_lines) + "\r\n"

    status_part = (
        "--bdy\r\n"
        "Content-Type: message/delivery-status\r\n\r\n"
        + status_blob +
        "\r\n"
    )

    closing = "--bdy--\r\n"
    return headers + text_part + status_part + closing


class TestStandardBounces:
    def test_5xx_is_hard_bounce(self):
        raw = _multipart_dsn(
            action="failed",
            status="5.1.1",
            diagnostic="smtp; 550 5.1.1 No such user",
            original_recipient="rfc822; bounce+abc123@bounces.acme.com",
        )
        result = parse_dsn_message(raw)
        assert result.status == "hard_bounce"
        assert result.verp_token == "abc123"
        assert result.smtp_code is not None

    def test_4xx_is_soft_bounce(self):
        raw = _multipart_dsn(
            action="failed",
            status="4.7.0",
            diagnostic="smtp; 421 4.7.0 Try again later",
            original_recipient="rfc822; bounce+softtok@bounces.acme.com",
        )
        result = parse_dsn_message(raw)
        assert result.status == "soft_bounce"
        assert result.verp_token == "softtok"

    def test_5xx_with_blocked_keyword_is_blocked(self):
        raw = _multipart_dsn(
            action="failed",
            status="5.7.1",
            diagnostic="smtp; 550 Message blocked by Spamhaus policy",
            original_recipient="rfc822; bounce+blktok@bounces.acme.com",
        )
        result = parse_dsn_message(raw)
        assert result.status == "blocked"

    def test_delayed_action_is_deferred(self):
        raw = _multipart_dsn(
            action="delayed",
            status="4.4.7",
            diagnostic="smtp; greylisted",
            original_recipient="rfc822; bounce+deftok@bounces.acme.com",
        )
        result = parse_dsn_message(raw)
        assert result.status == "deferred"

    def test_delivered_action_is_delivered(self):
        raw = _multipart_dsn(
            action="delivered",
            status="2.0.0",
            diagnostic="smtp; 250 OK",
            original_recipient="rfc822; bounce+oktok@bounces.acme.com",
        )
        result = parse_dsn_message(raw)
        assert result.status == "delivered"


class TestComplaints:
    def test_arf_abuse_is_complaint(self):
        raw = _multipart_dsn(
            action="failed",
            feedback_type="abuse",
            original_recipient="rfc822; bounce+abusetok@bounces.acme.com",
        )
        result = parse_dsn_message(raw)
        assert result.status == "complaint"


class TestTokenExtraction:
    def test_token_from_original_recipient(self):
        raw = _multipart_dsn(
            original_recipient="rfc822; bounce+origtok@bounces.acme.com",
        )
        assert parse_dsn_message(raw).verp_token == "origtok"

    def test_token_from_final_recipient_when_original_missing(self):
        raw = _multipart_dsn(
            final_recipient="rfc822; bounce+finaltok@bounces.acme.com",
            original_recipient=None,
        )
        assert parse_dsn_message(raw).verp_token == "finaltok"

    def test_token_from_diagnostic_body(self):
        raw = _multipart_dsn(
            diagnostic=(
                "smtp; 550 No mailbox for "
                "bounce+diagtok@bounces.acme.com"
            ),
            final_recipient="rfc822; recipient@example.com",
            original_recipient=None,
        )
        assert parse_dsn_message(raw).verp_token == "diagtok"

    def test_no_token_returns_none(self):
        raw = _multipart_dsn(
            diagnostic="smtp; 550 No such user here",
            final_recipient="rfc822; recipient@example.com",
            original_recipient=None,
        )
        assert parse_dsn_message(raw).verp_token is None


class TestMalformedInputs:
    def test_empty_input_is_unknown(self):
        result = parse_dsn_message(b"")
        assert result.status == "unknown"
        assert result.verp_token is None

    def test_garbage_bytes_does_not_raise(self):
        # Any random bytes — the parser must coerce gracefully.
        result = parse_dsn_message(b"\x00\x01\x02 not an email")
        assert result.status in {"unknown", "hard_bounce", "soft_bounce"}

    def test_plain_text_inline_bounce(self):
        """Older MTAs put the bounce inline as text/plain instead of
        multipart/report. Parser still extracts the verdict from the
        body keywords."""
        raw = (
            "From: postmaster@example.com\r\n"
            "To: bounce+inlinetok@bounces.acme.com\r\n"
            "Subject: Undelivered Mail\r\n"
            "Content-Type: text/plain\r\n\r\n"
            "550 No such user here. The original recipient was "
            "bounce+inlinetok@bounces.acme.com.\r\n"
        )
        result = parse_dsn_message(raw)
        # Inline bounces don't carry Action: but we still extract the
        # token and a verdict from the body.
        assert result.verp_token == "inlinetok"
        # 5xx in body without proper status header is best-effort →
        # likely 'hard_bounce' or 'unknown'; both are acceptable.
        assert result.status in {"hard_bounce", "unknown"}


class TestBytesAndStringInputs:
    def test_bytes_input(self):
        raw = _multipart_dsn(
            original_recipient="rfc822; bounce+bytestok@bounces.acme.com",
        ).encode("utf-8")
        assert parse_dsn_message(raw).verp_token == "bytestok"

    def test_string_input(self):
        raw = _multipart_dsn(
            original_recipient="rfc822; bounce+strtok@bounces.acme.com",
        )
        assert parse_dsn_message(raw).verp_token == "strtok"
