"""Pin the ``_send_failure_verdict`` belt-and-suspenders fallback.

The function reads the SMTP rejection from BOTH
``outcome.smtp_response_message`` AND ``outcome.error`` so the
classifier survives any upstream format change in
``app/pilot_send/sender.py`` that puts the body text in only one of
those fields.

These tests pin both paths with the verbatim Microsoft S3150 string
seen in production and the verbatim Yahoo TSS04 string.
"""

from __future__ import annotations

from app.db.pilot_send_tracker import (
    VERDICT_HARD_BOUNCE,
    VERDICT_INFRA_BLOCKED,
    VERDICT_PROVIDER_DEFERRED,
    VERDICT_UNKNOWN,
)
from app.pilot_send.launch import _send_failure_verdict
from app.pilot_send.sender import PilotSendOutcome


_MSFT_BODY = (
    "5.7.1 Unfortunately, messages from [192.3.105.145] weren't sent. "
    "Please contact your Internet service provider since part of "
    "their network is on our block list (S3150)"
)

_YAHOO_BODY = (
    "[TSS04] Messages from 192.3.105.145 temporarily deferred due to "
    "unexpected volume or user complaints"
)


def _outcome(
    *,
    code: int | None,
    message: str | None,
    error: str | None,
) -> PilotSendOutcome:
    return PilotSendOutcome(
        email="x@example.com",
        verp_token="tok",
        sent=False,
        message_id=None,
        smtp_response_code=code,
        smtp_response_message=message,
        error=error,
    )


class TestSmtpResponseMessagePath:
    """Existing happy path — message in smtp_response_message field."""

    def test_microsoft_s3150_in_message_is_infra_blocked(self):
        out = _outcome(code=550, message=_MSFT_BODY, error=None)
        assert _send_failure_verdict(out) == VERDICT_INFRA_BLOCKED

    def test_yahoo_tss04_in_message_is_provider_deferred(self):
        out = _outcome(code=421, message=_YAHOO_BODY, error=None)
        assert _send_failure_verdict(out) == VERDICT_PROVIDER_DEFERRED


class TestErrorFieldFallback:
    """New defense — when smtp_response_message is empty/None and the
    body lives only in ``outcome.error``, the classifier still fires."""

    def test_microsoft_s3150_in_error_only_is_infra_blocked(self):
        # Mimics how sender.py formats SMTPSenderRefused into error:
        # ``smtp_error:SMTPSenderRefused:(550, b"5.7.1 ...")``
        error_str = f'smtp_error:SMTPSenderRefused:(550, b"{_MSFT_BODY}", "sender@bounces.acme.com")'
        out = _outcome(code=550, message="", error=error_str)
        assert _send_failure_verdict(out) == VERDICT_INFRA_BLOCKED

    def test_microsoft_s3150_in_error_with_none_message(self):
        error_str = f'smtp_error:SMTPSenderRefused:(550, b"{_MSFT_BODY}")'
        out = _outcome(code=550, message=None, error=error_str)
        assert _send_failure_verdict(out) == VERDICT_INFRA_BLOCKED

    def test_yahoo_tss04_in_error_only_is_provider_deferred(self):
        error_str = f'smtp_error:SMTPDataError:(421, b"{_YAHOO_BODY}")'
        out = _outcome(code=421, message="", error=error_str)
        assert _send_failure_verdict(out) == VERDICT_PROVIDER_DEFERRED


class TestNoFalsePositives:
    """The fallback must NOT mis-classify recipient-level rejections
    just because they share a 5xx code."""

    def test_genuine_user_unknown_stays_hard_bounce(self):
        out = _outcome(
            code=550,
            message="5.1.1 user unknown",
            error="rcpt_refused",
        )
        assert _send_failure_verdict(out) == VERDICT_HARD_BOUNCE

    def test_no_message_no_error_no_code_is_unknown(self):
        out = _outcome(code=None, message=None, error=None)
        assert _send_failure_verdict(out) == VERDICT_UNKNOWN
