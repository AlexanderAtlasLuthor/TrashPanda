"""V2.10.12 — DSN multipart/report parser.

Takes a raw RFC822 message (bytes or string) and extracts the
canonical bounce verdict + diagnostic. The parser handles the
three flavours the IMAP poller will encounter:

1. **multipart/report** with ``message/delivery-status`` —
   the standard machine-readable DSN. Carries ``Action:``,
   ``Status:`` (RFC 3463 enhanced status), ``Diagnostic-Code:``,
   and ``Final-Recipient:`` headers.
2. **single-part bounce** — older MTAs return the bounce inline
   in the body. Best-effort scan for keywords + 5xx/4xx codes.
3. **abuse complaint (ARF)** — ``Feedback-Type: abuse`` header
   on a multipart/report. Maps to ``complaint``.

The parser never raises; malformed input → ``status="unknown"``
with the raw subject preserved as the diagnostic.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from email import message_from_bytes, message_from_string, policy


@dataclass(frozen=True, slots=True)
class DSNParseResult:
    """One parsed bounce.

    ``verp_token`` is the token extracted from the original
    envelope-from / Original-Recipient headers. The verdict store
    looks the row up by token, NOT by recipient address (the
    address can be aliased / forwarded en route).
    """

    verp_token: str | None
    status: str  # one of the canonical verdicts
    diagnostic: str | None
    smtp_code: str | None
    final_recipient: str | None
    raw_subject: str | None


# Keywords commonly found in policy / blocked / spam diagnostics. We
# err on the side of "policy" only when the message clearly says so.
_BLOCKED_KEYWORDS: tuple[str, ...] = (
    "blocked",
    "policy",
    "spam",
    "blacklist",
    "spamhaus",
    "barracuda",
    "rejected",
    "dmarc",
    "spf failed",
    "dkim failed",
    "denied",
    "abuse",
)


# Soft-fail heuristics — used when a body has no enhanced status.
_SOFT_KEYWORDS: tuple[str, ...] = (
    "greylist",
    "deferred",
    "try again",
    "temporar",  # matches "temporary" / "temporarily"
    "rate limit",
    "too many",
)


# Infrastructure-level rejections: the recipient provider rejected
# our sender IP / network / route. Says nothing about whether the
# recipient address exists. We surface these separately so they
# don't pollute the customer's do_not_send list when the real cause
# is sender reputation.
_INFRA_BLOCK_PATTERNS: tuple[re.Pattern[str], ...] = (
    # Microsoft Outlook/Hotmail/Live S3150 family.
    re.compile(r"\bS3150\b", re.IGNORECASE),
    # "messages from [1.2.3.4] weren't sent" / "were not sent".
    re.compile(
        r"messages from \[?[\d.]+\]?\s+(?:weren'?t sent|were not sent)",
        re.IGNORECASE,
    ),
    # "part of their network is on our block list".
    re.compile(
        r"part of (?:their|your) network is on (?:our|the) block\s?list",
        re.IGNORECASE,
    ),
    # Public RBL listings (Spamhaus / Barracuda / SORBS / UCEPROTECT).
    re.compile(
        r"\b(?:spamhaus|barracuda|sorbs|uceprotect)\b.*"
        r"\b(?:listed|listing|blocklist|black\s?list)\b",
        re.IGNORECASE | re.DOTALL,
    ),
    # 5.7.1 + explicit sender-IP / blocklist context.
    re.compile(
        r"\b5\.7\.1\b.*(?:block\s?list|sender reputation|your ip)",
        re.IGNORECASE | re.DOTALL,
    ),
)


# Provider-deferred patterns: throttled / volume / reputation
# limits, not a recipient signal. Yahoo's TSS04 is the canonical
# example. Always transient.
_PROVIDER_DEFER_PATTERNS: tuple[re.Pattern[str], ...] = (
    # Yahoo / AOL "TSSnn" reputation throttling tags.
    re.compile(r"\b\[?TSS?\d{2,3}\]?\b", re.IGNORECASE),
    re.compile(
        r"temporarily deferred due to "
        r"(?:unexpected volume|user complaints|reputation)",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b421\b.*(?:throttl|too many connections|rate limit"
        r"|unexpected volume)",
        re.IGNORECASE | re.DOTALL,
    ),
)


def _is_infra_block(diagnostic: str) -> bool:
    if not diagnostic:
        return False
    return any(p.search(diagnostic) for p in _INFRA_BLOCK_PATTERNS)


def _is_provider_deferred(diagnostic: str) -> bool:
    if not diagnostic:
        return False
    return any(p.search(diagnostic) for p in _PROVIDER_DEFER_PATTERNS)


def _from_text(text: str) -> tuple[str | None, str | None]:
    """Extract (smtp_code, normalized_text) from a free-form blob."""
    if not text:
        return None, None
    t = text.strip()
    if not t:
        return None, None
    # First numeric SMTP code on the string.
    match = re.search(r"\b(\d{3})\b", t)
    code = match.group(1) if match else None
    return code, t[:500]


def _verdict_from_status_action(
    status_code: str | None,
    action: str | None,
    diagnostic: str,
) -> str:
    """Apply the canonical mapping. Status code is the primary
    signal; action and diagnostic disambiguate.

    Sender-side rejections (recipient provider rejected our IP /
    network / route) are detected first — they would otherwise be
    mis-classified as ``hard_bounce`` and pollute the customer's
    do_not_send list with addresses that may be perfectly valid.
    """
    diag = diagnostic or ""
    diag_lower = diag.lower()

    # Sender-side first: independent of action / status, because the
    # SMTP code reflects the sender being rejected, not the recipient.
    if _is_infra_block(diag):
        return "infrastructure_blocked"
    if _is_provider_deferred(diag):
        return "provider_deferred"

    # Action: failed → terminal. Use the enhanced status to split
    # hard vs soft.
    if action == "failed":
        if status_code:
            major = status_code.split(".")[0]
            if major == "5":
                if any(k in diag_lower for k in _BLOCKED_KEYWORDS):
                    return "blocked"
                return "hard_bounce"
            if major == "4":
                return "soft_bounce"
        # No status → diagnose from text.
        if any(k in diag_lower for k in _BLOCKED_KEYWORDS):
            return "blocked"
        if any(k in diag_lower for k in _SOFT_KEYWORDS):
            return "soft_bounce"
        return "hard_bounce"

    if action == "delayed":
        return "deferred"

    if action == "delivered":
        return "delivered"

    # Some MTAs omit Action: and only include Diagnostic-Code:.
    if status_code:
        major = status_code.split(".")[0]
        if major == "5":
            if any(k in diag_lower for k in _BLOCKED_KEYWORDS):
                return "blocked"
            return "hard_bounce"
        if major == "4":
            return "soft_bounce"

    if any(k in diag_lower for k in _BLOCKED_KEYWORDS):
        return "blocked"
    if any(k in diag_lower for k in _SOFT_KEYWORDS):
        return "soft_bounce"
    return "unknown"


def _parse_delivery_status(part) -> dict:
    """Return ``{action, status, diagnostic, final_recipient,
    original_recipient}`` from a ``message/delivery-status`` part."""
    out: dict = {}
    payload = part.get_payload()
    if not isinstance(payload, list):
        # Some MTAs put it as plain text instead of multipart.
        text = part.get_payload(decode=True)
        if isinstance(text, (bytes, bytearray)):
            text = text.decode("utf-8", errors="replace")
        if not text:
            return out
        for line in text.splitlines():
            if ":" not in line:
                continue
            key, _, value = line.partition(":")
            key = key.strip().lower()
            value = value.strip()
            if key in {
                "action", "status", "diagnostic-code",
                "final-recipient", "original-recipient",
            }:
                out[key] = value
        return out

    # Standard multipart/delivery-status: each "part" is a per-
    # recipient block with header-style key:value lines. We merge
    # all blocks but the first matching field wins.
    for block in payload:
        for header_name in (
            "Action",
            "Status",
            "Diagnostic-Code",
            "Final-Recipient",
            "Original-Recipient",
        ):
            value = block.get(header_name)
            if value and header_name.lower() not in out:
                out[header_name.lower()] = str(value).strip()
    return out


def parse_dsn_message(raw: bytes | str) -> DSNParseResult:
    """Parse one bounce message into a :class:`DSNParseResult`.

    Never raises. ``raw`` may be ``bytes`` or ``str``; the function
    coerces both. Returns ``status="unknown"`` for messages we
    can't make sense of.
    """
    from .verp import extract_token_from_envelope

    if not raw:
        return DSNParseResult(
            verp_token=None,
            status="unknown",
            diagnostic=None,
            smtp_code=None,
            final_recipient=None,
            raw_subject=None,
        )

    try:
        if isinstance(raw, (bytes, bytearray)):
            msg = message_from_bytes(raw, policy=policy.default)
        else:
            msg = message_from_string(raw, policy=policy.default)
    except Exception:
        return DSNParseResult(
            verp_token=None,
            status="unknown",
            diagnostic=str(raw)[:300] if raw else None,
            smtp_code=None,
            final_recipient=None,
            raw_subject=None,
        )

    raw_subject = str(msg.get("Subject") or "").strip()
    feedback_type = str(msg.get("Feedback-Type") or "").strip().lower()
    is_complaint = feedback_type == "abuse"

    status_dict: dict = {}
    body_text = ""

    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            if ctype == "message/delivery-status":
                status_dict = _parse_delivery_status(part)
            elif ctype == "text/plain" and not body_text:
                payload = part.get_payload(decode=True)
                if isinstance(payload, (bytes, bytearray)):
                    body_text = payload.decode("utf-8", errors="replace")
                else:
                    body_text = str(payload or "")
    else:
        payload = msg.get_payload(decode=True)
        if isinstance(payload, (bytes, bytearray)):
            body_text = payload.decode("utf-8", errors="replace")
        else:
            body_text = str(payload or "")

    action = (status_dict.get("action") or "").lower() or None
    smtp_status = status_dict.get("status") or None
    diagnostic = status_dict.get("diagnostic-code") or ""
    final_recipient = status_dict.get("final-recipient") or None
    original_recipient = status_dict.get("original-recipient")

    if not diagnostic and body_text:
        # Use the raw body as the diagnostic if no Diagnostic-Code:
        # was present.
        diagnostic = body_text

    smtp_code, _ = _from_text(diagnostic)

    if is_complaint:
        verdict = "complaint"
    else:
        verdict = _verdict_from_status_action(
            smtp_status, action, diagnostic,
        )

    # Token extraction order: Original-Recipient → Final-Recipient
    # → diagnostic body → raw subject. The first hit wins.
    token: str | None = None
    for source in (
        original_recipient,
        final_recipient,
        diagnostic,
        body_text,
        raw_subject,
        str(msg.get("To") or ""),
        str(msg.get("X-Failed-Recipients") or ""),
    ):
        token = extract_token_from_envelope(source or "")
        if token:
            break

    return DSNParseResult(
        verp_token=token,
        status=verdict,
        diagnostic=(diagnostic or None) and diagnostic[:500],
        smtp_code=smtp_code or smtp_status,
        final_recipient=final_recipient,
        raw_subject=raw_subject or None,
    )


__all__ = [
    "DSNParseResult",
    "parse_dsn_message",
]
