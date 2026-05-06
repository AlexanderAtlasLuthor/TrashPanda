"""V2.10.12 — Direct-to-MX SMTP sender for the pilot batch.

Operator chose ``smtplib direct-to-MX`` (no relay) per the V2.10.12
plan, so this module composes a full MIME message and speaks SMTP
directly to the destination MX. The transport is injectable
(``SMTPTransport`` protocol) so tests run without a real socket.

Caveats the operator owns
-------------------------

* Port 25 outbound must be open on the host running TrashPanda.
  RackNerd and most cloud IPs block this — the operator either
  uses a non-blocking provider or a self-hosted physical/dedicated
  IP.
* DKIM / SPF / DMARC must be set up for ``sender_address``'s
  domain. Without them, Yahoo / Gmail / Outlook reject silently
  (``5xx``) and TrashPanda will record the row as ``hard_bounce``.
* The ``return_path_domain`` MUST receive bounces — usually a
  catch-all subaddress on the operator's mailbox or a dedicated
  bounce domain that delivers to the IMAP mailbox the poller reads.

What this module does NOT do
----------------------------

* DKIM signing — handled by the operator's DNS / MTA.
* IP warmup — the operator schedules pilot batches over time.
* Per-domain rate limiting — basic per-call delay only; the
  operator should batch-size for real-world MX manners.
* Encrypted body / attachments — the pilot is plain MIME.
"""

from __future__ import annotations

import logging
import smtplib
import socket
import ssl
import time
import uuid
from dataclasses import dataclass
from email.message import EmailMessage
from email.utils import formatdate, make_msgid
from typing import Callable, Iterable, Protocol


_LOGGER = logging.getLogger(__name__)


# Default per-recipient pause in seconds — protects the destination
# MX from being flooded by 100 RCPTs in one go. Operators tune via
# config.
DEFAULT_PER_RECIPIENT_DELAY_SECONDS: float = 1.0
DEFAULT_SMTP_TIMEOUT_SECONDS: float = 30.0
DEFAULT_SMTP_PORT: int = 25


@dataclass(frozen=True, slots=True)
class PilotSendOutcome:
    """One row's outcome from the sender. Mirrors what the tracker
    records on ``mark_sent`` plus enough error context for the
    operator UI to render a useful message on failure."""

    email: str
    verp_token: str
    sent: bool
    message_id: str | None
    smtp_response_code: int | None
    smtp_response_message: str | None
    error: str | None


class SMTPTransport(Protocol):
    """Abstract transport. Production uses ``smtplib.SMTP``."""

    def sendmail(
        self,
        from_addr: str,
        to_addrs: list[str] | str,
        msg: bytes,
    ) -> dict: ...

    def quit(self) -> tuple[int, bytes]: ...


# --------------------------------------------------------------------------- #
# MX resolution
# --------------------------------------------------------------------------- #


def _resolve_mx(domain: str, *, timeout: float) -> list[str]:
    """Return MX hosts ordered by priority. Falls back to ``[domain]``
    when DNS has no MX (some domains use the A-record fallback per
    RFC 5321 §5.1)."""
    try:
        import dns.resolver

        resolver = dns.resolver.Resolver()
        resolver.lifetime = timeout
        resolver.timeout = timeout
        answers = resolver.resolve(domain, "MX")
        records = sorted(
            ((int(r.preference), str(r.exchange).rstrip(".")) for r in answers),
            key=lambda tup: tup[0],
        )
        return [host for _, host in records]
    except Exception as exc:  # pragma: no cover - defensive
        _LOGGER.debug("MX lookup failed for %s: %s", domain, exc)
        return [domain]


# --------------------------------------------------------------------------- #
# Message composition
# --------------------------------------------------------------------------- #


def _build_message(
    *,
    recipient: str,
    sender_name: str,
    sender_address: str,
    return_path: str,
    subject: str,
    body_text: str,
    body_html: str,
    reply_to: str | None,
) -> tuple[EmailMessage, str]:
    """Return ``(message, message_id)``.

    The message includes:

    * ``From:`` — sender_name and sender_address.
    * ``Sender:`` — same address (helps with DKIM alignment).
    * ``Return-Path:`` — the per-recipient VERP token. Most MTAs
      strip and re-add this header; we set it for explicitness.
    * ``Reply-To:`` — operator-supplied (e.g. operator@operator.com).
    * ``Auto-Submitted: auto-generated`` — flags this as automated;
      stops vacation-responders from looping.
    * ``Precedence: bulk`` — the same hint, RFC-style.
    * ``Message-ID:`` — generated locally so we can match on it
      when DSN preserves it.
    """
    msg = EmailMessage()
    msg["From"] = (
        f"{sender_name} <{sender_address}>" if sender_name else sender_address
    )
    msg["Sender"] = sender_address
    msg["Return-Path"] = return_path
    if reply_to:
        msg["Reply-To"] = reply_to
    msg["To"] = recipient
    msg["Subject"] = subject
    msg["Date"] = formatdate(localtime=False)
    msg["Auto-Submitted"] = "auto-generated"
    msg["Precedence"] = "bulk"
    message_id = make_msgid(domain=sender_address.rsplit("@", 1)[-1])
    msg["Message-ID"] = message_id
    msg.set_content(body_text or "")
    if body_html:
        msg.add_alternative(body_html, subtype="html")
    return msg, message_id


# --------------------------------------------------------------------------- #
# Sender
# --------------------------------------------------------------------------- #


class SMTPSender:
    """Send pilot messages directly to recipient MX hosts.

    The transport factory is injectable so tests pass a fake
    ``smtplib.SMTP``. Each call to :meth:`send_one` opens a fresh
    connection — pilot batches are small (≤100) and per-domain
    pooling adds complexity without real benefit at this scale.
    """

    def __init__(
        self,
        *,
        smtp_factory: Callable[..., SMTPTransport] | None = None,
        sleep_fn: Callable[[float], None] = time.sleep,
        clock_fn: Callable[[], float] = time.perf_counter,
        timeout_seconds: float = DEFAULT_SMTP_TIMEOUT_SECONDS,
        per_recipient_delay_seconds: float = DEFAULT_PER_RECIPIENT_DELAY_SECONDS,
        ehlo_hostname: str = "trashpanda.local",
    ) -> None:
        self._factory = smtp_factory or self._default_factory
        self._sleep = sleep_fn
        self._clock = clock_fn
        self._timeout = timeout_seconds
        self._delay = per_recipient_delay_seconds
        self._ehlo = ehlo_hostname

    @staticmethod
    def _default_factory(host: str, port: int = DEFAULT_SMTP_PORT,
                         timeout: float = DEFAULT_SMTP_TIMEOUT_SECONDS):
        # Real production transport. Tests use the injectable
        # ``smtp_factory`` to bypass this without monkeypatching
        # smtplib globally.
        smtp = smtplib.SMTP(host, port=port, timeout=timeout)
        smtp.ehlo()
        # Try STARTTLS — most modern MX servers expect it. Failure
        # is non-fatal because some legacy MX still accept plaintext.
        try:
            smtp.starttls(context=ssl.create_default_context())
            smtp.ehlo()
        except smtplib.SMTPException:
            pass
        return smtp

    # --- Public API --------------------------------------------------- #

    def send_one(
        self,
        *,
        recipient: str,
        verp_token: str,
        return_path: str,
        sender_name: str,
        sender_address: str,
        subject: str,
        body_text: str,
        body_html: str = "",
        reply_to: str | None = None,
    ) -> PilotSendOutcome:
        """Send a single pilot message. Never raises."""
        # Guard malformed recipient before composing — saves a probe
        # and avoids passing empty addresses to the SMTP layer.
        if not recipient or "@" not in recipient:
            return PilotSendOutcome(
                email=recipient,
                verp_token=verp_token,
                sent=False,
                message_id=None,
                smtp_response_code=None,
                smtp_response_message=None,
                error="invalid_recipient",
            )
        msg, message_id = _build_message(
            recipient=recipient,
            sender_name=sender_name,
            sender_address=sender_address,
            return_path=return_path,
            subject=subject,
            body_text=body_text,
            body_html=body_html,
            reply_to=reply_to,
        )
        try:
            domain = recipient.rsplit("@", 1)[-1].strip().lower()
        except Exception:
            return PilotSendOutcome(
                email=recipient,
                verp_token=verp_token,
                sent=False,
                message_id=None,
                smtp_response_code=None,
                smtp_response_message=None,
                error="invalid_recipient",
            )

        mx_hosts = _resolve_mx(domain, timeout=self._timeout)
        last_code: int | None = None
        last_msg: str | None = None
        last_error: str | None = None

        for mx_host in mx_hosts:
            try:
                transport = self._factory(
                    host=mx_host,
                    port=DEFAULT_SMTP_PORT,
                    timeout=self._timeout,
                )
            except (
                socket.timeout,
                ConnectionRefusedError,
                OSError,
                smtplib.SMTPException,
            ) as exc:
                last_error = f"connect_failed:{type(exc).__name__}:{exc}"[:200]
                continue
            try:
                # ``sendmail`` returns a dict of refused recipients —
                # empty means success.
                refused = transport.sendmail(
                    return_path,
                    [recipient],
                    msg.as_bytes(),
                )
                if refused:
                    code, message = next(iter(refused.values()))
                    last_code = int(code) if isinstance(code, int) else None
                    last_msg = (
                        message.decode("utf-8", errors="replace")
                        if isinstance(message, (bytes, bytearray))
                        else str(message)
                    )
                    last_error = "rcpt_refused"
                    try:
                        transport.quit()
                    except Exception:
                        pass
                    continue
            except smtplib.SMTPRecipientsRefused as exc:
                first = next(iter(exc.recipients.values()), (None, b""))
                last_code = first[0]
                last_msg = (
                    first[1].decode("utf-8", errors="replace")
                    if isinstance(first[1], (bytes, bytearray))
                    else str(first[1])
                )
                last_error = "rcpt_refused"
                try:
                    transport.quit()
                except Exception:
                    pass
                continue
            except smtplib.SMTPException as exc:
                last_error = f"smtp_error:{type(exc).__name__}:{exc}"[:200]
                last_code = getattr(exc, "smtp_code", None)
                last_msg = getattr(exc, "smtp_error", None)
                if isinstance(last_msg, (bytes, bytearray)):
                    last_msg = last_msg.decode("utf-8", errors="replace")
                try:
                    transport.quit()
                except Exception:
                    pass
                continue
            else:
                try:
                    transport.quit()
                except Exception:
                    pass
                return PilotSendOutcome(
                    email=recipient,
                    verp_token=verp_token,
                    sent=True,
                    message_id=message_id,
                    smtp_response_code=250,
                    smtp_response_message="accepted",
                    error=None,
                )

        return PilotSendOutcome(
            email=recipient,
            verp_token=verp_token,
            sent=False,
            message_id=None,
            smtp_response_code=last_code,
            smtp_response_message=last_msg,
            error=last_error or "all_mx_failed",
        )

    def send_batch(
        self,
        *,
        recipients: Iterable[tuple[str, str]],  # (email, verp_token)
        return_path_domain: str,
        verp_local_part: str,
        sender_name: str,
        sender_address: str,
        subject: str,
        body_text: str,
        body_html: str = "",
        reply_to: str | None = None,
    ) -> list[PilotSendOutcome]:
        """Send each recipient sequentially. Per-recipient pause
        between sends; one MX failure does not block the rest of
        the batch."""
        from .verp import encode_verp_token

        out: list[PilotSendOutcome] = []
        for index, (email, token) in enumerate(recipients):
            return_path = encode_verp_token(
                token,
                return_path_domain=return_path_domain,
                local_part=verp_local_part,
            )
            outcome = self.send_one(
                recipient=email,
                verp_token=token,
                return_path=return_path,
                sender_name=sender_name,
                sender_address=sender_address,
                subject=subject,
                body_text=body_text,
                body_html=body_html,
                reply_to=reply_to,
            )
            out.append(outcome)
            if self._delay > 0 and index < len(out) - 1:
                self._sleep(self._delay)
        return out


__all__ = [
    "DEFAULT_PER_RECIPIENT_DELAY_SECONDS",
    "DEFAULT_SMTP_PORT",
    "DEFAULT_SMTP_TIMEOUT_SECONDS",
    "PilotSendOutcome",
    "SMTPSender",
    "SMTPTransport",
]
