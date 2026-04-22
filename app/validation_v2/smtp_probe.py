"""V2 Phase 4 — selective SMTP probing primitive.

This module is intentionally minimal: it exposes a single probe
function that opens one SMTP conversation per call and collapses every
failure mode into a :class:`SMTPResult` so callers never have to catch
networking exceptions themselves.

Safety notes
------------
* ``probe_email_smtplib`` is the *only* path that touches the network.
  Callers that want a no-network run use ``probe_email_dry_run``.
* The catch-all heuristic piggybacks on the same open connection —
  no second TCP connect is made.
* We swallow every exception (``SMTPException``, ``OSError``,
  ``ConnectionError``, DNS errors) and translate them into
  ``SMTPResult(success=False, inconclusive=True)``.
* Response messages are truncated to 200 chars to avoid logging noisy
  server banners.
"""

from __future__ import annotations

import logging
import socket
import uuid
from dataclasses import dataclass


_LOGGER = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Result dataclass                                                            #
# --------------------------------------------------------------------------- #


@dataclass(slots=True, frozen=True)
class SMTPResult:
    """Normalised outcome of a single SMTP probe.

    ``verdict`` is a derived short string meant for the ``smtp_result``
    CSV column. It collapses the four booleans into one of four values:
    ``catch_all``, ``inconclusive``, ``deliverable``, ``undeliverable``.
    """

    success: bool
    response_code: int | None
    response_message: str
    is_catch_all_like: bool
    inconclusive: bool

    @property
    def verdict(self) -> str:
        # Order matters — catch-all can only fire when the real address
        # was also accepted, but the catch-all verdict is more useful to
        # surface than plain "deliverable".
        if self.is_catch_all_like:
            return "catch_all"
        if self.inconclusive:
            return "inconclusive"
        if self.success:
            return "deliverable"
        return "undeliverable"


# --------------------------------------------------------------------------- #
# Dry-run probe                                                               #
# --------------------------------------------------------------------------- #


def probe_email_dry_run(email: str, **_kwargs: object) -> SMTPResult:
    """Deterministic probe used when ``dry_run=True``.

    Returns ``inconclusive=True`` so downstream consumers never mistake
    a dry-run result for a real delivery signal. Accepts (and ignores)
    arbitrary kwargs so it can be dropped in as a substitute for
    :func:`probe_email_smtplib`.
    """
    return SMTPResult(
        success=False,
        response_code=None,
        response_message="dry_run",
        is_catch_all_like=False,
        inconclusive=True,
    )


# --------------------------------------------------------------------------- #
# Real probe (smtplib)                                                        #
# --------------------------------------------------------------------------- #


def _dns_mx_lookup(domain: str, *, timeout: float) -> str | None:
    """Return highest-priority MX host for ``domain`` or None on failure."""
    try:
        import dns.resolver

        resolver = dns.resolver.Resolver()
        resolver.lifetime = timeout
        resolver.timeout = timeout
        answers = resolver.resolve(domain, "MX")
        if not answers:
            return None
        best = min(answers, key=lambda r: int(r.preference))
        return str(best.exchange).rstrip(".")
    except Exception:
        return None


def probe_email_smtplib(
    email: str,
    *,
    sender: str,
    timeout: float = 4.0,
    local_hostname: str | None = None,
) -> SMTPResult:
    """Issue one ``RCPT TO`` probe against the address's MX.

    All exceptions are caught and translated into an
    ``inconclusive=True`` result so the caller can iterate over many
    addresses without try/except scaffolding.
    """
    import smtplib

    local_part, _, domain = (email or "").rpartition("@")
    if not local_part or not domain:
        return SMTPResult(False, None, "invalid email syntax", False, True)

    mx_host = _dns_mx_lookup(domain, timeout=timeout)
    if mx_host is None:
        return SMTPResult(False, None, "no mx record", False, True)

    helo_name = local_hostname or socket.gethostname() or "localhost"

    try:
        with smtplib.SMTP(mx_host, 25, timeout=timeout) as smtp:
            smtp.helo(helo_name)
            mail_code, _ = smtp.mail(sender)
            if mail_code not in (250, 251):
                # Likely greylisting or policy rejection on the envelope
                # sender. Can't tell deliverability either way.
                return SMTPResult(False, mail_code, "MAIL FROM rejected", False, True)

            code, raw_msg = smtp.rcpt(email)
            success = code in (250, 251)
            response = (
                raw_msg.decode("utf-8", errors="replace")
                if isinstance(raw_msg, bytes)
                else str(raw_msg)
            )[:200]

            is_catch_all = False
            if success:
                fake_local = f"trashpanda-probe-{uuid.uuid4().hex[:12]}"
                fake_addr = f"{fake_local}@{domain}"
                try:
                    fake_code, _ = smtp.rcpt(fake_addr)
                    is_catch_all = fake_code in (250, 251)
                except smtplib.SMTPException:
                    pass

            inconclusive = not success and (code is None or 400 <= int(code) < 500)
            return SMTPResult(
                success=success,
                response_code=code,
                response_message=response,
                is_catch_all_like=is_catch_all,
                inconclusive=inconclusive,
            )
    except (smtplib.SMTPException, OSError, ConnectionError) as exc:
        return SMTPResult(False, None, str(exc)[:200], False, True)
    except Exception as exc:  # pragma: no cover - defensive guard
        _LOGGER.debug("smtp_probe: unexpected error probing %s: %s", email, exc)
        return SMTPResult(False, None, f"unexpected: {exc}"[:200], False, True)


__all__ = [
    "SMTPResult",
    "probe_email_dry_run",
    "probe_email_smtplib",
]
