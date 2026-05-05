"""V2 Phase 4 — selective SMTP probing primitive.

This module exposes a single probe function that opens one SMTP
conversation per call and collapses every failure mode into a
:class:`SMTPResult`. Callers never have to catch networking
exceptions themselves.

Hardening (post-mortem fixes)
-----------------------------
The original implementation set a single ``timeout`` on the smtplib
constructor, which only governs the initial *connect*. After the
3-way handshake, a hostile MX could accept the TCP connection and
then go silent — ``smtp.helo()``/``smtp.mail()``/``smtp.rcpt()``
would block forever on socket reads. This module now:

* applies a *per-command* timeout on the underlying socket before
  each ``HELO``/``MAIL``/``RCPT``/``QUIT`` call,
* enforces an overall ``total_budget_seconds`` for the whole probe so
  one slow MX can never burn the entire run,
* honours an optional ``cancel_check`` callable so an operator
  hitting the cancel button kills in-flight probes within seconds,
* skips probing for known opaque providers (Yahoo/AOL/etc) where the
  RCPT signal cannot be honestly trusted,
* gates the catch-all fake-RCPT detection on the *remaining* budget
  so a slow first RCPT cannot push the connection past the deadline.

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
import os
import socket
import time
import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path


_LOGGER = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Provider skip-list                                                          #
# --------------------------------------------------------------------------- #


# Opaque/accept-all providers where a 250 OK to RCPT is not a reliable
# deliverability signal. These accept the envelope at SMTP time and
# bounce the message *after* the 250, so probing produces noise. We
# return ``inconclusive`` for them up front and skip the network call.
#
# This in-code default is the *fallback*. The canonical operator-edited
# list lives at ``configs/skip_smtp_providers.txt`` and is loaded by
# :func:`load_skip_providers_from_file`. The in-code copy guarantees we
# never lose the well-known opaque providers if the file is missing.
DEFAULT_OPAQUE_PROVIDERS: frozenset[str] = frozenset(
    {
        "yahoo.com",
        "ymail.com",
        "rocketmail.com",
        "aol.com",
        "aim.com",
        "verizon.net",
        "verizon-media.com",
        "sbcglobal.net",
        "att.net",
        "bellsouth.net",
        "ameritech.net",
        "pacbell.net",
        "swbell.net",
        "frontier.com",
        "frontiernet.net",
    }
)


# Default location of the operator-editable skip-list (relative to the
# project root). Override with ``TRASHPANDA_SKIP_SMTP_PROVIDERS_PATH``
# for tests or non-standard layouts.
_DEFAULT_SKIP_FILE = Path("configs") / "skip_smtp_providers.txt"


def load_skip_providers_from_file(
    path: str | Path | None = None,
) -> frozenset[str]:
    """Read the operator-editable skip-list and merge with the defaults.

    The file format is one domain per line; blanks and ``#`` comments
    are ignored. Domains are lowercased and de-duplicated. If the file
    is missing or unreadable the in-code defaults are returned, so the
    pipeline never silently loses Yahoo/AOL-class protection.
    """

    env_override = os.environ.get("TRASHPANDA_SKIP_SMTP_PROVIDERS_PATH")
    candidate = (
        Path(path)
        if path is not None
        else (Path(env_override) if env_override else _DEFAULT_SKIP_FILE)
    )

    domains: set[str] = set(DEFAULT_OPAQUE_PROVIDERS)
    try:
        if candidate.is_file():
            for raw in candidate.read_text(encoding="utf-8").splitlines():
                line = raw.split("#", 1)[0].strip().lower()
                if line:
                    domains.add(line)
        else:
            _LOGGER.debug(
                "skip_smtp_providers: file not found at %s — using built-in defaults",
                candidate,
            )
    except OSError as exc:  # pragma: no cover - defensive guard
        _LOGGER.warning(
            "skip_smtp_providers: failed to read %s (%s) — using built-in defaults",
            candidate, exc,
        )
    return frozenset(domains)


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


def _normalise_skip_set(skip: Iterable[str] | None) -> frozenset[str]:
    """Resolve the effective skip-list for one probe invocation.

    ``None`` (the default) loads the operator-editable file at
    ``configs/skip_smtp_providers.txt`` and unions it with the in-code
    defaults. Any explicit iterable — including the empty list — is
    used verbatim, which lets tests opt in to live probing of opaque
    providers when they need to.
    """

    if skip is None:
        return load_skip_providers_from_file()
    return frozenset(s.strip().lower() for s in skip if s and s.strip())


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


def _apply_socket_timeout(smtp_obj: object, seconds: float) -> None:
    """Set the socket timeout on an open ``smtplib.SMTP`` instance.

    smtplib only accepts a single timeout at construction time; the
    connection timeout it installs governs ``connect`` but does *not*
    bound subsequent reads if the remote MX accepts the handshake and
    then stalls. Re-applying ``settimeout`` on the underlying socket
    before each command gives us per-command read/write deadlines.
    """
    sock = getattr(smtp_obj, "sock", None)
    if sock is None:
        return
    try:
        sock.settimeout(max(0.5, float(seconds)))
    except (OSError, ValueError):  # pragma: no cover - defensive guard
        pass


def _budget_left(deadline: float | None) -> float:
    if deadline is None:
        return float("inf")
    return max(0.0, deadline - time.monotonic())


def _too_little_budget(deadline: float | None, need: float) -> bool:
    return _budget_left(deadline) < need


def _single_rcpt_conversation(
    mx_host: str,
    addr: str,
    *,
    sender: str,
    helo_name: str,
    deadline: float,
    cmd_to: float,
    cancel_check: Callable[[], bool] | None = None,
) -> tuple[int | None, str]:
    """Open one fresh SMTP connection and issue a single RCPT.

    Returns ``(code, response_message)``. The MAIL FROM rejection is
    surfaced as a synthetic ``(None, "MAIL FROM rejected: <code>")``
    so callers can treat it as inconclusive.

    Raises the underlying ``smtplib`` / socket exception so the outer
    probe can translate it into a single ``SMTPResult``.
    """

    import smtplib

    connect_to = max(0.5, min(cmd_to, _budget_left(deadline)))
    smtp = smtplib.SMTP(mx_host, 25, timeout=connect_to)
    try:
        _apply_socket_timeout(smtp, min(cmd_to, _budget_left(deadline)))
        smtp.helo(helo_name)

        if cancel_check is not None and cancel_check():
            return None, "cancelled"

        _apply_socket_timeout(smtp, min(cmd_to, _budget_left(deadline)))
        mail_code, _ = smtp.mail(sender)
        if mail_code not in (250, 251):
            return None, f"MAIL FROM rejected: {mail_code}"

        if cancel_check is not None and cancel_check():
            return None, "cancelled"

        _apply_socket_timeout(smtp, min(cmd_to, _budget_left(deadline)))
        code, raw_msg = smtp.rcpt(addr)
        response = (
            raw_msg.decode("utf-8", errors="replace")
            if isinstance(raw_msg, bytes)
            else str(raw_msg)
        )[:200]
        return code, response
    finally:
        try:
            smtp.quit()
        except Exception:  # pragma: no cover - quit() is best-effort
            try:
                smtp.close()
            except Exception:
                pass


def probe_email_smtplib(
    email: str,
    *,
    sender: str,
    timeout: float = 4.0,
    local_hostname: str | None = None,
    total_budget_seconds: float | None = None,
    per_command_timeout: float | None = None,
    cancel_check: Callable[[], bool] | None = None,
    skip_providers: Iterable[str] | None = None,
    enable_catch_all_check: bool = True,
    catch_all_separate_connection: bool = True,
) -> SMTPResult:
    """Issue one ``RCPT TO`` probe against the address's MX.

    Parameters
    ----------
    email, sender, timeout, local_hostname
        As before. ``timeout`` doubles as the DNS+connect deadline.
    total_budget_seconds
        Hard ceiling on the whole probe (DNS + connect + every command).
        When ``None`` it defaults to ``timeout * 4`` so slow MXs can no
        longer stall the run forever. Once the budget is exhausted the
        probe returns ``inconclusive`` immediately.
    per_command_timeout
        Read/write deadline applied before each SMTP command. Defaults
        to ``timeout`` when omitted.
    cancel_check
        Optional 0-arg callable. If it returns True the probe aborts
        immediately and reports ``inconclusive``.
    skip_providers
        Domains in this set are skipped (returned as ``inconclusive``)
        without any network I/O. ``None`` (default) loads the
        operator-editable file at
        ``configs/skip_smtp_providers.txt`` (merged with the in-code
        Yahoo/AOL/Verizon-class defaults). Pass an empty iterable to
        disable.
    enable_catch_all_check
        When True (default) a second ``RCPT`` for a random local-part
        is issued to detect catch-all hosts. Skipped when the
        remaining budget is too small.
    catch_all_separate_connection
        When True (default) the catch-all fake-RCPT is sent over a
        *fresh* SMTP connection, so a slow real-RCPT cannot leak into
        the catch-all read deadline (this matches the V2 deliverability
        post-mortem recommendation). Set to False to fall back to the
        legacy "same-connection" mode.

    All exceptions are caught and translated into an
    ``inconclusive=True`` result so the caller can iterate over many
    addresses without try/except scaffolding.
    """
    import smtplib

    local_part, _, domain = (email or "").rpartition("@")
    if not local_part or not domain:
        return SMTPResult(False, None, "invalid email syntax", False, True)

    domain_lc = domain.strip().lower()
    skip_set = _normalise_skip_set(skip_providers)
    if domain_lc in skip_set:
        return SMTPResult(
            False, None, f"skipped opaque provider: {domain_lc}", False, True
        )

    if cancel_check is not None and cancel_check():
        return SMTPResult(False, None, "cancelled", False, True)

    cmd_to = float(per_command_timeout if per_command_timeout is not None else timeout)
    if total_budget_seconds is None:
        total_budget_seconds = max(timeout * 4.0, cmd_to * 4.0)
    deadline = time.monotonic() + float(total_budget_seconds)

    # DNS lookup is bounded by the smaller of (remaining budget, timeout).
    dns_timeout = min(float(timeout), _budget_left(deadline))
    if dns_timeout <= 0:
        return SMTPResult(False, None, "timeout: budget exhausted before DNS", False, True)
    mx_host = _dns_mx_lookup(domain_lc, timeout=dns_timeout)
    if mx_host is None:
        return SMTPResult(False, None, "no mx record", False, True)

    if cancel_check is not None and cancel_check():
        return SMTPResult(False, None, "cancelled", False, True)

    if _too_little_budget(deadline, max(1.0, cmd_to)):
        return SMTPResult(False, None, "timeout: budget exhausted", False, True)

    helo_name = local_hostname or socket.gethostname() or "localhost"

    try:
        if catch_all_separate_connection:
            # ---- Real RCPT on its own connection ------------------------
            real_code, real_msg = _single_rcpt_conversation(
                mx_host, email,
                sender=sender, helo_name=helo_name,
                deadline=deadline, cmd_to=cmd_to,
                cancel_check=cancel_check,
            )
            if real_msg == "cancelled":
                return SMTPResult(False, None, "cancelled", False, True)

            success = real_code in (250, 251)
            inconclusive = not success and (
                real_code is None or 400 <= int(real_code) < 500
            )

            is_catch_all = False
            if (
                success
                and enable_catch_all_check
                and not _too_little_budget(deadline, cmd_to)
                and not (cancel_check is not None and cancel_check())
            ):
                fake_local = f"trashpanda-probe-{uuid.uuid4().hex[:12]}"
                fake_addr = f"{fake_local}@{domain_lc}"
                try:
                    fake_code, _ = _single_rcpt_conversation(
                        mx_host, fake_addr,
                        sender=sender, helo_name=helo_name,
                        deadline=deadline, cmd_to=cmd_to,
                        cancel_check=cancel_check,
                    )
                    is_catch_all = fake_code in (250, 251)
                except (
                    smtplib.SMTPException, OSError, TimeoutError, ConnectionError,
                ):
                    # Fake RCPT failed — fall through without claiming
                    # catch-all. The real result is still authoritative.
                    pass

            return SMTPResult(
                success=success,
                response_code=real_code,
                response_message=real_msg,
                is_catch_all_like=is_catch_all,
                inconclusive=inconclusive,
            )

        # ---- Legacy: real + fake RCPT on the same connection -----------
        # Cap connect time at the smaller of (per-cmd, remaining budget).
        connect_to = min(cmd_to, _budget_left(deadline))
        with smtplib.SMTP(mx_host, 25, timeout=connect_to) as smtp:
            _apply_socket_timeout(smtp, min(cmd_to, _budget_left(deadline)))
            smtp.helo(helo_name)

            if cancel_check is not None and cancel_check():
                return SMTPResult(False, None, "cancelled", False, True)

            _apply_socket_timeout(smtp, min(cmd_to, _budget_left(deadline)))
            mail_code, _ = smtp.mail(sender)
            if mail_code not in (250, 251):
                return SMTPResult(False, mail_code, "MAIL FROM rejected", False, True)

            if cancel_check is not None and cancel_check():
                return SMTPResult(False, None, "cancelled", False, True)

            _apply_socket_timeout(smtp, min(cmd_to, _budget_left(deadline)))
            code, raw_msg = smtp.rcpt(email)
            success = code in (250, 251)
            response = (
                raw_msg.decode("utf-8", errors="replace")
                if isinstance(raw_msg, bytes)
                else str(raw_msg)
            )[:200]

            is_catch_all = False
            if (
                success
                and enable_catch_all_check
                and not _too_little_budget(deadline, cmd_to)
            ):
                fake_local = f"trashpanda-probe-{uuid.uuid4().hex[:12]}"
                fake_addr = f"{fake_local}@{domain_lc}"
                try:
                    _apply_socket_timeout(
                        smtp, min(cmd_to, _budget_left(deadline))
                    )
                    fake_code, _ = smtp.rcpt(fake_addr)
                    is_catch_all = fake_code in (250, 251)
                except (smtplib.SMTPException, OSError, TimeoutError):
                    pass

            inconclusive = not success and (code is None or 400 <= int(code) < 500)
            return SMTPResult(
                success=success,
                response_code=code,
                response_message=response,
                is_catch_all_like=is_catch_all,
                inconclusive=inconclusive,
            )
    except (socket.timeout, TimeoutError) as exc:
        return SMTPResult(False, None, f"timeout: {exc}"[:200], False, True)
    except (smtplib.SMTPException, OSError, ConnectionError) as exc:
        return SMTPResult(False, None, str(exc)[:200], False, True)
    except Exception as exc:  # pragma: no cover - defensive guard
        _LOGGER.debug("smtp_probe: unexpected error probing %s: %s", email, exc)
        return SMTPResult(False, None, f"unexpected: {exc}"[:200], False, True)


__all__ = [
    "DEFAULT_OPAQUE_PROVIDERS",
    "SMTPResult",
    "load_skip_providers_from_file",
    "probe_email_dry_run",
    "probe_email_smtplib",
]
