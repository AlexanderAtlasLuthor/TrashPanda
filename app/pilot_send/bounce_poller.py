"""V2.10.12 — IMAP bounce poller.

Connects to the operator's bounce mailbox via IMAP, fetches recent
``UNSEEN`` messages, parses each one as a DSN, and applies the
verdict to the pilot tracker via ``record_dsn``.

The IMAP client is injectable so tests run without a real socket.
Production uses ``imaplib.IMAP4_SSL`` (or ``IMAP4`` when
``use_ssl=False``).

Concurrency
-----------

The poller is single-process. Operators that want continuous
polling run it via a systemd timer (similar to the V2.10.11 retry
worker) — `python -m app.pilot_send.bounce_poller --once` is a
clean entry point. The shipped systemd unit lives in
``deploy/trashpanda-pilot-bounce-poller.{service,timer}``.
"""

from __future__ import annotations

import imaplib
import logging
import os
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Protocol

from ..db.pilot_send_tracker import open_for_run, PILOT_TRACKER_FILENAME
from .bounce_parser import parse_dsn_message
from .config import IMAPCredentials, read_pilot_config


_LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class BouncePollResult:
    """Aggregated counts from one poll pass."""

    queue_path: Path
    fetched: int = 0
    parsed: int = 0
    matched: int = 0
    unmatched_tokens: int = 0
    parse_errors: int = 0
    verdict_breakdown: dict[str, int] = field(default_factory=dict)


class IMAPClient(Protocol):
    """Minimal IMAP surface the poller uses. ``imaplib.IMAP4`` and
    ``imaplib.IMAP4_SSL`` both satisfy this."""

    def login(self, user: str, password: str) -> tuple: ...
    def select(self, mailbox: str) -> tuple: ...
    def search(self, charset, *criteria) -> tuple: ...
    def fetch(self, message_set, message_parts) -> tuple: ...
    def store(self, message_set, command, flags) -> tuple: ...
    def logout(self) -> tuple: ...
    def close(self) -> tuple: ...


# --------------------------------------------------------------------------- #
# Default IMAP factory
# --------------------------------------------------------------------------- #


def _default_imap_factory(creds: IMAPCredentials) -> IMAPClient:
    if creds.use_ssl:
        return imaplib.IMAP4_SSL(creds.host, port=creds.port)
    return imaplib.IMAP4(creds.host, port=creds.port)


def _password_from_env(creds: IMAPCredentials) -> str:
    if not creds.password_env:
        return ""
    return os.environ.get(creds.password_env, "")


# --------------------------------------------------------------------------- #
# Poller
# --------------------------------------------------------------------------- #


def poll_bounces(
    run_dir: str | Path,
    *,
    imap_factory: Callable[[IMAPCredentials], IMAPClient] | None = None,
    password_lookup: Callable[[IMAPCredentials], str] | None = None,
    mark_seen: bool = True,
    limit: int = 200,
    now: datetime | None = None,
) -> BouncePollResult:
    """Run one bounce-polling pass.

    Parameters
    ----------
    run_dir:
        Per-run output directory (carries the tracker DB and the
        ``pilot_send_config.json`` with IMAP credentials).
    imap_factory / password_lookup:
        Injectable for tests. Production uses the imaplib defaults
        and reads the password from the env var named in the
        config.
    mark_seen:
        Whether to set the ``\\Seen`` flag on processed messages.
        Tests pass False to keep the IMAP fixture state stable
        across assertions.
    limit:
        Hard cap on the number of messages parsed per pass.

    Never raises on transport errors — returns a
    :class:`BouncePollResult` with zeros.
    """
    run_dir_path = Path(run_dir)
    queue_path = run_dir_path / PILOT_TRACKER_FILENAME
    result = BouncePollResult(queue_path=queue_path)

    if not queue_path.is_file():
        return result

    config = read_pilot_config(run_dir_path)
    creds = config.imap
    if not creds.is_configured():
        return result

    factory = imap_factory or _default_imap_factory
    pw_lookup = password_lookup or _password_from_env
    password = pw_lookup(creds)
    if not password:
        return result

    try:
        client = factory(creds)
    except Exception as exc:
        _LOGGER.warning("IMAP connect failed: %s", exc)
        return result

    try:
        client.login(creds.username, password)
        client.select(creds.folder)
        status, data = client.search(None, "UNSEEN")
        if status != "OK" or not data or not data[0]:
            return result
        ids = data[0].split()[: int(limit)]
        result.fetched = len(ids)
        if not ids:
            return result

        verdict_counts: dict[str, int] = {}
        with closing(open_for_run(run_dir_path)) as tracker:
            for msg_id in ids:
                try:
                    fetch_status, fetch_data = client.fetch(
                        msg_id, "(RFC822)",
                    )
                    if fetch_status != "OK" or not fetch_data:
                        result.parse_errors += 1
                        continue
                    raw = None
                    for entry in fetch_data:
                        if isinstance(entry, tuple) and len(entry) >= 2:
                            raw = entry[1]
                            break
                    if raw is None:
                        result.parse_errors += 1
                        continue
                    parsed = parse_dsn_message(raw)
                    result.parsed += 1
                    if parsed.verp_token is None:
                        result.unmatched_tokens += 1
                    else:
                        applied = tracker.record_dsn(
                            parsed.verp_token,
                            dsn_status=parsed.status,
                            dsn_diagnostic=parsed.diagnostic,
                            dsn_smtp_code=parsed.smtp_code,
                            now=now,
                        )
                        if applied:
                            result.matched += 1
                        else:
                            result.unmatched_tokens += 1
                    verdict_counts[parsed.status] = (
                        verdict_counts.get(parsed.status, 0) + 1
                    )
                    if mark_seen:
                        try:
                            client.store(msg_id, "+FLAGS", "(\\Seen)")
                        except Exception:
                            pass
                except Exception as exc:  # pragma: no cover - defensive
                    _LOGGER.debug(
                        "DSN parse failed for IMAP id %s: %s", msg_id, exc,
                    )
                    result.parse_errors += 1

        result.verdict_breakdown = verdict_counts
    finally:
        try:
            client.close()
        except Exception:
            pass
        try:
            client.logout()
        except Exception:
            pass

    return result


__all__ = [
    "BouncePollResult",
    "IMAPClient",
    "poll_bounces",
]
