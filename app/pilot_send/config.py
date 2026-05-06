"""V2.10.12 — Per-run pilot send configuration.

Persisted as JSON at ``<run_dir>/pilot_send_config.json``. Owns
the operator-supplied template (subject + body), sender identity,
IMAP credentials for bounce polling, and the wait-window knobs.

The ``authorization_confirmed`` flag is the single most important
field here. The launch endpoint refuses to send a batch unless the
operator has explicitly checked it — a hard guard against
accidentally pinging a list TrashPanda doesn't have permission to
send to.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from ..db.pilot_send_tracker import (
    DEFAULT_EXPIRY_HOURS,
    DEFAULT_WAIT_WINDOW_HOURS,
    PILOT_CONFIG_FILENAME,
)


@dataclass(slots=True)
class IMAPCredentials:
    """Credentials for the bounce mailbox the IMAP poller reads.

    ``password_env`` is the *name* of an environment variable that
    holds the actual password — credentials are NEVER persisted to
    disk in the run directory. The poller reads them at poll time.
    """

    host: str = ""
    port: int = 993
    use_ssl: bool = True
    username: str = ""
    password_env: str = "TRASHPANDA_BOUNCE_IMAP_PASSWORD"
    folder: str = "INBOX"

    def is_configured(self) -> bool:
        return bool(self.host and self.username)


@dataclass(slots=True)
class PilotMessageTemplate:
    """The actual content of the pilot email.

    Operators must supply both ``subject`` and ``body_text``. ``body_html``
    is optional — when omitted, the message is text-only.
    """

    subject: str = ""
    body_text: str = ""
    body_html: str = ""
    sender_address: str = ""
    sender_name: str = "TrashPanda"
    reply_to: str = ""

    def is_complete(self) -> bool:
        return bool(
            self.subject.strip()
            and self.body_text.strip()
            and self.sender_address.strip()
        )


@dataclass(slots=True)
class PilotSendConfig:
    """Top-level pilot send configuration."""

    template: PilotMessageTemplate = field(default_factory=PilotMessageTemplate)
    imap: IMAPCredentials = field(default_factory=IMAPCredentials)
    return_path_domain: str = ""
    wait_window_hours: int = DEFAULT_WAIT_WINDOW_HOURS
    expiry_hours: int = DEFAULT_EXPIRY_HOURS
    # Hard cap operators can't override on a per-batch basis. The
    # ``selector`` may still be told a smaller batch_size at launch.
    max_batch_size: int = 100
    # Operator must check this before launch. The launch endpoint
    # explicitly verifies it; missing the flag is a 400 with reason
    # ``authorization_required``.
    authorization_confirmed: bool = False
    authorization_note: str = ""

    def is_ready_to_send(self) -> bool:
        return (
            self.template.is_complete()
            and bool(self.return_path_domain.strip())
            and self.authorization_confirmed
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def write_pilot_config(
    run_dir: str | Path,
    config: PilotSendConfig,
) -> Path:
    """Atomically persist ``config`` next to the tracker DB."""
    path = Path(run_dir) / PILOT_CONFIG_FILENAME
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(config.to_dict(), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return path


def read_pilot_config(run_dir: str | Path) -> PilotSendConfig:
    """Load ``pilot_send_config.json`` or return defaults."""
    path = Path(run_dir) / PILOT_CONFIG_FILENAME
    if not path.is_file():
        return PilotSendConfig()
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return PilotSendConfig()
    template_raw = raw.get("template") or {}
    imap_raw = raw.get("imap") or {}
    return PilotSendConfig(
        template=PilotMessageTemplate(
            subject=str(template_raw.get("subject") or ""),
            body_text=str(template_raw.get("body_text") or ""),
            body_html=str(template_raw.get("body_html") or ""),
            sender_address=str(template_raw.get("sender_address") or ""),
            sender_name=str(template_raw.get("sender_name") or "TrashPanda"),
            reply_to=str(template_raw.get("reply_to") or ""),
        ),
        imap=IMAPCredentials(
            host=str(imap_raw.get("host") or ""),
            port=int(imap_raw.get("port") or 993),
            use_ssl=bool(imap_raw.get("use_ssl", True)),
            username=str(imap_raw.get("username") or ""),
            password_env=str(
                imap_raw.get("password_env") or "TRASHPANDA_BOUNCE_IMAP_PASSWORD"
            ),
            folder=str(imap_raw.get("folder") or "INBOX"),
        ),
        return_path_domain=str(raw.get("return_path_domain") or ""),
        wait_window_hours=int(
            raw.get("wait_window_hours") or DEFAULT_WAIT_WINDOW_HOURS
        ),
        expiry_hours=int(raw.get("expiry_hours") or DEFAULT_EXPIRY_HOURS),
        max_batch_size=int(raw.get("max_batch_size") or 100),
        authorization_confirmed=bool(raw.get("authorization_confirmed", False)),
        authorization_note=str(raw.get("authorization_note") or ""),
    )


__all__ = [
    "IMAPCredentials",
    "PilotMessageTemplate",
    "PilotSendConfig",
    "read_pilot_config",
    "write_pilot_config",
]
