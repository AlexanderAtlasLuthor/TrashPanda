"""V2.10.12 — Controlled in-house pilot send + bounce-proven verification.

Take a small batch of rescatable rows (V2.10.11's
``ready_probable``, ``review_low_risk``, ``review_catch_all_consumer``,
``review_timeout_retry``), send them a real campaign-shaped message
via direct-to-MX SMTP with a VERP envelope-from, then poll a bounce
mailbox via IMAP and parse the resulting DSN messages to assign a
real verdict (``delivered`` / ``hard_bounce`` / ``soft_bounce`` /
``blocked`` / ``deferred`` / ``complaint`` / ``unknown``).

The endpoint of the flow is two new client deliverables:

* ``delivery_verified.xlsx`` — rows the pilot proved deliverable.
* ``updated_do_not_send.xlsx`` — rows the pilot proved bounce on
  send (merged with the existing ``do_not_send.xlsx`` from
  V2.10.10.b).

In-house, no third-party providers
----------------------------------

The sender speaks SMTP directly to the destination MX using
``smtplib`` (operator's choice — V2.10.12 plan). The IMAP poller
opens an authenticated session to the operator's bounce mailbox
and parses ``multipart/report`` DSNs into the canonical verdict
vocabulary. Both transports are injectable for tests.
"""

from __future__ import annotations

from .bounce_parser import (
    DSNParseResult,
    parse_dsn_message,
)
from .bounce_poller import (
    BouncePollResult,
    poll_bounces,
)
from .config import (
    IMAPCredentials,
    PilotMessageTemplate,
    PilotSendConfig,
    read_pilot_config,
    write_pilot_config,
)
from .finalize import (
    FinalizeResult,
    finalize_pilot,
)
from .launch import (
    LaunchResult,
    launch_pilot,
)
from .selector import (
    PilotCandidate,
    select_candidates,
)
from .sender import (
    PilotSendOutcome,
    SMTPSender,
    SMTPTransport,
)
from .verp import (
    decode_verp_token,
    encode_verp_token,
    extract_token_from_envelope,
    new_verp_token,
)

__all__ = [
    "BouncePollResult",
    "DSNParseResult",
    "FinalizeResult",
    "IMAPCredentials",
    "LaunchResult",
    "PilotCandidate",
    "PilotMessageTemplate",
    "PilotSendConfig",
    "PilotSendOutcome",
    "SMTPSender",
    "SMTPTransport",
    "decode_verp_token",
    "encode_verp_token",
    "extract_token_from_envelope",
    "finalize_pilot",
    "launch_pilot",
    "new_verp_token",
    "parse_dsn_message",
    "poll_bounces",
    "read_pilot_config",
    "select_candidates",
    "write_pilot_config",
]
