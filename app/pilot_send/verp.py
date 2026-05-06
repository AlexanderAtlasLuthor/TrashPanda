"""V2.10.12 — VERP envelope-from token encoding.

VERP (Variable Envelope Return Path) encodes a per-recipient token
into the SMTP envelope-from address so bounce DSNs come back to a
unique address that we can map back to the original send. The
canonical form TrashPanda emits is:

    bounce+<token>@<return_path_domain>

where ``<token>`` is a URL-safe random hex string we generate at
batch-build time. Recipient mailboxes bounce to ``Return-Path:``,
which the operator's MX delivers to ``bounce@`` (catch-all subaddress
or alias). The IMAP poller decodes ``+<token>`` from the original
recipient or message-id header in the DSN and looks the row up in
the tracker.

We deliberately do NOT use Message-ID for tracking. Some MTAs strip
it on bounce and re-insert their own; some pre-mortem bounces never
generate a DSN with the Message-ID at all. The envelope-from is the
only header guaranteed to round-trip.
"""

from __future__ import annotations

import re
import secrets

from email.headerregistry import Address


# Canonical local-part prefix. Operators can rename via config; the
# decode helper accepts any prefix as long as the structure is
# ``<prefix>+<token>``.
DEFAULT_VERP_LOCAL_PART: str = "bounce"

# Token alphabet: URL-safe hex. 16 chars = 64 bits of entropy — more
# than enough to make collisions across realistic batch sizes
# astronomically unlikely.
_TOKEN_BYTES: int = 8


def new_verp_token() -> str:
    """Return a new random 16-character hex token."""
    return secrets.token_hex(_TOKEN_BYTES)


def encode_verp_token(
    token: str,
    *,
    return_path_domain: str,
    local_part: str = DEFAULT_VERP_LOCAL_PART,
) -> str:
    """Build the VERP envelope-from address.

    Raises ``ValueError`` for empty / malformed inputs so a
    misconfigured pilot batch never silently sends with a bare
    ``bounce@`` envelope (which would lose tracking).
    """
    if not token or not token.strip():
        raise ValueError("verp token must be non-empty")
    if not return_path_domain or "@" in return_path_domain:
        raise ValueError(
            "return_path_domain must be a bare domain "
            f"(no '@'); got {return_path_domain!r}"
        )
    if not local_part or "+" in local_part or "@" in local_part:
        raise ValueError(
            "local_part must not contain '+' or '@'; got "
            f"{local_part!r}"
        )
    return f"{local_part}+{token.strip()}@{return_path_domain.strip()}"


_VERP_PATTERN = re.compile(
    r"(?P<local>[A-Za-z0-9._%-]+)\+(?P<token>[A-Za-z0-9._%-]+)@"
    r"(?P<domain>[A-Za-z0-9.-]+)"
)


def decode_verp_token(envelope_from: str) -> str | None:
    """Extract the token from a VERP envelope-from address.

    Returns ``None`` if the address does not have the ``+token@``
    structure. Used by the IMAP poller to find the tracker row a
    given DSN belongs to.
    """
    if not envelope_from:
        return None
    match = _VERP_PATTERN.fullmatch(envelope_from.strip().strip("<>"))
    if match is None:
        return None
    return match.group("token")


def extract_token_from_envelope(text: str) -> str | None:
    """Find the first VERP token anywhere inside a free-form blob.

    The DSN body / headers can carry the original envelope-from in
    several places (``Original-Recipient``, ``Final-Recipient``,
    ``Return-Path``, the textual diagnostic). This helper scans all
    of them at once. Returns the first token found.
    """
    if not text:
        return None
    match = _VERP_PATTERN.search(text)
    if match is None:
        return None
    return match.group("token")


__all__ = [
    "DEFAULT_VERP_LOCAL_PART",
    "decode_verp_token",
    "encode_verp_token",
    "extract_token_from_envelope",
    "new_verp_token",
]
