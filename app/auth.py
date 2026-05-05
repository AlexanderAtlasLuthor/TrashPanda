"""Optional bearer-token authentication for operator routes.

Design constraints
------------------
* Backwards compatible: when ``TRASHPANDA_OPERATOR_TOKEN`` is unset
  the dependency is a no-op so local development and existing tests
  keep passing.
* Constant-time comparison via :func:`hmac.compare_digest` so a
  timing-side-channel cannot leak the token.
* Multiple tokens supported (comma-separated env var) so deploy-time
  rotation does not require a service restart — keep the old token
  valid for one rollout and then drop it.

Wiring
------
The FastAPI router includes this dependency on every operator route.
Routes that should remain unauthenticated (e.g. ``/healthz``) live on
the main ``app`` and never go through this dependency.
"""

from __future__ import annotations

import hmac
import logging
import os
from typing import Optional

from fastapi import Header, HTTPException


_LOGGER = logging.getLogger(__name__)

# Env var name kept short and obvious. ``_TOKENS`` (plural) is also
# accepted so deployments mid-rotation can list both the new and old
# tokens. Tokens are split on commas; whitespace is trimmed.
ENV_VAR_PRIMARY = "TRASHPANDA_OPERATOR_TOKEN"
ENV_VAR_LIST = "TRASHPANDA_OPERATOR_TOKENS"


def _load_configured_tokens() -> tuple[str, ...]:
    """Read the configured token(s) from the environment.

    Returns an empty tuple when nothing is configured — the caller
    interprets that as "auth disabled" and skips the bearer check.
    """

    primary = (os.environ.get(ENV_VAR_PRIMARY) or "").strip()
    listed = os.environ.get(ENV_VAR_LIST) or ""
    raw = ",".join(part for part in (primary, listed) if part)
    tokens = tuple(
        t for t in (chunk.strip() for chunk in raw.split(",")) if t
    )
    return tokens


def auth_enabled() -> bool:
    """Return True when at least one operator token is configured."""

    return bool(_load_configured_tokens())


def _extract_bearer(value: str | None) -> str | None:
    if not value:
        return None
    parts = value.strip().split(None, 1)
    if len(parts) != 2:
        return None
    scheme, token = parts
    if scheme.lower() != "bearer":
        return None
    return token.strip() or None


def require_operator_token(
    authorization: Optional[str] = Header(default=None),
    x_trashpanda_operator_token: Optional[str] = Header(default=None),
) -> None:
    """FastAPI dependency that enforces the operator bearer token.

    Authentication source order:

      1. ``Authorization: Bearer <token>`` (canonical).
      2. ``X-TrashPanda-Operator-Token: <token>`` (convenience for
         operator UIs that prefer a custom header).

    When no token is configured server-side the dependency becomes a
    no-op so local development and the existing test suite are
    unaffected. When at least one token IS configured every operator
    request must present a matching value.
    """

    configured = _load_configured_tokens()
    if not configured:
        return  # auth disabled

    presented = (
        _extract_bearer(authorization)
        or (x_trashpanda_operator_token or "").strip()
    )
    if not presented:
        raise HTTPException(
            status_code=401,
            detail={
                "error": {
                    "error_type": "operator_auth_required",
                    "message": (
                        "Operator endpoints require a bearer token. "
                        "Send Authorization: Bearer <token>."
                    ),
                    "details": {},
                }
            },
            headers={"WWW-Authenticate": 'Bearer realm="trashpanda-operator"'},
        )

    for valid in configured:
        if hmac.compare_digest(presented, valid):
            return

    raise HTTPException(
        status_code=403,
        detail={
            "error": {
                "error_type": "operator_auth_invalid",
                "message": "Operator token is not valid.",
                "details": {},
            }
        },
    )


__all__ = [
    "ENV_VAR_LIST",
    "ENV_VAR_PRIMARY",
    "auth_enabled",
    "require_operator_token",
]
