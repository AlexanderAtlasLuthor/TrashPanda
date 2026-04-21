"""DefaultExclusionService: hard policy gate for validation.

Exclusion is binary: a request either survives the gate and
continues through the engine, or it is rejected before any
service does meaningful work. The rejection carries a short
machine-readable reason the engine can fold into the result's
metadata and reason-code trail.

Rules applied, in order (first match wins):

    1. ``policy.excluded_domains`` membership
    2. Missing / empty domain or otherwise malformed shape
    3. ``syntax_valid == False``

The service is purely a decision function. It does not mutate
the request or the policy, and it does not do any I/O.
"""

from __future__ import annotations

from ..interfaces import ExclusionService
from ..policy import ValidationPolicy
from ..request import ValidationRequest


# Exposed reason codes. These are *not* the engine's reason
# codes; they are the fine-grained reasons the service surfaces
# to the engine, which then maps them into the engine-level
# reason codes emitted on the result. Keeping the two layers
# separate lets the engine evolve its vocabulary independently.
REASON_EXCLUDED_DOMAIN = "excluded_domain"
REASON_INVALID_DOMAIN = "invalid_domain"
REASON_SYNTAX_INVALID = "syntax_invalid"


class DefaultExclusionService(ExclusionService):
    """Default exclusion rules: policy list, shape, syntax.

    Callers that want richer rules (e.g. opt-out lists fetched at
    startup) can subclass or wrap this service; the engine only
    talks to the ABC.
    """

    def is_excluded(
        self,
        request: ValidationRequest,
        policy: ValidationPolicy,
    ) -> bool:
        """Return True if the request must be excluded.

        Parameters:
            request: The frozen validation request.
            policy: The active policy; only
                ``policy.excluded_domains`` is consulted.
        """
        return self.check(request, policy) is not None

    # ------------------------------------------------------------------
    # Extended API (not on the ABC)
    # ------------------------------------------------------------------

    def check(
        self,
        request: ValidationRequest,
        policy: ValidationPolicy,
    ) -> str | None:
        """Return the first-matching exclusion reason, or ``None``.

        Separated from :meth:`is_excluded` so the engine can carry
        the reason into the result without a second evaluation.
        ``None`` means "not excluded".
        """
        raw_domain = request.domain or ""
        domain = raw_domain.strip().lower()

        if domain and domain in policy.excluded_domains:
            return REASON_EXCLUDED_DOMAIN

        if _is_invalid_domain_shape(raw_domain, domain, request.domain_present):
            return REASON_INVALID_DOMAIN

        if not request.syntax_valid:
            return REASON_SYNTAX_INVALID

        return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_invalid_domain_shape(
    raw_domain: str, domain: str, domain_present: bool
) -> bool:
    """Return True iff the domain shape is obviously malformed.

    This is a *shape* check, not a DNS check. The domain fails if
    any of the following hold:

      * ``domain_present`` is False (upstream said there was no
        domain portion at all), or
      * the raw input contains any whitespace (upstream should
        have normalized this — if it slipped through, treat it
        as malformed rather than silently trimming), or
      * the domain string is empty after normalization, or
      * the domain lacks a dot (no TLD), or
      * the domain starts or ends with a dot / hyphen, or
      * any label in the domain starts or ends with a hyphen
        (detected via the ``-.`` / ``.-`` substrings).

    A legitimate-looking but nonexistent domain (e.g.
    ``this-does-not-exist.example``) passes this check — the
    purpose here is to reject input we can rule out without any
    network lookup, not to guess at deliverability.
    """
    if not domain_present:
        return True
    if any(c.isspace() for c in raw_domain):
        return True
    if not domain:
        return True
    if "." not in domain:
        return True
    if domain.startswith((".", "-")) or domain.endswith((".", "-")):
        return True
    if "-." in domain or ".-" in domain:
        return True
    return False


__all__ = [
    "DefaultExclusionService",
    "REASON_EXCLUDED_DOMAIN",
    "REASON_INVALID_DOMAIN",
    "REASON_SYNTAX_INVALID",
]
