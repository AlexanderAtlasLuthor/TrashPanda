"""Network execution policy and the ``is_validation_allowed`` gate.

The policy is a tiny declarative bundle of three booleans —
whether any network I/O is permitted, whether SMTP probing is
permitted, and whether catch-all analysis is permitted. Subphase
3 only reads ``allow_network`` (SMTP and catch-all stay gated but
unused until future subphases). The other flags are modelled now
so callers can configure the full policy shape today and future
subphases slot in without API churn.

``is_validation_allowed`` is a pure decision function: given the
policy plus the upstream exclusion / candidate results, it
returns ``(allowed, reason)``. Rate limiting is represented in
the ``reason`` vocabulary but is evaluated outside this helper —
the engine calls the rate limiter directly (so the caller can
distinguish "policy allows but rate limiter said no" from
"policy disabled").
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


# Reason-code vocabulary. Listed here so callers can switch on
# the result without duplicating string literals.
EXCLUSION_RESULT_NONE = None  # documentation hint; exclusion_result is str | None

EXECUTION_REASON_EXCLUDED = "excluded"
EXECUTION_REASON_NOT_CANDIDATE = "not_candidate"
EXECUTION_REASON_NETWORK_DISABLED = "network_disabled"
EXECUTION_REASON_RATE_LIMITED = "rate_limited"
EXECUTION_REASON_ALLOWED = "allowed"


@dataclass(frozen=True)
class NetworkExecutionPolicy:
    """Master switches for outbound validation work.

    Attributes:
        allow_network: If False, the engine must not perform any
            outbound network I/O. This is the primary kill switch
            for the active-validation layer.
        allow_smtp: Enables the SMTP probe step once Subphase 4
            lands. Ignored today — Subphase 3 never probes.
        allow_catch_all: Enables catch-all analysis. Ignored
            today for the same reason as ``allow_smtp``.
    """

    allow_network: bool
    allow_smtp: bool
    allow_catch_all: bool

    @classmethod
    def disabled(cls) -> "NetworkExecutionPolicy":
        """Convenience: a policy that blocks everything."""
        return cls(
            allow_network=False, allow_smtp=False, allow_catch_all=False
        )

    @classmethod
    def network_only(cls) -> "NetworkExecutionPolicy":
        """Convenience: network allowed, SMTP and catch-all blocked.

        Useful for tests that want to exercise the "allowed"
        branch without accidentally opening the SMTP gate.
        """
        return cls(
            allow_network=True, allow_smtp=False, allow_catch_all=False
        )


def is_validation_allowed(
    policy: NetworkExecutionPolicy | None,
    candidate_decision: Any,
    exclusion_result: Any,
) -> tuple[bool, str]:
    """Return ``(allowed, reason)`` for the execution-policy gate.

    Decision order:

      1. If ``exclusion_result`` is truthy → blocked / ``"excluded"``.
      2. If ``candidate_decision`` is falsy → blocked / ``"not_candidate"``.
      3. If ``policy`` exists and ``allow_network`` is False →
         blocked / ``"network_disabled"``.
      4. Otherwise → allowed / ``"allowed"``.

    ``candidate_decision`` is accepted loosely — a plain bool, a
    ``CandidateDecision`` NamedTuple, or any truthy object. The
    engine already normalizes its candidate result to a bool
    before calling this helper, but accepting richer types here
    keeps the function reusable for ad-hoc callers.

    ``exclusion_result`` follows the
    :class:`~app.validation_v2.services.exclusion.DefaultExclusionService`
    convention: ``None`` means "not excluded", any other value
    (typically a reason string) means excluded. Truthiness is
    sufficient — tests can pass ``True`` to mean "excluded".

    ``"rate_limited"`` is part of the reason vocabulary but is not
    produced by this helper (see module docstring).
    """
    if exclusion_result:
        return False, EXECUTION_REASON_EXCLUDED

    # ``candidate_decision`` may be a bool, a ``CandidateDecision``
    # NamedTuple, or ``None``. Normalize via truthiness.
    accepted = _normalize_candidate(candidate_decision)
    if not accepted:
        return False, EXECUTION_REASON_NOT_CANDIDATE

    if policy is not None and not policy.allow_network:
        return False, EXECUTION_REASON_NETWORK_DISABLED

    return True, EXECUTION_REASON_ALLOWED


def _normalize_candidate(candidate_decision: Any) -> bool:
    """Normalize a candidate decision to a plain bool.

    Accepts:

      * ``None`` → False (no selector result is treated as "not a candidate")
      * a plain bool / int → its truthiness
      * anything with an ``accepted`` attribute (e.g. a
        :class:`CandidateDecision` NamedTuple) → ``bool(.accepted)``
      * any other truthy object → True

    The leniency is intentional: the helper is a reusable
    decision function and should not force callers to pre-coerce.
    """
    if candidate_decision is None:
        return False
    accepted_attr = getattr(candidate_decision, "accepted", None)
    if accepted_attr is not None:
        return bool(accepted_attr)
    return bool(candidate_decision)


__all__ = [
    "NetworkExecutionPolicy",
    "is_validation_allowed",
    "EXECUTION_REASON_EXCLUDED",
    "EXECUTION_REASON_NOT_CANDIDATE",
    "EXECUTION_REASON_NETWORK_DISABLED",
    "EXECUTION_REASON_RATE_LIMITED",
    "EXECUTION_REASON_ALLOWED",
]
