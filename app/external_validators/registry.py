"""V2.10.11 — external validator registry + protocol."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable


# Verdict vocabulary every adapter must map onto. Mirrors the
# language used by most third-party APIs but stays narrow enough
# that the consensus aggregator can reason about it without per-
# vendor special cases.
VERDICT_VALID: str = "valid"
VERDICT_INVALID: str = "invalid"
VERDICT_CATCH_ALL: str = "catch_all"
VERDICT_UNKNOWN: str = "unknown"
VERDICT_RISKY: str = "risky"

VERDICTS: tuple[str, ...] = (
    VERDICT_VALID,
    VERDICT_INVALID,
    VERDICT_CATCH_ALL,
    VERDICT_UNKNOWN,
    VERDICT_RISKY,
)


@dataclass(frozen=True, slots=True)
class ExternalValidationResult:
    """One adapter's result for one email.

    ``raw_response`` is whatever the vendor's SDK returned; it is
    preserved verbatim so a future audit can reconstruct the
    decision. The ``error`` field is set when the adapter could not
    reach the vendor (network failure, auth error, rate limit) — in
    that case ``verdict`` is ``unknown`` and ``confidence`` is 0.
    """

    validator_name: str
    verdict: str
    confidence: float = 0.0
    raw_response: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


@runtime_checkable
class ExternalEmailValidator(Protocol):
    """Adapter contract for a third-party email validator.

    Adapters must be picklable and thread-safe — the SMTP stage may
    call ``probe`` concurrently from worker threads. Adapters should
    NOT raise on vendor errors; instead, return a result with
    ``verdict=unknown`` and ``error=<short description>``.
    """

    name: str

    def probe(
        self,
        email: str,
        *,
        timeout: float,
    ) -> ExternalValidationResult: ...


# --------------------------------------------------------------------------- #
# Process-wide registry
# --------------------------------------------------------------------------- #


_REGISTRY: dict[str, ExternalEmailValidator] = {}


def register(validator: ExternalEmailValidator) -> None:
    """Add (or replace) ``validator`` in the registry.

    Adapters call this at import time. Re-registering the same name
    silently replaces the prior entry — the right behaviour for hot-
    reloading dev environments and for tests that swap fakes in.
    """
    name = getattr(validator, "name", None)
    if not isinstance(name, str) or not name.strip():
        raise ValueError(
            "ExternalEmailValidator.name must be a non-empty string"
        )
    if not callable(getattr(validator, "probe", None)):
        raise ValueError(
            f"validator {name!r} is missing a callable probe(email, *, timeout)"
        )
    _REGISTRY[name] = validator


def registered_validators() -> tuple[ExternalEmailValidator, ...]:
    """Return a snapshot of currently-registered validators.

    Order is the registration order (Python dicts preserve insertion
    order from 3.7+), which keeps consensus deterministic across
    runs as long as the operator's adapter list is stable.
    """
    return tuple(_REGISTRY.values())


def clear_registry() -> None:
    """Remove every registered validator. Tests use this for isolation."""
    _REGISTRY.clear()


__all__ = [
    "ExternalEmailValidator",
    "ExternalValidationResult",
    "VERDICTS",
    "VERDICT_CATCH_ALL",
    "VERDICT_INVALID",
    "VERDICT_RISKY",
    "VERDICT_UNKNOWN",
    "VERDICT_VALID",
    "clear_registry",
    "register",
    "registered_validators",
]
