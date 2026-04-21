"""ValidationResult: structured output of ValidationEngineV2.validate.

A result is a plain, JSON-serializable record. It carries the
top-level status, a calibrated deliverability probability, optional
SMTP / catch-all / reputation details (all ``None`` until later
subphases implement them), a reason-code trail, a deterministic
human-readable explanation, a structured breakdown dict, and an
open-ended metadata dict.

The result is *not* frozen. Downstream stages occasionally need to
merge extra metadata before emitting (e.g. a pipeline stage
attaching its own instrumentation). Mutation is allowed but not
required — the skeleton engine never mutates a result after it
returns one.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .types import CatchAllStatus, SmtpProbeStatus, ValidationStatus


@dataclass
class ValidationResult:
    """Structured output of a single validation attempt.

    Attributes:
        validation_status: Top-level outcome. Stored as a plain
            string so the result is trivially JSON-serializable;
            callers can still compare against :class:`ValidationStatus`
            members because the enum uses a ``str`` mixin.
        deliverability_probability: Calibrated probability of
            deliverability in [0.0, 1.0]. The skeleton returns a
            placeholder; future subphases will fill this in from
            probe / reputation data.
        smtp_probe_status: Outcome of the SMTP probe step. ``None``
            when the engine has not attempted one and has no record
            of a skip. Storing ``None`` (instead of always-present
            ``not_attempted``) preserves the distinction between
            "engine did not record this" and "engine explicitly
            chose not to probe".
        catch_all_status: Catch-all classification, or ``None`` when
            not assessed.
        provider_reputation: Opaque reputation label (e.g.
            ``"gmail"``, ``"microsoft"``, ``"unknown"``). Kept as
            ``str | None`` so future providers can be added without
            a schema change.
        retry_recommended: Whether the engine recommends re-running
            validation later (e.g. for a transient DNS failure).
        validation_reason_codes: Reason codes the validation engine
            emitted, in arrival order. Tuple so the result can be
            safely shared / cached.
        validation_explanation: Deterministic human-readable
            explanation. Never contains wall-clock timestamps or
            random ordering so audit exports are stable.
        breakdown: Open-ended structured dict for per-step details
            (domain intelligence output, reputation output, probe
            transcript, etc.). JSON-serializable by construction:
            callers must only put JSON-safe values in here.
        metadata: Open-ended dict for caller-attached context that
            does not belong in the structured breakdown.
    """

    validation_status: str = ValidationStatus.DELIVERABLE_UNCERTAIN.value
    deliverability_probability: float = 0.0
    smtp_probe_status: str | None = None
    catch_all_status: str | None = None
    provider_reputation: str | None = None
    retry_recommended: bool = False
    validation_reason_codes: tuple[str, ...] = ()
    validation_explanation: str = ""
    breakdown: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable dict snapshot of this result.

        Enum values are coerced to plain strings (the ``str`` mixin
        makes this a no-op for stored values, but defensive casting
        makes the method safe even if a caller assigned a raw
        :class:`ValidationStatus` / :class:`SmtpProbeStatus` /
        :class:`CatchAllStatus` member).

        ``breakdown`` and ``metadata`` are copied (shallow) so
        consumers cannot mutate the result by mutating the dict they
        received.
        """
        return {
            "validation_status": _coerce_enum(self.validation_status),
            "deliverability_probability": float(self.deliverability_probability),
            "smtp_probe_status": _coerce_enum_optional(self.smtp_probe_status),
            "catch_all_status": _coerce_enum_optional(self.catch_all_status),
            "provider_reputation": self.provider_reputation,
            "retry_recommended": bool(self.retry_recommended),
            "validation_reason_codes": list(self.validation_reason_codes),
            "validation_explanation": self.validation_explanation,
            "breakdown": dict(self.breakdown),
            "metadata": dict(self.metadata),
        }


def _coerce_enum(value: Any) -> str:
    if isinstance(value, (ValidationStatus, SmtpProbeStatus, CatchAllStatus)):
        return value.value
    return str(value)


def _coerce_enum_optional(value: Any) -> str | None:
    if value is None:
        return None
    return _coerce_enum(value)


__all__ = ["ValidationResult"]
