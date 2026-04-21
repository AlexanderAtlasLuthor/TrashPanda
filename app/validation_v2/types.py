"""Centralized vocabulary for Validation Engine V2.

Every string token the engine emits — ``validation_status``,
``smtp_probe_status``, ``catch_all_status`` — lives here. Downstream
consumers (the engine, tests, future telemetry sinks, future pipeline
integration) import from this module instead of scattering string
literals across the codebase. That way, renaming a status or adding a
new one is a single-file change with a single test surface.

This module is intentionally pure data: no imports from other V2
modules, no logic, no side effects. It is safe to import from
anywhere in the V2 package.
"""

from __future__ import annotations

from enum import Enum


class ValidationStatus(str, Enum):
    """Top-level outcome emitted by ValidationEngineV2.

    ``str`` mixin keeps these JSON-serializable and directly
    comparable to plain strings — downstream code that stores the
    status as a column value does not need to know about Enum.
    """

    DELIVERABLE_LIKELY = "deliverable_likely"
    DELIVERABLE_UNCERTAIN = "deliverable_uncertain"
    RISKY_CATCH_ALL = "risky_catch_all"
    TEMPORARILY_UNVERIFIABLE = "temporarily_unverifiable"
    UNDELIVERABLE_LIKELY = "undeliverable_likely"
    EXCLUDED_BY_POLICY = "excluded_by_policy"


class SmtpProbeStatus(str, Enum):
    """State of the SMTP probe step for a given validation attempt.

    The engine in this subphase never actually probes — it emits
    ``NOT_ATTEMPTED`` or ``SKIPPED`` only. Future subphases will fill
    in the remaining cases.
    """

    NOT_ATTEMPTED = "not_attempted"
    ATTEMPTED_SUCCESS = "attempted_success"
    ATTEMPTED_TEMP_FAIL = "attempted_temp_fail"
    ATTEMPTED_REJECT = "attempted_reject"
    SKIPPED = "skipped"


class CatchAllStatus(str, Enum):
    """Catch-all domain classification.

    Tracks how confident V2 is that the domain accepts mail for any
    local part. ``UNKNOWN`` is the default for the skeleton engine
    since no catch-all analysis is implemented yet.
    """

    CONFIRMED = "confirmed"
    LIKELY = "likely"
    UNLIKELY = "unlikely"
    UNKNOWN = "unknown"


# Public string sets. These mirror the Enum values and exist so code
# that prefers ``in VALIDATION_STATUSES`` over ``isinstance(…, Enum)``
# has a single source of truth.
VALIDATION_STATUSES: frozenset[str] = frozenset(s.value for s in ValidationStatus)
SMTP_PROBE_STATUSES: frozenset[str] = frozenset(s.value for s in SmtpProbeStatus)
CATCH_ALL_STATUSES: frozenset[str] = frozenset(s.value for s in CatchAllStatus)


# Reason code vocabulary emitted by the skeleton engine. Future
# subphases will extend this set; keeping it here keeps the
# vocabulary discoverable.
class ReasonCode(str, Enum):
    """Reason codes the skeleton engine can emit.

    Scoped to what the current subphase actually produces. New
    subphases append — they do not rename existing codes.
    """

    VALIDATION_SKIPPED = "validation_skipped"
    NO_ACTIVE_PROBE = "no_active_probe"
    EXCLUDED_BY_POLICY = "excluded_by_policy"
    NOT_A_CANDIDATE = "not_a_candidate"


__all__ = [
    "ValidationStatus",
    "SmtpProbeStatus",
    "CatchAllStatus",
    "ReasonCode",
    "VALIDATION_STATUSES",
    "SMTP_PROBE_STATUSES",
    "CATCH_ALL_STATUSES",
]
