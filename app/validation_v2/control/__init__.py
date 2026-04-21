"""Control plane for Validation Engine V2 (Subphase 3).

This subpackage adds the observability + control layer that future
SMTP / catch-all / retry subphases will plug into. It does not
perform any network I/O, does not call SMTP, and does not change
the engine's happy-path semantics for requests that reach the
execution-policy gate.

Modules:

    * ``telemetry``        — structured event records + pluggable sink.
    * ``rate_limit``       — per-domain / global sliding-window limiter.
    * ``execution_policy`` — declarative "can we validate this?" gate.
    * ``decision_trace``   — step-by-step audit trail for every request.

The engine wires these in as optional instance attributes
(``engine.telemetry_sink``, ``engine.rate_limiter``,
``engine.execution_policy``) so the constructor signature stays
frozen across subphases.
"""

from __future__ import annotations

from .decision_trace import ProbeDecisionTrace
from .execution_policy import (
    EXECUTION_REASON_ALLOWED,
    EXECUTION_REASON_EXCLUDED,
    EXECUTION_REASON_NETWORK_DISABLED,
    EXECUTION_REASON_NOT_CANDIDATE,
    EXECUTION_REASON_RATE_LIMITED,
    NetworkExecutionPolicy,
    is_validation_allowed,
)
from .rate_limit import InMemoryRateLimiter, RateLimiter, RateLimitPolicy
from .telemetry import (
    EVENT_CANDIDATE_SKIPPED,
    EVENT_EXCLUDED,
    EVENT_VALIDATION_ALLOWED,
    EVENT_VALIDATION_BLOCKED_BY_POLICY,
    EVENT_VALIDATION_STARTED,
    EVENT_SMTP_PROBE_COMPLETED,
    EVENT_SMTP_PROBE_FAILED,
    EVENT_SMTP_PROBE_STARTED,
    EVENT_SMTP_RETRY_ATTEMPTED,
    EVENT_SMTP_RETRY_SKIPPED,
    EVENT_CATCH_ALL_CLASSIFIED,
    InMemoryTelemetrySink,
    TelemetryEvent,
    TelemetrySink,
)

__all__ = [
    # Telemetry
    "TelemetryEvent",
    "TelemetrySink",
    "InMemoryTelemetrySink",
    "EVENT_VALIDATION_STARTED",
    "EVENT_EXCLUDED",
    "EVENT_CANDIDATE_SKIPPED",
    "EVENT_VALIDATION_ALLOWED",
    "EVENT_VALIDATION_BLOCKED_BY_POLICY",
    "EVENT_SMTP_PROBE_STARTED",
    "EVENT_SMTP_PROBE_COMPLETED",
    "EVENT_SMTP_PROBE_FAILED",
    "EVENT_SMTP_RETRY_ATTEMPTED",
    "EVENT_SMTP_RETRY_SKIPPED",
    "EVENT_CATCH_ALL_CLASSIFIED",
    # Rate limit
    "RateLimitPolicy",
    "RateLimiter",
    "InMemoryRateLimiter",
    # Execution policy
    "NetworkExecutionPolicy",
    "is_validation_allowed",
    "EXECUTION_REASON_ALLOWED",
    "EXECUTION_REASON_EXCLUDED",
    "EXECUTION_REASON_NOT_CANDIDATE",
    "EXECUTION_REASON_NETWORK_DISABLED",
    "EXECUTION_REASON_RATE_LIMITED",
    # Decision trace
    "ProbeDecisionTrace",
]
