"""Telemetry primitives for the V2 control plane.

A telemetry event is a frozen, structured record — event type,
timestamp, domain, and a free-form metadata dict. Sinks receive
events and route them wherever they need to go (in-memory buffer
for tests, log file, metrics bus, etc.).

The engine's golden rule applies here: telemetry failures must
never affect validation outcomes. The engine wraps every
``emit`` call in a try / except, and :class:`InMemoryTelemetrySink`
also swallows any unexpected errors defensively.

Event vocabulary is small and stable; new events append rather
than rename. The five constants defined here are the full set
emitted by Subphase 3.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


# Canonical event-type tokens. Keeping them as module-level
# constants (rather than an Enum) makes them trivially JSON-safe
# and lets callers compare with plain ``==``.
EVENT_VALIDATION_STARTED = "validation_started"
EVENT_EXCLUDED = "excluded"
EVENT_CANDIDATE_SKIPPED = "candidate_skipped"
EVENT_VALIDATION_ALLOWED = "validation_allowed"
EVENT_VALIDATION_BLOCKED_BY_POLICY = "validation_blocked_by_policy"


@dataclass(frozen=True)
class TelemetryEvent:
    """A single structured telemetry record.

    Attributes:
        event_type: Short machine token identifying the event. One
            of the ``EVENT_*`` constants in this module; callers
            may add their own as long as they follow the
            snake_case convention.
        timestamp: Epoch seconds (or any monotonic float chosen by
            the emitter's clock source). The engine does not
            interpret this — it is stored verbatim for downstream
            analysis.
        domain: Domain the event is about. Empty string is
            acceptable when no domain context exists.
        metadata: Free-form payload. Expected to be
            JSON-serializable but not validated — sinks that care
            must validate themselves.
    """

    event_type: str
    timestamp: float
    domain: str
    metadata: dict[str, Any]


class TelemetrySink(ABC):
    """Abstract sink that receives :class:`TelemetryEvent` records."""

    @abstractmethod
    def emit(self, event: TelemetryEvent) -> None:
        """Emit ``event``. Implementations must not raise upwards.

        A well-behaved sink never propagates an exception to its
        caller — the engine's outcome must not depend on a sink's
        health.
        """
        raise NotImplementedError


class InMemoryTelemetrySink(TelemetrySink):
    """Buffered in-memory sink suitable for tests and dev inspection.

    The sink stores every event in insertion order. When
    ``max_buffer_size`` is set, the oldest events are dropped
    (FIFO) once the buffer fills. A ``None`` buffer size means
    unbounded — fine for tests, but not appropriate for a
    long-running process.
    """

    def __init__(self, *, max_buffer_size: int | None = None) -> None:
        if max_buffer_size is not None and max_buffer_size < 0:
            raise ValueError("max_buffer_size must be >= 0 or None")
        self._events: list[TelemetryEvent] = []
        self._max_buffer_size = max_buffer_size

    @property
    def events(self) -> list[TelemetryEvent]:
        """Return a shallow copy of the recorded events.

        A copy (rather than the live list) prevents callers from
        mutating the sink's internal state inadvertently. Tests
        that want an O(1) length check can use :meth:`__len__`.
        """
        return list(self._events)

    def emit(self, event: TelemetryEvent) -> None:
        # Defensive: even the "simple" sink must never raise. If
        # something truly unexpected happens (memory pressure,
        # exotic dict types), we silently drop the event.
        try:
            self._events.append(event)
            if (
                self._max_buffer_size is not None
                and len(self._events) > self._max_buffer_size
            ):
                # Drop oldest. A deque would be asymptotically
                # better but the buffer is capped; correctness
                # matters more than microseconds here.
                overflow = len(self._events) - self._max_buffer_size
                del self._events[:overflow]
        except Exception:
            # Sink resilience is the sink's job. Never raise to
            # the engine.
            pass

    def clear(self) -> None:
        """Drop all buffered events. Handy between test cases."""
        self._events.clear()

    def __len__(self) -> int:
        return len(self._events)

    def event_types(self) -> list[str]:
        """Return the list of event_types emitted, in order."""
        return [ev.event_type for ev in self._events]


__all__ = [
    "TelemetryEvent",
    "TelemetrySink",
    "InMemoryTelemetrySink",
    "EVENT_VALIDATION_STARTED",
    "EVENT_EXCLUDED",
    "EVENT_CANDIDATE_SKIPPED",
    "EVENT_VALIDATION_ALLOWED",
    "EVENT_VALIDATION_BLOCKED_BY_POLICY",
]
