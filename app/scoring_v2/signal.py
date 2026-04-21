"""ScoringSignal: a single evaluated signal used by ScoringEngineV2.

A signal is the atomic unit of evidence the engine collects. Each signal
carries a normalized strength, a relative weight, and a confidence —
enough detail to produce an auditable score breakdown and, later, a
calibrated probabilistic interpretation.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


Direction = Literal["positive", "negative", "neutral"]

_VALID_DIRECTIONS: frozenset[str] = frozenset({"positive", "negative", "neutral"})


@dataclass(slots=True, frozen=True)
class ScoringSignal:
    """One evaluated signal contributing to a row's final score.

    Attributes:
        name: Stable identifier for the signal class (e.g. ``"mx"``,
            ``"typo"``). May match ``reason_code`` but does not have to.
        direction: Whether the signal pushes the score up, down, or is
            purely informational. One of "positive", "negative",
            "neutral".
        value: Normalized signal strength in [0.0, 1.0]. This is the
            unsigned magnitude; ``direction`` carries the sign.
        weight: Relative importance, >= 0. A weight of 0 records the
            signal without contributing to the score.
        confidence: Reliability of this signal in [0.0, 1.0]. 1.0 means
            the underlying evidence is fully trusted; 0.0 means pure
            guess. The engine uses per-signal confidence to aggregate a
            row-level confidence.
        reason_code: Stable machine-readable token suitable for
            filtering and analytics (e.g. ``"mx_present"``,
            ``"nxdomain"``, ``"role_account"``).
        explanation: Human-readable string describing the finding.

    Instances are frozen: once constructed, fields cannot change. This
    makes signals safe to cache and pass by reference.
    """

    name: str
    direction: Direction
    value: float
    weight: float
    confidence: float
    reason_code: str
    explanation: str = ""

    def __post_init__(self) -> None:
        if self.direction not in _VALID_DIRECTIONS:
            raise ValueError(
                f"ScoringSignal.direction must be one of "
                f"{sorted(_VALID_DIRECTIONS)}, got {self.direction!r} "
                f"for signal {self.name!r}"
            )
        if not (0.0 <= self.value <= 1.0):
            raise ValueError(
                f"ScoringSignal.value must be in [0.0, 1.0], got "
                f"{self.value!r} for signal {self.name!r}"
            )
        if not (0.0 <= self.confidence <= 1.0):
            raise ValueError(
                f"ScoringSignal.confidence must be in [0.0, 1.0], got "
                f"{self.confidence!r} for signal {self.name!r}"
            )
        if self.weight < 0:
            raise ValueError(
                f"ScoringSignal.weight must be >= 0, got {self.weight!r} "
                f"for signal {self.name!r}"
            )
