"""Probability model types for Validation Engine V2."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class DeliverabilitySignal:
    name: str
    value: float
    weight: float
    source: str


@dataclass
class DeliverabilityResult:
    probability: float
    confidence: float
    signals: list[DeliverabilitySignal]
    base_probability: float | None = None
    base_confidence: float | None = None
    historical_influence: dict[str, Any] = field(default_factory=dict)


__all__ = ["DeliverabilitySignal", "DeliverabilityResult"]
