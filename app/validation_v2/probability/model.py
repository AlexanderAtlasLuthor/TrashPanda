"""Probability model types for Validation Engine V2."""

from __future__ import annotations

from dataclasses import dataclass


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


__all__ = ["DeliverabilitySignal", "DeliverabilityResult"]
