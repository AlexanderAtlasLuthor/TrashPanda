"""Deliverability probability layer for Validation Engine V2."""

from __future__ import annotations

from .aggregator import DeliverabilityAggregator
from .decision import ValidationDecision, ValidationDecisionPolicy
from .explanation import ExplanationBuilder
from .history_integration import MAX_HISTORY_ADJUSTMENT, apply_historical_adjustment
from .model import DeliverabilityResult, DeliverabilitySignal

__all__ = [
    "DeliverabilitySignal",
    "DeliverabilityResult",
    "DeliverabilityAggregator",
    "MAX_HISTORY_ADJUSTMENT",
    "apply_historical_adjustment",
    "ValidationDecision",
    "ValidationDecisionPolicy",
    "ExplanationBuilder",
]
