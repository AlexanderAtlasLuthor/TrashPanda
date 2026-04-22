"""Deliverability probability layer for Validation Engine V2."""

from __future__ import annotations

from .aggregator import DeliverabilityAggregator
from .decision import ValidationDecision, ValidationDecisionPolicy
from .explanation import ExplanationBuilder
from .history_integration import MAX_HISTORY_ADJUSTMENT, apply_historical_adjustment
from .model import DeliverabilityResult, DeliverabilitySignal

# V2 Phase 5 — concrete row-level deliverability probability model.
# Coexists with the pre-existing engine scaffolding above.
from .row_aggregator import (
    PROBABILITY_COLUMNS,
    ProbabilityConfig,
    ProbabilityPassResult,
    ProbabilityStats,
    run_probability_pass,
    write_probability_summary,
)
from .row_explanation import explain_deliverability
from .row_model import (
    DEFAULT_PROBABILITY_THRESHOLDS,
    DeliverabilityComputation,
    DeliverabilityInputs,
    Factor,
    ProbabilityThresholds,
    compute_deliverability_probability,
    inputs_from_row,
)

__all__ = [
    # Existing V2 engine scaffolding.
    "DeliverabilitySignal",
    "DeliverabilityResult",
    "DeliverabilityAggregator",
    "MAX_HISTORY_ADJUSTMENT",
    "apply_historical_adjustment",
    "ValidationDecision",
    "ValidationDecisionPolicy",
    "ExplanationBuilder",
    # V2 Phase 5.
    "DEFAULT_PROBABILITY_THRESHOLDS",
    "DeliverabilityComputation",
    "DeliverabilityInputs",
    "Factor",
    "PROBABILITY_COLUMNS",
    "ProbabilityConfig",
    "ProbabilityPassResult",
    "ProbabilityStats",
    "ProbabilityThresholds",
    "compute_deliverability_probability",
    "explain_deliverability",
    "inputs_from_row",
    "run_probability_pass",
    "write_probability_summary",
]
