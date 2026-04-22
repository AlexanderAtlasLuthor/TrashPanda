"""V2 Phase 6 — Decision Engine (automated actions layer).

Converts the Phase-5 ``deliverability_probability`` into one of three
deterministic actions: ``auto_approve``, ``manual_review``, or
``auto_reject``. Adds four columns to every technical CSV and writes a
``decision_summary.csv`` report.

Never alters V1 row placement by default. ``enable_bucket_override``
opts into annotating a target bucket in ``overridden_bucket`` without
physically moving rows.
"""

from __future__ import annotations

from .aggregator import (
    DECISION_COLUMNS,
    DecisionConfig,
    DecisionPassResult,
    DecisionStats,
    run_decision_pass,
    write_decision_summary,
)
from .decision_engine import (
    DecisionInputs,
    DecisionResult,
    apply_decision_policy,
    inputs_from_row,
)
from .decision_explanation import explain_decision
from .policy import (
    DEFAULT_DECISION_POLICY,
    DecisionPolicy,
    DecisionReason,
    FinalAction,
    OverrideBucket,
)


__all__ = [
    "DECISION_COLUMNS",
    "DEFAULT_DECISION_POLICY",
    "DecisionConfig",
    "DecisionInputs",
    "DecisionPassResult",
    "DecisionPolicy",
    "DecisionReason",
    "DecisionResult",
    "DecisionStats",
    "FinalAction",
    "OverrideBucket",
    "apply_decision_policy",
    "explain_decision",
    "inputs_from_row",
    "run_decision_pass",
    "write_decision_summary",
]
