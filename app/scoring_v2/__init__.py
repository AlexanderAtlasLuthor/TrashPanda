"""Scoring V2 — foundational abstractions for a signal-based scoring engine.

This package is isolated from the existing ``app.scoring`` module. It
introduces the data model (``ScoringSignal``, ``ScoreBreakdown``), the
evaluator contract (``SignalEvaluator``), the configuration bundle
(``ScoringProfile``), and a logic-free engine skeleton
(``ScoringEngineV2``) whose only job is to validate the collection
contract end-to-end.

Nothing here changes V1 scoring behavior, imports from V1, or integrates
with the pipeline. Those are future steps.
"""

from __future__ import annotations

from .breakdown import ScoreBreakdown
from .engine import ScoringEngineV2
from .evaluator import SignalEvaluator
from .profile import ScoringProfile
from .signal import ScoringSignal

__all__ = [
    "ScoringSignal",
    "ScoreBreakdown",
    "SignalEvaluator",
    "ScoringProfile",
    "ScoringEngineV2",
]
