"""Scoring V2 — foundational abstractions for a signal-based scoring engine.

This package is isolated from the existing ``app.scoring`` module. It
introduces the data model (``ScoringSignal``, ``ScoreBreakdown``), the
evaluator contract (``SignalEvaluator``), the configuration bundle
(``ScoringProfile``), a logic-free engine skeleton (``ScoringEngineV2``),
and the first concrete evaluators that emit signals from V1 row data.

Nothing here changes V1 scoring behavior, imports from V1, or integrates
with the pipeline. Those are future steps.
"""

from __future__ import annotations

from .breakdown import ScoreBreakdown
from .engine import ScoringEngineV2
from .evaluator import SignalEvaluator
from .evaluators import (
    DnsSignalEvaluator,
    DomainMatchSignalEvaluator,
    DomainPresenceSignalEvaluator,
    SyntaxSignalEvaluator,
    TypoCorrectionSignalEvaluator,
)
from .profile import ScoringProfile, build_default_profile
from .signal import ScoringSignal

__all__ = [
    # Core abstractions
    "ScoringSignal",
    "ScoreBreakdown",
    "SignalEvaluator",
    "ScoringProfile",
    "ScoringEngineV2",
    "build_default_profile",
    # First-wave evaluators
    "SyntaxSignalEvaluator",
    "DomainPresenceSignalEvaluator",
    "TypoCorrectionSignalEvaluator",
    "DomainMatchSignalEvaluator",
    "DnsSignalEvaluator",
]
