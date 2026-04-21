"""Scoring V2 — a signal-based scoring engine.

This package is isolated from the existing ``app.scoring`` module. It
introduces the data model (``ScoringSignal``, ``ScoreBreakdown``), the
evaluator contract (``SignalEvaluator``), the configuration bundle
(``ScoringProfile``), the aggregation-capable engine
(``ScoringEngineV2``), and the first concrete evaluators that emit
signals from V1 row data.

Nothing here changes V1 scoring behavior, imports from V1, or integrates
with the pipeline. Pipeline integration is a future step.
"""

from __future__ import annotations

from .breakdown import ScoreBreakdown
from .comparison import (
    COMPARISON_COLUMNS,
    compare_scoring,
    summarize_comparison,
    write_comparison_report,
)
from .engine import ScoringEngineV2
from .evaluator import SignalEvaluator
from .evaluators import (
    DnsSignalEvaluator,
    DomainMatchSignalEvaluator,
    DomainPresenceSignalEvaluator,
    SyntaxSignalEvaluator,
    TypoCorrectionSignalEvaluator,
)
from .factory import build_default_engine, build_default_profile
from .profile import ScoringProfile
from .signal import ScoringSignal

__all__ = [
    # Core abstractions
    "ScoringSignal",
    "ScoreBreakdown",
    "SignalEvaluator",
    "ScoringProfile",
    "ScoringEngineV2",
    # First-wave evaluators
    "SyntaxSignalEvaluator",
    "DomainPresenceSignalEvaluator",
    "TypoCorrectionSignalEvaluator",
    "DomainMatchSignalEvaluator",
    "DnsSignalEvaluator",
    # Default engine composition
    "build_default_engine",
    "build_default_profile",
    # Comparison utilities
    "COMPARISON_COLUMNS",
    "compare_scoring",
    "summarize_comparison",
    "write_comparison_report",
]
