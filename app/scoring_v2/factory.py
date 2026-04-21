"""Factory for building the calibrated default Scoring V2 engine."""

from __future__ import annotations

from .engine import ScoringEngineV2
from .evaluators import (
    DnsSignalEvaluator,
    DomainMatchSignalEvaluator,
    DomainPresenceSignalEvaluator,
    SyntaxSignalEvaluator,
    TypoCorrectionSignalEvaluator,
)
from .profile import ScoringProfile, build_default_profile


def build_default_engine(profile: ScoringProfile | None = None) -> ScoringEngineV2:
    """Return a ScoringEngineV2 with the standard calibrated evaluator stack."""
    return ScoringEngineV2(
        evaluators=[
            SyntaxSignalEvaluator(),
            DomainPresenceSignalEvaluator(),
            TypoCorrectionSignalEvaluator(),
            DomainMatchSignalEvaluator(),
            DnsSignalEvaluator(),
        ],
        profile=profile if profile is not None else build_default_profile(),
    )


__all__ = ["build_default_engine", "build_default_profile"]
