"""Factory for building a default Scoring V2 engine.

This is a thin convenience layer so callers (pipeline stage, tests,
ad-hoc scripts) can obtain a standard ``ScoringEngineV2`` without
duplicating the list of evaluators or the default hard-stop policy.
Keeping it in the ``scoring_v2`` package means the composition lives
next to the code it composes, not buried in the pipeline.
"""

from __future__ import annotations

from .engine import ScoringEngineV2
from .evaluators import (
    DnsSignalEvaluator,
    DomainMatchSignalEvaluator,
    DomainPresenceSignalEvaluator,
    SyntaxSignalEvaluator,
    TypoCorrectionSignalEvaluator,
)
from .profile import ScoringProfile


# Reason codes that force the row into ``hard_stop=True`` regardless of
# the numeric score. This is the V2 vocabulary, chosen to mirror the V1
# hard-fail equivalence so the two engines can be compared side by side.
_DEFAULT_HARD_STOP_POLICY: tuple[str, ...] = (
    "syntax_invalid",
    "no_domain",
    "nxdomain",
)


def build_default_profile(
    *,
    hard_stop_policy: tuple[str, ...] | list[str] | None = None,
) -> ScoringProfile:
    """Return a ``ScoringProfile`` with the V2 defaults.

    Thresholds come from ``ScoringProfile``'s own defaults (V2
    normalized ``[0.0, 1.0]`` space). A caller may pass a custom
    ``hard_stop_policy`` tuple/list — otherwise the default policy is
    used. The profile's ``weights`` and ``bucket_policy`` are left
    empty for this subphase.
    """
    policy = (
        list(hard_stop_policy)
        if hard_stop_policy is not None
        else list(_DEFAULT_HARD_STOP_POLICY)
    )
    return ScoringProfile(hard_stop_policy=policy)


def build_default_engine(profile: ScoringProfile | None = None) -> ScoringEngineV2:
    """Return a ``ScoringEngineV2`` with the standard V2 evaluator stack.

    The evaluator list is fixed here and matches the emission order
    relied on by hard-stop selection: syntax → domain presence → typo
    → domain match → DNS. If a ``profile`` is supplied it is used
    verbatim; otherwise ``build_default_profile`` is called.
    """
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
