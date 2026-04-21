"""Concrete passive-intelligence service implementations.

Each class here is a concrete implementation of one of the ABCs in
:mod:`app.validation_v2.interfaces`. The engine still depends on
the abstractions; these services slot in at construction time.

Nothing in this subpackage performs network I/O. Every
implementation is deterministic for a given input. That keeps the
engine reproducible and testable without fixtures or network
sandboxes.
"""

from __future__ import annotations

from .candidate_selector import (
    CandidateDecision,
    DefaultValidationCandidateSelector,
)
from .domain_intelligence import (
    COMMON_PROVIDERS,
    SimpleDomainIntelligenceService,
    SUSPICIOUS_DIGIT_RATIO_THRESHOLD,
    SUSPICIOUS_HYPHEN_THRESHOLD,
    SUSPICIOUS_LENGTH_THRESHOLD,
)
from .exclusion import (
    DefaultExclusionService,
    REASON_EXCLUDED_DOMAIN,
    REASON_INVALID_DOMAIN,
    REASON_SYNTAX_INVALID,
)
from .provider_reputation import (
    REPUTATION_SCORES,
    SimpleProviderReputationService,
    TRUST_LEVELS,
)
from .stores import DomainCacheStore, DomainRecord, PatternCacheStore

__all__ = [
    # Domain intelligence
    "SimpleDomainIntelligenceService",
    "COMMON_PROVIDERS",
    "SUSPICIOUS_LENGTH_THRESHOLD",
    "SUSPICIOUS_HYPHEN_THRESHOLD",
    "SUSPICIOUS_DIGIT_RATIO_THRESHOLD",
    # Provider reputation
    "SimpleProviderReputationService",
    "REPUTATION_SCORES",
    "TRUST_LEVELS",
    # Exclusion
    "DefaultExclusionService",
    "REASON_EXCLUDED_DOMAIN",
    "REASON_INVALID_DOMAIN",
    "REASON_SYNTAX_INVALID",
    # Candidate selector
    "DefaultValidationCandidateSelector",
    "CandidateDecision",
    # Stores
    "DomainCacheStore",
    "DomainRecord",
    "PatternCacheStore",
]
