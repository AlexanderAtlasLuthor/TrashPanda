"""Validation Engine V2 — foundational abstractions.

This package introduces the V2 validation architecture: the input
request (:class:`ValidationRequest`), the structured result
(:class:`ValidationResult`), the policy configuration
(:class:`ValidationPolicy`), the orchestration skeleton
(:class:`ValidationEngineV2`), the collaborator contracts
(:mod:`app.validation_v2.interfaces`), and the centralized status
vocabulary (:mod:`app.validation_v2.types`).

Nothing here integrates with the pipeline, performs network calls,
or depends on V1 scoring. Those concerns are future subphases.
"""

from __future__ import annotations

from . import control
from .engine import ValidationEngineV2
from .interfaces import (
    CatchAllAnalyzer,
    DomainIntelligenceService,
    ExclusionService,
    ProviderReputationService,
    RetryStrategy,
    SMTPProbeClient,
    TelemetrySink,
    ValidationCandidateSelector,
)
from .policy import ValidationPolicy
from .request import ValidationRequest
from .result import ValidationResult
from .types import (
    CATCH_ALL_STATUSES,
    CatchAllStatus,
    ReasonCode,
    SMTP_PROBE_STATUSES,
    SmtpProbeStatus,
    VALIDATION_STATUSES,
    ValidationStatus,
)

__all__ = [
    # Core abstractions
    "ValidationRequest",
    "ValidationResult",
    "ValidationPolicy",
    "ValidationEngineV2",
    # Interfaces
    "DomainIntelligenceService",
    "ProviderReputationService",
    "ExclusionService",
    "ValidationCandidateSelector",
    "SMTPProbeClient",
    "CatchAllAnalyzer",
    "RetryStrategy",
    "TelemetrySink",
    # Types / enums
    "ValidationStatus",
    "SmtpProbeStatus",
    "CatchAllStatus",
    "ReasonCode",
    "VALIDATION_STATUSES",
    "SMTP_PROBE_STATUSES",
    "CATCH_ALL_STATUSES",
    # Subphase 3 control-plane subpackage.
    "control",
]
