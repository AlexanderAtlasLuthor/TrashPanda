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
from .catch_all import (
    DEFAULT_CATCH_ALL_THRESHOLDS,
    NOT_REVIEW,
    REVIEW_CATCH_ALL,
    REVIEW_INCONSISTENT,
    REVIEW_LOW_CONFIDENCE,
    REVIEW_TIMEOUT,
    CatchAllSignal,
    CatchAllThresholds,
    classify_review_subclass,
    detect_catch_all_signals,
)
from .domain_memory import (
    DEFAULT_THRESHOLDS,
    LabelThresholds,
    classify_domain,
    compute_adjustment,
)
from .engine import ValidationEngineV2
from .explanation_v2 import (
    explain_domain_history,
    explain_row_with_history,
    readiness_label_for_report,
)
from .history_integration import (
    HistoryUpdateResult,
    build_observations_from_run,
    update_history_from_run,
    write_domain_history_summary,
)
from .history_models import (
    DomainHistoryRecord,
    DomainObservation,
    FinalDecision,
    HistoricalLabel,
)
from .history_store import DomainHistoryStore
from .decision import (
    DECISION_COLUMNS,
    DEFAULT_DECISION_POLICY,
    DecisionConfig,
    DecisionInputs,
    DecisionPassResult,
    DecisionPolicy,
    DecisionReason,
    DecisionResult,
    DecisionStats,
    FinalAction,
    OverrideBucket,
    apply_decision_policy,
    explain_decision,
    run_decision_pass,
)
from .probability import (
    DEFAULT_PROBABILITY_THRESHOLDS,
    DeliverabilityComputation,
    DeliverabilityInputs,
    Factor,
    PROBABILITY_COLUMNS,
    ProbabilityConfig,
    ProbabilityPassResult,
    ProbabilityStats,
    ProbabilityThresholds,
    compute_deliverability_probability,
    explain_deliverability,
    inputs_from_row,
    run_probability_pass,
)
from .smtp_integration import (
    SMTPProbeConfig,
    SMTPProbeResult,
    SMTPProbeStats,
    SMTP_COLUMNS,
    run_smtp_probing_pass,
)
from .smtp_probe import (
    SMTPResult,
    probe_email_dry_run,
    probe_email_smtplib,
)
from .scoring_adjustment import (
    AdjustmentConfig,
    AdjustmentDecision,
    AdjustmentStats,
    NEW_COLUMNS,
    bucket_from_output_reason,
    bucket_from_score,
    compute_row_adjustment,
    enrich_csv,
    enrich_run_outputs,
    write_adjustment_summary,
)
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
    # V2 Phase 1 – Domain Historical Memory
    "DEFAULT_THRESHOLDS",
    "DomainHistoryRecord",
    "DomainHistoryStore",
    "DomainObservation",
    "FinalDecision",
    "HistoricalLabel",
    "HistoryUpdateResult",
    "LabelThresholds",
    "build_observations_from_run",
    "classify_domain",
    "compute_adjustment",
    "explain_domain_history",
    "readiness_label_for_report",
    "update_history_from_run",
    "write_domain_history_summary",
    # V2 Phase 2 – Score adjustment + row-level explanations
    "AdjustmentConfig",
    "AdjustmentDecision",
    "AdjustmentStats",
    "NEW_COLUMNS",
    "bucket_from_output_reason",
    "bucket_from_score",
    "compute_row_adjustment",
    "enrich_csv",
    "enrich_run_outputs",
    "explain_row_with_history",
    "write_adjustment_summary",
    # V2 Phase 3 – Catch-all detection + review intelligence
    "CatchAllSignal",
    "CatchAllThresholds",
    "DEFAULT_CATCH_ALL_THRESHOLDS",
    "NOT_REVIEW",
    "REVIEW_CATCH_ALL",
    "REVIEW_INCONSISTENT",
    "REVIEW_LOW_CONFIDENCE",
    "REVIEW_TIMEOUT",
    "classify_review_subclass",
    "detect_catch_all_signals",
    # V2 Phase 4 – Selective SMTP probing
    "SMTPProbeConfig",
    "SMTPProbeResult",
    "SMTPProbeStats",
    "SMTPResult",
    "SMTP_COLUMNS",
    "probe_email_dry_run",
    "probe_email_smtplib",
    "run_smtp_probing_pass",
    # V2 Phase 5 – Probabilistic deliverability model
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
    # V2 Phase 6 – Decision Engine
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
    "run_decision_pass",
]
