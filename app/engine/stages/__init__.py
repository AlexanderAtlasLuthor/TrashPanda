"""Concrete stage implementations layered on top of the engine primitives.

Stages are grouped by phase:

  * ``preprocess``       — Subphase 2 (header/value normalization, metadata)
  * ``email_processing`` — Subphases 3 and 4 (syntax, domain, typo, compare)
  * ``enrichment``       — Subphases 5, 6, and completeness (DNS, scoring,
                           completeness score)
  * ``postprocessing``   — Email normalization, dedupe, and staging
                           persistence (the final three chunk steps)
"""

from __future__ import annotations

from .email_processing import (
    DomainComparisonStage,
    DomainExtractionStage,
    EmailSyntaxValidationStage,
    TypoCorrectionStage,
)
from .enrichment import (
    CompletenessStage,
    DNSEnrichmentStage,
    ScoringStage,
    TypoSuggestionValidationStage,
)
from .postprocessing import (
    DedupeStage,
    EmailNormalizationStage,
    StagingPersistenceStage,
)
from .scoring_v2 import ScoringComparisonStage, ScoringV2Stage
from .smtp_verification import (
    SMTP_VERIFICATION_OUTPUT_COLUMNS,
    SMTPVerificationStage,
)
from .catch_all_detection import (
    CATCH_ALL_DETECTION_OUTPUT_COLUMNS,
    CatchAllDetectionStage,
)
from .domain_intelligence import (
    DOMAIN_INTELLIGENCE_OUTPUT_COLUMNS,
    DomainIntelligenceStage,
)
from .decision import DECISION_STAGE_OUTPUT_COLUMNS, DecisionStage
from .preprocess import (
    HeaderNormalizationStage,
    StructuralValidationStage,
    TechnicalMetadataStage,
    ValueNormalizationStage,
)

__all__ = [
    # Preprocessing
    "HeaderNormalizationStage",
    "StructuralValidationStage",
    "TechnicalMetadataStage",
    "ValueNormalizationStage",
    # Email processing
    "EmailSyntaxValidationStage",
    "DomainExtractionStage",
    "TypoCorrectionStage",
    "DomainComparisonStage",
    # Enrichment
    "DNSEnrichmentStage",
    "TypoSuggestionValidationStage",
    "ScoringStage",
    "ScoringV2Stage",
    "ScoringComparisonStage",
    "CompletenessStage",
    # V2.2 SMTP Verification
    "SMTPVerificationStage",
    "SMTP_VERIFICATION_OUTPUT_COLUMNS",
    # V2.3 Catch-all Detection
    "CatchAllDetectionStage",
    "CATCH_ALL_DETECTION_OUTPUT_COLUMNS",
    # V2.6 Domain Intelligence
    "DomainIntelligenceStage",
    "DOMAIN_INTELLIGENCE_OUTPUT_COLUMNS",
    # V2.1 Decision Authority
    "DecisionStage",
    "DECISION_STAGE_OUTPUT_COLUMNS",
    # Postprocessing
    "EmailNormalizationStage",
    "DedupeStage",
    "StagingPersistenceStage",
]
