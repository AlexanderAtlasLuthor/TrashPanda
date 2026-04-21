"""Concrete stage implementations layered on top of the engine primitives.

Stages are grouped by phase:

  * ``preprocess``       — Subphase 2 (header/value normalization, metadata)
  * ``email_processing`` — Subphases 3 and 4 (syntax, domain, typo, compare)
  * ``enrichment``       — Subphases 5, 6, and completeness (DNS, scoring,
                           completeness score)
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
)
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
    "ScoringStage",
    "CompletenessStage",
]
