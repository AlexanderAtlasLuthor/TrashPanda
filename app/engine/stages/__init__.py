"""Concrete stage implementations layered on top of the engine primitives.

Stages are grouped by phase:

  * ``preprocess``       — Subphase 2 (header/value normalization, metadata)
  * ``email_processing`` — Subphases 3 and 4 (syntax, domain, typo, compare)
"""

from __future__ import annotations

from .email_processing import (
    DomainComparisonStage,
    DomainExtractionStage,
    EmailSyntaxValidationStage,
    TypoCorrectionStage,
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
]
