"""Controlled network probes for Validation Engine V2."""

from __future__ import annotations

from .smtp_classifier import (
    SMTP_STATUS_INVALID,
    SMTP_STATUS_NOT_ATTEMPTED,
    SMTP_STATUS_UNCERTAIN,
    SMTP_STATUS_VALID,
    SMTPResultClassifier,
)
from .smtp_client import SafeSMTPProbeClient
from .smtp_result import SMTPProbeResult
from .catch_all import CatchAllAnalyzer, CatchAllAssessment
from .retry import IntelligentRetryStrategy, RetryDecision

__all__ = [
    "SMTPProbeResult",
    "SafeSMTPProbeClient",
    "SMTPResultClassifier",
    "SMTP_STATUS_VALID",
    "SMTP_STATUS_INVALID",
    "SMTP_STATUS_UNCERTAIN",
    "SMTP_STATUS_NOT_ATTEMPTED",
    "CatchAllAnalyzer",
    "CatchAllAssessment",
    "IntelligentRetryStrategy",
    "RetryDecision",
]
