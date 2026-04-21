"""Conservative catch-all assessment from existing SMTP signals.

No additional mailbox probes are performed here. The assessment uses the
current SMTP result, known/provider-ish hints from the domain cache, and
historical counters already accumulated for the domain.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..services.stores import DomainCacheStore
from .smtp_result import SMTPProbeResult


CATCH_ALL_CONFIRMED = "confirmed"
CATCH_ALL_LIKELY = "likely"
CATCH_ALL_UNLIKELY = "unlikely"
CATCH_ALL_UNKNOWN = "unknown"


@dataclass(frozen=True)
class CatchAllAssessment:
    classification: str
    confidence: float
    signals: dict[str, Any]


class CatchAllAnalyzer:
    """Probabilistic accept-all classifier with no extra probing."""

    def assess(
        self,
        domain: str,
        smtp_result: SMTPProbeResult,
        classification: dict[str, object],
        cache: DomainCacheStore | None = None,
    ) -> CatchAllAssessment:
        record = cache.get(domain) if cache is not None else None
        counters = dict(record.counters) if record is not None else {}
        smtp_valid = bool(classification.get("smtp_valid"))
        smtp_invalid = bool(classification.get("smtp_invalid"))

        random_accepts = counters.get("catch_all_random_accepts", 0)
        invalid_accepts = counters.get("catch_all_invalid_accepts", 0)
        confirmed_signals = random_accepts + invalid_accepts
        hard_rejects = counters.get("smtp_550_rejects", 0)

        provider_type = record.provider_type if record is not None else None
        unknown_or_suspicious = (
            provider_type in (None, "unknown", "suspicious")
            or counters.get("suspicious_pattern", 0) > 0
        )

        signals: dict[str, Any] = {
            "smtp_code": smtp_result.code,
            "smtp_valid": smtp_valid,
            "smtp_invalid": smtp_invalid,
            "provider_type": provider_type,
            "random_invalid_accept_signals": confirmed_signals,
            "hard_reject_signals": hard_rejects,
        }

        if confirmed_signals >= 2:
            assessment = CatchAllAssessment(
                CATCH_ALL_CONFIRMED,
                0.95,
                {**signals, "reason": "historical_random_invalid_accepts"},
            )
        elif smtp_valid and unknown_or_suspicious:
            assessment = CatchAllAssessment(
                CATCH_ALL_LIKELY,
                0.65,
                {**signals, "reason": "smtp_250_unknown_or_suspicious_domain"},
            )
        elif smtp_invalid or hard_rejects >= 2:
            assessment = CatchAllAssessment(
                CATCH_ALL_UNLIKELY,
                0.75,
                {**signals, "reason": "consistent_hard_rejects"},
            )
        else:
            assessment = CatchAllAssessment(
                CATCH_ALL_UNKNOWN,
                0.0,
                {**signals, "reason": "insufficient_signal"},
            )

        if cache is not None:
            updated = cache.record_domain(domain)
            updated.catch_all_classification = assessment.classification
            updated.catch_all_confidence = assessment.confidence
            updated.counters["catch_all_signal_count"] = (
                updated.counters.get("catch_all_signal_count", 0) + 1
            )
            cache.set(domain, updated)

        return assessment


__all__ = [
    "CatchAllAssessment",
    "CatchAllAnalyzer",
    "CATCH_ALL_CONFIRMED",
    "CATCH_ALL_LIKELY",
    "CATCH_ALL_UNLIKELY",
    "CATCH_ALL_UNKNOWN",
]
