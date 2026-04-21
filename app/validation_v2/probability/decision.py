"""Decision layer for deliverability probabilities."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ValidationDecision:
    status: str
    action: str


class ValidationDecisionPolicy:
    def decide(self, probability: float) -> ValidationDecision:
        if probability >= 0.85:
            return ValidationDecision("valid", "send")
        if probability >= 0.60:
            return ValidationDecision("likely_valid", "send_with_monitoring")
        if probability >= 0.40:
            return ValidationDecision("uncertain", "review")
        if probability >= 0.20:
            return ValidationDecision("risky", "verify")
        return ValidationDecision("invalid", "block")


__all__ = ["ValidationDecision", "ValidationDecisionPolicy"]
