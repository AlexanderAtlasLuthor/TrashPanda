"""Deterministic explanation builder for deliverability decisions."""

from __future__ import annotations

from .decision import ValidationDecision
from .model import DeliverabilityResult, DeliverabilitySignal


class ExplanationBuilder:
    def build(
        self,
        signals: list[DeliverabilitySignal],
        result: DeliverabilityResult,
        decision: ValidationDecision,
    ) -> dict[str, object]:
        ordered = sorted(
            signals,
            key=lambda s: (abs(s.value - 0.5) * s.weight, s.name),
            reverse=True,
        )
        positives = [
            _signal_dict(s)
            for s in ordered
            if s.value >= 0.6
        ][:3]
        negatives = [
            _signal_dict(s)
            for s in ordered
            if s.value <= 0.4
        ][:3]
        factors = [_signal_dict(s) for s in sorted(signals, key=lambda s: s.name)]
        historical_influence = dict(result.historical_influence or {})
        historical_text = ""
        if historical_influence.get("applied"):
            historical_text = (
                " adjusted by historical reputation signals "
                f"(adjustment={historical_influence.get('adjustment', 0.0):+.2f});"
            )
        text = (
            f"Deliverability is {decision.status} "
            f"(p={result.probability:.2f}, confidence={result.confidence:.2f}); "
            f"recommended action: {decision.action}."
            f"{historical_text}"
        )
        return {
            "top_positive_signals": positives,
            "top_negative_signals": negatives,
            "explanation_text": text,
            "contributing_factors": factors,
            "historical_influence": historical_influence,
        }


def _signal_dict(signal: DeliverabilitySignal) -> dict[str, object]:
    return {
        "name": signal.name,
        "value": signal.value,
        "weight": signal.weight,
        "source": signal.source,
    }


__all__ = ["ExplanationBuilder"]
