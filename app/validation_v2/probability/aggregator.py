"""Weighted deliverability aggregation."""

from __future__ import annotations

from .model import DeliverabilityResult, DeliverabilitySignal


class DeliverabilityAggregator:
    def compute(
        self,
        signals: list[DeliverabilitySignal],
    ) -> DeliverabilityResult:
        usable = [s for s in signals if s.weight > 0]
        if not usable:
            return DeliverabilityResult(0.0, 0.0, [])

        total_weight = sum(s.weight for s in usable)
        probability = sum(_clamp(s.value) * s.weight for s in usable) / total_weight
        probability = _clamp(probability)
        confidence = _confidence(usable)
        return DeliverabilityResult(probability, confidence, list(usable))


def _confidence(signals: list[DeliverabilitySignal]) -> float:
    sources = {s.source for s in signals}
    confidence = 0.2
    if "structural" in sources:
        confidence += 0.1
    if "dns" in sources:
        confidence += 0.2
    if "reputation" in sources:
        confidence += 0.15
    if "smtp" in sources:
        confidence += 0.3
    if "catch_all" in sources:
        confidence += 0.1

    has_positive = any(s.value >= 0.75 for s in signals)
    has_negative = any(s.value <= 0.25 for s in signals)
    if has_positive and has_negative:
        confidence -= 0.2

    return _clamp(confidence)


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


__all__ = ["DeliverabilityAggregator"]
