"""Bounded historical adjustments for deliverability probability."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any


MAX_HISTORY_ADJUSTMENT = 0.15


def apply_historical_adjustment(
    *,
    base_probability: float,
    base_confidence: float,
    historical: Any,
) -> tuple[float, float, dict[str, Any]]:
    history = _normalize_history(historical)
    if not history.get("history_cache_hit"):
        influence = {
            "adjustment": 0.0,
            "confidence_delta": 0.0,
            "factors": [],
            "reason": "no_history_or_low_confidence",
            "applied": False,
        }
        return _clamp(base_probability), _clamp(base_confidence), influence

    raw_adjustment, factors = _raw_adjustment(history)
    confidence_gate = _confidence_gate(history)
    freshness_gate = 0.35 if _is_stale(history) else 1.0
    observation_gate = _observation_gate(history)
    gate = confidence_gate * freshness_gate * observation_gate
    adjustment = _clamp_adjustment(raw_adjustment * gate)

    confidence_delta, confidence_factors = _confidence_delta(history)
    factors.extend(confidence_factors)
    final_probability = _clamp(base_probability + adjustment)
    final_confidence = _clamp(base_confidence + confidence_delta)
    applied = abs(adjustment) > 0.000001 or abs(confidence_delta) > 0.000001
    influence = {
        "adjustment": adjustment,
        "confidence_delta": confidence_delta,
        "factors": factors,
        "reason": _reason_from_factors(factors, applied),
        "applied": applied,
    }
    return final_probability, final_confidence, influence


def _raw_adjustment(history: dict[str, Any]) -> tuple[float, list[str]]:
    adjustment = 0.0
    factors: list[str] = []

    domain_score = _optional_float(history.get("historical_domain_reputation"))
    provider_score = _optional_float(history.get("historical_provider_reputation"))
    valid_rate = _optional_float(history.get("historical_smtp_valid_rate"))
    invalid_rate = _optional_float(history.get("historical_smtp_invalid_rate"))
    timeout_rate = _optional_float(history.get("historical_timeout_rate"))
    catch_all_risk = _optional_float(history.get("historical_catch_all_risk"))

    if domain_score is not None:
        if domain_score >= 0.8:
            adjustment += 0.05
            factors.append("high_domain_reputation")
        elif domain_score <= 0.35:
            adjustment -= 0.06
            factors.append("low_domain_reputation")

    if provider_score is not None:
        if provider_score >= 0.8:
            adjustment += 0.03
            factors.append("high_provider_reputation")
        elif provider_score <= 0.35:
            adjustment -= 0.04
            factors.append("low_provider_reputation")

    if valid_rate is not None and valid_rate >= 0.75:
        adjustment += 0.02
        factors.append("high_historical_smtp_valid_rate")
    if invalid_rate is not None and invalid_rate >= 0.45:
        adjustment -= 0.04
        factors.append("high_historical_smtp_invalid_rate")
    if timeout_rate is not None and timeout_rate >= 0.45:
        adjustment -= 0.03
        factors.append("high_historical_timeout_rate")
    if catch_all_risk is not None:
        if catch_all_risk <= 0.15:
            adjustment += 0.02
            factors.append("low_catch_all_risk")
        elif catch_all_risk >= 0.65:
            adjustment -= 0.04
            factors.append("high_catch_all_risk")

    return _clamp_adjustment(adjustment), factors


def _confidence_delta(history: dict[str, Any]) -> tuple[float, list[str]]:
    observations = max(
        int(history.get("domain_observation_count") or 0),
        int(history.get("provider_observation_count") or 0),
    )
    delta = min(float(observations) / 50.0, 0.1)
    factors: list[str] = []
    if delta > 0:
        factors.append("history_observation_depth")

    if _is_stale(history):
        delta -= 0.1
        factors.append("stale_history")

    valid_rate = _optional_float(history.get("historical_smtp_valid_rate")) or 0.0
    invalid_rate = _optional_float(history.get("historical_smtp_invalid_rate")) or 0.0
    if valid_rate >= 0.3 and invalid_rate >= 0.3:
        delta -= 0.08
        factors.append("contradictory_history")

    return _clamp_delta(delta), factors


def _confidence_gate(history: dict[str, Any]) -> float:
    domain_conf = _optional_float(
        history.get("historical_domain_reputation_confidence")
    )
    provider_conf = _optional_float(
        history.get("historical_provider_reputation_confidence")
    )
    confidences = [v for v in (domain_conf, provider_conf) if v is not None]
    if not confidences:
        return 0.0
    if len(confidences) == 2:
        return min(confidences)
    return confidences[0]


def _observation_gate(history: dict[str, Any]) -> float:
    observations = max(
        int(history.get("domain_observation_count") or 0),
        int(history.get("provider_observation_count") or 0),
    )
    if observations < 3:
        return 0.0
    if observations < 10:
        return 0.5
    return 1.0


def _is_stale(history: dict[str, Any]) -> bool:
    return bool(history.get("domain_history_stale")) or bool(
        history.get("provider_history_stale")
    )


def _reason_from_factors(factors: list[str], applied: bool) -> str:
    if not applied:
        return "no_history_or_low_confidence"
    positives = {
        "high_domain_reputation",
        "high_provider_reputation",
        "high_historical_smtp_valid_rate",
        "low_catch_all_risk",
        "history_observation_depth",
    }
    negatives = {
        "low_domain_reputation",
        "low_provider_reputation",
        "high_historical_smtp_invalid_rate",
        "high_historical_timeout_rate",
        "high_catch_all_risk",
        "stale_history",
        "contradictory_history",
    }
    has_positive = any(f in positives for f in factors)
    has_negative = any(f in negatives for f in factors)
    if has_positive and not has_negative:
        return "high_domain_reputation_and_low_risk"
    if has_negative and not has_positive:
        return "poor_or_risky_historical_signals"
    return "mixed_historical_signals"


def _normalize_history(historical: Any) -> dict[str, Any]:
    if historical is None:
        return {}
    if isinstance(historical, dict):
        return dict(historical)
    to_dict = getattr(historical, "to_dict", None)
    if callable(to_dict):
        return dict(to_dict())
    if is_dataclass(historical):
        return asdict(historical)
    return {}


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return _clamp(float(value))
    except (TypeError, ValueError):
        return None


def _clamp_adjustment(value: float) -> float:
    return max(-MAX_HISTORY_ADJUSTMENT, min(MAX_HISTORY_ADJUSTMENT, float(value)))


def _clamp_delta(value: float) -> float:
    return max(-0.1, min(0.1, float(value)))


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


__all__ = ["MAX_HISTORY_ADJUSTMENT", "apply_historical_adjustment"]
