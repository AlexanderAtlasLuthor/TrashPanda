"""Small history-integration heuristics for catch-all and retry refinement."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any


def normalize_history(historical: Any) -> dict[str, Any]:
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


def catch_all_support(historical: Any) -> dict[str, Any]:
    history = normalize_history(historical)
    if not history.get("history_cache_hit"):
        return {"effect": "none", "reason": "no_history"}

    observations = int(history.get("domain_observation_count") or 0)
    confidence = _coerce_float(
        history.get("historical_domain_reputation_confidence")
    )
    risk = _coerce_float(history.get("historical_catch_all_risk"))
    stale = bool(history.get("domain_history_stale"))

    usable = observations >= 5 and confidence >= 0.35 and not stale
    strong = usable and risk >= 0.70 and confidence >= 0.55
    moderate = usable and risk >= 0.55
    contradictory = usable and risk <= 0.15 and confidence >= 0.55

    if strong:
        return {
            "effect": "strong_support",
            "reason": "historical_risk_supports_catch_all",
            "risk": risk,
            "confidence": confidence,
            "observations": observations,
        }
    if moderate:
        return {
            "effect": "moderate_support",
            "reason": "historical_risk_supports_likely",
            "risk": risk,
            "confidence": confidence,
            "observations": observations,
        }
    if contradictory:
        return {
            "effect": "contradicts_catch_all",
            "reason": "history_conflicts_with_current_catch_all_signal",
            "risk": risk,
            "confidence": confidence,
            "observations": observations,
        }
    return {
        "effect": "weak_or_stale",
        "reason": "history_not_strong_enough",
        "risk": risk,
        "confidence": confidence,
        "observations": observations,
        "stale": stale,
    }


def retry_support(historical: Any) -> dict[str, Any]:
    history = normalize_history(historical)
    if not history.get("history_cache_hit"):
        return {"effect": "none", "reason": "no_history"}

    observations = int(history.get("domain_observation_count") or 0)
    confidence = _coerce_float(
        history.get("historical_domain_reputation_confidence")
        or history.get("historical_provider_reputation_confidence")
    )
    valid_rate = _coerce_float(history.get("historical_smtp_valid_rate"))
    invalid_rate = _coerce_float(history.get("historical_smtp_invalid_rate"))
    uncertain_rate = _coerce_float(history.get("historical_smtp_uncertain_rate"))
    timeout_rate = _coerce_float(history.get("historical_timeout_rate"))
    stale = bool(history.get("domain_history_stale")) or bool(
        history.get("provider_history_stale")
    )

    usable = observations >= 5 and confidence >= 0.35 and not stale
    if not usable:
        return {
            "effect": "weak_or_stale",
            "reason": "history_not_strong_enough",
            "observations": observations,
            "confidence": confidence,
            "stale": stale,
        }

    if valid_rate >= 0.50 and invalid_rate <= 0.25 and timeout_rate <= 0.35:
        return {
            "effect": "supports_retry",
            "reason": "transient_error_and_history_supports_retry",
            "valid_rate": valid_rate,
            "invalid_rate": invalid_rate,
            "timeout_rate": timeout_rate,
            "confidence": confidence,
        }

    if invalid_rate >= 0.55 or uncertain_rate >= 0.60 or timeout_rate >= 0.70:
        return {
            "effect": "suppresses_retry",
            "reason": "history_suggests_retry_unhelpful",
            "valid_rate": valid_rate,
            "invalid_rate": invalid_rate,
            "uncertain_rate": uncertain_rate,
            "timeout_rate": timeout_rate,
            "confidence": confidence,
        }

    return {
        "effect": "neutral",
        "reason": "current_signal_only",
        "valid_rate": valid_rate,
        "invalid_rate": invalid_rate,
        "timeout_rate": timeout_rate,
        "confidence": confidence,
    }


def _coerce_float(value: Any) -> float:
    try:
        return max(0.0, min(1.0, float(value)))
    except (TypeError, ValueError):
        return 0.0


__all__ = ["catch_all_support", "normalize_history", "retry_support"]
