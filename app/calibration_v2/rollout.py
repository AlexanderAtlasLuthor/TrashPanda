"""Rollout readiness decisioning for Validation Engine V2."""

from __future__ import annotations

from typing import Any


def decide_rollout(
    drift_analysis: dict[str, Any],
    risk_analysis: dict[str, Any],
    report: dict[str, Any],
) -> dict[str, Any]:
    disagreement = drift_analysis.get("disagreement_rate")
    false_positive = risk_analysis.get("false_positive_estimate")
    uncertain_size = risk_analysis.get("uncertain_segment_size")
    total_rows = report.get("core_metrics", {}).get("volume", {}).get("total_rows", 0)
    uncertainty_rate = (
        None
        if uncertain_size is None or not total_rows
        else float(uncertain_size) / float(total_rows)
    )

    if _gte(disagreement, 0.35) or _gte(false_positive, 0.1) or _gte(uncertainty_rate, 0.4):
        state = "not_ready"
        strategy = "shadow"
        initial_percentage = 0
    elif _gte(disagreement, 0.2) or _gte(false_positive, 0.05) or _gte(uncertainty_rate, 0.25):
        state = "shadow_only"
        strategy = "shadow"
        initial_percentage = 0
    elif _gte(disagreement, 0.1) or _gte(false_positive, 0.02):
        state = "canary_ready"
        strategy = "canary"
        initial_percentage = 5
    else:
        state = "production_ready"
        strategy = "phased_rollout"
        initial_percentage = 25

    return {
        "readiness_state": state,
        "strategy": strategy,
        "initial_percentage": initial_percentage,
        "monitoring_required": True,
        "rollback_conditions": [
            "bounce_rate_exceeds_baseline",
            "complaint_rate_increases",
            "provider_specific_failures_spike",
            "manual_review_disagreement_exceeds_guardrail",
        ],
        "drivers": {
            "disagreement_rate": disagreement,
            "false_positive_estimate": false_positive,
            "uncertainty_rate": uncertainty_rate,
        },
    }


def _gte(value: float | None, threshold: float) -> bool:
    return value is not None and value >= threshold


__all__ = ["decide_rollout"]
