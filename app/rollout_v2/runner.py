"""Production-rollout simulation runner.

The runner is intentionally dependency-injected: callers can provide V1 and
V2 functions, while tests use deterministic fakes. No network behavior is
introduced here.
"""

from __future__ import annotations

from typing import Any, Callable

import pandas as pd

from .alerts import check_alerts
from .config import DEFAULT_ROLLOUT
from .feature_flags import RolloutConfig
from .metrics import RolloutMetricsCollector
from .monitoring import compute_health
from .router import route_row


V1Fn = Callable[[dict[str, Any]], dict[str, Any]]
V2Fn = Callable[[dict[str, Any]], dict[str, Any]]


def run_rollout(
    df: pd.DataFrame,
    config: RolloutConfig = DEFAULT_ROLLOUT,
    *,
    v1_fn: V1Fn | None = None,
    v2_fn: V2Fn | None = None,
) -> dict[str, Any]:
    collector = RolloutMetricsCollector()
    processed_rows: list[dict[str, Any]] = []

    for _, row in df.iterrows():
        row_dict = row.to_dict()
        route = route_row(row_dict, config)
        collector.record_event("row_routed", {"route": route})

        v1_result = _safe_v1(row_dict, v1_fn)
        v2_result: dict[str, Any] | None = None
        v2_error = None
        if route["use_v2"] or route["shadow_v2"]:
            try:
                v2_result = _safe_v2(row_dict, v2_fn)
                collector.record_event("v2_result", v2_result)
            except Exception as exc:
                v2_error = str(exc)
                collector.record_event("v2_error", {"error": v2_error})
                route = {**route, "use_v1": True, "use_v2": False}

        final_engine = "v2" if route["use_v2"] and v2_error is None else "v1"
        final_result = v2_result if final_engine == "v2" else v1_result
        processed_rows.append(
            {
                **row_dict,
                "rollout_use_v1": route["use_v1"],
                "rollout_use_v2": route["use_v2"],
                "rollout_shadow_v2": route["shadow_v2"],
                "rollout_final_engine": final_engine,
                "rollout_v2_error": v2_error,
                "rollout_decision": final_result.get("decision"),
                "v2_validation_status": (
                    None if v2_result is None else v2_result.get("validation_status")
                ),
                "v2_action_recommendation": (
                    None if v2_result is None else v2_result.get("action_recommendation")
                ),
            }
        )

    processed = pd.DataFrame(processed_rows)
    snapshot = collector.get_metrics_snapshot()
    health = compute_health(snapshot)
    alerts = check_alerts(health)
    return {
        "processed_dataframe": processed,
        "metrics_snapshot": snapshot,
        "health_report": health,
        "alerts": alerts,
    }


def _safe_v1(row: dict[str, Any], v1_fn: V1Fn | None) -> dict[str, Any]:
    if v1_fn is None:
        return {
            "decision": row.get("preliminary_bucket", "v1"),
        }
    try:
        return v1_fn(row)
    except Exception as exc:
        return {
            "decision": "v1_error",
            "error": str(exc),
        }


def _safe_v2(row: dict[str, Any], v2_fn: V2Fn | None) -> dict[str, Any]:
    if v2_fn is None:
        return {
            "validation_status": row.get("validation_status"),
            "action_recommendation": row.get("action_recommendation"),
            "deliverability_probability": row.get("deliverability_probability"),
            "deliverability_confidence": row.get("deliverability_confidence"),
            "smtp_status": row.get("smtp_status"),
            "smtp_error_type": row.get("smtp_error_type"),
            "catch_all_status": row.get("catch_all_status"),
            "retry_attempted": row.get("retry_attempted"),
            "decision": row.get("action_recommendation", "v2"),
        }
    return v2_fn(row)


__all__ = ["run_rollout"]
