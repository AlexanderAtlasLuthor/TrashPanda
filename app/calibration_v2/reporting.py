"""Calibration report builder and writer."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from .drift import analyze_drift
from .provider_insights import build_provider_insights
from .recommendations import generate_recommendations
from .risk_analysis import analyze_risk
from .rollout import decide_rollout
from .thresholds import analyze_thresholds


def build_calibration_report(
    evaluation_report: dict[str, Any],
    df: pd.DataFrame | None = None,
) -> dict[str, Any]:
    threshold = analyze_thresholds(evaluation_report, df)
    drift = analyze_drift(evaluation_report, df)
    providers = build_provider_insights(evaluation_report)
    risk = analyze_risk(evaluation_report, df)
    recommendations = generate_recommendations(threshold, drift, providers, risk)
    rollout = decide_rollout(drift, risk, evaluation_report)
    return {
        "summary": {
            "total_rows": evaluation_report.get("core_metrics", {})
            .get("volume", {})
            .get("total_rows"),
            "readiness_state": rollout["readiness_state"],
            "recommendation_count": len(recommendations),
        },
        "threshold_analysis": threshold,
        "drift_analysis": drift,
        "provider_insights": providers,
        "risk_analysis": risk,
        "recommendations": recommendations,
        "rollout_strategy": rollout,
    }


def write_calibration_report(report: dict[str, Any], path: str | Path) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(_json_safe(report), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output_path


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if hasattr(value, "item"):
        return value.item()
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


__all__ = ["build_calibration_report", "write_calibration_report"]
