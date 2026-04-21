"""Threshold sensitivity analysis for calibration decisions."""

from __future__ import annotations

from typing import Any

import pandas as pd


DEFAULT_THRESHOLDS = {
    "send": 0.85,
    "send_with_monitoring": 0.60,
    "review": 0.40,
    "verify": 0.20,
}


def analyze_thresholds(
    report: dict[str, Any],
    df: pd.DataFrame | None = None,
    *,
    thresholds: dict[str, float] | None = None,
    margin: float = 0.05,
) -> dict[str, Any]:
    active = thresholds or DEFAULT_THRESHOLDS
    if df is None or "deliverability_probability" not in df.columns:
        return {
            "sensitivity_analysis": {},
            "threshold_pressure_points": [],
            "recommended_adjustments": [],
        }

    probs = pd.to_numeric(df["deliverability_probability"], errors="coerce").dropna()
    total = int(len(probs))
    sensitivity: dict[str, Any] = {}
    pressure_points = []
    recommendations = []

    for name, threshold in active.items():
        near = probs[(probs >= threshold - margin) & (probs <= threshold + margin)]
        lower_flip = probs[(probs >= threshold - margin) & (probs < threshold)]
        higher_flip = probs[(probs >= threshold) & (probs <= threshold + margin)]
        entry = {
            "threshold": threshold,
            "near_boundary_count": int(len(near)),
            "near_boundary_pct": _pct(len(near), total),
            "would_flip_if_lowered_count": int(len(lower_flip)),
            "would_flip_if_raised_count": int(len(higher_flip)),
        }
        sensitivity[name] = entry
        if entry["near_boundary_pct"] >= 0.05:
            pressure_points.append(
                {
                    "threshold": name,
                    "pressure_pct": entry["near_boundary_pct"],
                    "reason": "many_rows_near_boundary",
                }
            )
        if name == "send" and entry["would_flip_if_lowered_count"] > 0:
            recommendations.append(
                {
                    "threshold": "send",
                    "current": threshold,
                    "suggested": round(max(0.0, threshold - 0.03), 2),
                    "affected_rows": entry["would_flip_if_lowered_count"],
                    "risk": "medium",
                    "reason": "evaluate slightly lower send threshold with canary only",
                }
            )

    return {
        "sensitivity_analysis": sensitivity,
        "threshold_pressure_points": pressure_points,
        "recommended_adjustments": recommendations,
    }


def _pct(count: int, total: int) -> float:
    return 0.0 if total == 0 else float(count) / float(total)


__all__ = ["DEFAULT_THRESHOLDS", "analyze_thresholds"]
