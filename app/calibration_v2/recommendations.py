"""Human-readable calibration recommendations."""

from __future__ import annotations

from typing import Any


def generate_recommendations(
    threshold_analysis: dict[str, Any],
    drift_analysis: dict[str, Any],
    provider_insights: dict[str, Any],
    risk_analysis: dict[str, Any],
) -> list[dict[str, Any]]:
    recommendations: list[dict[str, Any]] = []

    for adjustment in threshold_analysis.get("recommended_adjustments", []):
        recommendations.append(
            {
                "recommendation": (
                    f"Evaluate lowering {adjustment['threshold']} threshold "
                    f"from {adjustment['current']} to {adjustment['suggested']}."
                ),
                "impact_estimate": {
                    "affected_rows": adjustment.get("affected_rows"),
                },
                "risk_level": adjustment.get("risk", "medium"),
                "affected_segments": ["near_threshold"],
                "reason": adjustment.get("reason"),
            }
        )

    if risk_analysis.get("false_positive_estimate", 0) and risk_analysis[
        "false_positive_estimate"
    ] >= 0.05:
        recommendations.append(
            {
                "recommendation": (
                    "Do not auto-send when SMTP is uncertain or confidence is below 0.7."
                ),
                "impact_estimate": {
                    "false_positive_estimate": risk_analysis[
                        "false_positive_estimate"
                    ]
                },
                "risk_level": "high",
                "affected_segments": [
                    s["segment"]
                    for s in risk_analysis.get("highest_risk_segments", [])[:3]
                ],
                "reason": "false_positive_risk_above_guardrail",
            }
        )

    special = provider_insights.get("needs_special_handling", [])
    if special:
        recommendations.append(
            {
                "recommendation": (
                    "Add stricter handling for providers or domains with elevated "
                    "catch-all likelihood."
                ),
                "impact_estimate": {"segment_count": len(special)},
                "risk_level": "medium",
                "affected_segments": [str(s["group"]) for s in special[:5]],
                "reason": "catch_all_risk",
            }
        )

    if drift_analysis.get("strong_disagreement_count", 0):
        recommendations.append(
            {
                "recommendation": (
                    "Prioritize manual review for V1 high-confidence rows that "
                    "Validation V2 would block."
                ),
                "impact_estimate": {
                    "rows": drift_analysis.get("strong_disagreement_count")
                },
                "risk_level": "high",
                "affected_segments": ["v1_v2_strong_disagreement"],
                "reason": "potential_false_negative_risk",
            }
        )

    if not recommendations:
        recommendations.append(
            {
                "recommendation": (
                    "No threshold change recommended yet; continue shadow-mode "
                    "measurement and outcome calibration."
                ),
                "impact_estimate": {"affected_rows": 0},
                "risk_level": "low",
                "affected_segments": [],
                "reason": "insufficient_pressure_for_change",
            }
        )

    return recommendations


__all__ = ["generate_recommendations"]
