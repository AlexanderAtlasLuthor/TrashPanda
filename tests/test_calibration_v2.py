from __future__ import annotations

import json

import pandas as pd

from app.calibration_v2.drift import analyze_drift
from app.calibration_v2.provider_insights import build_provider_insights
from app.calibration_v2.recommendations import generate_recommendations
from app.calibration_v2.reporting import build_calibration_report
from app.calibration_v2.risk_analysis import analyze_risk
from app.calibration_v2.rollout import decide_rollout
from app.calibration_v2.runner import run_calibration
from app.calibration_v2.thresholds import analyze_thresholds


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "email": "a@gmail.com",
                "provider_reputation": "tier_1",
                "corrected_domain": "gmail.com",
                "preliminary_bucket": "high_confidence",
                "bucket_v2": "high_confidence",
                "action_recommendation": "send",
                "validation_status": "valid",
                "deliverability_probability": 0.86,
                "deliverability_confidence": 0.92,
                "smtp_status": "valid",
                "catch_all_status": "unknown",
                "score_delta": 0.02,
                "bucket_changed": False,
                "v2_more_strict": False,
            },
            {
                "email": "b@unknown.com",
                "provider_reputation": "unknown",
                "corrected_domain": "unknown.com",
                "preliminary_bucket": "high_confidence",
                "bucket_v2": "review",
                "action_recommendation": "block",
                "validation_status": "invalid",
                "deliverability_probability": 0.18,
                "deliverability_confidence": 0.8,
                "smtp_status": "invalid",
                "catch_all_status": "unlikely",
                "score_delta": -0.6,
                "bucket_changed": True,
                "v2_more_strict": True,
            },
            {
                "email": "c@unknown.com",
                "provider_reputation": "unknown",
                "corrected_domain": "unknown.com",
                "preliminary_bucket": "review",
                "bucket_v2": "review",
                "action_recommendation": "send_with_monitoring",
                "validation_status": "likely_valid",
                "deliverability_probability": 0.83,
                "deliverability_confidence": 0.55,
                "smtp_status": "uncertain",
                "catch_all_status": "likely",
                "score_delta": 0.1,
                "bucket_changed": False,
                "v2_more_strict": True,
            },
            {
                "email": "d@corp.com",
                "provider_reputation": "enterprise",
                "corrected_domain": "corp.com",
                "preliminary_bucket": "review",
                "bucket_v2": "high_confidence",
                "action_recommendation": "send_with_monitoring",
                "validation_status": "likely_valid",
                "deliverability_probability": 0.82,
                "deliverability_confidence": 0.88,
                "smtp_status": "valid",
                "catch_all_status": "confirmed",
                "score_delta": 0.3,
                "bucket_changed": True,
                "v2_more_strict": False,
            },
        ]
    )


def _evaluation_report() -> dict:
    return {
        "core_metrics": {
            "volume": {"total_rows": 4},
            "comparison": {
                "bucket_change_pct": 0.5,
                "hard_decision_changed_pct": 0.25,
                "v2_more_strict_count": 2,
                "v2_more_permissive_count": 1,
            },
            "probability_confidence": {
                "high_probability_rate": 0.25,
                "low_probability_rate": 0.25,
            },
            "validation_distributions": {
                "validation_status": {
                    "valid": {"count": 1, "pct": 0.25},
                    "likely_valid": {"count": 2, "pct": 0.5},
                    "invalid": {"count": 1, "pct": 0.25},
                }
            },
        },
        "provider_analysis": {
            "by_provider_reputation": [
                {
                    "group": "unknown",
                    "row_count": 2,
                    "avg_deliverability_probability": 0.505,
                    "avg_deliverability_confidence": 0.675,
                    "bucket_changed_pct": 0.5,
                    "smtp_status_distribution": {
                        "uncertain": {"count": 1, "pct": 0.5}
                    },
                    "catch_all_status_distribution": {
                        "likely": {"count": 1, "pct": 0.5}
                    },
                },
                {
                    "group": "tier_1",
                    "row_count": 1,
                    "avg_deliverability_probability": 0.86,
                    "avg_deliverability_confidence": 0.92,
                    "bucket_changed_pct": 0.0,
                    "smtp_status_distribution": {"valid": {"count": 1, "pct": 1.0}},
                    "catch_all_status_distribution": {
                        "unknown": {"count": 1, "pct": 1.0}
                    },
                },
            ],
            "by_corrected_domain": [],
        },
    }


def test_threshold_sensitivity_logic() -> None:
    analysis = analyze_thresholds(_evaluation_report(), _frame())

    send = analysis["sensitivity_analysis"]["send"]
    assert send["near_boundary_count"] == 3
    assert send["would_flip_if_lowered_count"] == 2
    assert analysis["recommended_adjustments"][0]["threshold"] == "send"


def test_drift_detection() -> None:
    drift = analyze_drift(_evaluation_report(), _frame())

    assert drift["disagreement_rate"] == 0.5
    assert drift["strong_disagreement_count"] == 1
    assert drift["score_delta_extremes"]["min"] == -0.6
    assert drift["systematic_bias"][0]["segment"] == "unknown"


def test_provider_grouping_insights() -> None:
    insights = build_provider_insights(_evaluation_report())

    assert insights["risky_providers"][0]["group"] == "unknown"
    assert insights["high_confidence_providers"][0]["group"] == "tier_1"
    assert insights["needs_special_handling"][0]["group"] == "unknown"


def test_recommendation_generation() -> None:
    threshold = analyze_thresholds(_evaluation_report(), _frame())
    drift = analyze_drift(_evaluation_report(), _frame())
    providers = build_provider_insights(_evaluation_report())
    risk = analyze_risk(_evaluation_report(), _frame())

    recommendations = generate_recommendations(threshold, drift, providers, risk)

    assert recommendations
    assert any("threshold" in rec["recommendation"] for rec in recommendations)
    assert any(rec["risk_level"] == "high" for rec in recommendations)


def test_rollout_decision() -> None:
    drift = analyze_drift(_evaluation_report(), _frame())
    risk = analyze_risk(_evaluation_report(), _frame())
    rollout = decide_rollout(drift, risk, _evaluation_report())

    assert rollout["readiness_state"] == "not_ready"
    assert rollout["strategy"] == "shadow"
    assert rollout["monitoring_required"] is True


def test_report_json_serializable() -> None:
    report = build_calibration_report(_evaluation_report(), _frame())
    payload = json.dumps(report, sort_keys=True)

    assert "threshold_analysis" in payload
    assert report["summary"]["readiness_state"] == "not_ready"


def test_runner_writes_expected_outputs(tmp_path) -> None:
    report_path = tmp_path / "evaluation_report.json"
    output_dir = tmp_path / "calibration"
    report_path.write_text(json.dumps(_evaluation_report()), encoding="utf-8")

    summary = run_calibration(report_path, output_dir)

    assert (output_dir / "calibration_report.json").exists()
    assert (output_dir / "summary.txt").exists()
    assert summary["readiness_state"] == "not_ready"
