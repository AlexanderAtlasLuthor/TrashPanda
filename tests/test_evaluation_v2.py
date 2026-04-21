from __future__ import annotations

import json

import pandas as pd
import pytest

from app.evaluation_v2.distributions import probability_histogram
from app.evaluation_v2.loader import (
    describe_available_columns,
    load_evaluation_frame,
)
from app.evaluation_v2.metrics import compute_core_metrics
from app.evaluation_v2.provider_analysis import build_provider_analysis
from app.evaluation_v2.reporting import build_evaluation_report
from app.evaluation_v2.runner import run_evaluation
from app.evaluation_v2.samples import extract_all_samples


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "email": "a@gmail.com",
                "domain": "gmail.com",
                "corrected_domain": "gmail.com",
                "source_file": "one.csv",
                "score": 90,
                "preliminary_bucket": "high_confidence",
                "hard_fail": False,
                "score_v2": 0.9,
                "confidence_v2": 0.95,
                "bucket_v2": "high_confidence",
                "score_delta": 0.02,
                "abs_score_delta": 0.02,
                "bucket_changed": False,
                "v2_higher_bucket": False,
                "v2_lower_bucket": False,
                "hard_decision_changed": False,
                "v2_more_strict": False,
                "v2_more_permissive": False,
                "low_confidence_v2": False,
                "deliverability_probability": 0.92,
                "deliverability_confidence": 0.4,
                "validation_status": "valid",
                "action_recommendation": "send",
                "smtp_status": "valid",
                "smtp_code": 250,
                "smtp_latency": 10.0,
                "smtp_error_type": pd.NA,
                "catch_all_status": "likely",
                "catch_all_confidence": 0.7,
                "retry_attempted": False,
                "retry_outcome": "none",
                "provider_reputation": "tier_1",
            },
            {
                "email": "b@example.com",
                "domain": "example.com",
                "corrected_domain": "example.com",
                "source_file": "one.csv",
                "score": 88,
                "preliminary_bucket": "high_confidence",
                "hard_fail": False,
                "score_v2": 0.25,
                "confidence_v2": 0.5,
                "bucket_v2": "review",
                "score_delta": -0.6,
                "abs_score_delta": 0.6,
                "bucket_changed": True,
                "v2_higher_bucket": False,
                "v2_lower_bucket": True,
                "hard_decision_changed": False,
                "v2_more_strict": True,
                "v2_more_permissive": False,
                "low_confidence_v2": True,
                "deliverability_probability": 0.18,
                "deliverability_confidence": 0.8,
                "validation_status": "invalid",
                "action_recommendation": "block",
                "smtp_status": "invalid",
                "smtp_code": 550,
                "smtp_latency": 20.0,
                "smtp_error_type": pd.NA,
                "catch_all_status": "unlikely",
                "catch_all_confidence": 0.8,
                "retry_attempted": False,
                "retry_outcome": "none",
                "provider_reputation": "unknown",
            },
            {
                "email": "c@slow.com",
                "domain": "slow.com",
                "corrected_domain": "slow.com",
                "source_file": "two.csv",
                "score": 50,
                "preliminary_bucket": "review",
                "hard_fail": False,
                "score_v2": 0.55,
                "confidence_v2": 0.7,
                "bucket_v2": "review",
                "score_delta": 0.1,
                "abs_score_delta": 0.1,
                "bucket_changed": False,
                "v2_higher_bucket": False,
                "v2_lower_bucket": False,
                "hard_decision_changed": False,
                "v2_more_strict": False,
                "v2_more_permissive": True,
                "low_confidence_v2": False,
                "deliverability_probability": 0.52,
                "deliverability_confidence": 0.65,
                "validation_status": "uncertain",
                "action_recommendation": "review",
                "smtp_status": "uncertain",
                "smtp_code": 451,
                "smtp_latency": 30.0,
                "smtp_error_type": "timeout",
                "catch_all_status": "unknown",
                "catch_all_confidence": 0.1,
                "retry_attempted": True,
                "retry_outcome": "fail",
                "provider_reputation": "unknown",
            },
            {
                "email": "d@corp.com",
                "domain": "corp.com",
                "corrected_domain": "corp.com",
                "source_file": "two.csv",
                "score": 70,
                "preliminary_bucket": "review",
                "hard_fail": False,
                "score_v2": 0.82,
                "confidence_v2": 0.88,
                "bucket_v2": "high_confidence",
                "score_delta": 0.3,
                "abs_score_delta": 0.3,
                "bucket_changed": True,
                "v2_higher_bucket": True,
                "v2_lower_bucket": False,
                "hard_decision_changed": True,
                "v2_more_strict": False,
                "v2_more_permissive": True,
                "low_confidence_v2": False,
                "deliverability_probability": 0.76,
                "deliverability_confidence": 0.91,
                "validation_status": "likely_valid",
                "action_recommendation": "send_with_monitoring",
                "smtp_status": "valid",
                "smtp_code": 250,
                "smtp_latency": 15.0,
                "smtp_error_type": pd.NA,
                "catch_all_status": "confirmed",
                "catch_all_confidence": 0.95,
                "retry_attempted": False,
                "retry_outcome": "none",
                "provider_reputation": "enterprise",
            },
        ]
    )


def test_loader_handles_missing_optional_columns() -> None:
    df = load_evaluation_frame(pd.DataFrame({"email": ["a@example.com"]}))
    description = describe_available_columns(df)

    assert description["identity_present"] == ["email"]
    assert "score_v2" in description["optional_missing"]


def test_loader_requires_identity_column() -> None:
    with pytest.raises(ValueError):
        load_evaluation_frame(pd.DataFrame({"score": [1]}))


def test_core_metrics_correct() -> None:
    metrics = compute_core_metrics(_frame())

    assert metrics["volume"]["total_rows"] == 4
    assert metrics["volume"]["unique_domains"] == 4
    assert metrics["comparison"]["bucket_change_count"] == 2
    assert metrics["comparison"]["v2_higher_bucket_count"] == 1
    assert metrics["comparison"]["v2_lower_bucket_count"] == 1
    assert metrics["probability_confidence"]["high_probability_rate"] == 0.25
    assert metrics["smtp"]["timeout_rate"] == 0.25
    assert metrics["catch_all"]["avg_catch_all_confidence"] == pytest.approx(0.6375)


def test_distributions_correct() -> None:
    assert probability_histogram(_frame()) == {
        "<0.2": 1,
        "0.2-0.4": 0,
        "0.4-0.6": 1,
        "0.6-0.85": 1,
        ">=0.85": 1,
    }


def test_provider_grouping_correct() -> None:
    analysis = build_provider_analysis(_frame(), top_n=5)

    provider_groups = analysis["by_provider_reputation"]
    unknown = next(group for group in provider_groups if group["group"] == "unknown")
    assert unknown["row_count"] == 2
    assert unknown["avg_deliverability_probability"] == pytest.approx(0.35)
    assert unknown["v2_lower_bucket_pct"] == 0.5


def test_sample_extraction_works() -> None:
    samples = extract_all_samples(_frame(), limit=2)

    assert len(samples["high_probability_risky"]) == 1
    assert len(samples["low_probability_v1_high_confidence"]) == 1
    assert len(samples["timeout_retry_cases"]) == 1
    assert len(samples["catch_all_cases"]) == 2
    assert len(samples["biggest_disagreements"]) == 2


def test_report_json_serializable() -> None:
    report = build_evaluation_report(_frame(), sample_limit=2)
    payload = json.dumps(report, sort_keys=True)

    assert "core_metrics" in payload
    assert report["sample_summary"]["catch_all_cases"] == 2


def test_runner_writes_expected_outputs(tmp_path) -> None:
    input_path = tmp_path / "evaluation_input.csv"
    output_dir = tmp_path / "evaluation"
    _frame().to_csv(input_path, index=False)

    summary = run_evaluation(input_path, output_dir, sample_limit=2)

    assert (output_dir / "evaluation_report.json").exists()
    assert (output_dir / "sample_high_probability_risky.csv").exists()
    assert (output_dir / "sample_low_probability_v1_high_confidence.csv").exists()
    assert (output_dir / "sample_timeout_retry_cases.csv").exists()
    assert (output_dir / "sample_catch_all_cases.csv").exists()
    assert (output_dir / "sample_biggest_disagreements.csv").exists()
    assert summary["rows"] == 4
