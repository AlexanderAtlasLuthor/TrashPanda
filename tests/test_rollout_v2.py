from __future__ import annotations

import pandas as pd

from app.rollout_v2.alerts import check_alerts
from app.rollout_v2.config import DEFAULT_ROLLOUT, build_rollout_config
from app.rollout_v2.feature_flags import RolloutConfig, is_v2_enabled_for_row
from app.rollout_v2.metrics import RolloutMetricsCollector
from app.rollout_v2.monitoring import compute_health
from app.rollout_v2.router import route_row
from app.rollout_v2.runner import run_rollout


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "email": "a@gmail.com",
                "domain": "gmail.com",
                "preliminary_bucket": "high_confidence",
                "validation_status": "valid",
                "action_recommendation": "send",
                "deliverability_probability": 0.92,
                "deliverability_confidence": 0.95,
                "smtp_status": "valid",
                "smtp_error_type": None,
                "catch_all_status": "unknown",
                "retry_attempted": False,
            },
            {
                "email": "b@slow.com",
                "domain": "slow.com",
                "preliminary_bucket": "review",
                "validation_status": "uncertain",
                "action_recommendation": "review",
                "deliverability_probability": 0.52,
                "deliverability_confidence": 0.45,
                "smtp_status": "uncertain",
                "smtp_error_type": "timeout",
                "catch_all_status": "likely",
                "retry_attempted": True,
            },
            {
                "email": "c@blocked.com",
                "domain": "blocked.com",
                "preliminary_bucket": "invalid",
                "validation_status": "invalid",
                "action_recommendation": "block",
                "deliverability_probability": 0.1,
                "deliverability_confidence": 0.8,
                "smtp_status": "invalid",
                "smtp_error_type": None,
                "catch_all_status": "unlikely",
                "retry_attempted": False,
            },
        ]
    )


def test_default_config_is_safe_disabled() -> None:
    assert DEFAULT_ROLLOUT.enabled is False
    assert is_v2_enabled_for_row({"domain": "gmail.com"}, DEFAULT_ROLLOUT) is False


def test_feature_flag_allow_block_and_full() -> None:
    config = RolloutConfig(
        enabled=True,
        strategy="full",
        percentage=100,
        allow_domains={"gmail.com"},
        block_domains={"blocked.com"},
    )

    assert is_v2_enabled_for_row({"domain": "gmail.com"}, config) is True
    assert is_v2_enabled_for_row({"domain": "slow.com"}, config) is False
    assert is_v2_enabled_for_row({"domain": "blocked.com"}, config) is False


def test_deterministic_canary_percentage() -> None:
    config = RolloutConfig(enabled=True, strategy="canary", percentage=50)
    row = {"email": "stable@example.com", "domain": "example.com"}

    decisions = [is_v2_enabled_for_row(row, config) for _ in range(5)]

    assert decisions == [decisions[0]] * 5


def test_routing_correctness() -> None:
    shadow = build_rollout_config(enabled=True, strategy="shadow", percentage=0)
    full = build_rollout_config(enabled=True, strategy="full", percentage=100)

    assert route_row({"domain": "gmail.com"}, shadow) == {
        "use_v1": True,
        "use_v2": False,
        "shadow_v2": True,
    }
    assert route_row({"domain": "gmail.com"}, full) == {
        "use_v1": False,
        "use_v2": True,
        "shadow_v2": False,
    }


def test_metrics_aggregation() -> None:
    collector = RolloutMetricsCollector()
    collector.record_event(
        "row_routed",
        {"route": {"use_v1": False, "use_v2": True, "shadow_v2": False}},
    )
    collector.record_event(
        "v2_result",
        {
            "validation_status": "uncertain",
            "action_recommendation": "review",
            "deliverability_probability": 0.3,
            "deliverability_confidence": 0.4,
            "smtp_status": "uncertain",
            "smtp_error_type": "timeout",
            "catch_all_status": "likely",
            "retry_attempted": True,
        },
    )

    snapshot = collector.get_metrics_snapshot()

    assert snapshot["volume"]["total_rows"] == 1
    assert snapshot["volume"]["v2_rows"] == 1
    assert snapshot["outcomes"]["validation_status"]["uncertain"] == 1
    assert snapshot["risk"]["high_risk_count"] == 1
    assert snapshot["errors"]["smtp_errors"] == 1
    assert snapshot["errors"]["retries_triggered"] == 1


def test_monitoring_and_alerts() -> None:
    collector = RolloutMetricsCollector()
    for _ in range(4):
        collector.record_event(
            "row_routed",
            {"route": {"use_v1": False, "use_v2": True, "shadow_v2": False}},
        )
    collector.record_event(
        "v2_result",
        {
            "deliverability_probability": 0.2,
            "deliverability_confidence": 0.4,
            "smtp_status": "uncertain",
            "smtp_error_type": "timeout",
            "catch_all_status": "likely",
        },
    )

    health = compute_health(collector.get_metrics_snapshot())
    alerts = check_alerts(health)

    assert health["uncertainty_rate"] == 0.25
    assert any("uncertainty_rate" in alert for alert in alerts)
    assert any("smtp_failure_rate" in alert for alert in alerts)


def test_run_rollout_shadow_and_safe_fallback() -> None:
    def exploding_v2(row):
        raise RuntimeError("v2 failed")

    config = build_rollout_config(enabled=True, strategy="full", percentage=100)
    result = run_rollout(_frame(), config, v2_fn=exploding_v2)
    processed = result["processed_dataframe"]

    assert set(processed["rollout_final_engine"]) == {"v1"}
    assert result["metrics_snapshot"]["errors"]["v2_errors"] == 3
    assert processed["rollout_v2_error"].notna().all()


def test_run_rollout_outputs_metrics_health_alerts() -> None:
    config = build_rollout_config(enabled=True, strategy="shadow", percentage=0)
    result = run_rollout(_frame(), config)

    assert result["metrics_snapshot"]["volume"]["v1_rows"] == 3
    assert result["metrics_snapshot"]["volume"]["shadow_rows"] == 3
    assert "uncertainty_rate" in result["health_report"]
    assert isinstance(result["alerts"], list)
    assert set(result["processed_dataframe"]["rollout_final_engine"]) == {"v1"}
