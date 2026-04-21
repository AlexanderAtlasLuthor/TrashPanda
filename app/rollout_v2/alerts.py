"""Alert thresholds for rollout health metrics."""

from __future__ import annotations


ALERT_THRESHOLDS = {
    "uncertainty_rate": 0.25,
    "smtp_failure_rate": 0.15,
    "catch_all_rate": 0.30,
    "false_positive_proxy": 0.20,
}


def check_alerts(health_metrics: dict[str, float]) -> list[str]:
    alerts = []
    for metric, threshold in ALERT_THRESHOLDS.items():
        value = health_metrics.get(metric, 0.0)
        if value >= threshold:
            alerts.append(f"{metric}={value:.3f} exceeds threshold {threshold:.3f}")
    return alerts


__all__ = ["ALERT_THRESHOLDS", "check_alerts"]
