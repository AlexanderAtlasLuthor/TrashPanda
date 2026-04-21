"""Live health indicators derived from rollout metrics."""

from __future__ import annotations

from typing import Any


def compute_health(metrics_snapshot: dict[str, Any]) -> dict[str, float]:
    volume = metrics_snapshot.get("volume", {})
    risk = metrics_snapshot.get("risk", {})
    errors = metrics_snapshot.get("errors", {})
    v2_rows = int(volume.get("v2_rows", 0)) + int(volume.get("shadow_rows", 0))
    total_rows = int(volume.get("total_rows", 0))
    denominator = max(v2_rows, 1)
    return {
        "false_positive_proxy": _rate(
            risk.get("high_risk_count", 0) + risk.get("catch_all_count", 0),
            denominator,
        ),
        "uncertainty_rate": _rate(risk.get("low_confidence_count", 0), denominator),
        "retry_rate": _rate(errors.get("retries_triggered", 0), denominator),
        "smtp_failure_rate": _rate(errors.get("smtp_errors", 0), denominator),
        "catch_all_rate": _rate(risk.get("catch_all_count", 0), denominator),
        "v2_coverage_rate": _rate(v2_rows, max(total_rows, 1)),
    }


def _rate(count: int | float, total: int | float) -> float:
    return 0.0 if total == 0 else float(count) / float(total)


__all__ = ["compute_health"]
