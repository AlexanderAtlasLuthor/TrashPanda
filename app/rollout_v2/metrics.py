"""Lightweight in-memory rollout metrics."""

from __future__ import annotations

from copy import deepcopy
from typing import Any


class RolloutMetricsCollector:
    def __init__(self) -> None:
        self._metrics = {
            "volume": {
                "total_rows": 0,
                "v1_rows": 0,
                "v2_rows": 0,
                "shadow_rows": 0,
            },
            "outcomes": {
                "validation_status": {},
                "action_recommendation": {},
            },
            "risk": {
                "high_risk_count": 0,
                "low_confidence_count": 0,
                "smtp_uncertain_count": 0,
                "catch_all_count": 0,
            },
            "errors": {
                "smtp_errors": 0,
                "retries_triggered": 0,
                "v2_errors": 0,
                "metrics_errors": 0,
            },
        }

    def record_event(self, event_type: str, payload: dict[str, Any]) -> None:
        try:
            if event_type == "row_routed":
                self._record_route(payload)
            elif event_type == "v2_result":
                self._record_v2_result(payload)
            elif event_type == "v2_error":
                self._metrics["errors"]["v2_errors"] += 1
        except Exception:
            self._metrics["errors"]["metrics_errors"] += 1

    def get_metrics_snapshot(self) -> dict[str, Any]:
        return deepcopy(self._metrics)

    def _record_route(self, payload: dict[str, Any]) -> None:
        self._metrics["volume"]["total_rows"] += 1
        route = payload.get("route", {})
        if route.get("use_v1"):
            self._metrics["volume"]["v1_rows"] += 1
        if route.get("use_v2"):
            self._metrics["volume"]["v2_rows"] += 1
        if route.get("shadow_v2"):
            self._metrics["volume"]["shadow_rows"] += 1

    def _record_v2_result(self, payload: dict[str, Any]) -> None:
        status = payload.get("validation_status")
        action = payload.get("action_recommendation")
        if status:
            _increment(self._metrics["outcomes"]["validation_status"], str(status))
        if action:
            _increment(self._metrics["outcomes"]["action_recommendation"], str(action))
        probability = _float(payload.get("deliverability_probability"))
        confidence = _float(payload.get("deliverability_confidence"))
        if probability is not None and probability < 0.4:
            self._metrics["risk"]["high_risk_count"] += 1
        if confidence is not None and confidence < 0.7:
            self._metrics["risk"]["low_confidence_count"] += 1
        if payload.get("smtp_status") == "uncertain":
            self._metrics["risk"]["smtp_uncertain_count"] += 1
        if payload.get("catch_all_status") in {"confirmed", "likely"}:
            self._metrics["risk"]["catch_all_count"] += 1
        if payload.get("smtp_error_type"):
            self._metrics["errors"]["smtp_errors"] += 1
        if bool(payload.get("retry_attempted")):
            self._metrics["errors"]["retries_triggered"] += 1


def _increment(mapping: dict[str, int], key: str) -> None:
    mapping[key] = mapping.get(key, 0) + 1


def _float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


_DEFAULT_COLLECTOR = RolloutMetricsCollector()


def record_event(event_type: str, payload: dict[str, Any]) -> None:
    _DEFAULT_COLLECTOR.record_event(event_type, payload)


def get_metrics_snapshot() -> dict[str, Any]:
    return _DEFAULT_COLLECTOR.get_metrics_snapshot()


__all__ = [
    "RolloutMetricsCollector",
    "record_event",
    "get_metrics_snapshot",
]
