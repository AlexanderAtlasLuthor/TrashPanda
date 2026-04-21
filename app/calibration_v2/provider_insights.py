"""Provider and domain risk insights for calibration playbooks."""

from __future__ import annotations

from typing import Any


def build_provider_insights(report: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    provider_groups = report.get("provider_analysis", {}).get(
        "by_provider_reputation", []
    )
    domain_groups = report.get("provider_analysis", {}).get("by_corrected_domain", [])
    combined = list(provider_groups) + list(domain_groups)

    risky = []
    high_confidence = []
    special = []
    for group in combined:
        smtp_uncertain = _dist_pct(group, "smtp_status_distribution", "uncertain")
        catch_all_likely = _dist_pct(group, "catch_all_status_distribution", "likely")
        catch_all_confirmed = _dist_pct(
            group, "catch_all_status_distribution", "confirmed"
        )
        bucket_changed = group.get("bucket_changed_pct")
        avg_conf = group.get("avg_deliverability_confidence")
        avg_prob = group.get("avg_deliverability_probability")

        if (
            (smtp_uncertain or 0.0) >= 0.2
            or (catch_all_likely or 0.0) + (catch_all_confirmed or 0.0) >= 0.2
            or (avg_conf is not None and avg_conf < 0.6)
            or (bucket_changed is not None and bucket_changed >= 0.3)
        ):
            risky.append(_insight(group, "elevated_validation_risk"))

        if (
            avg_prob is not None
            and avg_prob >= 0.8
            and avg_conf is not None
            and avg_conf >= 0.8
            and (smtp_uncertain or 0.0) == 0.0
        ):
            high_confidence.append(_insight(group, "stable_high_confidence"))

        if (catch_all_likely or 0.0) + (catch_all_confirmed or 0.0) >= 0.2:
            special.append(_insight(group, "catch_all_handling"))

    return {
        "risky_providers": risky[:20],
        "high_confidence_providers": high_confidence[:20],
        "needs_special_handling": special[:20],
    }


def _dist_pct(group: dict[str, Any], dist_name: str, key: str) -> float | None:
    value = group.get(dist_name, {}).get(key)
    if not isinstance(value, dict):
        return None
    return value.get("pct")


def _insight(group: dict[str, Any], reason: str) -> dict[str, Any]:
    return {
        "group": group.get("group"),
        "row_count": group.get("row_count"),
        "avg_deliverability_probability": group.get(
            "avg_deliverability_probability"
        ),
        "avg_deliverability_confidence": group.get(
            "avg_deliverability_confidence"
        ),
        "bucket_changed_pct": group.get("bucket_changed_pct"),
        "reason": reason,
    }


__all__ = ["build_provider_insights"]
