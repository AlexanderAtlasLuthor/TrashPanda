"""Conservative risk estimates for calibration decisions."""

from __future__ import annotations

from typing import Any

import pandas as pd


def analyze_risk(report: dict[str, Any], df: pd.DataFrame | None = None) -> dict[str, Any]:
    core = report.get("core_metrics", {})
    probability = core.get("probability_confidence", {})
    validation = core.get("validation_distributions", {})
    uncertain_segment_size = _dist_count(validation, "validation_status", "uncertain")

    result: dict[str, Any] = {
        "false_positive_estimate": None,
        "false_negative_estimate": None,
        "uncertain_segment_size": uncertain_segment_size,
        "highest_risk_segments": [],
    }
    if df is None:
        return result

    total = len(df)
    if total == 0:
        result["false_positive_estimate"] = 0.0
        result["false_negative_estimate"] = 0.0
        return result

    prob = _numeric(df, "deliverability_probability")
    conf = _numeric(df, "deliverability_confidence")
    action = df.get("action_recommendation", pd.Series("", index=df.index))
    catch_all = df.get("catch_all_status", pd.Series("", index=df.index))
    smtp = df.get("smtp_status", pd.Series("", index=df.index))

    false_positive = (
        action.isin({"send", "send_with_monitoring"})
        & (
            prob.lt(0.6)
            | conf.lt(0.6)
            | catch_all.isin({"confirmed", "likely"})
            | smtp.eq("uncertain")
        )
    )
    false_negative = action.isin({"block", "verify"}) & prob.ge(0.6) & conf.ge(0.7)
    uncertainty = conf.lt(0.6) | action.eq("review") | smtp.eq("uncertain")

    result["false_positive_estimate"] = float(false_positive.sum()) / float(total)
    result["false_negative_estimate"] = float(false_negative.sum()) / float(total)
    result["uncertain_segment_size"] = int(uncertainty.sum())
    result["highest_risk_segments"] = _highest_risk_segments(df, false_positive, uncertainty)
    result["context"] = {
        "high_probability_rate": probability.get("high_probability_rate"),
        "low_probability_rate": probability.get("low_probability_rate"),
    }
    return result


def _highest_risk_segments(
    df: pd.DataFrame,
    false_positive: pd.Series,
    uncertainty: pd.Series,
) -> list[dict[str, Any]]:
    if "provider_reputation" not in df.columns:
        return []
    rows = []
    work = df.assign(_fp=false_positive, _uncertain=uncertainty)
    for provider, group in work.groupby("provider_reputation"):
        row_count = len(group)
        if row_count == 0:
            continue
        risk_score = (
            float(group["_fp"].sum()) / row_count
            + float(group["_uncertain"].sum()) / row_count
        )
        rows.append(
            {
                "segment": str(provider),
                "row_count": int(row_count),
                "risk_score": risk_score,
                "false_positive_count": int(group["_fp"].sum()),
                "uncertain_count": int(group["_uncertain"].sum()),
            }
        )
    rows.sort(key=lambda item: (item["risk_score"], item["row_count"]), reverse=True)
    return rows[:10]


def _dist_count(report: dict[str, Any], dist_name: str, key: str) -> int | None:
    value = report.get(dist_name, {}).get(key)
    if not isinstance(value, dict):
        return None
    return value.get("count")


def _numeric(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series([pd.NA] * len(df), index=df.index, dtype="Float64")
    return pd.to_numeric(df[column], errors="coerce")


__all__ = ["analyze_risk"]
