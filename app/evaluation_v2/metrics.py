"""Core JSON-serializable metrics for evaluation reports."""

from __future__ import annotations

from typing import Any

import pandas as pd


def compute_core_metrics(df: pd.DataFrame) -> dict[str, Any]:
    return {
        "volume": volume_metrics(df),
        "validation_distributions": {
            "validation_status": value_distribution(df, "validation_status"),
            "action_recommendation": value_distribution(df, "action_recommendation"),
        },
        "scoring_distributions": {
            "preliminary_bucket": value_distribution(df, "preliminary_bucket"),
            "bucket_v2": value_distribution(df, "bucket_v2"),
        },
        "comparison": comparison_metrics(df),
        "probability_confidence": probability_confidence_metrics(df),
        "smtp": smtp_metrics(df),
        "catch_all": catch_all_metrics(df),
    }


def volume_metrics(df: pd.DataFrame) -> dict[str, int | None]:
    return {
        "total_rows": int(len(df)),
        "unique_domains": _nunique_first_available(
            df, ("corrected_domain", "domain")
        ),
        "unique_emails": _nunique(df, "email"),
    }


def value_distribution(df: pd.DataFrame, column: str) -> dict[str, dict[str, float | int]]:
    if column not in df.columns:
        return {}
    total = len(df)
    counts = df[column].fillna("missing").value_counts(dropna=False).sort_index()
    return {
        str(key): {
            "count": int(count),
            "pct": _pct(count, total),
        }
        for key, count in counts.items()
    }


def comparison_metrics(df: pd.DataFrame) -> dict[str, int | float | None]:
    return {
        "bucket_change_count": _bool_count(df, "bucket_changed"),
        "bucket_change_pct": _bool_pct(df, "bucket_changed"),
        "v2_higher_bucket_count": _bool_count(df, "v2_higher_bucket"),
        "v2_higher_bucket_pct": _bool_pct(df, "v2_higher_bucket"),
        "v2_lower_bucket_count": _bool_count(df, "v2_lower_bucket"),
        "v2_lower_bucket_pct": _bool_pct(df, "v2_lower_bucket"),
        "hard_decision_changed_count": _bool_count(df, "hard_decision_changed"),
        "hard_decision_changed_pct": _bool_pct(df, "hard_decision_changed"),
        "v2_more_strict_count": _bool_count(df, "v2_more_strict"),
        "v2_more_permissive_count": _bool_count(df, "v2_more_permissive"),
    }


def probability_confidence_metrics(df: pd.DataFrame) -> dict[str, float | None]:
    probability = _numeric(df, "deliverability_probability")
    deliverability_confidence = _numeric(df, "deliverability_confidence")
    confidence_v2 = _numeric(df, "confidence_v2")
    return {
        "avg_deliverability_probability": _mean(probability),
        "median_deliverability_probability": _median(probability),
        "avg_deliverability_confidence": _mean(deliverability_confidence),
        "median_deliverability_confidence": _median(deliverability_confidence),
        "avg_confidence_v2": _mean(confidence_v2),
        "median_confidence_v2": _median(confidence_v2),
        "low_confidence_v2_rate": _bool_pct(df, "low_confidence_v2"),
        "high_probability_rate": _threshold_rate(probability, ">=", 0.85),
        "low_probability_rate": _threshold_rate(probability, "<", 0.2),
    }


def smtp_metrics(df: pd.DataFrame) -> dict[str, Any]:
    smtp_status = df.get("smtp_status")
    return {
        "smtp_status_distribution": value_distribution(df, "smtp_status"),
        "avg_smtp_latency": _mean(_numeric(df, "smtp_latency")),
        "timeout_rate": _equals_rate(df, "smtp_error_type", "timeout"),
        "uncertain_rate": _equals_rate(df, "smtp_status", "uncertain"),
        "invalid_rate": _equals_rate(df, "smtp_status", "invalid"),
        "attempted_rate": (
            None
            if smtp_status is None
            else _pct(smtp_status.notna().sum(), len(df))
        ),
    }


def catch_all_metrics(df: pd.DataFrame) -> dict[str, Any]:
    return {
        "catch_all_status_distribution": value_distribution(df, "catch_all_status"),
        "avg_catch_all_confidence": _mean(_numeric(df, "catch_all_confidence")),
    }


def _nunique(df: pd.DataFrame, column: str) -> int | None:
    if column not in df.columns:
        return None
    return int(df[column].dropna().nunique())


def _nunique_first_available(
    df: pd.DataFrame,
    columns: tuple[str, ...],
) -> int | None:
    for column in columns:
        value = _nunique(df, column)
        if value is not None:
            return value
    return None


def _bool_count(df: pd.DataFrame, column: str) -> int | None:
    if column not in df.columns:
        return None
    return int(_as_bool(df[column]).sum())


def _bool_pct(df: pd.DataFrame, column: str) -> float | None:
    count = _bool_count(df, column)
    if count is None:
        return None
    return _pct(count, len(df))


def _as_bool(series: pd.Series) -> pd.Series:
    if series.dtype == bool:
        return series.fillna(False)
    return series.fillna(False).map(
        lambda value: str(value).strip().lower() in {"true", "1", "yes", "y"}
    )


def _numeric(df: pd.DataFrame, column: str) -> pd.Series | None:
    if column not in df.columns:
        return None
    return pd.to_numeric(df[column], errors="coerce")


def _mean(series: pd.Series | None) -> float | None:
    if series is None or series.dropna().empty:
        return None
    return float(series.mean())


def _median(series: pd.Series | None) -> float | None:
    if series is None or series.dropna().empty:
        return None
    return float(series.median())


def _threshold_rate(
    series: pd.Series | None,
    op: str,
    threshold: float,
) -> float | None:
    if series is None:
        return None
    valid = series.dropna()
    if valid.empty:
        return None
    count = (valid >= threshold).sum() if op == ">=" else (valid < threshold).sum()
    return _pct(count, len(valid))


def _equals_rate(df: pd.DataFrame, column: str, value: str) -> float | None:
    if column not in df.columns:
        return None
    return _pct((df[column] == value).sum(), len(df))


def _pct(count: int | float, total: int) -> float:
    if total == 0:
        return 0.0
    return float(count) / float(total)


__all__ = [
    "compute_core_metrics",
    "volume_metrics",
    "value_distribution",
    "comparison_metrics",
    "probability_confidence_metrics",
    "smtp_metrics",
    "catch_all_metrics",
]
