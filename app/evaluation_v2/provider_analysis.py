"""Provider and domain level evaluation breakdowns."""

from __future__ import annotations

from typing import Any

import pandas as pd

from .metrics import value_distribution


def build_provider_analysis(
    df: pd.DataFrame,
    *,
    top_n: int = 20,
) -> dict[str, list[dict[str, Any]]]:
    return {
        "by_corrected_domain": group_breakdown(df, "corrected_domain", top_n=top_n),
        "by_provider_reputation": group_breakdown(
            df,
            "provider_reputation",
            top_n=top_n,
        ),
    }


def group_breakdown(
    df: pd.DataFrame,
    group_column: str,
    *,
    top_n: int = 20,
) -> list[dict[str, Any]]:
    if group_column not in df.columns:
        return []

    groups = []
    for value, group in df.groupby(group_column, dropna=True):
        groups.append(
            {
                "group": str(value),
                "row_count": int(len(group)),
                "avg_deliverability_probability": _mean(
                    group, "deliverability_probability"
                ),
                "avg_deliverability_confidence": _mean(
                    group, "deliverability_confidence"
                ),
                "validation_status_distribution": value_distribution(
                    group, "validation_status"
                ),
                "smtp_status_distribution": value_distribution(group, "smtp_status"),
                "catch_all_status_distribution": value_distribution(
                    group, "catch_all_status"
                ),
                "bucket_changed_pct": _bool_pct(group, "bucket_changed"),
                "v2_higher_bucket_pct": _bool_pct(group, "v2_higher_bucket"),
                "v2_lower_bucket_pct": _bool_pct(group, "v2_lower_bucket"),
            }
        )

    groups.sort(
        key=lambda item: (
            int(item["row_count"]),
            str(item["group"]),
        ),
        reverse=True,
    )
    return groups[:top_n]


def _mean(df: pd.DataFrame, column: str) -> float | None:
    if column not in df.columns:
        return None
    values = pd.to_numeric(df[column], errors="coerce").dropna()
    if values.empty:
        return None
    return float(values.mean())


def _bool_pct(df: pd.DataFrame, column: str) -> float | None:
    if column not in df.columns or len(df) == 0:
        return None
    values = df[column].fillna(False).map(
        lambda value: str(value).strip().lower() in {"true", "1", "yes", "y"}
    )
    return float(values.sum()) / float(len(df))


__all__ = ["build_provider_analysis", "group_breakdown"]
