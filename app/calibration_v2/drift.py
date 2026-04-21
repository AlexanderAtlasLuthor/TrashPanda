"""Drift and disagreement analysis across V1, Scoring V2, and Validation V2."""

from __future__ import annotations

from typing import Any

import pandas as pd


def analyze_drift(report: dict[str, Any], df: pd.DataFrame | None = None) -> dict[str, Any]:
    comparison = report.get("core_metrics", {}).get("comparison", {})
    drift: dict[str, Any] = {
        "disagreement_rate": comparison.get("bucket_change_pct"),
        "hard_decision_changed_rate": comparison.get("hard_decision_changed_pct"),
        "v2_more_strict_count": comparison.get("v2_more_strict_count"),
        "v2_more_permissive_count": comparison.get("v2_more_permissive_count"),
        "strong_disagreement_count": None,
        "score_delta_extremes": {},
        "systematic_bias": [],
    }
    if df is None:
        return drift

    if {"preliminary_bucket", "action_recommendation"}.issubset(df.columns):
        strong = df[
            df["preliminary_bucket"].eq("high_confidence")
            & df["action_recommendation"].eq("block")
        ]
        drift["strong_disagreement_count"] = int(len(strong))

    if "score_delta" in df.columns:
        deltas = pd.to_numeric(df["score_delta"], errors="coerce").dropna()
        if not deltas.empty:
            drift["score_delta_extremes"] = {
                "min": float(deltas.min()),
                "max": float(deltas.max()),
                "median": float(deltas.median()),
            }

    if {"provider_reputation", "v2_more_strict"}.issubset(df.columns):
        strict = _bool(df["v2_more_strict"])
        grouped = df.assign(_strict=strict).groupby("provider_reputation")
        for provider, group in grouped:
            rate = float(group["_strict"].sum()) / float(len(group))
            if len(group) >= 2 and rate >= 0.5:
                drift["systematic_bias"].append(
                    {
                        "segment": str(provider),
                        "bias": "v2_strict",
                        "rate": rate,
                        "row_count": int(len(group)),
                    }
                )

    return drift


def _bool(series: pd.Series) -> pd.Series:
    return series.fillna(False).map(
        lambda value: str(value).strip().lower() in {"true", "1", "yes", "y"}
    )


__all__ = ["analyze_drift"]
