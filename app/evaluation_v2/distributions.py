"""Bucketed distribution helpers for evaluation reporting."""

from __future__ import annotations

import pandas as pd


def probability_histogram(df: pd.DataFrame) -> dict[str, int]:
    return _bucket_counts(
        df,
        "deliverability_probability",
        [
            ("<0.2", None, 0.2, False),
            ("0.2-0.4", 0.2, 0.4, False),
            ("0.4-0.6", 0.4, 0.6, False),
            ("0.6-0.85", 0.6, 0.85, False),
            (">=0.85", 0.85, None, True),
        ],
    )


def deliverability_confidence_buckets(df: pd.DataFrame) -> dict[str, int]:
    return _bucket_counts(
        df,
        "deliverability_confidence",
        [
            ("<0.5", None, 0.5, False),
            ("0.5-0.75", 0.5, 0.75, False),
            ("0.75-0.9", 0.75, 0.9, False),
            (">=0.9", 0.9, None, True),
        ],
    )


def score_v2_buckets(df: pd.DataFrame) -> dict[str, int]:
    return _bucket_counts(
        df,
        "score_v2",
        [
            ("<0.2", None, 0.2, False),
            ("0.2-0.4", 0.2, 0.4, False),
            ("0.4-0.6", 0.4, 0.6, False),
            ("0.6-0.8", 0.6, 0.8, False),
            (">=0.8", 0.8, None, True),
        ],
    )


def confidence_v2_buckets(df: pd.DataFrame) -> dict[str, int]:
    return _bucket_counts(
        df,
        "confidence_v2",
        [
            ("<0.5", None, 0.5, False),
            ("0.5-0.75", 0.5, 0.75, False),
            ("0.75-0.9", 0.75, 0.9, False),
            (">=0.9", 0.9, None, True),
        ],
    )


def build_distribution_report(df: pd.DataFrame) -> dict[str, dict[str, int]]:
    return {
        "deliverability_probability": probability_histogram(df),
        "deliverability_confidence": deliverability_confidence_buckets(df),
        "score_v2": score_v2_buckets(df),
        "confidence_v2": confidence_v2_buckets(df),
    }


def _bucket_counts(
    df: pd.DataFrame,
    column: str,
    buckets: list[tuple[str, float | None, float | None, bool]],
) -> dict[str, int]:
    counts = {label: 0 for label, *_ in buckets}
    if column not in df.columns:
        return counts
    values = pd.to_numeric(df[column], errors="coerce").dropna()
    for label, lower, upper, include_upper_open in buckets:
        mask = pd.Series(True, index=values.index)
        if lower is not None:
            mask &= values >= lower
        if upper is not None:
            mask &= values < upper
        if include_upper_open and upper is None:
            mask &= values >= lower  # type: ignore[operator]
        counts[label] = int(mask.sum())
    return counts


__all__ = [
    "probability_histogram",
    "deliverability_confidence_buckets",
    "score_v2_buckets",
    "confidence_v2_buckets",
    "build_distribution_report",
]
