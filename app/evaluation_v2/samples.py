"""Manual-review sample extraction for V2 evaluation."""

from __future__ import annotations

from typing import Any

import pandas as pd


DEFAULT_COLUMNS = (
    "email",
    "domain",
    "corrected_domain",
    "source_file",
    "score",
    "preliminary_bucket",
    "score_v2",
    "confidence_v2",
    "bucket_v2",
    "score_delta",
    "abs_score_delta",
    "deliverability_probability",
    "deliverability_confidence",
    "validation_status",
    "action_recommendation",
    "smtp_status",
    "smtp_error_type",
    "catch_all_status",
    "retry_attempted",
    "provider_reputation",
)


def extract_all_samples(
    df: pd.DataFrame,
    *,
    limit: int = 20,
    as_records: bool = False,
) -> dict[str, pd.DataFrame | list[dict[str, Any]]]:
    samples = {
        "high_probability_risky": high_probability_risky(df, limit=limit),
        "low_probability_v1_high_confidence": low_probability_v1_high_confidence(
            df, limit=limit
        ),
        "timeout_retry_cases": timeout_retry_cases(df, limit=limit),
        "catch_all_cases": catch_all_cases(df, limit=limit),
        "biggest_disagreements": biggest_disagreements(df, limit=limit),
    }
    if as_records:
        return {name: frame.to_dict(orient="records") for name, frame in samples.items()}
    return samples


def sample_counts(df: pd.DataFrame, *, limit: int = 20) -> dict[str, int]:
    return {
        name: int(len(frame))
        for name, frame in extract_all_samples(df, limit=limit).items()
    }


def high_probability_risky(df: pd.DataFrame, *, limit: int = 20) -> pd.DataFrame:
    mask = _numeric(df, "deliverability_probability").ge(0.85)
    if "catch_all_status" in df.columns:
        mask &= df["catch_all_status"].isin({"confirmed", "likely"}) | _numeric(
            df, "deliverability_confidence"
        ).lt(0.5)
    else:
        mask &= _numeric(df, "deliverability_confidence").lt(0.5)
    return _select(df[mask], limit=limit, sort_by="deliverability_probability")


def low_probability_v1_high_confidence(
    df: pd.DataFrame,
    *,
    limit: int = 20,
) -> pd.DataFrame:
    if "preliminary_bucket" not in df.columns:
        return _empty(df)
    mask = (
        df["preliminary_bucket"].eq("high_confidence")
        & _numeric(df, "deliverability_probability").lt(0.4)
    )
    return _select(df[mask], limit=limit, sort_by="deliverability_probability", ascending=True)


def timeout_retry_cases(df: pd.DataFrame, *, limit: int = 20) -> pd.DataFrame:
    mask = pd.Series(False, index=df.index)
    if "smtp_status" in df.columns:
        mask |= df["smtp_status"].eq("uncertain")
    if "smtp_error_type" in df.columns:
        mask |= df["smtp_error_type"].eq("timeout")
    if "retry_attempted" in df.columns:
        mask |= _bool_series(df["retry_attempted"])
    return _select(df[mask], limit=limit, sort_by="deliverability_probability")


def catch_all_cases(df: pd.DataFrame, *, limit: int = 20) -> pd.DataFrame:
    if "catch_all_status" not in df.columns:
        return _empty(df)
    mask = df["catch_all_status"].isin({"confirmed", "likely"})
    return _select(df[mask], limit=limit, sort_by="catch_all_confidence")


def biggest_disagreements(df: pd.DataFrame, *, limit: int = 20) -> pd.DataFrame:
    frame = df.copy()
    if "abs_score_delta" not in frame.columns:
        frame["abs_score_delta"] = _numeric(frame, "score_delta").abs()
    if "validation_status" in frame.columns and "bucket_v2" in frame.columns:
        status_rank = frame["validation_status"].map(
            {"invalid": 0, "risky": 1, "uncertain": 2, "likely_valid": 3, "valid": 4}
        )
        bucket_rank = frame["bucket_v2"].map(
            {"invalid": 0, "review": 2, "high_confidence": 4}
        )
        frame["_status_bucket_gap"] = (status_rank - bucket_rank).abs()
    else:
        frame["_status_bucket_gap"] = 0
    frame["_disagreement_score"] = _numeric(frame, "abs_score_delta").fillna(0) + (
        frame["_status_bucket_gap"].fillna(0) * 0.25
    )
    return _select(frame, limit=limit, sort_by="_disagreement_score").drop(
        columns=[c for c in ("_status_bucket_gap", "_disagreement_score") if c in frame.columns],
        errors="ignore",
    )


def _select(
    df: pd.DataFrame,
    *,
    limit: int,
    sort_by: str,
    ascending: bool = False,
) -> pd.DataFrame:
    frame = df.copy()
    if sort_by in frame.columns:
        frame = frame.sort_values(sort_by, ascending=ascending, na_position="last")
    columns = [c for c in DEFAULT_COLUMNS if c in frame.columns]
    return frame.loc[:, columns].head(limit).reset_index(drop=True)


def _empty(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[[], [c for c in DEFAULT_COLUMNS if c in df.columns]].copy()


def _numeric(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series([pd.NA] * len(df), index=df.index, dtype="Float64")
    return pd.to_numeric(df[column], errors="coerce")


def _bool_series(series: pd.Series) -> pd.Series:
    return series.fillna(False).map(
        lambda value: str(value).strip().lower() in {"true", "1", "yes", "y"}
    )


__all__ = [
    "extract_all_samples",
    "sample_counts",
    "high_probability_risky",
    "low_probability_v1_high_confidence",
    "timeout_retry_cases",
    "catch_all_cases",
    "biggest_disagreements",
]
