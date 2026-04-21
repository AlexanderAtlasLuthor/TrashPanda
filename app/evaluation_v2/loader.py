"""Input loading and column inventory for offline V2 evaluation."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


IDENTITY_COLUMNS = ("email", "domain", "corrected_domain")
OPTIONAL_COLUMNS = (
    "score",
    "preliminary_bucket",
    "hard_fail",
    "score_v2",
    "confidence_v2",
    "bucket_v2",
    "hard_stop_v2",
    "reason_codes_v2",
    "explanation_v2",
    "score_delta",
    "abs_score_delta",
    "bucket_changed",
    "v2_higher_bucket",
    "v2_lower_bucket",
    "hard_decision_changed",
    "v2_more_strict",
    "v2_more_permissive",
    "low_confidence_v2",
    "deliverability_probability",
    "deliverability_confidence",
    "validation_status",
    "action_recommendation",
    "smtp_status",
    "smtp_code",
    "smtp_latency",
    "smtp_error_type",
    "catch_all_status",
    "catch_all_confidence",
    "retry_attempted",
    "retry_outcome",
    "provider_reputation",
    "source_file",
)


def load_evaluation_frame(source: str | Path | pd.DataFrame) -> pd.DataFrame:
    """Load a CSV/parquet/DataFrame and normalize obvious missing values."""
    if isinstance(source, pd.DataFrame):
        df = source.copy()
    else:
        path = Path(source)
        suffix = path.suffix.lower()
        if suffix == ".csv":
            df = pd.read_csv(path)
        elif suffix in {".parquet", ".pq"}:
            df = pd.read_parquet(path)
        else:
            raise ValueError(f"unsupported evaluation input type: {suffix}")

    _validate_identity_columns(df)
    return _normalize_missing_values(df)


def describe_available_columns(df: pd.DataFrame) -> dict[str, Any]:
    present = set(df.columns)
    identity_present = [c for c in IDENTITY_COLUMNS if c in present]
    optional_present = [c for c in OPTIONAL_COLUMNS if c in present]
    optional_missing = [c for c in OPTIONAL_COLUMNS if c not in present]
    return {
        "columns": list(df.columns),
        "identity_present": identity_present,
        "identity_missing": [c for c in IDENTITY_COLUMNS if c not in present],
        "optional_present": optional_present,
        "optional_missing": optional_missing,
        "row_count": int(len(df)),
    }


def _validate_identity_columns(df: pd.DataFrame) -> None:
    if not any(column in df.columns for column in IDENTITY_COLUMNS):
        raise ValueError(
            "evaluation input must include at least one identity column: "
            "email, domain, or corrected_domain"
        )


def _normalize_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out = out.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "NULL": pd.NA})
    return out


__all__ = [
    "IDENTITY_COLUMNS",
    "OPTIONAL_COLUMNS",
    "load_evaluation_frame",
    "describe_available_columns",
]
