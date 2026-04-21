"""Report builders and writers for offline V2 evaluation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from .distributions import build_distribution_report
from .loader import describe_available_columns
from .metrics import compute_core_metrics
from .provider_analysis import build_provider_analysis
from .samples import extract_all_samples, sample_counts


def build_evaluation_report(
    df: pd.DataFrame,
    *,
    sample_limit: int = 20,
    provider_top_n: int = 20,
) -> dict[str, Any]:
    return {
        "schema": describe_available_columns(df),
        "core_metrics": compute_core_metrics(df),
        "distributions": build_distribution_report(df),
        "provider_analysis": build_provider_analysis(df, top_n=provider_top_n),
        "sample_summary": sample_counts(df, limit=sample_limit),
    }


def write_evaluation_report(report: dict[str, Any], path: str | Path) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(_json_safe(report), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output_path


def write_sample_exports(
    df: pd.DataFrame,
    output_dir: str | Path,
    *,
    sample_limit: int = 20,
) -> dict[str, str]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    samples = extract_all_samples(df, limit=sample_limit)
    written: dict[str, str] = {}
    for name, sample in samples.items():
        path = output_path / f"sample_{name}.csv"
        sample.to_csv(path, index=False)
        written[name] = str(path)
    return written


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if hasattr(value, "item"):
        return value.item()
    if pd.isna(value):
        return None
    return value


__all__ = [
    "build_evaluation_report",
    "write_evaluation_report",
    "write_sample_exports",
]
