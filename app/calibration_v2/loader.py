"""Load C2 evaluation reports and optional raw frames."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from app.evaluation_v2.loader import load_evaluation_frame


def load_calibration_inputs(
    evaluation_report: str | Path | dict[str, Any],
    raw_frame: str | Path | pd.DataFrame | None = None,
) -> tuple[dict[str, Any], pd.DataFrame | None]:
    if isinstance(evaluation_report, dict):
        report = dict(evaluation_report)
    else:
        report = json.loads(Path(evaluation_report).read_text(encoding="utf-8"))

    frame = load_evaluation_frame(raw_frame) if raw_frame is not None else None
    return report, frame


def extract_core_metric(
    report: dict[str, Any],
    path: tuple[str, ...],
    default: Any = None,
) -> Any:
    current: Any = report
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current


__all__ = ["load_calibration_inputs", "extract_core_metric"]
