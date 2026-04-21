"""Simple orchestration entry point for offline V2 evaluation."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .loader import load_evaluation_frame
from .reporting import (
    build_evaluation_report,
    write_evaluation_report,
    write_sample_exports,
)


def run_evaluation(
    input_path: str | Path,
    output_dir: str | Path,
    *,
    sample_limit: int = 20,
) -> dict[str, Any]:
    df = load_evaluation_frame(input_path)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    report = build_evaluation_report(df, sample_limit=sample_limit)
    report_path = write_evaluation_report(
        report,
        output_path / "evaluation_report.json",
    )
    sample_paths = write_sample_exports(
        df,
        output_path,
        sample_limit=sample_limit,
    )

    summary = {
        "rows": int(len(df)),
        "report_path": str(report_path),
        "sample_exports": sample_paths,
    }
    print(
        "Evaluation complete: "
        f"{summary['rows']} rows, report={summary['report_path']}"
    )
    return summary


__all__ = ["run_evaluation"]
