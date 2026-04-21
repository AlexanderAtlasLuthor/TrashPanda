"""Runner for the offline calibration playbook."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .loader import load_calibration_inputs
from .reporting import build_calibration_report, write_calibration_report


def run_calibration(
    evaluation_report_path: str | Path,
    output_dir: str | Path,
    raw_frame_path: str | Path | None = None,
) -> dict[str, Any]:
    evaluation_report, frame = load_calibration_inputs(
        evaluation_report_path,
        raw_frame_path,
    )
    report = build_calibration_report(evaluation_report, frame)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    report_path = write_calibration_report(
        report,
        output_path / "calibration_report.json",
    )
    summary_path = output_path / "summary.txt"
    summary_path.write_text(_summary_text(report), encoding="utf-8")
    print(f"Calibration complete: {report_path}")
    return {
        "report_path": str(report_path),
        "summary_path": str(summary_path),
        "readiness_state": report["rollout_strategy"]["readiness_state"],
    }


def _summary_text(report: dict[str, Any]) -> str:
    rollout = report["rollout_strategy"]
    lines = [
        "TrashPanda Calibration Summary",
        f"Readiness: {rollout['readiness_state']}",
        f"Strategy: {rollout['strategy']}",
        f"Initial percentage: {rollout['initial_percentage']}",
        f"Recommendations: {len(report['recommendations'])}",
    ]
    return "\n".join(lines) + "\n"


__all__ = ["run_calibration"]
