"""Job result rehydration after backend restart."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from app import server
from app.api_boundary import JobStatus


def test_load_job_result_rehydrates_completed_run_from_disk(
    tmp_path: Path,
    monkeypatch,
):
    server.JOB_STORE.clear()
    monkeypatch.setattr(server, "RUNTIME_ROOT", tmp_path / "runtime")
    run_dir = (
        tmp_path
        / "runtime"
        / "jobs"
        / "job_rehydrate"
        / "run_20260506_120000_abc"
    )
    run_dir.mkdir(parents=True)
    summary = pd.DataFrame(
        [
            {"metric": "total_input_rows", "value": 3},
            {"metric": "total_valid", "value": 1},
            {"metric": "total_review", "value": 1},
            {"metric": "total_invalid_or_bounce_risk", "value": 1},
        ]
    )
    with pd.ExcelWriter(run_dir / "summary_report.xlsx", engine="openpyxl") as writer:
        summary.to_excel(writer, sheet_name="totals", index=False)

    result = server._load_job_result("job_rehydrate")

    assert result is not None
    assert result.status == JobStatus.COMPLETED
    assert result.run_dir == run_dir
    assert result.summary is not None
    assert result.summary.total_valid == 1
