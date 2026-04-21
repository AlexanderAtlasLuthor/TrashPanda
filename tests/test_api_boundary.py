"""Tests for the Phase 5 API boundary (``app.api_boundary``)."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

from app.api_boundary import (
    JobErrorType,
    JobResult,
    JobStatus,
    JobSummary,
    collect_job_artifacts,
    job_result_to_dict,
    load_job_summary,
    run_cleaning_job,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SAMPLE_CSV = PROJECT_ROOT / "examples" / "sample_contacts.csv"


# --------------------------------------------------------------------------- #
# A. Successful run
# --------------------------------------------------------------------------- #


@pytest.fixture(scope="module")
def successful_job(tmp_path_factory: pytest.TempPathFactory) -> JobResult:
    """Run the pipeline once on the bundled sample CSV and reuse the result."""

    assert SAMPLE_CSV.is_file(), f"Sample input missing: {SAMPLE_CSV}"
    output_root = tmp_path_factory.mktemp("api_boundary_success")
    result = run_cleaning_job(
        input_path=SAMPLE_CSV,
        output_root=output_root,
        job_id="test_job_success",
    )
    return result


def test_successful_run_status_completed(successful_job: JobResult) -> None:
    assert successful_job.status == JobStatus.COMPLETED
    assert successful_job.error is None
    assert successful_job.job_id == "test_job_success"
    assert successful_job.input_filename == SAMPLE_CSV.name
    assert isinstance(successful_job.started_at, datetime)
    assert isinstance(successful_job.finished_at, datetime)
    assert successful_job.finished_at >= successful_job.started_at


def test_successful_run_artifacts_populated(successful_job: JobResult) -> None:
    artifacts = successful_job.artifacts
    assert artifacts is not None
    assert artifacts.run_dir.is_dir()

    tech = artifacts.technical_csvs
    # All three technical CSVs must exist for a real run.
    assert tech.clean_high_confidence is not None and tech.clean_high_confidence.is_file()
    assert (
        tech.review_medium_confidence is not None
        and tech.review_medium_confidence.is_file()
    )
    assert tech.removed_invalid is not None and tech.removed_invalid.is_file()

    reports = artifacts.reports
    assert reports.processing_report_json is not None
    assert reports.processing_report_json.is_file()
    assert reports.processing_report_csv is not None
    assert reports.processing_report_csv.is_file()


def test_successful_run_summary_populated(successful_job: JobResult) -> None:
    summary = successful_job.summary
    assert summary is not None
    # Sample file has 5 rows; the reporting layer should see them all.
    assert summary.total_input_rows >= 1
    # Every bucket count must be a non-negative integer.
    for field_name in (
        "total_valid",
        "total_review",
        "total_invalid_or_bounce_risk",
        "duplicates_removed",
        "typo_corrections",
        "disposable_emails",
        "placeholder_or_fake_emails",
        "role_based_emails",
    ):
        value = getattr(summary, field_name)
        assert isinstance(value, int) and value >= 0, f"bad {field_name}={value!r}"


# --------------------------------------------------------------------------- #
# B. File not found
# --------------------------------------------------------------------------- #


def test_file_not_found(tmp_path: Path) -> None:
    result = run_cleaning_job(
        input_path=tmp_path / "does_not_exist.csv",
        output_root=tmp_path / "out",
        job_id="test_job_missing",
    )
    assert result.status == JobStatus.FAILED
    assert result.error is not None
    assert result.error.error_type == JobErrorType.FILE_NOT_FOUND
    assert result.artifacts is None
    assert result.summary is None
    # Error must be plain-data serialisable (no traceback as message).
    assert "Traceback" not in result.error.message


# --------------------------------------------------------------------------- #
# C. Missing required email column
# --------------------------------------------------------------------------- #


def test_missing_required_email_column(tmp_path: Path) -> None:
    bad_csv = tmp_path / "no_email_column.csv"
    bad_csv.write_text(
        "id,first_name,last_name\n1,Alice,Stone\n2,Bob,Jones\n",
        encoding="utf-8",
    )
    result = run_cleaning_job(
        input_path=bad_csv,
        output_root=tmp_path / "out",
        job_id="test_job_no_email",
    )
    assert result.status == JobStatus.FAILED
    assert result.error is not None
    assert result.error.error_type == JobErrorType.MISSING_REQUIRED_COLUMNS
    assert "email" in result.error.message.lower()


# --------------------------------------------------------------------------- #
# D. JSON serialisation
# --------------------------------------------------------------------------- #


def test_job_result_to_dict_is_json_clean(successful_job: JobResult) -> None:
    payload = job_result_to_dict(successful_job)

    # Must round-trip through json without a custom encoder.
    round_tripped = json.loads(json.dumps(payload))
    assert round_tripped == payload

    # Top-level contract.
    assert payload["job_id"] == "test_job_success"
    assert payload["status"] == JobStatus.COMPLETED
    assert payload["input_filename"] == SAMPLE_CSV.name
    assert payload["error"] is None

    # Datetimes are ISO strings.
    assert isinstance(payload["started_at"], str)
    datetime.fromisoformat(payload["started_at"])
    assert isinstance(payload["finished_at"], str)
    datetime.fromisoformat(payload["finished_at"])

    # Paths are plain strings.
    assert isinstance(payload["run_dir"], str)
    tech = payload["artifacts"]["technical_csvs"]
    assert isinstance(tech["clean_high_confidence"], str)

    # Summary survives as a flat dict of ints.
    summary = payload["summary"]
    assert isinstance(summary, dict)
    for key in JobSummary.__dataclass_fields__:  # type: ignore[attr-defined]
        assert key in summary
        assert isinstance(summary[key], int)


def test_job_result_to_dict_on_failure(tmp_path: Path) -> None:
    result = run_cleaning_job(
        input_path=tmp_path / "nope.csv",
        output_root=tmp_path / "out",
        job_id="test_job_fail_serialize",
    )
    payload = job_result_to_dict(result)
    json.dumps(payload)  # must not raise

    assert payload["status"] == JobStatus.FAILED
    assert payload["error"]["error_type"] == JobErrorType.FILE_NOT_FOUND
    assert payload["summary"] is None
    assert payload["artifacts"] is None


# --------------------------------------------------------------------------- #
# E. Helper: artifact discovery on an existing run directory
# --------------------------------------------------------------------------- #


def test_collect_job_artifacts_only_returns_existing(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_fake"
    run_dir.mkdir()
    # Create only two of the expected artifacts.
    (run_dir / "clean_high_confidence.csv").write_text("a\n", encoding="utf-8")
    (run_dir / "processing_report.json").write_text("{}", encoding="utf-8")

    artifacts = collect_job_artifacts(run_dir)

    assert artifacts.technical_csvs.clean_high_confidence is not None
    assert artifacts.technical_csvs.review_medium_confidence is None
    assert artifacts.technical_csvs.removed_invalid is None
    assert artifacts.reports.processing_report_json is not None
    assert artifacts.reports.processing_report_csv is None
    assert artifacts.client_outputs.valid_emails is None


def test_load_job_summary_from_json_only(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_jsononly"
    run_dir.mkdir()
    (run_dir / "processing_report.json").write_text(
        json.dumps(
            {
                "total_rows_processed": 100,
                "total_clean_high_confidence": 70,
                "total_review": 20,
                "total_removed_invalid": 10,
                "total_duplicates_removed": 3,
                "total_typo_corrections": 5,
            }
        ),
        encoding="utf-8",
    )

    summary = load_job_summary(run_dir)
    assert summary is not None
    assert summary.total_input_rows == 100
    assert summary.total_valid == 70
    assert summary.total_review == 20
    assert summary.total_invalid_or_bounce_risk == 10
    assert summary.duplicates_removed == 3
    assert summary.typo_corrections == 5
    # Reason-based counts not in the JSON report → remain 0.
    assert summary.disposable_emails == 0
    assert summary.placeholder_or_fake_emails == 0
    assert summary.role_based_emails == 0


def test_load_job_summary_missing_reports_returns_none(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_empty"
    run_dir.mkdir()
    assert load_job_summary(run_dir) is None
