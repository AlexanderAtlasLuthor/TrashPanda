"""Tests for the approved_original_format artifact (Phase 2 extension).

Verifies:
  A. The file is generated after a successful pipeline run.
  B. It preserves the original input column order exactly.
  C. It contains only high_confidence (Ready to send) rows — review rows excluded.
  D. It is discoverable via collect_job_artifacts (and therefore enters the ZIP).
  E. Existing client outputs are unaffected.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from app.api_boundary import (
    JobResult,
    JobStatus,
    collect_job_artifacts,
    run_cleaning_job,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SAMPLE_CSV = PROJECT_ROOT / "examples" / "sample_contacts.csv"

# Original columns from examples/sample_contacts.csv — used to assert preservation.
EXPECTED_COLUMNS = [
    "id", "email", "domain", "fname", "lname",
    "state", "address", "county", "city", "zip", "website", "ip",
]


@pytest.fixture(scope="module")
def job(tmp_path_factory: pytest.TempPathFactory) -> JobResult:
    assert SAMPLE_CSV.is_file(), f"Sample input missing: {SAMPLE_CSV}"
    out = tmp_path_factory.mktemp("approved_fmt")
    return run_cleaning_job(
        input_path=SAMPLE_CSV,
        output_root=out,
        job_id="test_approved_fmt",
    )


# ---------------------------------------------------------------------------
# A. File is generated
# ---------------------------------------------------------------------------

def test_approved_original_format_generated(job: JobResult) -> None:
    assert job.status == JobStatus.COMPLETED, f"Job failed: {job.error}"
    co = job.artifacts.client_outputs
    assert co.approved_original_format is not None, "approved_original_format path is None"
    assert co.approved_original_format.is_file(), "approved_original_format.xlsx does not exist"


# ---------------------------------------------------------------------------
# B. Original columns are preserved
# ---------------------------------------------------------------------------

def test_approved_original_format_preserves_columns(job: JobResult) -> None:
    path = job.artifacts.client_outputs.approved_original_format
    df = pd.read_excel(path, dtype=str)
    assert list(df.columns) == EXPECTED_COLUMNS, (
        f"Column mismatch.\n  got:      {list(df.columns)}\n  expected: {EXPECTED_COLUMNS}"
    )


# ---------------------------------------------------------------------------
# C. Only high_confidence rows (review excluded)
# ---------------------------------------------------------------------------

def test_approved_original_format_row_count_matches_approved(job: JobResult) -> None:
    run_dir = job.artifacts.run_dir

    clean_df = pd.read_csv(run_dir / "clean_high_confidence.csv", dtype=str, keep_default_na=False)
    approved_df = pd.read_excel(job.artifacts.client_outputs.approved_original_format, dtype=str)

    expected = len(clean_df)
    assert len(approved_df) == expected, (
        f"Row count mismatch: approved_original_format has {len(approved_df)} rows, "
        f"expected {expected} (clean_high_confidence only)"
    )


def test_approved_original_format_emails_match_approved_set(job: JobResult) -> None:
    """The email values in the output match only the high_confidence emails from the pipeline."""
    run_dir = job.artifacts.run_dir

    # Build the set of approved row numbers from clean_high_confidence only.
    clean_df = pd.read_csv(run_dir / "clean_high_confidence.csv", dtype=str, keep_default_na=False)

    approved_row_nums: set[int] = set()
    if "source_row_number" in clean_df.columns:
        approved_row_nums.update(int(v) for v in clean_df["source_row_number"] if v)

    approved_df = pd.read_excel(job.artifacts.client_outputs.approved_original_format, dtype=str)

    # Every row in approved_original_format must come from an approved source row.
    # We verify this indirectly: the email column values must all appear in the
    # original input at one of the approved row positions.
    orig_df = pd.read_csv(SAMPLE_CSV, dtype=str, keep_default_na=False, na_filter=False)
    approved_emails_from_original = set(
        orig_df.iloc[rn - 2]["email"].strip()
        for rn in approved_row_nums
        if 0 <= rn - 2 < len(orig_df)
    )

    output_emails = {str(e).strip() for e in approved_df["email"]}
    assert output_emails == approved_emails_from_original, (
        f"Email mismatch.\n  output: {output_emails}\n  expected: {approved_emails_from_original}"
    )


# ---------------------------------------------------------------------------
# D. Included in ZIP (discoverable via collect_job_artifacts)
# ---------------------------------------------------------------------------

def test_approved_original_format_in_artifacts(job: JobResult) -> None:
    rediscovered = collect_job_artifacts(job.artifacts.run_dir)
    assert rediscovered.client_outputs.approved_original_format is not None
    assert rediscovered.client_outputs.approved_original_format.is_file()


# ---------------------------------------------------------------------------
# E. Existing outputs are unaffected
# ---------------------------------------------------------------------------

def test_existing_outputs_unaffected(job: JobResult) -> None:
    co = job.artifacts.client_outputs
    assert co.valid_emails is not None and co.valid_emails.is_file()
    assert co.review_emails is not None and co.review_emails.is_file()
    assert co.invalid_or_bounce_risk is not None and co.invalid_or_bounce_risk.is_file()
    assert co.summary_report is not None and co.summary_report.is_file()

    tech = job.artifacts.technical_csvs
    assert tech.clean_high_confidence is not None and tech.clean_high_confidence.is_file()
    assert tech.review_medium_confidence is not None and tech.review_medium_confidence.is_file()
    assert tech.removed_invalid is not None and tech.removed_invalid.is_file()
