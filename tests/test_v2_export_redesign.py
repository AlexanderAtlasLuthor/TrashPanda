"""Subphase V2.5 — Export System Redesign.

Pins the V2.5 contract end-to-end:

  * Materialization emits the legacy three CSVs **plus** two new
    V2-semantic separated CSVs (``removed_duplicates.csv``,
    ``removed_hard_fail.csv``).
  * ``clean_high_confidence.csv`` contains only V2 ``auto_approve``.
  * ``review_medium_confidence.csv`` contains only V2 ``manual_review``
    (or the V2.1 conservative-fallback rows with missing decisions).
  * ``removed_invalid.csv`` (legacy union) keeps every removed row for
    back-compat. The new files are strict subsets.
  * Client workbooks include the V2 verification columns
    (``final_action``, ``decision_reason``, ``deliverability_probability``,
    ``smtp_status``, ``catch_all_*``).
  * ``approved_original_format`` cannot leak ``manual_review``,
    ``auto_reject``, duplicate, or V1 hard-fail rows.
  * The summary workbook surfaces V2-meaningful counts.
  * No live network is used — the autouse fixture in ``conftest.py``
    keeps the suite offline.

V2.1 + V2.2 + V2.3 + V2.4 invariants remain pinned by the existing
suites; V2.5 adds the export-layer contract on top without changing
routing.
"""

from __future__ import annotations

import csv
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from app.api_boundary import (
    JobResult,
    JobStatus,
    collect_job_artifacts,
    run_cleaning_job,
)
from app.client_output import (
    BUCKET_CONFIG,
    SUPPLEMENTARY_BUCKET_CONFIG,
    V2_VERIFICATION_COLUMNS,
    generate_approved_original_format,
    generate_client_outputs,
)
from app.config import load_config, resolve_project_paths
from app.dedupe import DedupeIndex
from app.engine.stages.catch_all_detection import (
    CATCH_ALL_STATUS_CONFIRMED,
    CATCH_ALL_STATUS_NOT,
    CATCH_ALL_STATUS_NOT_TESTED,
)
from app.engine.stages.smtp_verification import (
    SMTP_STATUS_INVALID,
    SMTP_STATUS_NOT_TESTED,
    SMTP_STATUS_VALID,
)
from app.models import RunContext
from app.pipeline import EmailCleaningPipeline
from app.storage import StagingDB
from app.v2_decision_policy import (
    REASON_DUPLICATE,
    REASON_HARD_FAIL,
    REASON_HIGH_PROBABILITY,
    REASON_LOW_PROBABILITY,
    REASON_MEDIUM_PROBABILITY,
    REASON_SMTP_INVALID,
)


# ---------------------------------------------------------------------------
# Helpers — staging frame + materialize wrapper, mirroring V2.1+V2.4 tests
# ---------------------------------------------------------------------------


def _build_run_context(tmp_path: Path) -> RunContext:
    run_dir = tmp_path / "run"
    logs_dir = tmp_path / "logs"
    temp_dir = tmp_path / "tmp"
    for p in (run_dir, logs_dir, temp_dir):
        p.mkdir(parents=True, exist_ok=True)
    return RunContext(
        run_id="run_v25_test",
        run_dir=run_dir,
        logs_dir=logs_dir,
        temp_dir=temp_dir,
        staging_db_path=run_dir / "staging.sqlite3",
        started_at=datetime.now(),
    )


def _staging_columns() -> list[str]:
    return [
        "email", "email_normalized", "source_file", "source_row_number",
        "chunk_index", "global_ordinal", "hard_fail", "score",
        "preliminary_bucket", "completeness_score", "is_canonical",
        "duplicate_flag", "duplicate_reason", "domain", "corrected_domain",
        "domain_from_email", "has_mx_record", "has_a_record", "domain_exists",
        "typo_corrected",
        "smtp_status", "smtp_was_candidate", "smtp_confirmed_valid",
        "smtp_response_code",
        "catch_all_status", "catch_all_flag", "catch_all_confidence",
        "catch_all_method",
        "deliverability_probability", "decision_confidence",
        "final_action", "decision_reason",
        "v2_final_bucket",
    ]


def _staging_frame(rows: list[dict]) -> pd.DataFrame:
    cols = _staging_columns()
    out: dict[str, list] = {c: [] for c in cols}
    for r in rows:
        for c in cols:
            out[c].append(r.get(c))
    return pd.DataFrame(out)


def _row(
    email: str,
    *,
    final_action: str,
    decision_reason: str,
    deliverability_probability: float = 0.85,
    is_canonical: bool = True,
    hard_fail: bool = False,
    smtp_status: str = SMTP_STATUS_VALID,
    catch_all_status: str = CATCH_ALL_STATUS_NOT,
    catch_all_flag: bool = False,
    bucket_v2: str = "ready",
    smtp_confirmed_valid: bool | None = None,
    source_row_number: int = 2,
    global_ordinal: int = 0,
) -> dict:
    norm = email.lower()
    return {
        "email": email,
        "email_normalized": norm,
        "source_file": "f.csv",
        "source_row_number": source_row_number,
        "chunk_index": 0,
        "global_ordinal": global_ordinal,
        "hard_fail": hard_fail,
        "score": 75,
        "preliminary_bucket": "high_confidence",
        "completeness_score": 3,
        "is_canonical": is_canonical,
        "duplicate_flag": not is_canonical,
        "duplicate_reason": None if is_canonical else "duplicate_lower_score",
        "domain": email.split("@", 1)[1] if "@" in email else "",
        "corrected_domain": email.split("@", 1)[1] if "@" in email else "",
        "domain_from_email": email.split("@", 1)[1] if "@" in email else "",
        "has_mx_record": True,
        "has_a_record": False,
        "domain_exists": True,
        "typo_corrected": False,
        "smtp_status": smtp_status,
        "smtp_was_candidate": True,
        "smtp_confirmed_valid": (
            smtp_confirmed_valid
            if smtp_confirmed_valid is not None
            else (smtp_status == SMTP_STATUS_VALID)
        ),
        "smtp_response_code": 250 if smtp_status == SMTP_STATUS_VALID else 550,
        "catch_all_status": catch_all_status,
        "catch_all_flag": catch_all_flag,
        "catch_all_confidence": 0.85 if catch_all_status != CATCH_ALL_STATUS_NOT_TESTED else 0.0,
        "catch_all_method": "smtp_valid_no_random_accept",
        "deliverability_probability": deliverability_probability,
        "decision_confidence": deliverability_probability,
        "final_action": final_action,
        "decision_reason": decision_reason,
        "v2_final_bucket": bucket_v2,
    }


def _mixed_rows() -> list[dict]:
    """A representative bag of rows hitting every V2.5 routing path.

    ``source_row_number`` is unique per row (2..8) so dedupe identity
    works and the indices line up with a 7-row test CSV. Two rows
    share ``email='dup@gmail.com'`` to exercise dedupe.
    """
    return [
        # Row 2 — approved.
        _row("approved@gmail.com", final_action="auto_approve",
             decision_reason=REASON_HIGH_PROBABILITY,
             source_row_number=2, global_ordinal=0),
        # Row 3 — manual_review.
        _row("review@gmail.com", final_action="manual_review",
             decision_reason=REASON_MEDIUM_PROBABILITY,
             deliverability_probability=0.60, bucket_v2="review",
             source_row_number=3, global_ordinal=1),
        # Row 4 — auto_reject (low probability).
        _row("rejected@gmail.com", final_action="auto_reject",
             decision_reason=REASON_LOW_PROBABILITY,
             deliverability_probability=0.20, bucket_v2="invalid",
             source_row_number=4, global_ordinal=2),
        # Row 5 — auto_reject (SMTP invalid).
        _row("smtp_invalid@gmail.com", final_action="auto_reject",
             decision_reason=REASON_SMTP_INVALID,
             smtp_status=SMTP_STATUS_INVALID, bucket_v2="invalid",
             source_row_number=5, global_ordinal=3),
        # Row 6 — canonical winner of a duplicate pair (auto_approve).
        _row("dup@gmail.com", final_action="auto_approve",
             decision_reason=REASON_HIGH_PROBABILITY,
             is_canonical=True,
             source_row_number=6, global_ordinal=4),
        # Row 7 — non-canonical duplicate (loser).
        _row("dup@gmail.com", final_action="auto_approve",
             decision_reason=REASON_HIGH_PROBABILITY,
             is_canonical=False,
             source_row_number=7, global_ordinal=5),
        # Row 8 — V1 hard fail.
        _row("hard@bad.invalid", final_action="auto_reject",
             decision_reason=REASON_HARD_FAIL, hard_fail=True,
             deliverability_probability=0.0, bucket_v2="invalid",
             source_row_number=8, global_ordinal=6),
    ]


def _materialize_with(tmp_path: Path, rows: list[dict]) -> RunContext:
    cfg = load_config(base_dir=resolve_project_paths().project_root)
    logger = logging.getLogger("v25_test")
    logger.addHandler(logging.NullHandler())
    pipeline = EmailCleaningPipeline(config=cfg, logger=logger)

    rc = _build_run_context(tmp_path)
    staging = StagingDB(rc.staging_db_path)
    dedupe = DedupeIndex()

    for row in rows:
        if row.get("email_normalized") and bool(row.get("is_canonical", False)):
            dedupe.process_row(
                email_normalized=row["email_normalized"],
                hard_fail=bool(row.get("hard_fail", False)),
                score=int(row.get("score") or 0),
                completeness_score=int(row.get("completeness_score") or 0),
                source_file=str(row.get("source_file") or ""),
                source_row_number=int(row.get("source_row_number") or 0),
            )

    staging.append_chunk(_staging_frame(rows))
    pipeline._materialize(staging, dedupe, rc)
    staging.close()
    return rc


def _read(path: Path) -> list[dict[str, str]]:
    if not path.is_file() or path.stat().st_size == 0:
        return []
    with path.open(encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


# ---------------------------------------------------------------------------
# Layer 1 — Materialize emits separated CSVs
# ---------------------------------------------------------------------------


class TestSeparatedCSVs:
    def test_legacy_three_files_still_emitted(self, tmp_path: Path):
        rc = _materialize_with(tmp_path, _mixed_rows())
        for name in (
            "clean_high_confidence.csv",
            "review_medium_confidence.csv",
            "removed_invalid.csv",
        ):
            assert (rc.run_dir / name).is_file(), f"legacy file missing: {name}"

    def test_new_v25_files_emitted(self, tmp_path: Path):
        rc = _materialize_with(tmp_path, _mixed_rows())
        for name in ("removed_duplicates.csv", "removed_hard_fail.csv"):
            assert (rc.run_dir / name).is_file(), f"V2.5 file missing: {name}"

    def test_clean_csv_contains_only_auto_approve(self, tmp_path: Path):
        rc = _materialize_with(tmp_path, _mixed_rows())
        rows = _read(rc.run_dir / "clean_high_confidence.csv")
        # All rows in clean must have auto_approve as final_action.
        assert all(r["final_action"] == "auto_approve" for r in rows)
        # The canonical "approved" + canonical winner of the dup pair
        # both land here. Manual_review, auto_reject, hard_fail do not.
        emails = sorted(r["email"] for r in rows)
        assert emails == ["approved@gmail.com", "dup@gmail.com"]

    def test_review_csv_contains_only_manual_review(self, tmp_path: Path):
        rc = _materialize_with(tmp_path, _mixed_rows())
        rows = _read(rc.run_dir / "review_medium_confidence.csv")
        assert all(r["final_action"] == "manual_review" for r in rows)
        assert [r["email"] for r in rows] == ["review@gmail.com"]

    def test_removed_invalid_is_legacy_union(self, tmp_path: Path):
        rc = _materialize_with(tmp_path, _mixed_rows())
        rows = _read(rc.run_dir / "removed_invalid.csv")
        emails = {r["email"] for r in rows}
        # auto_reject, smtp_invalid, duplicate, hard_fail all here.
        assert "rejected@gmail.com" in emails
        assert "smtp_invalid@gmail.com" in emails
        assert "dup@gmail.com" in emails       # the duplicate (loser)
        assert "hard@bad.invalid" in emails

    def test_removed_duplicates_csv_contains_only_duplicates(self, tmp_path: Path):
        rc = _materialize_with(tmp_path, _mixed_rows())
        rows = _read(rc.run_dir / "removed_duplicates.csv")
        # Exactly the non-canonical "dup@gmail.com" row.
        assert len(rows) == 1
        assert rows[0]["email"] == "dup@gmail.com"
        assert rows[0]["final_output_reason"] == "removed_duplicate"

    def test_removed_hard_fail_csv_contains_only_hard_fails(self, tmp_path: Path):
        rc = _materialize_with(tmp_path, _mixed_rows())
        rows = _read(rc.run_dir / "removed_hard_fail.csv")
        assert len(rows) == 1
        assert rows[0]["email"] == "hard@bad.invalid"
        assert rows[0]["final_output_reason"] == "removed_hard_fail"

    def test_separated_files_are_strict_subsets_of_removed_invalid(
        self, tmp_path: Path
    ):
        rc = _materialize_with(tmp_path, _mixed_rows())
        union = {r["email"] for r in _read(rc.run_dir / "removed_invalid.csv")}
        dups = {r["email"] for r in _read(rc.run_dir / "removed_duplicates.csv")}
        hfs = {r["email"] for r in _read(rc.run_dir / "removed_hard_fail.csv")}
        assert dups.issubset(union)
        assert hfs.issubset(union)


# ---------------------------------------------------------------------------
# Layer 2 — V2 verification columns appear in client workbooks
# ---------------------------------------------------------------------------


class TestClientWorkbooksHaveV2Fields:
    def test_review_workbook_includes_v2_verification_columns(
        self, tmp_path: Path
    ):
        rc = _materialize_with(tmp_path, _mixed_rows())
        generate_client_outputs(rc.run_dir)
        path = rc.run_dir / "review_emails.xlsx"
        assert path.is_file()
        df = pd.read_excel(path, dtype=str)
        # Expected V2 verification columns in the review workbook.
        for col in (
            "final_action",
            "decision_reason",
            "decision_confidence",
            "deliverability_probability",
            "smtp_status",
            "catch_all_status",
            "catch_all_flag",
            "final_output_reason",
        ):
            assert col in df.columns, f"missing V2 column in review_emails.xlsx: {col}"

    def test_safe_workbook_includes_v2_verification_columns(
        self, tmp_path: Path
    ):
        rc = _materialize_with(tmp_path, _mixed_rows())
        generate_client_outputs(rc.run_dir)
        path = rc.run_dir / "valid_emails.xlsx"
        assert path.is_file()
        df = pd.read_excel(path, dtype=str)
        # Safe export must show evidence for every approved row.
        for col in (
            "final_action",
            "smtp_status",
            "smtp_confirmed_valid",
            "catch_all_status",
            "catch_all_flag",
        ):
            assert col in df.columns

    def test_duplicate_workbook_emitted_when_duplicates_exist(
        self, tmp_path: Path
    ):
        rc = _materialize_with(tmp_path, _mixed_rows())
        generate_client_outputs(rc.run_dir)
        path = rc.run_dir / "duplicate_emails.xlsx"
        assert path.is_file()
        df = pd.read_excel(path, dtype=str)
        emails = list(df["email"]) if "email" in df.columns else []
        assert emails == ["dup@gmail.com"]

    def test_hard_fail_workbook_emitted_when_hard_fails_exist(
        self, tmp_path: Path
    ):
        rc = _materialize_with(tmp_path, _mixed_rows())
        generate_client_outputs(rc.run_dir)
        path = rc.run_dir / "hard_fail_emails.xlsx"
        assert path.is_file()
        df = pd.read_excel(path, dtype=str)
        emails = list(df["email"]) if "email" in df.columns else []
        assert emails == ["hard@bad.invalid"]


# ---------------------------------------------------------------------------
# Layer 3 — Approved original-format export contract
# ---------------------------------------------------------------------------


class TestApprovedOriginalFormatLeakInvariants:
    """``generate_approved_original_format`` must source rows ONLY from
    ``clean_high_confidence.csv`` (V2 ``auto_approve`` only)."""

    def _make_original_input(self, tmp_path: Path) -> Path:
        """Write a 7-row original CSV mirroring our staged emails."""
        original_csv = tmp_path / "f.csv"
        original_csv.write_text(
            "email,domain,fname\n"
            "approved@gmail.com,gmail.com,Alice\n"
            "review@gmail.com,gmail.com,Bob\n"
            "rejected@gmail.com,gmail.com,Carol\n"
            "smtp_invalid@gmail.com,gmail.com,Dave\n"
            "dup@gmail.com,gmail.com,Eve1\n"
            "dup@gmail.com,gmail.com,Eve2\n"
            "hard@bad.invalid,bad.invalid,Frank\n",
            encoding="utf-8",
        )
        return original_csv

    def test_only_auto_approve_rows_reach_approved_original_format(
        self, tmp_path: Path
    ):
        rc = _materialize_with(tmp_path, _mixed_rows())
        original_csv = self._make_original_input(tmp_path)

        out_path = generate_approved_original_format(
            rc.run_dir, [original_csv]
        )
        assert out_path is not None and out_path.is_file()

        df = pd.read_excel(out_path, dtype=str)
        emails = sorted(df["email"].tolist())
        # Only the two auto_approve canonical rows make it.
        assert emails == ["approved@gmail.com", "dup@gmail.com"]
        # Manual_review must NOT appear.
        assert "review@gmail.com" not in emails
        # Auto_reject (low_probability + smtp_invalid) must NOT appear.
        assert "rejected@gmail.com" not in emails
        assert "smtp_invalid@gmail.com" not in emails
        # Hard_fail must NOT appear.
        assert "hard@bad.invalid" not in emails

    def test_no_safe_rows_no_approved_original_format(self, tmp_path: Path):
        # All rows manual_review → zero clean rows.
        rows = [
            _row(
                "review_only@gmail.com",
                final_action="manual_review",
                decision_reason=REASON_MEDIUM_PROBABILITY,
                deliverability_probability=0.60,
                bucket_v2="review",
            )
        ]
        rc = _materialize_with(tmp_path, rows)

        original_csv = tmp_path / "f.csv"
        original_csv.write_text(
            "email,domain,fname\nreview_only@gmail.com,gmail.com,X\n",
            encoding="utf-8",
        )
        out_path = generate_approved_original_format(
            rc.run_dir, [original_csv]
        )
        assert out_path is None  # absent, no crash

    def test_smtp_invalid_cannot_leak_into_approved_original_format(
        self, tmp_path: Path
    ):
        rows = [
            # Auto-approve canonical row.
            _row(
                "ok@gmail.com",
                final_action="auto_approve",
                decision_reason=REASON_HIGH_PROBABILITY,
            ),
            # Auto-reject due to SMTP invalid.
            _row(
                "bad@gmail.com",
                final_action="auto_reject",
                decision_reason=REASON_SMTP_INVALID,
                smtp_status=SMTP_STATUS_INVALID,
                bucket_v2="invalid",
                source_row_number=3,
                global_ordinal=1,
            ),
        ]
        rc = _materialize_with(tmp_path, rows)
        original_csv = tmp_path / "f.csv"
        original_csv.write_text(
            "email,domain,fname\n"
            "ok@gmail.com,gmail.com,A\n"
            "bad@gmail.com,gmail.com,B\n",
            encoding="utf-8",
        )
        out_path = generate_approved_original_format(
            rc.run_dir, [original_csv]
        )
        df = pd.read_excel(out_path, dtype=str)
        assert sorted(df["email"].tolist()) == ["ok@gmail.com"]


# ---------------------------------------------------------------------------
# Layer 4 — Summary report carries V2-meaningful counts
# ---------------------------------------------------------------------------


class TestSummaryReportV2Counts:
    def test_summary_includes_v2_metrics(self, tmp_path: Path):
        rc = _materialize_with(tmp_path, _mixed_rows())
        generate_client_outputs(rc.run_dir)
        summary = pd.read_excel(rc.run_dir / "summary_report.xlsx", sheet_name="totals")
        metrics = dict(zip(summary["metric"].astype(str), summary["value"]))

        # V2.5 — V2-semantic counts must be present.
        for key in (
            "safe_approved_count",
            "manual_review_count",
            "rejected_count",
            "duplicate_count",
            "hard_fail_count",
            "smtp_verified_count",
            "catch_all_risk_count",
            "unknown_or_unverified_count",
        ):
            assert key in metrics, f"missing V2 summary metric: {key}"

        # And the counts are consistent with the staged rows.
        assert int(metrics["safe_approved_count"]) == 2
        assert int(metrics["manual_review_count"]) == 1
        assert int(metrics["duplicate_count"]) == 1
        assert int(metrics["hard_fail_count"]) == 1
        # rejected = total_invalid - duplicate - hard_fail.
        # total_invalid = 4 (rejected + smtp_invalid + dup + hard_fail);
        # 4 - 1 - 1 = 2 → rejected (rejected@gmail.com + smtp_invalid).
        assert int(metrics["rejected_count"]) == 2
        # SMTP-verified = rows with smtp_confirmed_valid=True in clean.
        assert int(metrics["smtp_verified_count"]) == 2

    def test_summary_legacy_metrics_still_present(self, tmp_path: Path):
        """V2.5 must not drop the legacy summary keys."""
        rc = _materialize_with(tmp_path, _mixed_rows())
        generate_client_outputs(rc.run_dir)
        summary = pd.read_excel(
            rc.run_dir / "summary_report.xlsx", sheet_name="totals"
        )
        metrics = dict(zip(summary["metric"].astype(str), summary["value"]))
        for key in (
            "total_input_rows",
            "total_valid",
            "total_review",
            "total_invalid_or_bounce_risk",
            "duplicates_removed",
            "typo_corrections",
        ):
            assert key in metrics


# ---------------------------------------------------------------------------
# Layer 5 — Manifest discovery (collect_job_artifacts)
# ---------------------------------------------------------------------------


class TestArtifactManifest:
    def test_collect_job_artifacts_includes_new_csvs(self, tmp_path: Path):
        rc = _materialize_with(tmp_path, _mixed_rows())
        artifacts = collect_job_artifacts(rc.run_dir)
        assert artifacts.technical_csvs.removed_duplicates is not None
        assert artifacts.technical_csvs.removed_hard_fail is not None
        assert artifacts.technical_csvs.removed_duplicates.is_file()
        assert artifacts.technical_csvs.removed_hard_fail.is_file()

    def test_collect_job_artifacts_includes_new_xlsx_workbooks(
        self, tmp_path: Path
    ):
        rc = _materialize_with(tmp_path, _mixed_rows())
        generate_client_outputs(rc.run_dir)
        artifacts = collect_job_artifacts(rc.run_dir)
        assert artifacts.client_outputs.duplicate_emails is not None
        assert artifacts.client_outputs.hard_fail_emails is not None
        assert artifacts.client_outputs.duplicate_emails.is_file()
        assert artifacts.client_outputs.hard_fail_emails.is_file()

    def test_legacy_artifacts_still_discovered(self, tmp_path: Path):
        rc = _materialize_with(tmp_path, _mixed_rows())
        generate_client_outputs(rc.run_dir)
        artifacts = collect_job_artifacts(rc.run_dir)
        # Legacy fields still resolved.
        assert artifacts.technical_csvs.clean_high_confidence is not None
        assert artifacts.technical_csvs.review_medium_confidence is not None
        assert artifacts.technical_csvs.removed_invalid is not None
        assert artifacts.client_outputs.valid_emails is not None
        assert artifacts.client_outputs.review_emails is not None
        assert artifacts.client_outputs.invalid_or_bounce_risk is not None
        assert artifacts.client_outputs.summary_report is not None


# ---------------------------------------------------------------------------
# Layer 6 — Export does not route on V1 fields
# ---------------------------------------------------------------------------


class TestExportRoutingNotV1Driven:
    def test_v1_high_confidence_with_v2_review_routes_to_review_export(
        self, tmp_path: Path
    ):
        """A row whose V1 ``preliminary_bucket`` was ``high_confidence``
        but V2 said ``manual_review`` must NEVER land in the safe export.
        This mirrors the V2.1 acceptance test, lifted to the export level.
        """
        rows = [
            _row(
                "v1high_v2review@gmail.com",
                final_action="manual_review",
                decision_reason=REASON_MEDIUM_PROBABILITY,
                deliverability_probability=0.60,
                bucket_v2="review",
            )
        ]
        rc = _materialize_with(tmp_path, rows)
        generate_client_outputs(rc.run_dir)

        valid_df = pd.read_excel(rc.run_dir / "valid_emails.xlsx", dtype=str)
        review_df = pd.read_excel(rc.run_dir / "review_emails.xlsx", dtype=str)

        valid_emails = list(valid_df.get("email", pd.Series(dtype=str)))
        review_emails = list(review_df.get("email", pd.Series(dtype=str)))

        assert valid_emails == []
        assert review_emails == ["v1high_v2review@gmail.com"]


# ---------------------------------------------------------------------------
# Layer 7 — End-to-end via run_cleaning_job (real api_boundary path)
# ---------------------------------------------------------------------------


class TestRunCleaningJobEndToEnd:
    """The full ``run_cleaning_job`` path produces the new V2.5 artifacts.

    Under the autouse offline SMTP stub, no row clears the auto_approve
    threshold, so the safe-export workbook may be empty. We assert
    presence of the new files and the manifest fields rather than
    specific row counts.
    """

    def test_pipeline_writes_v25_separated_csvs(
        self, tmp_path_factory: pytest.TempPathFactory
    ):
        sample = (
            Path(__file__).resolve().parent.parent
            / "examples"
            / "sample_contacts.csv"
        )
        if not sample.is_file():
            pytest.skip("examples/sample_contacts.csv not available")

        out = tmp_path_factory.mktemp("v25_e2e")
        result = run_cleaning_job(
            input_path=sample,
            output_root=out,
            job_id="test_v25_e2e",
        )
        assert result.status == JobStatus.COMPLETED, f"Job failed: {result.error}"
        run_dir = result.artifacts.run_dir
        # V2.5 — separated CSVs exist after a real run.
        assert (run_dir / "removed_duplicates.csv").is_file()
        assert (run_dir / "removed_hard_fail.csv").is_file()
        # And the manifest exposes them.
        assert result.artifacts.technical_csvs.removed_duplicates is not None
        assert result.artifacts.technical_csvs.removed_hard_fail is not None
