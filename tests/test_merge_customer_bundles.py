"""Tests for ``scripts/merge_customer_bundles``."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from scripts.merge_customer_bundles import merge_bundles
from app.customer_bundle import (
    CLEAN_DELIVERABLE_CSV,
    CUSTOMER_BUNDLE_DIRNAME,
    CUSTOMER_README,
    HIGH_RISK_REMOVED_CSV,
    REVIEW_PROVIDER_LIMITED_CSV,
)
from app.pilot_send.evidence import SMTP_EVIDENCE_REPORT_FILENAME


def _make_bundle(
    base: Path,
    *,
    clean: list[str] | None = None,
    review: list[str] | None = None,
    removed: list[str] | None = None,
    evidence: list[dict] | None = None,
) -> Path:
    bundle_dir = base / CUSTOMER_BUNDLE_DIRNAME
    bundle_dir.mkdir(parents=True, exist_ok=True)

    def _write(filename: str, emails: list[str] | None) -> None:
        df = pd.DataFrame({"email": emails or []})
        df.to_csv(bundle_dir / filename, index=False)

    _write(CLEAN_DELIVERABLE_CSV, clean)
    _write(REVIEW_PROVIDER_LIMITED_CSV, review)
    _write(HIGH_RISK_REMOVED_CSV, removed)
    pd.DataFrame(evidence or []).to_csv(
        bundle_dir / SMTP_EVIDENCE_REPORT_FILENAME, index=False,
    )
    return base


def _read_emails(path: Path) -> set[str]:
    df = pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False)
    return set(df["email"].astype(str).str.lower())


class TestBasicMerge:
    def test_concatenates_disjoint_clean_lists(self, tmp_path: Path):
        a = _make_bundle(tmp_path / "a", clean=["a@x.com", "b@x.com"])
        b = _make_bundle(tmp_path / "b", clean=["c@x.com", "d@x.com"])
        out = tmp_path / "merged"

        counts = merge_bundles([a, b], output_dir=out)

        assert counts["clean_deliverable"] == 4
        emails = _read_emails(
            out / CUSTOMER_BUNDLE_DIRNAME / CLEAN_DELIVERABLE_CSV,
        )
        assert emails == {"a@x.com", "b@x.com", "c@x.com", "d@x.com"}

    def test_dedupes_overlap_by_email_case_insensitive(self, tmp_path: Path):
        a = _make_bundle(tmp_path / "a", clean=["Alice@x.com"])
        b = _make_bundle(tmp_path / "b", clean=["alice@X.COM", "bob@x.com"])
        out = tmp_path / "merged"

        counts = merge_bundles([a, b], output_dir=out)

        assert counts["clean_deliverable"] == 2  # alice deduped


class TestContradictionResolution:
    def test_removed_in_any_bundle_excludes_from_clean(self, tmp_path: Path):
        a = _make_bundle(tmp_path / "a", clean=["x@y.com"])
        b = _make_bundle(tmp_path / "b", removed=["x@y.com"])
        out = tmp_path / "merged"

        counts = merge_bundles([a, b], output_dir=out)

        clean = _read_emails(
            out / CUSTOMER_BUNDLE_DIRNAME / CLEAN_DELIVERABLE_CSV,
        )
        removed = _read_emails(
            out / CUSTOMER_BUNDLE_DIRNAME / HIGH_RISK_REMOVED_CSV,
        )
        assert "x@y.com" not in clean
        assert "x@y.com" in removed
        assert counts["clean_deliverable"] == 0
        assert counts["high_risk_removed"] == 1

    def test_review_in_any_bundle_excludes_from_clean(self, tmp_path: Path):
        a = _make_bundle(tmp_path / "a", clean=["x@y.com"])
        b = _make_bundle(tmp_path / "b", review=["x@y.com"])
        out = tmp_path / "merged"

        merge_bundles([a, b], output_dir=out)

        clean = _read_emails(
            out / CUSTOMER_BUNDLE_DIRNAME / CLEAN_DELIVERABLE_CSV,
        )
        review = _read_emails(
            out / CUSTOMER_BUNDLE_DIRNAME / REVIEW_PROVIDER_LIMITED_CSV,
        )
        assert "x@y.com" not in clean
        assert "x@y.com" in review

    def test_removed_excludes_from_review_too(self, tmp_path: Path):
        a = _make_bundle(tmp_path / "a", review=["x@y.com"])
        b = _make_bundle(tmp_path / "b", removed=["x@y.com"])
        out = tmp_path / "merged"

        merge_bundles([a, b], output_dir=out)

        review = _read_emails(
            out / CUSTOMER_BUNDLE_DIRNAME / REVIEW_PROVIDER_LIMITED_CSV,
        )
        removed = _read_emails(
            out / CUSTOMER_BUNDLE_DIRNAME / HIGH_RISK_REMOVED_CSV,
        )
        assert "x@y.com" not in review
        assert "x@y.com" in removed


class TestStructure:
    def test_emits_readme_with_source_list_and_counts(self, tmp_path: Path):
        a = _make_bundle(
            tmp_path / "a",
            clean=["a@x.com"], removed=["b@x.com"],
        )
        b = _make_bundle(
            tmp_path / "b",
            clean=["c@x.com"], review=["d@x.com"],
        )
        out = tmp_path / "merged"

        merge_bundles([a, b], output_dir=out)

        readme = (
            out / CUSTOMER_BUNDLE_DIRNAME / CUSTOMER_README
        ).read_text(encoding="utf-8")
        assert "merged customer bundle" in readme.lower()
        # Source paths surface in the README so the operator can audit.
        assert "a/customer_bundle" in readme
        assert "b/customer_bundle" in readme
        # Counts are surfaced.
        assert "2 rows" in readme  # clean = a + c

    def test_accepts_bundle_dir_directly(self, tmp_path: Path):
        # If the operator passes the customer_bundle dir directly
        # (rather than the run dir), still works.
        a_run = _make_bundle(tmp_path / "a", clean=["a@x.com"])
        a_bundle = a_run / CUSTOMER_BUNDLE_DIRNAME
        out = tmp_path / "merged"

        counts = merge_bundles([a_bundle], output_dir=out)
        assert counts["clean_deliverable"] == 1

    def test_missing_bundle_raises(self, tmp_path: Path):
        empty = tmp_path / "empty"
        empty.mkdir()
        with pytest.raises(FileNotFoundError):
            merge_bundles([empty], output_dir=tmp_path / "out")


class TestEvidenceReportPassthrough:
    def test_evidence_rows_preserved_and_deduped(self, tmp_path: Path):
        a = _make_bundle(
            tmp_path / "a",
            evidence=[
                {"email": "a@x.com", "bucket": "delivered",
                 "evidence_class": "accepted"},
                {"email": "shared@x.com", "bucket": "hard_bounce",
                 "evidence_class": "rcpt_refused"},
            ],
        )
        b = _make_bundle(
            tmp_path / "b",
            evidence=[
                # Duplicate email — first occurrence wins.
                {"email": "shared@x.com", "bucket": "delivered",
                 "evidence_class": "accepted"},
                {"email": "b@x.com", "bucket": "infrastructure_blocked",
                 "evidence_class": "infra_blocked"},
            ],
        )
        out = tmp_path / "merged"

        counts = merge_bundles([a, b], output_dir=out)
        assert counts["smtp_evidence_report"] == 3
        df = pd.read_csv(
            out / CUSTOMER_BUNDLE_DIRNAME / SMTP_EVIDENCE_REPORT_FILENAME,
            dtype=str, keep_default_na=False, na_filter=False,
        )
        # First-occurrence wins on the duplicate.
        shared_row = df.loc[df["email"] == "shared@x.com"].iloc[0]
        assert shared_row["bucket"] == "hard_bounce"
