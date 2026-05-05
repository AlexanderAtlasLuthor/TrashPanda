"""Tests for the Emergency Extra Strict Offline cleaner."""

from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd
import pytest

from app.extra_strict_clean import (
    ExtraStrictConfig,
    run_extra_strict_clean,
)


_FIELDS = [
    "email",
    "domain",
    "corrected_domain",
    "hard_fail",
    "smtp_status",
    "catch_all_flag",
    "domain_risk_level",
    "deliverability_probability",
    "final_action",
    "v2_final_bucket",
]


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in _FIELDS})


@pytest.fixture()
def sample_run(tmp_path: Path) -> Path:
    """Build a tiny run directory with one row per outcome we care about."""

    # clean_high_confidence: a few obviously safe rows
    _write_csv(
        tmp_path / "clean_high_confidence.csv",
        [
            {
                "email": "alice@example-corp.com",
                "domain": "example-corp.com",
                "smtp_status": "valid",
                "deliverability_probability": "0.95",
                "final_action": "auto_approve",
                "v2_final_bucket": "clean",
            },
            {
                "email": "bob@example-corp.com",
                "domain": "example-corp.com",
                "smtp_status": "not_tested",
                "deliverability_probability": "0.85",
                "final_action": "auto_approve",
                "v2_final_bucket": "clean",
            },
        ],
    )

    # review_medium_confidence: yahoo (catch-all/opaque), low probability,
    # role-based.
    _write_csv(
        tmp_path / "review_medium_confidence.csv",
        [
            {
                "email": "person@yahoo.com",
                "domain": "yahoo.com",
                "smtp_status": "not_tested",
                "deliverability_probability": "0.85",
                "final_action": "manual_review",
                "v2_final_bucket": "review",
            },
            {
                "email": "weak@example-corp.com",
                "domain": "example-corp.com",
                "smtp_status": "not_tested",
                "deliverability_probability": "0.40",
                "final_action": "manual_review",
                "v2_final_bucket": "review",
            },
            {
                "email": "info@example-corp.com",
                "domain": "example-corp.com",
                "smtp_status": "not_tested",
                "deliverability_probability": "0.90",
                "final_action": "manual_review",
                "v2_final_bucket": "review",
            },
            {
                "email": "risky@bad-corp.com",
                "domain": "bad-corp.com",
                "smtp_status": "not_tested",
                "deliverability_probability": "0.85",
                "domain_risk_level": "high",
                "final_action": "manual_review",
                "v2_final_bucket": "review",
            },
            {
                "email": "ca-detected@catchall-corp.com",
                "domain": "catchall-corp.com",
                "smtp_status": "valid",
                "catch_all_flag": "true",
                "deliverability_probability": "0.90",
                "final_action": "manual_review",
                "v2_final_bucket": "review",
            },
        ],
    )

    # removed_invalid: hard fail + duplicate
    _write_csv(
        tmp_path / "removed_invalid.csv",
        [
            {
                "email": "broken@",
                "domain": "",
                "hard_fail": "true",
                "smtp_status": "not_tested",
                "final_action": "auto_reject",
                "v2_final_bucket": "hard_fail",
            },
            {
                "email": "dup@example-corp.com",
                "domain": "example-corp.com",
                "smtp_status": "not_tested",
                "final_action": "auto_reject",
                "v2_final_bucket": "duplicate",
            },
        ],
    )
    return tmp_path


def _read_xlsx(path: Path) -> pd.DataFrame:
    return pd.read_excel(path, dtype=str).fillna("")


class TestExtraStrictClean:
    def test_yahoo_class_goes_to_review_not_primary(self, sample_run: Path) -> None:
        result = run_extra_strict_clean(sample_run)
        review = _read_xlsx(result.review_xlsx)
        primary = _read_xlsx(result.primary_xlsx)

        assert "person@yahoo.com" in review["email"].tolist()
        assert "person@yahoo.com" not in primary["email"].tolist()

    def test_low_probability_is_suppressed(self, sample_run: Path) -> None:
        result = run_extra_strict_clean(sample_run)
        removed = _read_xlsx(result.removed_xlsx)
        assert "weak@example-corp.com" in removed["email"].tolist()

    def test_role_based_is_suppressed(self, sample_run: Path) -> None:
        result = run_extra_strict_clean(sample_run)
        removed = _read_xlsx(result.removed_xlsx)
        assert "info@example-corp.com" in removed["email"].tolist()

    def test_high_risk_domain_is_suppressed(self, sample_run: Path) -> None:
        result = run_extra_strict_clean(sample_run)
        removed = _read_xlsx(result.removed_xlsx)
        assert "risky@bad-corp.com" in removed["email"].tolist()

    def test_catch_all_flag_routes_to_review(self, sample_run: Path) -> None:
        result = run_extra_strict_clean(sample_run)
        review = _read_xlsx(result.review_xlsx)
        assert "ca-detected@catchall-corp.com" in review["email"].tolist()

    def test_smtp_valid_high_probability_is_confirmed_safe(
        self, sample_run: Path
    ) -> None:
        result = run_extra_strict_clean(sample_run)
        primary = _read_xlsx(result.primary_xlsx)
        rows = primary[primary["email"] == "alice@example-corp.com"]
        assert not rows.empty
        assert rows.iloc[0]["trashpanda_final_action"] == "confirmed_safe"
        assert rows.iloc[0]["trashpanda_confirmation_level"] == "smtp_confirmed"

    def test_offline_high_probability_is_recommended_send(
        self, sample_run: Path
    ) -> None:
        result = run_extra_strict_clean(sample_run)
        primary = _read_xlsx(result.primary_xlsx)
        rows = primary[primary["email"] == "bob@example-corp.com"]
        assert not rows.empty
        assert rows.iloc[0]["trashpanda_final_action"] == "recommended_send"
        assert rows.iloc[0]["trashpanda_confirmation_level"] == "offline_only"

    def test_hard_fail_is_suppressed(self, sample_run: Path) -> None:
        result = run_extra_strict_clean(sample_run)
        removed = _read_xlsx(result.removed_xlsx)
        emails = removed["email"].tolist()
        assert any(e.startswith("broken@") for e in emails)

    def test_summary_and_readme_written(self, sample_run: Path) -> None:
        result = run_extra_strict_clean(sample_run)
        assert result.summary_txt.is_file()
        assert result.readme_txt.is_file()
        assert (result.out_dir / "extra_strict_summary.json").is_file()
        text = result.readme_txt.read_text(encoding="utf-8")
        assert "clean_final_extra_strict.xlsx" in text

    def test_threshold_configurable(self, sample_run: Path) -> None:
        config = ExtraStrictConfig(min_deliverability_probability=0.30)
        result = run_extra_strict_clean(sample_run, config=config)
        primary = _read_xlsx(result.primary_xlsx)
        # weak@... had probability 0.40 — at threshold 0.30 it survives
        # *if* the other gates also pass. It is on example-corp.com,
        # not role-based, no domain risk → recommended_send.
        assert "weak@example-corp.com" in primary["email"].tolist()

    def test_allow_role_based(self, sample_run: Path) -> None:
        config = ExtraStrictConfig(role_based_excluded=False)
        result = run_extra_strict_clean(sample_run, config=config)
        primary = _read_xlsx(result.primary_xlsx)
        assert "info@example-corp.com" in primary["email"].tolist()

    def test_empty_run_dir_does_not_crash(self, tmp_path: Path) -> None:
        result = run_extra_strict_clean(tmp_path)
        assert result.total_rows == 0
        assert result.primary_xlsx.is_file()

    def test_explanatory_columns_present(self, sample_run: Path) -> None:
        result = run_extra_strict_clean(sample_run)
        primary = _read_xlsx(result.primary_xlsx)
        expected = {
            "trashpanda_final_action",
            "trashpanda_risk_tier",
            "trashpanda_smtp_status",
            "trashpanda_confirmation_level",
            "trashpanda_provider_class",
            "trashpanda_deliverability_probability",
            "trashpanda_reason",
            "trashpanda_recommended_action",
            "trashpanda_deliverability_note",
        }
        assert expected.issubset(set(primary.columns))
