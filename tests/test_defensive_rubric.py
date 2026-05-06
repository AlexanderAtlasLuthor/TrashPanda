"""V2.10.14 — defensive_rubric tests."""

from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd

from app.defensive_rubric import (
    CLASSIFICATION_CLEAN,
    CLASSIFICATION_REMOVED,
    CLASSIFICATION_RISKY,
    CSV_COLUMNS,
    DEFENSIVE_RUBRIC_REPORT_FILENAME,
    classify_row,
    classify_run,
    emit_rubric,
)


def _row(**overrides) -> dict:
    """Default 'all-pass' row, override layers as needed."""
    base = {
        "email": "user@example.com",
        "syntax_valid": "true",
        "has_mx_record": "true",
        "score_reasons": "",
        "client_reason": "",
        "domain_risk_level": "low",
    }
    base.update(overrides)
    return base


class TestClassification:
    def test_all_layers_pass_is_clean(self):
        result = classify_row(_row())
        assert result.classification == CLASSIFICATION_CLEAN
        assert result.syntax_pass and result.mx_pass
        assert result.disposable_pass and result.role_pass
        assert result.domain_risk_pass

    def test_syntax_fail_removes_row(self):
        result = classify_row(_row(syntax_valid="false"))
        assert result.classification == CLASSIFICATION_REMOVED
        assert "syntax" in result.reason

    def test_no_mx_removes_row(self):
        result = classify_row(_row(has_mx_record="false"))
        assert result.classification == CLASSIFICATION_REMOVED
        assert "no_mx" in result.reason

    def test_disposable_via_score_reasons_removes_row(self):
        result = classify_row(_row(score_reasons="disposable"))
        assert result.classification == CLASSIFICATION_REMOVED
        assert "disposable" in result.reason

    def test_disposable_via_client_reason_removes_row(self):
        result = classify_row(
            _row(client_reason="Temporary/disposable email"),
        )
        assert result.classification == CLASSIFICATION_REMOVED
        assert not result.disposable_pass

    def test_role_account_makes_row_risky(self):
        result = classify_row(_row(score_reasons="role_account"))
        assert result.classification == CLASSIFICATION_RISKY
        assert "role_account" in result.reason
        # Underlying layer-pass flag reflects the failure.
        assert not result.role_pass

    def test_high_domain_risk_makes_row_risky(self):
        result = classify_row(_row(domain_risk_level="high"))
        assert result.classification == CLASSIFICATION_RISKY
        assert "domain_risk=high" in result.reason

    def test_unknown_domain_risk_makes_row_risky(self):
        # We don't know → don't promise. Conservative.
        result = classify_row(_row(domain_risk_level=""))
        assert result.classification == CLASSIFICATION_RISKY
        assert "domain_risk=unknown" in result.reason

    def test_removal_trumps_risky(self):
        # Disposable + role: still removed (more severe wins).
        result = classify_row(
            _row(score_reasons="disposable|role_account"),
        )
        assert result.classification == CLASSIFICATION_REMOVED

    def test_score_reasons_substring_does_not_false_match(self):
        # Token-based matching — "non_role_account" should not trigger
        # role detection. (Hypothetical future token.)
        result = classify_row(_row(score_reasons="some_other_signal"))
        assert result.classification == CLASSIFICATION_CLEAN


class TestRunIntegration:
    def test_classify_run_reads_all_three_csvs(self, tmp_path: Path):
        pd.DataFrame([
            _row(email="clean@x.com"),
        ]).to_csv(tmp_path / "clean_high_confidence.csv", index=False)
        pd.DataFrame([
            _row(email="role@x.com", score_reasons="role_account"),
        ]).to_csv(tmp_path / "review_medium_confidence.csv", index=False)
        pd.DataFrame([
            _row(email="bad@x.com", syntax_valid="false"),
        ]).to_csv(tmp_path / "removed_invalid.csv", index=False)

        rubric = classify_run(tmp_path)

        assert set(rubric.keys()) == {"clean@x.com", "role@x.com", "bad@x.com"}
        assert rubric["clean@x.com"].classification == CLASSIFICATION_CLEAN
        assert rubric["role@x.com"].classification == CLASSIFICATION_RISKY
        assert rubric["bad@x.com"].classification == CLASSIFICATION_REMOVED

    def test_emit_rubric_writes_report(self, tmp_path: Path):
        pd.DataFrame([
            _row(email="a@x.com"),
            _row(email="b@x.com", domain_risk_level="high"),
        ]).to_csv(tmp_path / "clean_high_confidence.csv", index=False)

        path, rubric = emit_rubric(tmp_path)
        assert path.is_file()
        assert path.name == DEFENSIVE_RUBRIC_REPORT_FILENAME

        with path.open("r", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        assert len(rows) == 2
        # Schema check: exact column set.
        assert tuple(rows[0].keys()) == CSV_COLUMNS

    def test_empty_run_dir_emits_header_only(self, tmp_path: Path):
        path, rubric = emit_rubric(tmp_path)
        assert path.is_file()
        assert rubric == {}
        with path.open("r", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        assert rows == []

    def test_duplicate_email_first_wins(self, tmp_path: Path):
        # Same email in clean and removed CSVs — clean (first read) wins.
        pd.DataFrame([_row(email="dup@x.com")]).to_csv(
            tmp_path / "clean_high_confidence.csv", index=False,
        )
        pd.DataFrame([
            _row(email="dup@x.com", syntax_valid="false"),
        ]).to_csv(tmp_path / "removed_invalid.csv", index=False)

        rubric = classify_run(tmp_path)
        assert rubric["dup@x.com"].classification == CLASSIFICATION_CLEAN
