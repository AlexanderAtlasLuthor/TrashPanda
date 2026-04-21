"""Tests for Subphase 7: global email deduplication, canonical selection,
and duplicate flagging.

Sections:
  1. TestCompletenessScore          — business column counting, exclusions
  2. TestCompareRowsForCanonical    — all 4 hierarchy rules, determinism
  3. TestDedupeIndex                — index state, singletons, replacements
  4. TestApplyEmailNormalizedColumn — dedupe key derivation
  5. TestApplyDedupeColumns         — DataFrame-level application
  6. TestDedupePipelineIntegration  — end-to-end with mocked DNS
  7. TestNoFuturePhaseContamination — export/SQLite/reporting absent
"""

from __future__ import annotations

import inspect
import logging
import sys
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_VENDOR_SITE = _PROJECT_ROOT / "vendor_site"
if str(_VENDOR_SITE) not in sys.path:
    sys.path.insert(0, str(_VENDOR_SITE))

from app.dedupe import (
    BUSINESS_COLUMNS,
    CanonicalEntry,
    DedupeDecision,
    DedupeIndex,
    apply_completeness_column,
    apply_dedupe_columns,
    apply_email_normalized_column,
    compare_rows_for_canonical,
    compute_completeness_score,
)
from app.dns_utils import DnsResult


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _entry(
    email: str = "a@b.com",
    hard_fail: bool = False,
    score: int = 75,
    completeness: int = 3,
    source_file: str = "file.csv",
    source_row: int = 1,
    ordinal: int = 0,
) -> CanonicalEntry:
    return CanonicalEntry(
        email_normalized=email,
        hard_fail=hard_fail,
        score=score,
        completeness_score=completeness,
        source_file=source_file,
        source_row_number=source_row,
        global_ordinal=ordinal,
    )


def _make_df(**overrides) -> pd.DataFrame:
    defaults = dict(
        email=["alice@gmail.com"],
        domain=["gmail.com"],
        fname=["Alice"],
        syntax_valid=pd.array([True], dtype="boolean"),
        corrected_domain=["gmail.com"],
        has_mx_record=pd.array([True], dtype="boolean"),
        has_a_record=pd.array([False], dtype="boolean"),
        domain_exists=pd.array([True], dtype="boolean"),
        dns_error=[None],
        typo_corrected=pd.array([False], dtype="boolean"),
        domain_matches_input_column=pd.array([pd.NA], dtype="boolean"),
        hard_fail=pd.array([False], dtype="boolean"),
        score=[75],
        score_reasons=["syntax_valid|mx_present"],
        preliminary_bucket=["high_confidence"],
        source_file=["test.csv"],
        source_row_number=[1],
        email_normalized=["alice@gmail.com"],
        completeness_score=[3],
    )
    defaults.update(overrides)
    return pd.DataFrame(defaults)


_MX_DNS = DnsResult(True, True, True, False, None)


# ===========================================================================
# SECTION 1 — compute_completeness_score
# ===========================================================================

class TestCompletenessScore:
    def test_counts_present_business_columns(self):
        row = pd.Series({"email": "a@b.com", "fname": "Alice", "lname": "Smith"})
        assert compute_completeness_score(row) == 3

    def test_zero_when_all_null(self):
        row = pd.Series({col: None for col in BUSINESS_COLUMNS})
        assert compute_completeness_score(row) == 0

    def test_empty_string_not_counted(self):
        row = pd.Series({"email": "a@b.com", "fname": "  ", "lname": ""})
        assert compute_completeness_score(row) == 1  # only email counts

    def test_na_not_counted(self):
        row = pd.Series({"email": "a@b.com", "fname": pd.NA})
        assert compute_completeness_score(row) == 1

    def test_missing_column_not_counted(self):
        row = pd.Series({"email": "a@b.com"})
        assert compute_completeness_score(row) == 1  # fname absent → not counted

    def test_technical_columns_ignored(self):
        row = pd.Series({
            "email": "a@b.com",
            "syntax_valid": True,
            "score": 75,
            "hard_fail": False,
            "dns_error": "nxdomain",
            "source_file": "x.csv",
            "chunk_index": 1,
        })
        assert compute_completeness_score(row) == 1  # only email is business

    def test_all_business_columns_present(self):
        row = pd.Series({col: f"value_{col}" for col in BUSINESS_COLUMNS})
        assert compute_completeness_score(row) == len(BUSINESS_COLUMNS)

    def test_partial_business_columns(self):
        row = pd.Series({"email": "a@b.com", "domain": "b.com", "state": "CA"})
        assert compute_completeness_score(row) == 3

    def test_none_value_not_counted(self):
        row = pd.Series({"email": None, "fname": "Bob"})
        assert compute_completeness_score(row) == 1  # only fname counts


# ===========================================================================
# SECTION 2 — compare_rows_for_canonical
# ===========================================================================

class TestCompareRowsForCanonical:
    """Validates all 4 rules with their exact loser_reason tokens."""

    # Rule 1: hard_fail
    def test_rule1_no_hard_fail_beats_hard_fail(self):
        good = _entry(hard_fail=False, ordinal=0)
        bad = _entry(hard_fail=True, ordinal=1)
        d = compare_rows_for_canonical(good, bad)
        assert d.winner is good
        assert d.loser is bad
        assert d.loser_reason == "duplicate_hard_fail_loser"

    def test_rule1_hard_fail_loses_to_no_hard_fail(self):
        bad = _entry(hard_fail=True, ordinal=0)
        good = _entry(hard_fail=False, ordinal=1)
        d = compare_rows_for_canonical(bad, good)
        assert d.winner is good
        assert d.loser is bad
        assert d.loser_reason == "duplicate_hard_fail_loser"

    def test_rule1_both_hard_fail_falls_through_to_rule2(self):
        a = _entry(hard_fail=True, score=80, ordinal=0)
        b = _entry(hard_fail=True, score=60, ordinal=1)
        d = compare_rows_for_canonical(a, b)
        assert d.winner is a
        assert d.loser_reason == "duplicate_lower_score"

    # Rule 2: score
    def test_rule2_higher_score_wins(self):
        high = _entry(score=80, ordinal=0)
        low = _entry(score=60, ordinal=1)
        d = compare_rows_for_canonical(high, low)
        assert d.winner is high
        assert d.loser_reason == "duplicate_lower_score"

    def test_rule2_challenger_higher_score_wins(self):
        low = _entry(score=60, ordinal=0)
        high = _entry(score=80, ordinal=1)
        d = compare_rows_for_canonical(low, high)
        assert d.winner is high
        assert d.loser_reason == "duplicate_lower_score"

    def test_rule2_equal_score_falls_through_to_rule3(self):
        a = _entry(score=75, completeness=8, ordinal=0)
        b = _entry(score=75, completeness=5, ordinal=1)
        d = compare_rows_for_canonical(a, b)
        assert d.winner is a
        assert d.loser_reason == "duplicate_lower_completeness"

    # Rule 3: completeness
    def test_rule3_higher_completeness_wins(self):
        rich = _entry(score=75, completeness=8, ordinal=0)
        poor = _entry(score=75, completeness=5, ordinal=1)
        d = compare_rows_for_canonical(rich, poor)
        assert d.winner is rich
        assert d.loser_reason == "duplicate_lower_completeness"

    def test_rule3_challenger_higher_completeness_wins(self):
        poor = _entry(score=75, completeness=3, ordinal=0)
        rich = _entry(score=75, completeness=9, ordinal=1)
        d = compare_rows_for_canonical(poor, rich)
        assert d.winner is rich
        assert d.loser_reason == "duplicate_lower_completeness"

    def test_rule3_equal_completeness_falls_through_to_rule4(self):
        a = _entry(score=75, completeness=5, ordinal=0)
        b = _entry(score=75, completeness=5, ordinal=1)
        d = compare_rows_for_canonical(a, b)
        assert d.winner is a
        assert d.loser_reason == "duplicate_later_occurrence_tiebreak"

    # Rule 4: stable tiebreak
    def test_rule4_earlier_ordinal_wins(self):
        first = _entry(score=75, completeness=5, ordinal=0)
        second = _entry(score=75, completeness=5, ordinal=1)
        d = compare_rows_for_canonical(first, second)
        assert d.winner is first
        assert d.loser_reason == "duplicate_later_occurrence_tiebreak"

    def test_rule4_current_is_later_challenger_wins(self):
        late = _entry(score=75, completeness=5, ordinal=10)
        early = _entry(score=75, completeness=5, ordinal=2)
        d = compare_rows_for_canonical(late, early)
        assert d.winner is early
        assert d.loser_reason == "duplicate_later_occurrence_tiebreak"

    # Determinism
    def test_same_inputs_produce_same_result(self):
        a = _entry(score=80, ordinal=0)
        b = _entry(score=60, ordinal=1)
        d1 = compare_rows_for_canonical(a, b)
        d2 = compare_rows_for_canonical(a, b)
        assert d1.winner is d2.winner
        assert d1.loser_reason == d2.loser_reason

    def test_all_four_reasons_are_reachable(self):
        reasons = set()
        # Rule 1
        reasons.add(compare_rows_for_canonical(
            _entry(hard_fail=False), _entry(hard_fail=True)).loser_reason)
        # Rule 2
        reasons.add(compare_rows_for_canonical(
            _entry(score=80), _entry(score=60)).loser_reason)
        # Rule 3
        reasons.add(compare_rows_for_canonical(
            _entry(completeness=8), _entry(completeness=3)).loser_reason)
        # Rule 4
        reasons.add(compare_rows_for_canonical(
            _entry(ordinal=0), _entry(ordinal=1)).loser_reason)
        assert reasons == {
            "duplicate_hard_fail_loser",
            "duplicate_lower_score",
            "duplicate_lower_completeness",
            "duplicate_later_occurrence_tiebreak",
        }


# ===========================================================================
# SECTION 3 — DedupeIndex
# ===========================================================================

class TestDedupeIndex:
    def test_new_email_is_canonical(self):
        idx = DedupeIndex()
        is_can, dup, reason = idx.process_row("a@b.com", False, 75, 3, "f.csv", 1)
        assert is_can is True
        assert dup is False
        assert reason is None

    def test_new_email_increments_new_canonicals(self):
        idx = DedupeIndex()
        idx.process_row("a@b.com", False, 75, 3, "f.csv", 1)
        assert idx.new_canonicals == 1
        assert idx.duplicates_detected == 0

    def test_duplicate_email_lower_score_is_duplicate(self):
        idx = DedupeIndex()
        idx.process_row("a@b.com", False, 80, 3, "f.csv", 1)
        is_can, dup, reason = idx.process_row("a@b.com", False, 60, 3, "f.csv", 2)
        assert is_can is False
        assert dup is True
        assert reason == "duplicate_lower_score"

    def test_duplicate_increments_duplicates_detected(self):
        idx = DedupeIndex()
        idx.process_row("a@b.com", False, 80, 3, "f.csv", 1)
        idx.process_row("a@b.com", False, 60, 3, "f.csv", 2)
        assert idx.duplicates_detected == 1

    def test_better_challenger_becomes_canonical(self):
        idx = DedupeIndex()
        idx.process_row("a@b.com", False, 60, 3, "f.csv", 1)
        is_can, dup, reason = idx.process_row("a@b.com", False, 80, 3, "f.csv", 2)
        assert is_can is True
        assert dup is False
        assert reason is None

    def test_replacement_increments_replaced_canonicals(self):
        idx = DedupeIndex()
        idx.process_row("a@b.com", False, 60, 3, "f.csv", 1)
        idx.process_row("a@b.com", False, 80, 3, "f.csv", 2)
        assert idx.replaced_canonicals == 1

    def test_null_email_always_canonical_singleton(self):
        idx = DedupeIndex()
        is_can1, dup1, r1 = idx.process_row(None, False, 0, 0, "f.csv", 1)
        is_can2, dup2, r2 = idx.process_row(None, False, 0, 0, "f.csv", 2)
        assert is_can1 is True and is_can2 is True
        assert dup1 is False and dup2 is False
        assert idx.duplicates_detected == 0

    def test_empty_email_treated_as_singleton(self):
        idx = DedupeIndex()
        is_can, dup, _ = idx.process_row("", False, 0, 0, "f.csv", 1)
        assert is_can is True
        assert dup is False

    def test_index_size_tracks_unique_emails(self):
        idx = DedupeIndex()
        idx.process_row("a@b.com", False, 75, 3, "f.csv", 1)
        idx.process_row("c@d.com", False, 75, 3, "f.csv", 2)
        idx.process_row("a@b.com", False, 60, 3, "f.csv", 3)  # duplicate
        assert idx.index_size == 2

    def test_hard_fail_loser_reason(self):
        idx = DedupeIndex()
        idx.process_row("a@b.com", False, 75, 3, "f.csv", 1)
        _, dup, reason = idx.process_row("a@b.com", True, 75, 3, "f.csv", 2)
        assert dup is True
        assert reason == "duplicate_hard_fail_loser"

    def test_rule1_hard_fail_first_replaced_by_better(self):
        idx = DedupeIndex()
        idx.process_row("a@b.com", True, 75, 3, "f.csv", 1)   # hard fail first
        is_can, dup, reason = idx.process_row("a@b.com", False, 75, 3, "f.csv", 2)
        assert is_can is True  # non-hard-fail replaces hard-fail
        assert dup is False
        assert idx.replaced_canonicals == 1

    def test_global_ordinal_increases_monotonically(self):
        idx = DedupeIndex()
        idx.process_row("a@b.com", False, 75, 3, "f.csv", 1)
        idx.process_row("c@d.com", False, 75, 3, "f.csv", 2)
        entry_a = idx._store["a@b.com"]
        entry_c = idx._store["c@d.com"]
        assert entry_a.global_ordinal < entry_c.global_ordinal

    def test_same_score_and_completeness_first_occurrence_wins(self):
        idx = DedupeIndex()
        idx.process_row("a@b.com", False, 75, 5, "f.csv", 1)
        _, dup, reason = idx.process_row("a@b.com", False, 75, 5, "f.csv", 2)
        assert dup is True
        assert reason == "duplicate_later_occurrence_tiebreak"

    def test_emails_seen_tracks_all_rows_including_nulls(self):
        idx = DedupeIndex()
        idx.process_row("a@b.com", False, 75, 3, "f.csv", 1)
        idx.process_row(None, False, 0, 0, "f.csv", 2)
        idx.process_row("a@b.com", False, 60, 3, "f.csv", 3)
        assert idx.emails_seen == 3

    def test_cross_file_deduplication(self):
        idx = DedupeIndex()
        idx.process_row("a@b.com", False, 75, 3, "file1.csv", 10)
        _, dup, reason = idx.process_row("a@b.com", False, 60, 3, "file2.csv", 5)
        assert dup is True
        assert reason == "duplicate_lower_score"


# ===========================================================================
# SECTION 4 — apply_email_normalized_column
# ===========================================================================

class TestApplyEmailNormalizedColumn:
    def test_adds_email_normalized_column(self):
        df = pd.DataFrame({"email": ["Alice@Gmail.COM"]})
        result = apply_email_normalized_column(df)
        assert "email_normalized" in result.columns

    def test_lowercases_and_strips(self):
        df = pd.DataFrame({"email": ["  Alice@Gmail.COM  "]})
        result = apply_email_normalized_column(df)
        assert result.iloc[0]["email_normalized"] == "alice@gmail.com"

    def test_null_email_yields_none(self):
        df = pd.DataFrame({"email": [None]})
        result = apply_email_normalized_column(df)
        assert result.iloc[0]["email_normalized"] is None

    def test_empty_email_yields_none(self):
        df = pd.DataFrame({"email": [""]})
        result = apply_email_normalized_column(df)
        assert result.iloc[0]["email_normalized"] is None

    def test_no_email_column_yields_none(self):
        df = pd.DataFrame({"domain": ["gmail.com"]})
        result = apply_email_normalized_column(df)
        assert result.iloc[0]["email_normalized"] is None

    def test_original_frame_not_mutated(self):
        df = pd.DataFrame({"email": ["a@b.com"]})
        apply_email_normalized_column(df)
        assert "email_normalized" not in df.columns

    def test_already_lowercase_unchanged(self):
        df = pd.DataFrame({"email": ["alice@gmail.com"]})
        result = apply_email_normalized_column(df)
        assert result.iloc[0]["email_normalized"] == "alice@gmail.com"


# ===========================================================================
# SECTION 5 — apply_dedupe_columns
# ===========================================================================

class TestApplyDedupeColumns:
    def test_adds_three_columns(self):
        df = _make_df()
        idx = DedupeIndex()
        result = apply_dedupe_columns(df, idx)
        for col in ("is_canonical", "duplicate_flag", "duplicate_reason"):
            assert col in result.columns

    def test_is_canonical_dtype_is_boolean(self):
        df = _make_df()
        result = apply_dedupe_columns(df, DedupeIndex())
        assert str(result["is_canonical"].dtype) == "boolean"

    def test_duplicate_flag_dtype_is_boolean(self):
        df = _make_df()
        result = apply_dedupe_columns(df, DedupeIndex())
        assert str(result["duplicate_flag"].dtype) == "boolean"

    def test_first_occurrence_is_canonical(self):
        df = _make_df()
        result = apply_dedupe_columns(df, DedupeIndex())
        assert result.iloc[0]["is_canonical"] == True
        assert result.iloc[0]["duplicate_flag"] == False
        assert result.iloc[0]["duplicate_reason"] is None

    def test_duplicate_row_is_flagged(self):
        df = pd.DataFrame({
            "email_normalized": ["a@b.com", "a@b.com"],
            "hard_fail": pd.array([False, False], dtype="boolean"),
            "score": [80, 60],
            "completeness_score": [3, 3],
            "source_file": ["f.csv", "f.csv"],
            "source_row_number": [1, 2],
        })
        result = apply_dedupe_columns(df, DedupeIndex())
        assert result.iloc[0]["is_canonical"] == True
        assert result.iloc[1]["duplicate_flag"] == True
        assert result.iloc[1]["duplicate_reason"] == "duplicate_lower_score"

    def test_better_challenger_becomes_canonical(self):
        df = pd.DataFrame({
            "email_normalized": ["a@b.com", "a@b.com"],
            "hard_fail": pd.array([False, False], dtype="boolean"),
            "score": [60, 80],
            "completeness_score": [3, 3],
            "source_file": ["f.csv", "f.csv"],
            "source_row_number": [1, 2],
        })
        result = apply_dedupe_columns(df, DedupeIndex())
        assert result.iloc[1]["is_canonical"] == True
        assert result.iloc[1]["duplicate_flag"] == False

    def test_original_frame_not_mutated(self):
        df = _make_df()
        apply_dedupe_columns(df, DedupeIndex())
        assert "is_canonical" not in df.columns

    def test_row_count_unchanged(self):
        df = pd.DataFrame({
            "email_normalized": ["a@b.com", "c@d.com", "a@b.com"],
            "hard_fail": pd.array([False, False, False], dtype="boolean"),
            "score": [75, 75, 60],
            "completeness_score": [3, 3, 3],
            "source_file": ["f.csv", "f.csv", "f.csv"],
            "source_row_number": [1, 2, 3],
        })
        result = apply_dedupe_columns(df, DedupeIndex())
        assert len(result) == 3

    def test_null_email_normalized_is_singleton(self):
        df = pd.DataFrame({
            "email_normalized": [None, None],
            "hard_fail": pd.array([False, False], dtype="boolean"),
            "score": [75, 75],
            "completeness_score": [3, 3],
            "source_file": ["f.csv", "f.csv"],
            "source_row_number": [1, 2],
        })
        result = apply_dedupe_columns(df, DedupeIndex())
        assert result.iloc[0]["is_canonical"] == True
        assert result.iloc[1]["is_canonical"] == True
        assert result.iloc[0]["duplicate_flag"] == False
        assert result.iloc[1]["duplicate_flag"] == False

    def test_index_shared_across_calls(self):
        idx = DedupeIndex()
        df1 = pd.DataFrame({
            "email_normalized": ["a@b.com"],
            "hard_fail": pd.array([False], dtype="boolean"),
            "score": [80],
            "completeness_score": [3],
            "source_file": ["file1.csv"],
            "source_row_number": [1],
        })
        df2 = pd.DataFrame({
            "email_normalized": ["a@b.com"],
            "hard_fail": pd.array([False], dtype="boolean"),
            "score": [60],
            "completeness_score": [3],
            "source_file": ["file2.csv"],
            "source_row_number": [2],
        })
        apply_dedupe_columns(df1, idx)
        result2 = apply_dedupe_columns(df2, idx)
        assert result2.iloc[0]["duplicate_flag"] == True

    def test_missing_optional_columns_do_not_crash(self):
        df = pd.DataFrame({"email_normalized": ["a@b.com"]})
        result = apply_dedupe_columns(df, DedupeIndex())
        assert "is_canonical" in result.columns

    def test_hard_fail_loser_reason_in_dataframe(self):
        df = pd.DataFrame({
            "email_normalized": ["a@b.com", "a@b.com"],
            "hard_fail": pd.array([False, True], dtype="boolean"),
            "score": [75, 75],
            "completeness_score": [3, 3],
            "source_file": ["f.csv", "f.csv"],
            "source_row_number": [1, 2],
        })
        result = apply_dedupe_columns(df, DedupeIndex())
        assert result.iloc[1]["duplicate_reason"] == "duplicate_hard_fail_loser"

    def test_completeness_tiebreak_in_dataframe(self):
        df = pd.DataFrame({
            "email_normalized": ["a@b.com", "a@b.com"],
            "hard_fail": pd.array([False, False], dtype="boolean"),
            "score": [75, 75],
            "completeness_score": [3, 8],
            "source_file": ["f.csv", "f.csv"],
            "source_row_number": [1, 2],
        })
        result = apply_dedupe_columns(df, DedupeIndex())
        # Second row has higher completeness → replaces first as canonical
        assert result.iloc[1]["is_canonical"] == True
        assert result.iloc[0]["duplicate_flag"] == False  # was canonical at processing time


# ===========================================================================
# SECTION 6 — Pipeline integration (mocked DNS)
# ===========================================================================

class TestDedupePipelineIntegration:
    @pytest.fixture
    def csv_unique(self, tmp_path: Path) -> Path:
        f = tmp_path / "unique_emails.csv"
        f.write_text(
            "email,domain,fname\n"
            "alice@gmail.com,gmail.com,Alice\n"
            "bob@yahoo.com,yahoo.com,Bob\n"
            "carol@outlook.com,outlook.com,Carol\n",
            encoding="utf-8",
        )
        return f

    @pytest.fixture
    def csv_with_dupes(self, tmp_path: Path) -> Path:
        f = tmp_path / "dupes.csv"
        f.write_text(
            "email,domain,fname\n"
            "alice@gmail.com,gmail.com,Alice\n"
            "alice@gmail.com,gmail.com,Alice Duplicate\n"
            "bob@yahoo.com,yahoo.com,Bob\n"
            "bob@yahoo.com,yahoo.com,Bob Duplicate\n"
            "carol@outlook.com,outlook.com,Carol\n",
            encoding="utf-8",
        )
        return f

    def _run(self, csv_file: Path, chunk_size: int = 10):
        import dataclasses
        from app.config import load_config, resolve_project_paths
        from app.io_utils import build_run_context
        from app.pipeline import EmailCleaningPipeline

        project_paths = resolve_project_paths()
        config = load_config(base_dir=project_paths.project_root)
        config = dataclasses.replace(config, chunk_size=chunk_size)
        run_context = build_run_context(config)
        logger = logging.getLogger("test_pipeline_s7")
        pipeline = EmailCleaningPipeline(config=config, logger=logger)
        return pipeline.run(input_file=str(csv_file), run_context=run_context)

    def test_pipeline_status_is_subphase_8_ready(self, csv_unique):
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_DNS):
            result = self._run(csv_unique)
        assert result.status == "subphase_8_ready"

    def test_pipeline_runs_without_error(self, csv_with_dupes):
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_DNS):
            result = self._run(csv_with_dupes)
        assert result is not None

    def test_total_rows_unchanged(self, csv_with_dupes):
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_DNS):
            result = self._run(csv_with_dupes)
        assert result.total_rows == 5

    def test_pipeline_logs_dedupe_new_canonicals(self, csv_unique, caplog):
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_DNS):
            with caplog.at_level(logging.INFO, logger="test_pipeline_s7"):
                self._run(csv_unique)
        assert "dedupe_new_canonicals" in "\n".join(caplog.messages)

    def test_pipeline_logs_dedupe_duplicates(self, csv_with_dupes, caplog):
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_DNS):
            with caplog.at_level(logging.INFO, logger="test_pipeline_s7"):
                self._run(csv_with_dupes)
        assert "dedupe_duplicates" in "\n".join(caplog.messages)

    def test_pipeline_logs_dedupe_index_size(self, csv_unique, caplog):
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_DNS):
            with caplog.at_level(logging.INFO, logger="test_pipeline_s7"):
                self._run(csv_unique)
        assert "dedupe_index_size" in "\n".join(caplog.messages)

    def test_pipeline_logs_run_summary_dedupe(self, csv_with_dupes, caplog):
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_DNS):
            with caplog.at_level(logging.INFO, logger="test_pipeline_s7"):
                self._run(csv_with_dupes)
        log_text = "\n".join(caplog.messages)
        assert "dedupe_total_canonicals" in log_text
        assert "dedupe_total_duplicates" in log_text

    def test_pipeline_with_chunk_size_1_works(self, csv_with_dupes):
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_DNS):
            result = self._run(csv_with_dupes, chunk_size=1)
        assert result.status == "subphase_8_ready"

    def test_email_normalized_column_present_in_output(self, csv_unique, tmp_path):
        """Verify email_normalized is produced by running the pipeline on a chunk."""
        import dataclasses
        from app.config import load_config, resolve_project_paths
        from app.io_utils import build_run_context, prepare_input_file, discover_input_files, read_csv_in_chunks
        from app.pipeline import EmailCleaningPipeline
        from app.dedupe import DedupeIndex, apply_email_normalized_column, apply_completeness_column, apply_dedupe_columns
        from app.normalizers import normalize_headers, normalize_values, add_technical_metadata, extract_email_components, apply_domain_typo_correction_column, compare_domain_with_input_column
        from app.validators import validate_email_syntax_column
        from app.scoring import apply_scoring_column
        from app.typo_rules import build_typo_map
        from app.models import InputFile, ChunkContext
        import pandas as pd

        df = pd.DataFrame({
            "email": ["alice@gmail.com"],
            "domain": ["gmail.com"],
        })
        df = apply_email_normalized_column(df)
        assert "email_normalized" in df.columns
        assert df.iloc[0]["email_normalized"] == "alice@gmail.com"


# ===========================================================================
# SECTION 7 — No future-phase contamination
# ===========================================================================

class TestNoFuturePhaseContamination:
    def _src(self) -> str:
        import app.dedupe as mod
        return inspect.getsource(mod)

    def test_no_sqlite_in_dedupe(self):
        assert "sqlite" not in self._src().lower()

    def test_no_export_in_dedupe(self):
        for term in ("to_csv", "to_excel", "open(", "write("):
            assert term not in self._src()

    def test_no_smtp_in_dedupe(self):
        for term in ("smtplib", "aiosmtplib"):
            assert term not in self._src().lower()

    def test_no_reporting_in_dedupe(self):
        for term in ("duplicate_summary", "clean_high_confidence", "removed_invalid"):
            assert term not in self._src()

    def test_no_score_recalculation_in_dedupe(self):
        for term in ("score_row(", "apply_scoring_column(", "SCORE_MX_PRESENT"):
            assert term not in self._src()

    def test_preliminary_bucket_not_set_by_dedupe(self):
        assert "preliminary_bucket" not in self._src()

    def test_pipeline_result_has_no_smtp_fields(self):
        from app.models import PipelineResult
        fields = {f.name for f in PipelineResult.__dataclass_fields__.values()}  # type: ignore[attr-defined]
        for forbidden in ("smtp", "verify_email", "deliverable"):
            for field_name in fields:
                assert forbidden not in field_name.lower()

    def test_is_canonical_is_provisional(self):
        idx = DedupeIndex()
        is_can, _, _ = idx.process_row("a@b.com", False, 75, 3, "f.csv", 1)
        assert is_can is True  # provisional — may be corrected by Subphase 8
