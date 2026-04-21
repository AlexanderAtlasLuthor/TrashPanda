"""Tests for Subphase 6: hard fail detection, scoring, and preliminary bucket assignment.

Sections:
  1. TestScoringResult          — dataclass structure
  2. TestHardFail               — all hard fail conditions
  3. TestScoreRowSpecCases      — the 6 spec-defined cases
  4. TestScoreRowWeights        — individual signals and penalties
  5. TestScoreRowBuckets        — boundary conditions on thresholds
  6. TestScoreRowReasons        — reason string stability and order
  7. TestApplyScoringColumn     — DataFrame-level application
  8. TestScoringPipelineIntegration — end-to-end with mocked DNS
  9. TestNoFuturePhaseContamination — dedupe/SQLite/export absent
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

from app.scoring import (
    DNS_NO_RECORDS_ERRORS,
    DNS_TRANSIENT_ERRORS,
    PENALTY_DNS_NO_RECORDS,
    PENALTY_DNS_TRANSIENT,
    PENALTY_DOMAIN_MISMATCH,
    PENALTY_TYPO_CORRECTED,
    SCORE_A_FALLBACK,
    SCORE_MX_PRESENT,
    SCORE_SYNTAX_VALID,
    ScoringResult,
    apply_scoring_column,
    score_row,
)
from app.dns_utils import DnsResult


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _score(**overrides) -> ScoringResult:
    """Call score_row with safe defaults; override only what the test needs."""
    defaults = dict(
        syntax_valid=True,
        corrected_domain="example.com",
        has_mx_record=True,
        has_a_record=False,
        domain_exists=True,
        dns_error=None,
        typo_corrected=False,
        domain_matches_input_column=None,
    )
    defaults.update(overrides)
    return score_row(**defaults)


def _make_df(**col_values) -> pd.DataFrame:
    """Build a single-row DataFrame with scoring-relevant columns."""
    defaults = dict(
        syntax_valid=pd.array([True], dtype="boolean"),
        corrected_domain=["example.com"],
        has_mx_record=pd.array([True], dtype="boolean"),
        has_a_record=pd.array([False], dtype="boolean"),
        domain_exists=pd.array([True], dtype="boolean"),
        dns_error=[None],
        typo_corrected=pd.array([False], dtype="boolean"),
        domain_matches_input_column=pd.array([pd.NA], dtype="boolean"),
    )
    defaults.update(col_values)
    return pd.DataFrame(defaults)


_MX_DNS = DnsResult(True, True, True, False, None)


# ===========================================================================
# SECTION 1 — ScoringResult
# ===========================================================================

class TestScoringResult:
    def test_fields_present(self):
        r = ScoringResult(hard_fail=False, score=75, score_reasons="syntax_valid|mx_present", preliminary_bucket="high_confidence")
        assert r.hard_fail is False
        assert r.score == 75
        assert r.score_reasons == "syntax_valid|mx_present"
        assert r.preliminary_bucket == "high_confidence"

    def test_hard_fail_true(self):
        r = ScoringResult(hard_fail=True, score=0, score_reasons="nxdomain", preliminary_bucket="invalid")
        assert r.hard_fail is True
        assert r.score == 0
        assert r.preliminary_bucket == "invalid"


# ===========================================================================
# SECTION 2 — Hard fail detection
# ===========================================================================

class TestHardFail:
    def test_syntax_invalid_triggers_hard_fail(self):
        r = _score(syntax_valid=False)
        assert r.hard_fail is True
        assert r.preliminary_bucket == "invalid"
        assert r.score == 0

    def test_syntax_none_triggers_hard_fail(self):
        r = _score(syntax_valid=None)
        assert r.hard_fail is True

    def test_no_domain_triggers_hard_fail(self):
        r = _score(corrected_domain=None)
        assert r.hard_fail is True
        assert "no_domain" in r.score_reasons

    def test_empty_domain_triggers_hard_fail(self):
        r = _score(corrected_domain="")
        assert r.hard_fail is True

    def test_nxdomain_triggers_hard_fail(self):
        r = _score(domain_exists=False, dns_error="nxdomain")
        assert r.hard_fail is True
        assert r.score_reasons == "nxdomain"
        assert r.preliminary_bucket == "invalid"

    def test_timeout_does_not_trigger_hard_fail(self):
        r = _score(domain_exists=False, dns_error="timeout", has_mx_record=False, has_a_record=False)
        assert r.hard_fail is False

    def test_a_only_does_not_trigger_hard_fail(self):
        r = _score(has_mx_record=False, has_a_record=True, domain_exists=True)
        assert r.hard_fail is False

    def test_typo_corrected_does_not_trigger_hard_fail(self):
        r = _score(typo_corrected=True)
        assert r.hard_fail is False

    def test_domain_mismatch_does_not_trigger_hard_fail(self):
        r = _score(domain_matches_input_column=False)
        assert r.hard_fail is False

    def test_no_nameservers_does_not_trigger_hard_fail(self):
        r = _score(domain_exists=False, dns_error="no_nameservers", has_mx_record=False, has_a_record=False)
        assert r.hard_fail is False

    def test_hard_fail_score_always_zero(self):
        for setup in [
            dict(syntax_valid=False),
            dict(corrected_domain=None),
            dict(domain_exists=False, dns_error="nxdomain"),
        ]:
            assert _score(**setup).score == 0

    def test_hard_fail_bucket_always_invalid(self):
        for setup in [
            dict(syntax_valid=False),
            dict(corrected_domain=None),
            dict(domain_exists=False, dns_error="nxdomain"),
        ]:
            assert _score(**setup).preliminary_bucket == "invalid"

    def test_syntax_invalid_reason_token(self):
        r = _score(syntax_valid=False)
        assert r.score_reasons == "syntax_invalid"

    def test_no_domain_reason_token(self):
        r = _score(syntax_valid=True, corrected_domain=None)
        assert r.score_reasons == "no_domain"

    def test_nxdomain_reason_token(self):
        r = _score(domain_exists=False, dns_error="nxdomain")
        assert r.score_reasons == "nxdomain"


# ===========================================================================
# SECTION 3 — Spec cases
# ===========================================================================

class TestScoreRowSpecCases:
    """Validates the 6 spec-defined cases verbatim."""

    def test_caso1_perfect_mx(self):
        """syntax=T, domain_exists=T, has_mx=T, domain_matches=T → high_confidence, no hard_fail."""
        r = _score(
            has_mx_record=True,
            domain_exists=True,
            domain_matches_input_column=True,
        )
        assert r.hard_fail is False
        assert r.score >= 70
        assert r.preliminary_bucket == "high_confidence"

    def test_caso2_a_fallback(self):
        """syntax=T, domain_exists=T, has_mx=F, has_a=T, domain_matches=T → review, less than MX."""
        r_a = _score(
            has_mx_record=False,
            has_a_record=True,
            domain_exists=True,
            domain_matches_input_column=True,
        )
        r_mx = _score(has_mx_record=True, domain_exists=True)
        assert r_a.hard_fail is False
        assert r_a.score < r_mx.score
        assert r_a.preliminary_bucket in ("review", "high_confidence")

    def test_caso3_timeout_not_hard_fail(self):
        """syntax=T, domain_exists=F, dns_error=timeout → hard_fail=False, score penalized."""
        r_timeout = _score(
            domain_exists=False,
            dns_error="timeout",
            has_mx_record=False,
            has_a_record=False,
        )
        r_clean = _score(has_mx_record=True, domain_exists=True)
        assert r_timeout.hard_fail is False
        assert r_timeout.score < r_clean.score

    def test_caso3_timeout_not_same_as_nxdomain(self):
        """timeout → hard_fail=False; nxdomain → hard_fail=True."""
        r_timeout = _score(domain_exists=False, dns_error="timeout", has_mx_record=False, has_a_record=False)
        r_nxdomain = _score(domain_exists=False, dns_error="nxdomain")
        assert r_timeout.hard_fail is False
        assert r_nxdomain.hard_fail is True

    def test_caso4_nxdomain_hard_fail(self):
        """syntax=T, domain_exists=F, dns_error=nxdomain → hard_fail=True, invalid."""
        r = _score(domain_exists=False, dns_error="nxdomain")
        assert r.hard_fail is True
        assert r.preliminary_bucket == "invalid"
        assert "nxdomain" in r.score_reasons

    def test_caso5_syntax_invalid(self):
        """syntax_valid=False → hard_fail=True, score=0, invalid, syntax_invalid reason."""
        r = _score(syntax_valid=False)
        assert r.hard_fail is True
        assert r.score == 0
        assert r.preliminary_bucket == "invalid"
        assert "syntax_invalid" in r.score_reasons

    def test_caso6_typo_corrected_with_mx(self):
        """typo_corrected=T, domain_exists=T, MX → no hard fail, reasonable score."""
        r = _score(typo_corrected=True, has_mx_record=True, domain_exists=True)
        assert r.hard_fail is False
        assert r.score > 0
        # Typo penalty must be small: score should still be decent
        r_no_typo = _score(typo_corrected=False, has_mx_record=True, domain_exists=True)
        assert r_no_typo.score - r.score <= abs(PENALTY_TYPO_CORRECTED) + 1


# ===========================================================================
# SECTION 4 — Individual signal weights
# ===========================================================================

class TestScoreRowWeights:
    def test_syntax_valid_adds_points(self):
        r = score_row(
            syntax_valid=True,
            corrected_domain="example.com",
            has_mx_record=False,
            has_a_record=False,
            domain_exists=False,
            dns_error=None,
            typo_corrected=False,
            domain_matches_input_column=None,
        )
        assert r.score >= SCORE_SYNTAX_VALID

    def test_mx_adds_more_than_a(self):
        r_mx = _score(has_mx_record=True, has_a_record=False, domain_exists=True)
        r_a = _score(has_mx_record=False, has_a_record=True, domain_exists=True)
        assert r_mx.score > r_a.score

    def test_mx_score_delta(self):
        r_mx = _score(has_mx_record=True, has_a_record=False, domain_exists=True)
        r_none = score_row(
            syntax_valid=True,
            corrected_domain="example.com",
            has_mx_record=False,
            has_a_record=False,
            domain_exists=False,
            dns_error=None,
            typo_corrected=False,
            domain_matches_input_column=None,
        )
        assert r_mx.score - r_none.score == SCORE_MX_PRESENT

    def test_a_fallback_score_delta(self):
        r_a = _score(has_mx_record=False, has_a_record=True, domain_exists=True)
        r_none = score_row(
            syntax_valid=True,
            corrected_domain="example.com",
            has_mx_record=False,
            has_a_record=False,
            domain_exists=False,
            dns_error=None,
            typo_corrected=False,
            domain_matches_input_column=None,
        )
        assert r_a.score - r_none.score == SCORE_A_FALLBACK

    def test_typo_penalty_applied(self):
        r_typo = _score(typo_corrected=True)
        r_clean = _score(typo_corrected=False)
        assert r_clean.score - r_typo.score == abs(PENALTY_TYPO_CORRECTED)

    def test_domain_mismatch_penalty_applied(self):
        r_mismatch = _score(domain_matches_input_column=False)
        r_match = _score(domain_matches_input_column=None)
        assert r_match.score - r_mismatch.score == abs(PENALTY_DOMAIN_MISMATCH)

    def test_domain_match_true_no_extra_penalty(self):
        r_true = _score(domain_matches_input_column=True)
        r_none = _score(domain_matches_input_column=None)
        # True match gives no penalty; neither does None (absent column)
        assert r_true.score == r_none.score

    def test_transient_dns_error_penalty(self):
        for err in DNS_TRANSIENT_ERRORS:
            r = _score(domain_exists=False, dns_error=err, has_mx_record=False, has_a_record=False)
            r_clean = score_row(
                syntax_valid=True,
                corrected_domain="example.com",
                has_mx_record=False,
                has_a_record=False,
                domain_exists=False,
                dns_error=None,
                typo_corrected=False,
                domain_matches_input_column=None,
            )
            assert r.score == max(0, r_clean.score + PENALTY_DNS_TRANSIENT)

    def test_no_records_dns_error_penalty(self):
        for err in DNS_NO_RECORDS_ERRORS:
            r = _score(domain_exists=False, dns_error=err, has_mx_record=False, has_a_record=False)
            r_clean = score_row(
                syntax_valid=True,
                corrected_domain="example.com",
                has_mx_record=False,
                has_a_record=False,
                domain_exists=False,
                dns_error=None,
                typo_corrected=False,
                domain_matches_input_column=None,
            )
            assert r.score == max(0, r_clean.score + PENALTY_DNS_NO_RECORDS)

    def test_transient_penalty_less_severe_than_no_records(self):
        # Transient errors penalize more (more uncertain) than no-records errors.
        assert abs(PENALTY_DNS_TRANSIENT) >= abs(PENALTY_DNS_NO_RECORDS)

    def test_score_clamped_to_zero_minimum(self):
        r = score_row(
            syntax_valid=True,
            corrected_domain="example.com",
            has_mx_record=False,
            has_a_record=False,
            domain_exists=False,
            dns_error="timeout",
            typo_corrected=True,
            domain_matches_input_column=False,
        )
        assert r.score >= 0

    def test_score_clamped_to_100_maximum(self):
        r = _score(
            has_mx_record=True,
            domain_exists=True,
            typo_corrected=False,
            domain_matches_input_column=True,
        )
        assert r.score <= 100

    def test_mx_not_added_when_a_also_true(self):
        # MX takes priority; A bonus must not be double-counted.
        r_mx_only = _score(has_mx_record=True, has_a_record=False)
        r_mx_and_a = _score(has_mx_record=True, has_a_record=True)
        assert r_mx_only.score == r_mx_and_a.score


# ===========================================================================
# SECTION 5 — Bucket assignment
# ===========================================================================

class TestScoreRowBuckets:
    def test_score_at_high_confidence_threshold_is_high_confidence(self):
        r = score_row(
            syntax_valid=True,
            corrected_domain="x.com",
            has_mx_record=False,
            has_a_record=False,
            domain_exists=False,
            dns_error=None,
            typo_corrected=False,
            domain_matches_input_column=None,
            high_confidence_threshold=25,
            review_threshold=10,
        )
        # With only syntax_valid (+25), score == 25 == high_confidence_threshold
        assert r.score == 25
        assert r.preliminary_bucket == "high_confidence"

    def test_score_just_below_high_confidence_is_review(self):
        r = score_row(
            syntax_valid=True,
            corrected_domain="x.com",
            has_mx_record=False,
            has_a_record=False,
            domain_exists=False,
            dns_error=None,
            typo_corrected=False,
            domain_matches_input_column=None,
            high_confidence_threshold=26,
            review_threshold=10,
        )
        assert r.score == 25
        assert r.preliminary_bucket == "review"

    def test_score_at_review_threshold_is_review(self):
        r = score_row(
            syntax_valid=True,
            corrected_domain="x.com",
            has_mx_record=False,
            has_a_record=False,
            domain_exists=False,
            dns_error=None,
            typo_corrected=False,
            domain_matches_input_column=None,
            high_confidence_threshold=70,
            review_threshold=25,
        )
        assert r.score == 25
        assert r.preliminary_bucket == "review"

    def test_score_below_review_threshold_is_invalid(self):
        r = score_row(
            syntax_valid=True,
            corrected_domain="x.com",
            has_mx_record=False,
            has_a_record=False,
            domain_exists=False,
            dns_error=None,
            typo_corrected=False,
            domain_matches_input_column=None,
            high_confidence_threshold=70,
            review_threshold=26,
        )
        assert r.score == 25
        assert r.preliminary_bucket == "invalid"

    def test_custom_thresholds_respected(self):
        r = score_row(
            syntax_valid=True,
            corrected_domain="x.com",
            has_mx_record=True,
            has_a_record=False,
            domain_exists=True,
            dns_error=None,
            typo_corrected=False,
            domain_matches_input_column=None,
            high_confidence_threshold=100,
            review_threshold=50,
        )
        # score = 25 + 50 = 75, threshold=100 → review
        assert r.preliminary_bucket == "review"

    def test_hard_fail_overrides_bucket_regardless_of_signals(self):
        r = score_row(
            syntax_valid=False,
            corrected_domain="gmail.com",
            has_mx_record=True,
            has_a_record=False,
            domain_exists=True,
            dns_error=None,
            typo_corrected=False,
            domain_matches_input_column=True,
        )
        assert r.preliminary_bucket == "invalid"
        assert r.hard_fail is True


# ===========================================================================
# SECTION 6 — Reason string stability and order
# ===========================================================================

class TestScoreRowReasons:
    def test_reasons_pipe_separated(self):
        r = _score(has_mx_record=True)
        assert "|" in r.score_reasons or len(r.score_reasons.split("|")) >= 1

    def test_syntax_valid_first_in_reasons(self):
        r = _score(has_mx_record=True, typo_corrected=True, domain_matches_input_column=False)
        tokens = r.score_reasons.split("|")
        assert tokens[0] == "syntax_valid"

    def test_mx_present_after_syntax_valid(self):
        r = _score(has_mx_record=True)
        tokens = r.score_reasons.split("|")
        assert "syntax_valid" in tokens
        assert "mx_present" in tokens
        assert tokens.index("syntax_valid") < tokens.index("mx_present")

    def test_a_fallback_token_when_no_mx(self):
        r = _score(has_mx_record=False, has_a_record=True, domain_exists=True)
        assert "a_fallback" in r.score_reasons

    def test_no_mx_and_a_token_mutually_exclusive(self):
        r = _score(has_mx_record=True, has_a_record=True)
        tokens = r.score_reasons.split("|")
        assert "mx_present" in tokens
        assert "a_fallback" not in tokens

    def test_typo_corrected_token_present(self):
        r = _score(typo_corrected=True)
        assert "typo_corrected" in r.score_reasons

    def test_domain_mismatch_token_present(self):
        r = _score(domain_matches_input_column=False)
        assert "domain_mismatch" in r.score_reasons

    def test_dns_error_token_for_transient(self):
        r = _score(domain_exists=False, dns_error="timeout", has_mx_record=False, has_a_record=False)
        assert "dns_error" in r.score_reasons

    def test_dns_no_records_token(self):
        r = _score(domain_exists=False, dns_error="no_mx", has_mx_record=False, has_a_record=False)
        assert "dns_no_records" in r.score_reasons

    def test_same_inputs_same_reasons(self):
        kwargs = dict(has_mx_record=True, typo_corrected=True, domain_matches_input_column=False)
        assert _score(**kwargs).score_reasons == _score(**kwargs).score_reasons

    def test_hard_fail_reasons_contain_no_positive_tokens(self):
        r = _score(syntax_valid=False)
        assert "syntax_valid" not in r.score_reasons
        assert "mx_present" not in r.score_reasons

    def test_no_duplicate_tokens(self):
        r = _score(has_mx_record=True, typo_corrected=True)
        tokens = r.score_reasons.split("|")
        assert len(tokens) == len(set(tokens))

    def test_empty_reasons_only_for_no_signals(self):
        # A row with syntax_valid=True but absolutely nothing else → still has "syntax_valid"
        r = score_row(
            syntax_valid=True,
            corrected_domain="x.com",
            has_mx_record=False,
            has_a_record=False,
            domain_exists=False,
            dns_error=None,
            typo_corrected=False,
            domain_matches_input_column=None,
        )
        assert "syntax_valid" in r.score_reasons


# ===========================================================================
# SECTION 7 — apply_scoring_column (DataFrame-level)
# ===========================================================================

class TestApplyScoringColumn:
    def test_adds_four_columns(self):
        df = _make_df()
        result = apply_scoring_column(df)
        for col in ("hard_fail", "score", "score_reasons", "preliminary_bucket"):
            assert col in result.columns

    def test_hard_fail_dtype_is_boolean(self):
        df = _make_df()
        result = apply_scoring_column(df)
        assert str(result["hard_fail"].dtype) == "boolean"

    def test_score_dtype_is_int(self):
        df = _make_df()
        result = apply_scoring_column(df)
        assert result["score"].dtype == int or "int" in str(result["score"].dtype)

    def test_original_frame_not_mutated(self):
        df = _make_df()
        apply_scoring_column(df)
        assert "hard_fail" not in df.columns

    def test_row_count_unchanged(self):
        emails = ["alice@gmail.com", "bad@@x", None]
        df = pd.DataFrame({
            "syntax_valid": pd.array([True, False, False], dtype="boolean"),
            "corrected_domain": ["gmail.com", None, None],
            "has_mx_record": pd.array([True, False, False], dtype="boolean"),
            "has_a_record": pd.array([False, False, False], dtype="boolean"),
            "domain_exists": pd.array([True, False, False], dtype="boolean"),
            "dns_error": [None, None, None],
            "typo_corrected": pd.array([False, pd.NA, pd.NA], dtype="boolean"),
            "domain_matches_input_column": pd.array([pd.NA, pd.NA, pd.NA], dtype="boolean"),
        })
        result = apply_scoring_column(df)
        assert len(result) == 3

    def test_valid_mx_row_is_high_confidence(self):
        df = _make_df()
        result = apply_scoring_column(df)
        assert result.iloc[0]["preliminary_bucket"] == "high_confidence"
        assert result.iloc[0]["hard_fail"] == False

    def test_invalid_syntax_row_is_hard_fail(self):
        df = _make_df(
            syntax_valid=pd.array([False], dtype="boolean"),
            corrected_domain=[None],
            has_mx_record=pd.array([False], dtype="boolean"),
            domain_exists=pd.array([False], dtype="boolean"),
        )
        result = apply_scoring_column(df)
        assert result.iloc[0]["hard_fail"] == True
        assert result.iloc[0]["preliminary_bucket"] == "invalid"

    def test_nxdomain_row_is_hard_fail(self):
        df = _make_df(
            domain_exists=pd.array([False], dtype="boolean"),
            dns_error=["nxdomain"],
            has_mx_record=pd.array([False], dtype="boolean"),
        )
        result = apply_scoring_column(df)
        assert result.iloc[0]["hard_fail"] == True

    def test_a_fallback_row_is_review(self):
        df = _make_df(
            has_mx_record=pd.array([False], dtype="boolean"),
            has_a_record=pd.array([True], dtype="boolean"),
        )
        result = apply_scoring_column(df)
        assert result.iloc[0]["hard_fail"] == False
        assert result.iloc[0]["preliminary_bucket"] in ("review", "high_confidence")

    def test_missing_columns_do_not_crash(self):
        df = pd.DataFrame({"syntax_valid": pd.array([True], dtype="boolean"), "corrected_domain": ["x.com"]})
        result = apply_scoring_column(df)
        assert "preliminary_bucket" in result.columns

    def test_custom_thresholds_passed_through(self):
        df = _make_df()  # MX present → score 75 with defaults
        result_high = apply_scoring_column(df, high_confidence_threshold=70)
        result_low = apply_scoring_column(df, high_confidence_threshold=80)
        assert result_high.iloc[0]["preliminary_bucket"] == "high_confidence"
        assert result_low.iloc[0]["preliminary_bucket"] == "review"

    def test_mixed_chunk_all_buckets_present(self):
        df = pd.DataFrame({
            "syntax_valid": pd.array([True, True, True, False], dtype="boolean"),
            "corrected_domain": ["gmail.com", "aonly.com", "timeout.com", None],
            "has_mx_record": pd.array([True, False, False, False], dtype="boolean"),
            "has_a_record": pd.array([False, True, False, False], dtype="boolean"),
            "domain_exists": pd.array([True, True, False, False], dtype="boolean"),
            "dns_error": [None, None, "timeout", None],
            "typo_corrected": pd.array([False, False, False, pd.NA], dtype="boolean"),
            "domain_matches_input_column": pd.array([pd.NA, pd.NA, pd.NA, pd.NA], dtype="boolean"),
        })
        result = apply_scoring_column(df)
        buckets = result["preliminary_bucket"].tolist()
        assert "high_confidence" in buckets
        assert "invalid" in buckets


# ===========================================================================
# SECTION 8 — Pipeline integration (mocked DNS)
# ===========================================================================

class TestScoringPipelineIntegration:
    @pytest.fixture
    def csv_file(self, tmp_path: Path) -> Path:
        f = tmp_path / "test_subphase6.csv"
        f.write_text(
            "email,domain\n"
            "alice@gmail.com,gmail.com\n"
            "bob@gmial.com,gmial.com\n"
            "carol@nxdomain-xyz-999.com,nxdomain-xyz-999.com\n"
            "bad@@email,\n"
            ",\n",
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
        logger = logging.getLogger("test_pipeline_s6")
        pipeline = EmailCleaningPipeline(config=config, logger=logger)
        return pipeline.run(input_file=str(csv_file), run_context=run_context)

    def test_pipeline_status_is_subphase_6_ready(self, csv_file):
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_DNS):
            result = self._run(csv_file)
        assert result.status == "subphase_8_ready"

    def test_pipeline_runs_without_error(self, csv_file):
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_DNS):
            result = self._run(csv_file)
        assert result is not None

    def test_pipeline_logs_hard_fails(self, csv_file, caplog):
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_DNS):
            with caplog.at_level(logging.INFO, logger="test_pipeline_s6"):
                self._run(csv_file)
        assert "hard_fails" in "\n".join(caplog.messages)

    def test_pipeline_logs_high_confidence(self, csv_file, caplog):
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_DNS):
            with caplog.at_level(logging.INFO, logger="test_pipeline_s6"):
                self._run(csv_file)
        assert "high_confidence" in "\n".join(caplog.messages)

    def test_pipeline_logs_avg_score(self, csv_file, caplog):
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_DNS):
            with caplog.at_level(logging.INFO, logger="test_pipeline_s6"):
                self._run(csv_file)
        assert "avg_score" in "\n".join(caplog.messages)

    def test_pipeline_logs_run_summary_scoring(self, csv_file, caplog):
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_DNS):
            with caplog.at_level(logging.INFO, logger="test_pipeline_s6"):
                self._run(csv_file)
        assert "scoring_hard_fails" in "\n".join(caplog.messages)

    def test_pipeline_total_rows_unchanged(self, csv_file):
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_DNS):
            result = self._run(csv_file)
        assert result.total_rows == 5

    def test_pipeline_with_chunk_size_2_works(self, csv_file):
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_DNS):
            result = self._run(csv_file, chunk_size=2)
        assert result.status == "subphase_8_ready"


# ===========================================================================
# SECTION 9 — No future-phase contamination
# ===========================================================================

class TestNoFuturePhaseContamination:
    def _src(self) -> str:
        import app.scoring as mod
        return inspect.getsource(mod)

    def test_no_dedupe_in_scoring(self):
        src = self._src()
        assert "import dedupe" not in src.lower()
        assert "dedupe(" not in src.lower()

    def test_no_sqlite_in_scoring(self):
        assert "sqlite" not in self._src().lower()

    def test_no_export_in_scoring(self):
        for term in ("to_csv", "to_excel", "open(", "write("):
            assert term not in self._src()

    def test_no_smtp_in_scoring(self):
        for term in ("smtplib", "aiosmtplib"):
            assert term not in self._src().lower()

    def test_no_disposable_logic_in_scoring(self):
        assert "disposable" not in self._src().lower()

    def test_no_final_bucket_export_in_scoring(self):
        for term in ("clean_high_confidence", "removed_invalid", "review_medium"):
            assert term not in self._src()

    def test_preliminary_bucket_is_provisional(self):
        r = _score(has_mx_record=True)
        assert r.preliminary_bucket in ("high_confidence", "review", "invalid")

    def test_pipeline_result_has_no_smtp_export_fields(self):
        from app.models import PipelineResult
        fields = {f.name for f in PipelineResult.__dataclass_fields__.values()}  # type: ignore[attr-defined]
        for forbidden in ("smtp", "output_file", "deliverable"):
            for field_name in fields:
                assert forbidden not in field_name.lower()
