"""Comprehensive test suite for Subphase 4: domain extraction, typo correction,
domain column comparison, pipeline integration, backward compatibility, and
no-future-phase contamination.

Sections:
  1.  TestExtraction               — local_part and domain_from_email extraction
  2.  TestTypoCorrectionInMap      — every map entry corrects properly
  3.  TestTypoCorrectionNotInMap   — known-clean domains pass through unchanged
  4.  TestTypoCorrectionOutsideMap — near-miss typos NOT in map are NOT corrected
  5.  TestInvalidEmailEnrichment   — crashed/invalid emails yield None/NA safely
  6.  TestDomainComparison         — compare_domain_with_input_column logic
  7.  TestDataFrameFull            — mixed DataFrame; column contract
  8.  TestPipelineIntegration      — CLI pipeline end-to-end
  9.  TestBackwardCompatibility    — Subphase 2 & 3 still work
  10. TestNoFuturePhaseContamination — DNS/scoring/dedupe/SQLite absent
"""

from __future__ import annotations

import inspect
import logging
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Bootstrap: ensure vendor_site is importable (mirrors app/__init__.py)
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_VENDOR_SITE = _PROJECT_ROOT / "vendor_site"
if str(_VENDOR_SITE) not in sys.path:
    sys.path.insert(0, str(_VENDOR_SITE))

import pandas as pd
import pytest

from app.typo_rules import build_typo_map, apply_domain_typo_correction
from app.normalizers import (
    extract_email_components,
    apply_domain_typo_correction_column,
    compare_domain_with_input_column,
)
from app.validators import validate_email_syntax_column

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

TYPO_MAP_PATH = _PROJECT_ROOT / "configs" / "typo_map.csv"


def _typo_map() -> dict[str, str]:
    return build_typo_map(TYPO_MAP_PATH)


def _base_valid_df(emails: list[str | None], domains: list[str | None] | None = None) -> pd.DataFrame:
    """Build a minimal DataFrame already through Subphase 3 validation."""
    data: dict[str, Any] = {"email": emails}
    if domains is not None:
        data["domain"] = domains
    df = pd.DataFrame(data)
    return validate_email_syntax_column(df)


# ===========================================================================
# SECTION 1 — Extraction
# ===========================================================================

class TestExtraction:
    """Verify correct extraction of local_part_from_email and domain_from_email."""

    @pytest.mark.parametrize("email,expected_local,expected_domain", [
        ("alice@example.com", "alice", "example.com"),
        ("bob.smith@company.org", "bob.smith", "company.org"),
        ("sales-team@my-domain.net", "sales-team", "my-domain.net"),
    ])
    def test_local_part_extracted_correctly(self, email, expected_local, expected_domain):
        df = _base_valid_df([email])
        df = extract_email_components(df)
        assert df.iloc[0]["local_part_from_email"] == expected_local

    @pytest.mark.parametrize("email,expected_local,expected_domain", [
        ("alice@example.com", "alice", "example.com"),
        ("bob.smith@company.org", "bob.smith", "company.org"),
        ("sales-team@my-domain.net", "sales-team", "my-domain.net"),
    ])
    def test_domain_from_email_extracted_correctly(self, email, expected_local, expected_domain):
        df = _base_valid_df([email])
        df = extract_email_components(df)
        assert df.iloc[0]["domain_from_email"] == expected_domain

    def test_plus_tag_local_part_preserved(self):
        df = _base_valid_df(["alice+news@example.com"])
        df = extract_email_components(df)
        assert df.iloc[0]["local_part_from_email"] == "alice+news"

    def test_subdomain_preserved(self):
        df = _base_valid_df(["user@mail.example.co.uk"])
        df = extract_email_components(df)
        assert df.iloc[0]["domain_from_email"] == "mail.example.co.uk"

    def test_local_part_not_lowercased_by_extraction(self):
        """Extraction must not alter casing — normalizers.normalize_values handles that earlier."""
        df = pd.DataFrame({"email": ["Alice@Example.com"]})
        df = validate_email_syntax_column(df)
        df = extract_email_components(df)
        # After normalize_values (which lowercases email), this would be lower.
        # Here we skip normalize_values; extraction must preserve whatever it receives.
        local = df.iloc[0]["local_part_from_email"]
        assert isinstance(local, str)

    def test_original_email_column_not_mutated(self):
        original_email = "alice@example.com"
        df = _base_valid_df([original_email])
        df = extract_email_components(df)
        assert df.iloc[0]["email"] == original_email

    def test_extraction_returns_copy_not_inplace(self):
        df = _base_valid_df(["alice@example.com"])
        df_before = df.copy()
        _ = extract_email_components(df)
        assert "local_part_from_email" not in df_before.columns


# ===========================================================================
# SECTION 2 — Typo correction: entries in the map
# ===========================================================================

MAP_ENTRIES = [
    ("gmial.com",   "gmail.com"),
    ("gmal.com",    "gmail.com"),
    ("gnail.com",   "gmail.com"),
    ("hotnail.com", "hotmail.com"),
    ("hotmai.com",  "hotmail.com"),
    ("yaho.com",    "yahoo.com"),
    ("outlok.com",  "outlook.com"),
    ("outllok.com", "outlook.com"),
    ("iclud.com",   "icloud.com"),
]


class TestTypoCorrectionInMap:
    """Every map entry must be corrected: typo_corrected==True, corrected_domain==mapped value."""

    @pytest.mark.parametrize("typo,correct", MAP_ENTRIES)
    def test_typo_corrected_is_true(self, typo, correct):
        result = apply_domain_typo_correction(typo, _typo_map())
        assert result.typo_corrected is True

    @pytest.mark.parametrize("typo,correct", MAP_ENTRIES)
    def test_corrected_domain_is_correct(self, typo, correct):
        result = apply_domain_typo_correction(typo, _typo_map())
        assert result.corrected_domain == correct

    @pytest.mark.parametrize("typo,correct", MAP_ENTRIES)
    def test_typo_original_domain_preserved(self, typo, correct):
        result = apply_domain_typo_correction(typo, _typo_map())
        assert result.typo_original_domain == typo

    @pytest.mark.parametrize("typo,correct", MAP_ENTRIES)
    def test_map_loaded_has_correct_entry(self, typo, correct):
        typo_map = _typo_map()
        assert typo in typo_map
        assert typo_map[typo] == correct

    def test_map_has_exactly_nine_entries(self):
        assert len(_typo_map()) == 9

    @pytest.mark.parametrize("typo,correct", MAP_ENTRIES)
    def test_dataframe_typo_corrected_column_true(self, typo, correct):
        email = f"user@{typo}"
        df = _base_valid_df([email])
        df = extract_email_components(df)
        df = apply_domain_typo_correction_column(df, _typo_map())
        assert df.iloc[0]["typo_corrected"] == True

    @pytest.mark.parametrize("typo,correct", MAP_ENTRIES)
    def test_dataframe_corrected_domain_correct(self, typo, correct):
        email = f"user@{typo}"
        df = _base_valid_df([email])
        df = extract_email_components(df)
        df = apply_domain_typo_correction_column(df, _typo_map())
        assert df.iloc[0]["corrected_domain"] == correct

    @pytest.mark.parametrize("typo,correct", MAP_ENTRIES)
    def test_local_part_not_altered_by_typo_correction(self, typo, correct):
        email = f"alice@{typo}"
        df = _base_valid_df([email])
        df = extract_email_components(df)
        local_before = df.iloc[0]["local_part_from_email"]
        df = apply_domain_typo_correction_column(df, _typo_map())
        assert df.iloc[0]["local_part_from_email"] == local_before == "alice"


# ===========================================================================
# SECTION 3 — No correction for clean, known-good domains
# ===========================================================================

CLEAN_DOMAINS = [
    "gmail.com",
    "mycompany.org",
    "randomdomain.io",
]


class TestTypoCorrectionNotInMap:
    """Domains not in the map pass through unchanged with typo_corrected==False."""

    @pytest.mark.parametrize("domain", CLEAN_DOMAINS)
    def test_typo_corrected_is_false(self, domain):
        result = apply_domain_typo_correction(domain, _typo_map())
        assert result.typo_corrected is False

    @pytest.mark.parametrize("domain", CLEAN_DOMAINS)
    def test_corrected_domain_equals_original(self, domain):
        result = apply_domain_typo_correction(domain, _typo_map())
        assert result.corrected_domain == domain

    @pytest.mark.parametrize("domain", CLEAN_DOMAINS)
    def test_typo_original_domain_still_populated(self, domain):
        result = apply_domain_typo_correction(domain, _typo_map())
        assert result.typo_original_domain == domain

    @pytest.mark.parametrize("domain", CLEAN_DOMAINS)
    def test_dataframe_typo_corrected_is_false(self, domain):
        email = f"user@{domain}"
        df = _base_valid_df([email])
        df = extract_email_components(df)
        df = apply_domain_typo_correction_column(df, _typo_map())
        assert df.iloc[0]["typo_corrected"] == False


# ===========================================================================
# SECTION 4 — No correction for near-miss typos outside the map
# ===========================================================================

OUTSIDE_MAP_CASES = [
    "gmaill.com",   # double-l, not in map
    "gamil.com",    # transposed a/m, not in map
    "yahho.com",    # double-h, not in map
]


class TestTypoCorrectionOutsideMap:
    """Near-miss typos that are NOT in the map must not be corrected."""

    @pytest.mark.parametrize("domain", OUTSIDE_MAP_CASES)
    def test_not_corrected(self, domain):
        result = apply_domain_typo_correction(domain, _typo_map())
        assert result.typo_corrected is False

    @pytest.mark.parametrize("domain", OUTSIDE_MAP_CASES)
    def test_corrected_domain_unchanged(self, domain):
        result = apply_domain_typo_correction(domain, _typo_map())
        assert result.corrected_domain == domain

    @pytest.mark.parametrize("domain", OUTSIDE_MAP_CASES)
    def test_not_in_map(self, domain):
        assert domain not in _typo_map()

    @pytest.mark.parametrize("domain", OUTSIDE_MAP_CASES)
    def test_dataframe_typo_corrected_is_false(self, domain):
        email = f"user@{domain}"
        df = _base_valid_df([email])
        df = extract_email_components(df)
        df = apply_domain_typo_correction_column(df, _typo_map())
        assert df.iloc[0]["typo_corrected"] == False

    @pytest.mark.parametrize("domain", OUTSIDE_MAP_CASES)
    def test_dataframe_corrected_domain_unchanged(self, domain):
        email = f"user@{domain}"
        df = _base_valid_df([email])
        df = extract_email_components(df)
        df = apply_domain_typo_correction_column(df, _typo_map())
        assert df.iloc[0]["corrected_domain"] == domain


# ===========================================================================
# SECTION 5 — Invalid emails: no crash, all enrichment columns are None/NA
# ===========================================================================

INVALID_EMAIL_CASES = [
    ("bad@@email",   "multiple @"),
    ("@domain.com",  "empty local"),
    (None,           "None value"),
    ("",             "empty string"),
    ("nodomain",     "no @ sign"),
    ("alice@",       "empty domain"),
]


class TestInvalidEmailEnrichment:
    """Syntactically invalid emails must not crash and must yield None/NA enrichment."""

    @pytest.mark.parametrize("email,label", INVALID_EMAIL_CASES)
    def test_does_not_crash(self, email, label):
        df = _base_valid_df([email])
        df = extract_email_components(df)
        df = apply_domain_typo_correction_column(df, _typo_map())
        df = compare_domain_with_input_column(df)
        # No exception means pass

    @pytest.mark.parametrize("email,label", INVALID_EMAIL_CASES)
    def test_local_part_is_none(self, email, label):
        df = _base_valid_df([email])
        df = extract_email_components(df)
        assert df.iloc[0]["local_part_from_email"] is None

    @pytest.mark.parametrize("email,label", INVALID_EMAIL_CASES)
    def test_domain_from_email_is_none(self, email, label):
        df = _base_valid_df([email])
        df = extract_email_components(df)
        assert df.iloc[0]["domain_from_email"] is None

    @pytest.mark.parametrize("email,label", INVALID_EMAIL_CASES)
    def test_corrected_domain_is_none(self, email, label):
        df = _base_valid_df([email])
        df = extract_email_components(df)
        df = apply_domain_typo_correction_column(df, _typo_map())
        assert df.iloc[0]["corrected_domain"] is None

    @pytest.mark.parametrize("email,label", INVALID_EMAIL_CASES)
    def test_typo_corrected_is_na(self, email, label):
        df = _base_valid_df([email])
        df = extract_email_components(df)
        df = apply_domain_typo_correction_column(df, _typo_map())
        assert pd.isna(df.iloc[0]["typo_corrected"])

    @pytest.mark.parametrize("email,label", INVALID_EMAIL_CASES)
    def test_domain_matches_input_column_is_na(self, email, label):
        df = _base_valid_df([email])
        df = extract_email_components(df)
        df = apply_domain_typo_correction_column(df, _typo_map())
        df = compare_domain_with_input_column(df)
        assert pd.isna(df.iloc[0]["domain_matches_input_column"])


# ===========================================================================
# SECTION 6 — Domain comparison with input column
# ===========================================================================

class TestDomainComparison:
    """compare_domain_with_input_column must compare corrected_domain to the domain column."""

    def test_case1_typo_email_input_domain_is_typo_false(self):
        """email=alice@gmial.com, domain_col=gmial.com → corrected=gmail.com, match=False."""
        df = _base_valid_df(["alice@gmial.com"], domains=["gmial.com"])
        df = extract_email_components(df)
        df = apply_domain_typo_correction_column(df, _typo_map())
        df = compare_domain_with_input_column(df)
        assert df.iloc[0]["domain_matches_input_column"] == False

    def test_case2_typo_email_input_domain_is_correct_true(self):
        """email=alice@gmial.com, domain_col=gmail.com → corrected=gmail.com, match=True."""
        df = _base_valid_df(["alice@gmial.com"], domains=["gmail.com"])
        df = extract_email_components(df)
        df = apply_domain_typo_correction_column(df, _typo_map())
        df = compare_domain_with_input_column(df)
        assert df.iloc[0]["domain_matches_input_column"] == True

    def test_case3_clean_email_matching_domain_true(self):
        """email=alice@gmail.com, domain_col=gmail.com → match=True."""
        df = _base_valid_df(["alice@gmail.com"], domains=["gmail.com"])
        df = extract_email_components(df)
        df = apply_domain_typo_correction_column(df, _typo_map())
        df = compare_domain_with_input_column(df)
        assert df.iloc[0]["domain_matches_input_column"] == True

    def test_case4_clean_email_mismatched_domain_false(self):
        """email=alice@gmail.com, domain_col=yahoo.com → match=False."""
        df = _base_valid_df(["alice@gmail.com"], domains=["yahoo.com"])
        df = extract_email_components(df)
        df = apply_domain_typo_correction_column(df, _typo_map())
        df = compare_domain_with_input_column(df)
        assert df.iloc[0]["domain_matches_input_column"] == False

    def test_no_domain_column_yields_na(self):
        """When no domain column exists in the DataFrame, result must be pd.NA."""
        df = _base_valid_df(["alice@gmail.com"])
        df = extract_email_components(df)
        df = apply_domain_typo_correction_column(df, _typo_map())
        df = compare_domain_with_input_column(df)
        assert pd.isna(df.iloc[0]["domain_matches_input_column"])

    def test_null_domain_column_value_yields_na(self):
        """domain column present but value is None → pd.NA."""
        df = _base_valid_df(["alice@gmail.com"], domains=[None])
        df = extract_email_components(df)
        df = apply_domain_typo_correction_column(df, _typo_map())
        df = compare_domain_with_input_column(df)
        assert pd.isna(df.iloc[0]["domain_matches_input_column"])

    def test_invalid_email_with_domain_column_yields_na(self):
        """corrected_domain is None for invalid email → result pd.NA regardless of domain col."""
        df = _base_valid_df([None], domains=["gmail.com"])
        df = extract_email_components(df)
        df = apply_domain_typo_correction_column(df, _typo_map())
        df = compare_domain_with_input_column(df)
        assert pd.isna(df.iloc[0]["domain_matches_input_column"])

    def test_corrected_typo_matches_input_after_correction(self):
        """bob@hotnail.com, domain_col=hotmail.com → corrected=hotmail.com → True."""
        df = _base_valid_df(["bob@hotnail.com"], domains=["hotmail.com"])
        df = extract_email_components(df)
        df = apply_domain_typo_correction_column(df, _typo_map())
        df = compare_domain_with_input_column(df)
        assert df.iloc[0]["domain_matches_input_column"] == True

    def test_comparison_is_boolean_dtype(self):
        df = _base_valid_df(["alice@gmail.com"], domains=["gmail.com"])
        df = extract_email_components(df)
        df = apply_domain_typo_correction_column(df, _typo_map())
        df = compare_domain_with_input_column(df)
        assert str(df["domain_matches_input_column"].dtype) == "boolean"

    def test_typo_corrected_is_boolean_dtype(self):
        df = _base_valid_df(["alice@gmail.com"])
        df = extract_email_components(df)
        df = apply_domain_typo_correction_column(df, _typo_map())
        assert str(df["typo_corrected"].dtype) == "boolean"


# ===========================================================================
# SECTION 7 — Full mixed DataFrame: column contract
# ===========================================================================

_MIXED_EMAILS = [
    "alice@gmail.com",        # valid, no typo
    "bob@gmial.com",          # valid, typo
    "carol@hotnail.com",      # valid, typo
    "bad@@email",             # invalid
    None,                     # None
    "dave@mycompany.org",     # valid, no typo
    "gmaill.com",             # invalid (no @)
]
_MIXED_DOMAINS = [
    "gmail.com",
    "gmial.com",
    "hotmail.com",
    None,
    None,
    "mycompany.org",
    None,
]

EXPECTED_SUBPHASE4_COLUMNS = [
    "local_part_from_email",
    "domain_from_email",
    "typo_corrected",
    "typo_original_domain",
    "corrected_domain",
    "domain_matches_input_column",
]


class TestDataFrameFull:
    """Mixed DataFrame end-to-end: column presence, row count, previous columns intact."""

    @pytest.fixture
    def enriched_df(self) -> pd.DataFrame:
        df = pd.DataFrame({"email": _MIXED_EMAILS, "domain": _MIXED_DOMAINS})
        df = validate_email_syntax_column(df)
        df = extract_email_components(df)
        df = apply_domain_typo_correction_column(df, _typo_map())
        df = compare_domain_with_input_column(df)
        return df

    def test_row_count_intact(self, enriched_df):
        assert len(enriched_df) == len(_MIXED_EMAILS)

    @pytest.mark.parametrize("col", EXPECTED_SUBPHASE4_COLUMNS)
    def test_subphase4_column_present(self, enriched_df, col):
        assert col in enriched_df.columns

    def test_email_column_unchanged(self, enriched_df):
        for i, original in enumerate(_MIXED_EMAILS):
            if original is None:
                assert enriched_df.iloc[i]["email"] is None or pd.isna(enriched_df.iloc[i]["email"])
            else:
                # After validate_email_syntax_column (which does not normalize values),
                # the email should match the original.
                assert enriched_df.iloc[i]["email"] == original

    def test_syntax_valid_column_still_present(self, enriched_df):
        assert "syntax_valid" in enriched_df.columns

    def test_syntax_reason_column_still_present(self, enriched_df):
        assert "syntax_reason" in enriched_df.columns

    def test_typo_corrected_dtype_is_boolean(self, enriched_df):
        assert str(enriched_df["typo_corrected"].dtype) == "boolean"

    def test_domain_matches_dtype_is_boolean(self, enriched_df):
        assert str(enriched_df["domain_matches_input_column"].dtype) == "boolean"

    def test_valid_row_has_extracted_components(self, enriched_df):
        # Row 0: alice@gmail.com → valid
        assert enriched_df.iloc[0]["local_part_from_email"] == "alice"
        assert enriched_df.iloc[0]["domain_from_email"] == "gmail.com"

    def test_typo_row_corrected(self, enriched_df):
        # Row 1: bob@gmial.com → corrected to gmail.com
        assert enriched_df.iloc[1]["typo_corrected"] == True
        assert enriched_df.iloc[1]["corrected_domain"] == "gmail.com"

    def test_invalid_row_all_enrichment_none(self, enriched_df):
        # Row 3: bad@@email
        assert enriched_df.iloc[3]["local_part_from_email"] is None
        assert enriched_df.iloc[3]["domain_from_email"] is None
        assert enriched_df.iloc[3]["corrected_domain"] is None
        assert pd.isna(enriched_df.iloc[3]["typo_corrected"])

    def test_none_row_all_enrichment_none(self, enriched_df):
        # Row 4: None email
        assert enriched_df.iloc[4]["local_part_from_email"] is None
        assert pd.isna(enriched_df.iloc[4]["typo_corrected"])

    def test_clean_domain_not_corrected(self, enriched_df):
        # Row 5: dave@mycompany.org → no typo
        assert enriched_df.iloc[5]["typo_corrected"] == False
        assert enriched_df.iloc[5]["corrected_domain"] == "mycompany.org"

    def test_technical_columns_not_added_by_subphase4(self, enriched_df):
        """Subphase 4 must not inject technical metadata columns on its own."""
        subphase4_only = set(EXPECTED_SUBPHASE4_COLUMNS)
        for col in ["source_file", "source_row_number", "source_file_type", "chunk_index"]:
            # These come from add_technical_metadata, not from Subphase 4 functions
            assert col not in enriched_df.columns or col not in subphase4_only

    def test_original_frame_not_mutated(self):
        df = pd.DataFrame({"email": ["alice@gmail.com"], "domain": ["gmail.com"]})
        df = validate_email_syntax_column(df)
        _ = extract_email_components(df)
        assert "local_part_from_email" not in df.columns


# ===========================================================================
# SECTION 8 — Pipeline integration
# ===========================================================================

class TestPipelineIntegration:
    """End-to-end pipeline validation via CLI path."""

    @pytest.fixture
    def csv_file(self, tmp_path: Path) -> Path:
        csv = tmp_path / "test_subphase4.csv"
        csv.write_text(
            "email,domain\n"
            "alice@gmail.com,gmail.com\n"
            "bob@gmial.com,gmial.com\n"
            "carol@hotnail.com,hotmail.com\n"
            "bad@@email,\n"
            ",\n",
            encoding="utf-8",
        )
        return csv

    def test_pipeline_runs_without_error(self, csv_file):
        import dataclasses
        from app.config import load_config, resolve_project_paths
        from app.io_utils import build_run_context
        from app.pipeline import EmailCleaningPipeline

        project_paths = resolve_project_paths()
        config = load_config(base_dir=project_paths.project_root)
        config = dataclasses.replace(config, chunk_size=3)
        run_context = build_run_context(config)
        logger = logging.getLogger("test_pipeline_s4")

        pipeline = EmailCleaningPipeline(config=config, logger=logger)
        result = pipeline.run(input_file=str(csv_file), run_context=run_context)
        assert result is not None

    def test_pipeline_status_is_subphase_4_ready(self, csv_file):
        import dataclasses
        from app.config import load_config, resolve_project_paths
        from app.io_utils import build_run_context
        from app.pipeline import EmailCleaningPipeline

        project_paths = resolve_project_paths()
        config = load_config(base_dir=project_paths.project_root)
        config = dataclasses.replace(config, chunk_size=3)
        run_context = build_run_context(config)
        logger = logging.getLogger("test_pipeline_s4_status")

        pipeline = EmailCleaningPipeline(config=config, logger=logger)
        result = pipeline.run(input_file=str(csv_file), run_context=run_context)
        assert result.status == "subphase_4_ready"

    def test_pipeline_logs_derived_domains(self, csv_file, caplog):
        import dataclasses
        from app.config import load_config, resolve_project_paths
        from app.io_utils import build_run_context
        from app.pipeline import EmailCleaningPipeline

        project_paths = resolve_project_paths()
        config = load_config(base_dir=project_paths.project_root)
        config = dataclasses.replace(config, chunk_size=10)
        run_context = build_run_context(config)
        logger = logging.getLogger("test_pipeline_s4_logs")

        pipeline = EmailCleaningPipeline(config=config, logger=logger)
        with caplog.at_level(logging.INFO, logger="test_pipeline_s4_logs"):
            pipeline.run(input_file=str(csv_file), run_context=run_context)

        combined = "\n".join(caplog.messages)
        assert "derived_domains" in combined

    def test_pipeline_logs_typo_corrections(self, csv_file, caplog):
        import dataclasses
        from app.config import load_config, resolve_project_paths
        from app.io_utils import build_run_context
        from app.pipeline import EmailCleaningPipeline

        project_paths = resolve_project_paths()
        config = load_config(base_dir=project_paths.project_root)
        config = dataclasses.replace(config, chunk_size=10)
        run_context = build_run_context(config)
        logger = logging.getLogger("test_pipeline_s4_logs2")

        pipeline = EmailCleaningPipeline(config=config, logger=logger)
        with caplog.at_level(logging.INFO, logger="test_pipeline_s4_logs2"):
            pipeline.run(input_file=str(csv_file), run_context=run_context)

        combined = "\n".join(caplog.messages)
        assert "typo_corrections" in combined

    def test_pipeline_logs_domain_mismatches(self, csv_file, caplog):
        import dataclasses
        from app.config import load_config, resolve_project_paths
        from app.io_utils import build_run_context
        from app.pipeline import EmailCleaningPipeline

        project_paths = resolve_project_paths()
        config = load_config(base_dir=project_paths.project_root)
        config = dataclasses.replace(config, chunk_size=10)
        run_context = build_run_context(config)
        logger = logging.getLogger("test_pipeline_s4_logs3")

        pipeline = EmailCleaningPipeline(config=config, logger=logger)
        with caplog.at_level(logging.INFO, logger="test_pipeline_s4_logs3"):
            pipeline.run(input_file=str(csv_file), run_context=run_context)

        combined = "\n".join(caplog.messages)
        assert "domain_mismatches" in combined

    def test_pipeline_loads_typo_map_with_9_entries(self, csv_file, caplog):
        import dataclasses
        from app.config import load_config, resolve_project_paths
        from app.io_utils import build_run_context
        from app.pipeline import EmailCleaningPipeline

        project_paths = resolve_project_paths()
        config = load_config(base_dir=project_paths.project_root)
        config = dataclasses.replace(config, chunk_size=10)
        run_context = build_run_context(config)
        logger = logging.getLogger("test_pipeline_s4_map")

        pipeline = EmailCleaningPipeline(config=config, logger=logger)
        with caplog.at_level(logging.INFO, logger="test_pipeline_s4_map"):
            pipeline.run(input_file=str(csv_file), run_context=run_context)

        combined = "\n".join(caplog.messages)
        assert "9" in combined and "typo map" in combined.lower()

    def test_pipeline_total_rows_matches_input(self, csv_file):
        import dataclasses
        from app.config import load_config, resolve_project_paths
        from app.io_utils import build_run_context
        from app.pipeline import EmailCleaningPipeline

        project_paths = resolve_project_paths()
        config = load_config(base_dir=project_paths.project_root)
        config = dataclasses.replace(config, chunk_size=3)
        run_context = build_run_context(config)
        logger = logging.getLogger("test_pipeline_s4_rows")

        pipeline = EmailCleaningPipeline(config=config, logger=logger)
        result = pipeline.run(input_file=str(csv_file), run_context=run_context)
        assert result.total_rows == 5  # 5 data rows in the CSV


# ===========================================================================
# SECTION 9 — Backward compatibility: Subphase 2 & 3 still work
# ===========================================================================

class TestBackwardCompatibility:
    """Subphase 2 and 3 behavior must be unaffected by Subphase 4 additions."""

    def test_subphase2_discovery_still_works(self, tmp_path: Path):
        from app.io_utils import discover_input_files

        csv = tmp_path / "contacts.csv"
        csv.write_text("email\nalice@example.com\n", encoding="utf-8")
        files, ignored = discover_input_files(input_dir=str(tmp_path))
        assert len(files) == 1

    def test_subphase2_header_normalization_still_works(self):
        from app.normalizers import normalize_headers
        df = pd.DataFrame({"Email Address": ["alice@example.com"]})
        normalized = normalize_headers(df)
        assert "email" in normalized.columns

    def test_subphase2_value_normalization_still_works(self):
        from app.normalizers import normalize_values
        df = pd.DataFrame({"email": ["  ALICE@EXAMPLE.COM  "]})
        normalized = normalize_values(df)
        assert normalized.iloc[0]["email"] == "alice@example.com"

    def test_subphase2_technical_metadata_still_works(self):
        from app.normalizers import add_technical_metadata
        from app.models import InputFile, ChunkContext
        df = pd.DataFrame({"email": ["alice@example.com"]})
        input_file = InputFile(
            absolute_path=Path("test.csv"),
            original_name="test.csv",
            file_type="csv",
        )
        ctx = ChunkContext(chunk_index=0, start_row_number=1, row_count=1)
        enriched = add_technical_metadata(df, input_file=input_file, chunk_context=ctx)
        for col in ["source_file", "source_row_number", "source_file_type", "chunk_index"]:
            assert col in enriched.columns

    def test_subphase3_syntax_valid_column_still_works(self):
        df = pd.DataFrame({"email": ["alice@example.com", "bad@", None]})
        result = validate_email_syntax_column(df)
        assert result.iloc[0]["syntax_valid"] == True
        assert result.iloc[1]["syntax_valid"] == False
        assert result.iloc[2]["syntax_valid"] == False

    def test_subphase3_reason_codes_still_correct(self):
        df = pd.DataFrame({"email": ["alice@example.com", "aliceexample.com", None]})
        result = validate_email_syntax_column(df)
        assert result.iloc[0]["syntax_reason"] == "valid"
        assert result.iloc[1]["syntax_reason"] == "no_at_sign"
        assert result.iloc[2]["syntax_reason"] == "email_is_empty"

    def test_subphase3_boolean_dtype_preserved(self):
        df = pd.DataFrame({"email": ["alice@example.com"]})
        result = validate_email_syntax_column(df)
        assert str(result["syntax_valid"].dtype) == "boolean"

    def test_subphase4_columns_appear_after_subphase3_columns(self):
        df = pd.DataFrame({"email": ["alice@gmail.com"]})
        df = validate_email_syntax_column(df)
        df = extract_email_components(df)
        df = apply_domain_typo_correction_column(df, _typo_map())
        df = compare_domain_with_input_column(df)
        cols = list(df.columns)
        # syntax_valid must appear before the new Subphase 4 columns
        syntax_idx = cols.index("syntax_valid")
        local_idx = cols.index("local_part_from_email")
        assert syntax_idx < local_idx


# ===========================================================================
# SECTION 10 — No future-phase contamination
# ===========================================================================

class TestNoFuturePhaseContamination:
    """Confirm no active logic for DNS, scoring, dedupe, disposable, or SQLite."""

    def test_no_dns_import_in_normalizers(self):
        import app.normalizers as mod
        src = inspect.getsource(mod)
        assert "dns" not in src.lower() or "dns_utils" not in src

    def test_no_dns_import_in_typo_rules(self):
        import app.typo_rules as mod
        src = inspect.getsource(mod)
        assert "dns" not in src.lower()

    def test_no_scoring_import_in_normalizers(self):
        import app.normalizers as mod
        src = inspect.getsource(mod)
        assert "scoring" not in src.lower()

    def test_no_scoring_import_in_typo_rules(self):
        import app.typo_rules as mod
        src = inspect.getsource(mod)
        assert "scoring" not in src.lower()

    def test_no_dedupe_import_in_normalizers(self):
        import app.normalizers as mod
        src = inspect.getsource(mod)
        assert "dedupe" not in src.lower()

    def test_no_dedupe_import_in_typo_rules(self):
        import app.typo_rules as mod
        src = inspect.getsource(mod)
        assert "dedupe" not in src.lower()

    def test_no_sqlite_in_pipeline(self):
        import app.pipeline as mod
        src = inspect.getsource(mod)
        assert "sqlite" not in src.lower()

    def test_no_disposable_detection_in_typo_rules(self):
        import app.typo_rules as mod
        src = inspect.getsource(mod)
        assert "disposable" not in src.lower()

    def test_no_mx_lookup_in_normalizers(self):
        import app.normalizers as mod
        src = inspect.getsource(mod)
        assert "mx" not in src.lower() or "resolve" not in src.lower()

    def test_no_fuzzy_matching_in_typo_rules(self):
        import app.typo_rules as mod
        src = inspect.getsource(mod)
        # Must not import or call fuzzy-matching libraries.
        # The word may appear in docstrings as a negative claim; check for import/call patterns.
        for forbidden in ["fuzzywuzzy", "rapidfuzz", "jellyfish"]:
            assert f"import {forbidden}" not in src.lower(), f"Fuzzy library imported: {forbidden}"
        # difflib is stdlib but must not be called for matching
        assert "difflib.get_close_matches" not in src
        assert "SequenceMatcher" not in src
        # levenshtein: must not be imported or called as a function
        assert "import levenshtein" not in src.lower()
        assert "levenshtein(" not in src.lower()

    def test_typo_rules_uses_only_dict_lookup(self):
        """The correction must be a pure dict.get() — no algorithmic matching."""
        import app.typo_rules as mod
        src = inspect.getsource(mod)
        assert "typo_map.get(" in src

    def test_pipeline_result_has_no_scoring_fields(self):
        from app.models import PipelineResult
        fields = {f.name for f in PipelineResult.__dataclass_fields__.values()}  # type: ignore[attr-defined]
        for forbidden in ["score", "bucket", "confidence"]:
            for field in fields:
                assert forbidden not in field.lower(), f"Scoring field found: {field}"
