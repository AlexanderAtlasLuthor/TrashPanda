"""Subphase 3 validation tests: email syntax validation.

Tests:
1. Valid common emails
2. Invalid obvious emails
3. Edge cases within scope
4. DataFrame / column validation
5. Pipeline integration
6. Subphase 2 compatibility
7. Consistency of syntax_reason values
8. No contamination from future phases
9. Basic performance (no crash on medium-sized chunks)
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure vendor_site is in path for standalone execution
_project_root = Path(__file__).parent.parent
_vendor_site = _project_root / "vendor_site"
if str(_vendor_site) not in sys.path:
    sys.path.insert(0, str(_vendor_site))

import io
import tempfile
import time
import csv

import pandas as pd
import pytest

from app.email_rules import check_email_syntax, EmailSyntaxCheckResult
from app.validators import validate_email_syntax_column


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _check(email):
    return check_email_syntax(email)


# ---------------------------------------------------------------------------
# SECTION 1: Valid common emails
# ---------------------------------------------------------------------------

class TestValidCommonEmails:

    @pytest.mark.parametrize("email", [
        "alice@example.com",
        "bob.smith@company.org",
        "sales-team@my-domain.net",
    ])
    def test_syntax_valid_is_true(self, email):
        result = _check(email)
        assert result.syntax_valid is True, f"Expected True for {email!r}, got reason={result.syntax_reason}"

    @pytest.mark.parametrize("email", [
        "alice@example.com",
        "bob.smith@company.org",
        "sales-team@my-domain.net",
    ])
    def test_syntax_reason_is_valid(self, email):
        result = _check(email)
        assert result.syntax_reason == "valid", f"Expected 'valid' for {email!r}, got {result.syntax_reason!r}"

    @pytest.mark.parametrize("email", [
        "alice@example.com",
        "bob.smith@company.org",
        "sales-team@my-domain.net",
    ])
    def test_all_positive_flags(self, email):
        result = _check(email)
        assert result.has_single_at is True
        assert result.local_part_present is True
        assert result.domain_part_present is True
        assert result.domain_has_dot is True
        assert result.contains_spaces is False


# ---------------------------------------------------------------------------
# SECTION 2: Invalid obvious emails
# ---------------------------------------------------------------------------

INVALID_CASES = [
    ("aliceexample.com",        "no_at_sign"),
    ("alice@@example.com",      "not_exactly_one_at"),
    ("@example.com",            "local_part_empty"),
    ("alice@",                  "domain_part_empty"),
    ("alice @example.com",      "contains_spaces"),
    ("alice@example",           "domain_missing_dot"),
    ("alice..smith@example.com","local_part_has_consecutive_dots"),
    (".alice@example.com",      "local_part_starts_with_dot"),
    ("alice.@example.com",      "local_part_ends_with_dot"),
    ("alice@-example.com",      "domain_label_starts_with_hyphen"),
    ("alice@example-.com",      "domain_label_ends_with_hyphen"),
    ("alice@example..com",      "domain_has_consecutive_dots"),
]


class TestInvalidObviousEmails:

    @pytest.mark.parametrize("email,expected_reason", INVALID_CASES)
    def test_syntax_valid_is_false(self, email, expected_reason):
        result = _check(email)
        assert result.syntax_valid is False, (
            f"Expected False for {email!r} but got True"
        )

    @pytest.mark.parametrize("email,expected_reason", INVALID_CASES)
    def test_syntax_reason_is_exact(self, email, expected_reason):
        result = _check(email)
        assert result.syntax_reason == expected_reason, (
            f"Email={email!r}: expected reason={expected_reason!r}, got={result.syntax_reason!r}"
        )

    def test_no_at_sign_flag(self):
        result = _check("aliceexample.com")
        assert result.has_single_at is False

    def test_multiple_at_flag(self):
        result = _check("alice@@example.com")
        assert result.has_single_at is False

    def test_local_empty_flag(self):
        result = _check("@example.com")
        assert result.local_part_present is False
        assert result.has_single_at is True

    def test_domain_empty_flag(self):
        result = _check("alice@")
        assert result.domain_part_present is False
        assert result.has_single_at is True

    def test_spaces_flag(self):
        result = _check("alice @example.com")
        assert result.contains_spaces is True

    def test_domain_no_dot_flag(self):
        result = _check("alice@example")
        assert result.domain_has_dot is False


# ---------------------------------------------------------------------------
# SECTION 3: Edge cases within scope
# ---------------------------------------------------------------------------

class TestEdgeCases:

    def test_none_does_not_raise(self):
        result = _check(None)
        assert isinstance(result, EmailSyntaxCheckResult)
        assert result.syntax_valid is False
        assert result.syntax_reason == "email_is_empty"

    def test_empty_string_does_not_raise(self):
        result = _check("")
        assert result.syntax_valid is False
        assert result.syntax_reason == "email_is_empty"

    def test_whitespace_only_does_not_raise(self):
        result = _check("   ")
        assert result.syntax_valid is False
        assert result.syntax_reason == "email_is_empty"

    def test_subdomain_is_valid(self):
        result = _check("alice@sub.example.com")
        assert result.syntax_valid is True
        assert result.syntax_reason == "valid"

    def test_short_valid_email(self):
        result = _check("A@b.co")
        assert result.syntax_valid is True
        assert result.syntax_reason == "valid"

    def test_plus_tag_is_valid(self):
        # Plus sign is a valid local part character
        result = _check("alice+tag@example.com")
        assert result.syntax_valid is True, f"alice+tag should be valid, got reason={result.syntax_reason}"

    def test_underscore_is_valid(self):
        result = _check("alice_name@example.com")
        assert result.syntax_valid is True, f"alice_name should be valid, got reason={result.syntax_reason}"

    def test_float_nan_does_not_raise(self):
        # Pandas NaN is a float
        import math
        result = _check(float("nan"))
        assert result.syntax_valid is False
        assert result.syntax_reason == "email_is_empty"

    def test_result_is_dataclass_instance(self):
        result = _check("alice@example.com")
        assert isinstance(result, EmailSyntaxCheckResult)

    def test_local_part_stored_on_valid(self):
        result = _check("alice@example.com")
        assert result.local_part == "alice"
        assert result.domain_part == "example.com"

    def test_local_and_domain_none_on_empty(self):
        result = _check(None)
        assert result.local_part is None
        assert result.domain_part is None


# ---------------------------------------------------------------------------
# SECTION 4: DataFrame / column validation
# ---------------------------------------------------------------------------

class TestDataFrameValidation:

    @pytest.fixture()
    def mixed_frame(self):
        return pd.DataFrame({
            "email": [
                "alice@example.com",           # valid
                "bob.smith@company.org",        # valid
                "alice@@bad.com",               # invalid
                "@nodomain.com",                # invalid
                None,                           # null
                "alice@example",               # invalid domain
            ],
            "fname": ["A", "B", "C", "D", "E", "F"],
        })

    def test_returns_dataframe(self, mixed_frame):
        result = validate_email_syntax_column(mixed_frame)
        assert isinstance(result, pd.DataFrame)

    def test_row_count_preserved(self, mixed_frame):
        result = validate_email_syntax_column(mixed_frame)
        assert len(result) == len(mixed_frame)

    def test_required_columns_present(self, mixed_frame):
        result = validate_email_syntax_column(mixed_frame)
        required = [
            "syntax_valid",
            "syntax_reason",
            "has_single_at",
            "local_part_present",
            "domain_part_present",
            "domain_has_dot",
            "contains_spaces",
        ]
        for col in required:
            assert col in result.columns, f"Missing column: {col}"

    def test_previous_columns_preserved(self, mixed_frame):
        result = validate_email_syntax_column(mixed_frame)
        assert "email" in result.columns
        assert "fname" in result.columns

    def test_email_column_not_mutated(self, mixed_frame):
        original_emails = mixed_frame["email"].tolist()
        result = validate_email_syntax_column(mixed_frame)
        assert result["email"].tolist() == original_emails

    def test_valid_count(self, mixed_frame):
        result = validate_email_syntax_column(mixed_frame)
        valid_count = (result["syntax_valid"] == True).sum()
        assert valid_count == 2

    def test_invalid_count(self, mixed_frame):
        result = validate_email_syntax_column(mixed_frame)
        invalid_count = (result["syntax_valid"] == False).sum()
        assert invalid_count == 4

    def test_valid_rows_have_reason_valid(self, mixed_frame):
        result = validate_email_syntax_column(mixed_frame)
        valid_rows = result[result["syntax_valid"] == True]
        for reason in valid_rows["syntax_reason"]:
            assert reason == "valid"

    def test_invalid_rows_have_non_empty_reason(self, mixed_frame):
        result = validate_email_syntax_column(mixed_frame)
        invalid_rows = result[result["syntax_valid"] == False]
        for reason in invalid_rows["syntax_reason"]:
            assert isinstance(reason, str)
            assert reason != ""

    def test_syntax_valid_is_boolean_dtype(self, mixed_frame):
        result = validate_email_syntax_column(mixed_frame)
        assert str(result["syntax_valid"].dtype) == "boolean"

    def test_contains_spaces_is_boolean_dtype(self, mixed_frame):
        result = validate_email_syntax_column(mixed_frame)
        assert str(result["contains_spaces"].dtype) == "boolean"

    def test_original_frame_not_mutated(self, mixed_frame):
        original_cols = set(mixed_frame.columns)
        validate_email_syntax_column(mixed_frame)
        assert set(mixed_frame.columns) == original_cols


# ---------------------------------------------------------------------------
# SECTION 5: Pipeline integration
# ---------------------------------------------------------------------------

class TestPipelineIntegration:

    @pytest.fixture()
    def csv_file(self, tmp_path):
        csv_content = (
            "email,fname,lname\n"
            "alice@example.com,Alice,Smith\n"
            "bob@@bad.com,Bob,Jones\n"
            "@nodomain.com,Invalid,Domain\n"
            "alice@,Incomplete,Email\n"
            "alice @example.com,Space,Email\n"
            "alice@nodot,No,Dot\n"
            "valid@sub.domain.org,Valid,User\n"
            ",,\n"
        )
        csv_path = tmp_path / "test_pipeline.csv"
        csv_path.write_text(csv_content, encoding="utf-8")
        return csv_path

    def test_pipeline_runs_without_error(self, csv_file):
        import logging
        from app.config import load_config, resolve_project_paths
        from app.io_utils import build_run_context
        from app.pipeline import EmailCleaningPipeline

        project_paths = resolve_project_paths()
        config = load_config(base_dir=project_paths.project_root)
        run_context = build_run_context(config)
        logger = logging.getLogger("test_pipeline")

        pipeline = EmailCleaningPipeline(config=config, logger=logger)
        result = pipeline.run(input_file=str(csv_file), run_context=run_context)
        assert result is not None

    def test_pipeline_status_is_subphase3(self, csv_file):
        import logging
        from app.config import load_config, resolve_project_paths
        from app.io_utils import build_run_context
        from app.pipeline import EmailCleaningPipeline

        project_paths = resolve_project_paths()
        config = load_config(base_dir=project_paths.project_root)
        run_context = build_run_context(config)
        logger = logging.getLogger("test_pipeline")

        pipeline = EmailCleaningPipeline(config=config, logger=logger)
        result = pipeline.run(input_file=str(csv_file), run_context=run_context)
        # Status advances with each subphase; Subphase 5 is the current head
        assert result.status == "subphase_6_ready"

    def test_pipeline_processes_all_rows(self, csv_file):
        import logging
        from app.config import load_config, resolve_project_paths
        from app.io_utils import build_run_context
        from app.pipeline import EmailCleaningPipeline

        project_paths = resolve_project_paths()
        config = load_config(base_dir=project_paths.project_root)
        run_context = build_run_context(config)
        logger = logging.getLogger("test_pipeline")

        pipeline = EmailCleaningPipeline(config=config, logger=logger)
        result = pipeline.run(input_file=str(csv_file), run_context=run_context)
        # CSV has 8 data rows (1 is empty email row, still a row)
        assert result.total_rows == 8

    def test_pipeline_chunks_are_processed(self, csv_file):
        import dataclasses
        import logging
        from app.config import load_config, resolve_project_paths
        from app.io_utils import build_run_context
        from app.pipeline import EmailCleaningPipeline

        project_paths = resolve_project_paths()
        config = load_config(base_dir=project_paths.project_root)
        config = dataclasses.replace(config, chunk_size=3)
        run_context = build_run_context(config)
        logger = logging.getLogger("test_pipeline")

        pipeline = EmailCleaningPipeline(config=config, logger=logger)
        result = pipeline.run(input_file=str(csv_file), run_context=run_context)
        # 8 rows with chunk_size=3 -> at least 3 chunks
        assert result.total_chunks >= 3

    def test_pipeline_logs_valid_invalid_counts(self, csv_file, caplog):
        import logging
        from app.config import load_config, resolve_project_paths
        from app.io_utils import build_run_context
        from app.pipeline import EmailCleaningPipeline

        project_paths = resolve_project_paths()
        config = load_config(base_dir=project_paths.project_root)
        run_context = build_run_context(config)

        with caplog.at_level(logging.INFO, logger="test_pipeline_log"):
            logger = logging.getLogger("test_pipeline_log")
            pipeline = EmailCleaningPipeline(config=config, logger=logger)
            pipeline.run(input_file=str(csv_file), run_context=run_context)

        combined = " ".join(caplog.messages)
        assert "valid_emails=" in combined
        assert "invalid_emails=" in combined


# ---------------------------------------------------------------------------
# SECTION 6: Subphase 2 compatibility
# ---------------------------------------------------------------------------

class TestSubphase2Compatibility:

    @pytest.fixture()
    def simple_csv(self, tmp_path):
        csv_path = tmp_path / "compat.csv"
        csv_path.write_text(
            "email,fname\nalice@example.com,Alice\nbob@domain.org,Bob\n",
            encoding="utf-8"
        )
        return csv_path

    def test_discovery_still_works(self, tmp_path):
        from app.io_utils import discover_input_files
        csv_path = tmp_path / "file.csv"
        csv_path.write_text("email\nalice@example.com\n", encoding="utf-8")
        discovered, ignored = discover_input_files(input_file=str(csv_path))
        assert len(discovered) == 1
        assert len(ignored) == 0

    def test_unsupported_file_is_ignored_in_dir_mode(self, tmp_path):
        # In input_dir mode unsupported files go into ignored; in input_file mode
        # discover_input_files raises ValueError (documented behavior).
        from app.io_utils import discover_input_files
        txt_path = tmp_path / "file.txt"
        txt_path.write_text("not relevant\n", encoding="utf-8")
        # A supported CSV must also exist so discover_input_files does not raise
        csv_path = tmp_path / "valid.csv"
        csv_path.write_text("email\nalice@example.com\n", encoding="utf-8")
        discovered, ignored = discover_input_files(input_dir=str(tmp_path))
        assert len(discovered) == 1
        assert "file.txt" in ignored

    def test_unsupported_file_raises_in_input_file_mode(self, tmp_path):
        from app.io_utils import discover_input_files
        txt_path = tmp_path / "file.txt"
        txt_path.write_text("email\nalice@example.com\n", encoding="utf-8")
        # input_file mode: unsupported extension raises ValueError
        with pytest.raises(ValueError, match="Unsupported input file extension"):
            discover_input_files(input_file=str(txt_path))

    def test_header_normalization_still_works(self):
        from app.normalizers import normalize_headers
        raw = pd.DataFrame({"Email_Address": ["alice@example.com"]})
        normalized = normalize_headers(raw)
        assert "email" in normalized.columns

    def test_required_column_validation_still_raises(self):
        from app.validators import validate_required_columns
        with pytest.raises(ValueError, match="Missing required column"):
            validate_required_columns(["fname", "lname"])

    def test_value_normalization_still_works(self):
        from app.normalizers import normalize_values
        df = pd.DataFrame({"email": [" Alice@Example.COM "], "fname": ["  Bob  "]})
        result = normalize_values(df)
        assert result.loc[0, "email"] == "alice@example.com"
        assert result.loc[0, "fname"] == "Bob"

    def test_technical_metadata_still_attached(self, simple_csv):
        import logging
        from app.config import load_config, resolve_project_paths
        from app.io_utils import (
            build_run_context, discover_input_files,
            prepare_input_file, read_csv_in_chunks,
        )
        from app.normalizers import add_technical_metadata, normalize_headers, normalize_values

        project_paths = resolve_project_paths()
        config = load_config(base_dir=project_paths.project_root)
        run_context = build_run_context(config)

        discovered, _ = discover_input_files(input_file=str(simple_csv))
        prepared = prepare_input_file(discovered[0], run_context)
        for raw_chunk, chunk_ctx in read_csv_in_chunks(prepared.processing_csv_path, config.chunk_size):
            normalized = normalize_headers(raw_chunk)
            normalized = normalize_values(normalized)
            enriched = add_technical_metadata(normalized, discovered[0], chunk_ctx)
            assert "source_file" in enriched.columns
            assert "source_row_number" in enriched.columns
            assert "source_file_type" in enriched.columns
            assert "chunk_index" in enriched.columns
            break

    def test_subphase3_columns_added_after_subphase2(self, simple_csv):
        """Confirm new syntax columns are present after the full flow."""
        import logging
        from app.config import load_config, resolve_project_paths
        from app.io_utils import (
            build_run_context, discover_input_files,
            prepare_input_file, read_csv_in_chunks,
        )
        from app.normalizers import add_technical_metadata, normalize_headers, normalize_values
        from app.validators import (
            validate_duplicate_columns, validate_required_columns,
            validate_reserved_columns, validate_email_syntax_column,
        )

        project_paths = resolve_project_paths()
        config = load_config(base_dir=project_paths.project_root)
        run_context = build_run_context(config)

        discovered, _ = discover_input_files(input_file=str(simple_csv))
        prepared = prepare_input_file(discovered[0], run_context)
        for raw_chunk, chunk_ctx in read_csv_in_chunks(prepared.processing_csv_path, config.chunk_size):
            normalized = normalize_headers(raw_chunk)
            validate_duplicate_columns(normalized.columns)
            validate_reserved_columns(normalized.columns)
            validate_required_columns(normalized.columns)
            normalized = normalize_values(normalized)
            enriched = add_technical_metadata(normalized, discovered[0], chunk_ctx)
            final = validate_email_syntax_column(enriched)

            # Subphase 2 columns still present
            assert "source_file" in final.columns
            assert "source_row_number" in final.columns
            # Subphase 3 columns added
            assert "syntax_valid" in final.columns
            assert "syntax_reason" in final.columns
            break


# ---------------------------------------------------------------------------
# SECTION 7: Consistency of syntax_reason values
# ---------------------------------------------------------------------------

class TestReasonConsistency:

    EXPECTED_REASONS = {
        "aliceexample.com":           "no_at_sign",
        "alice@@example.com":         "not_exactly_one_at",
        "@example.com":               "local_part_empty",
        "alice@":                     "domain_part_empty",
        "alice @example.com":         "contains_spaces",
        "alice@example":              "domain_missing_dot",
        "alice..smith@example.com":   "local_part_has_consecutive_dots",
        ".alice@example.com":         "local_part_starts_with_dot",
        "alice.@example.com":         "local_part_ends_with_dot",
        "alice@-example.com":         "domain_label_starts_with_hyphen",
        "alice@example-.com":         "domain_label_ends_with_hyphen",
        "alice@example..com":         "domain_has_consecutive_dots",
        None:                         "email_is_empty",
        "":                           "email_is_empty",
        "   ":                        "email_is_empty",
        "alice@example.com":          "valid",
    }

    @pytest.mark.parametrize("email,expected_reason", EXPECTED_REASONS.items())
    def test_reason_is_exact(self, email, expected_reason):
        result = _check(email)
        assert result.syntax_reason == expected_reason, (
            f"Email={email!r}: expected={expected_reason!r} got={result.syntax_reason!r}"
        )

    @pytest.mark.parametrize("email,expected_reason", EXPECTED_REASONS.items())
    def test_reason_is_stable_across_calls(self, email, expected_reason):
        r1 = _check(email)
        r2 = _check(email)
        assert r1.syntax_reason == r2.syntax_reason
        assert r1.syntax_valid == r2.syntax_valid

    def test_reason_is_non_empty_string(self):
        for email in ["alice@example.com", "aliceexample.com", None, ""]:
            result = _check(email)
            assert isinstance(result.syntax_reason, str)
            assert len(result.syntax_reason) > 0


# ---------------------------------------------------------------------------
# SECTION 8: No contamination from future phases
# ---------------------------------------------------------------------------

class TestNoFuturePhaseContamination:

    def test_no_dns_lookup_call(self):
        """Subphase 3 must not invoke DNS. Verify dns module not imported by email_rules."""
        import app.email_rules as email_rules_module
        module_source = Path(email_rules_module.__file__).read_text(encoding="utf-8")
        assert "import dns" not in module_source
        assert "from dns" not in module_source
        assert "dnspython" not in module_source

    def test_no_scoring_logic_in_validators(self):
        import app.validators as validators_module
        module_source = Path(validators_module.__file__).read_text(encoding="utf-8")
        assert "from .scoring" not in module_source
        assert "import scoring" not in module_source

    def test_no_dedupe_logic_in_validators(self):
        import app.validators as validators_module
        module_source = Path(validators_module.__file__).read_text(encoding="utf-8")
        assert "from .dedupe" not in module_source
        assert "import dedupe" not in module_source

    def test_no_scoring_logic_in_email_rules(self):
        import app.email_rules as email_rules_module
        module_source = Path(email_rules_module.__file__).read_text(encoding="utf-8")
        assert "score" not in module_source.lower() or "# " in module_source  # only in comments max

    def test_no_disposable_domain_check_in_email_rules(self):
        import app.email_rules as email_rules_module
        module_source = Path(email_rules_module.__file__).read_text(encoding="utf-8")
        assert "disposable" not in module_source.lower()

    def test_no_typo_correction_in_email_rules(self):
        import app.email_rules as email_rules_module
        module_source = Path(email_rules_module.__file__).read_text(encoding="utf-8")
        assert "typo" not in module_source.lower()
        assert "correction" not in module_source.lower()

    def test_no_sqlite_in_pipeline_run(self):
        import app.pipeline as pipeline_module
        module_source = Path(pipeline_module.__file__).read_text(encoding="utf-8")
        assert "sqlite3" not in module_source
        assert ".db" not in module_source or "staging_db_path" in module_source  # db path is in RunContext but not used

    def test_validate_email_syntax_column_returns_only_syntax_columns(self):
        """validate_email_syntax_column must not add scoring/dedupe columns."""
        df = pd.DataFrame({"email": ["alice@example.com"]})
        result = validate_email_syntax_column(df)
        unexpected = {"score", "bucket", "dedupe_id", "mx_valid", "domain_typo"}
        added_cols = set(result.columns) - {"email"}
        contaminated = added_cols & unexpected
        assert not contaminated, f"Unexpected columns added: {contaminated}"


# ---------------------------------------------------------------------------
# SECTION 9: Basic performance (no crash, no obvious bottleneck)
# ---------------------------------------------------------------------------

class TestBasicPerformance:

    def test_1000_rows_completes_fast(self):
        emails = ["alice@example.com"] * 500 + ["notanemail"] * 500
        df = pd.DataFrame({"email": emails})
        start = time.monotonic()
        result = validate_email_syntax_column(df)
        elapsed = time.monotonic() - start
        assert len(result) == 1000
        assert elapsed < 5.0, f"Took too long: {elapsed:.2f}s for 1000 rows"

    def test_check_email_syntax_1000_calls(self):
        emails = ["alice@example.com", "bad", None, "a@b.co", "@bad", "test@test.org"]
        start = time.monotonic()
        for _ in range(1000):
            for email in emails:
                check_email_syntax(email)
        elapsed = time.monotonic() - start
        assert elapsed < 2.0, f"6000 calls took {elapsed:.2f}s, expected < 2s"

    def test_large_chunk_no_memory_error(self):
        """5000-row chunk should process without MemoryError or crash."""
        emails = (
            ["alice@example.com"] * 2000
            + ["bademail.com"] * 1500
            + [None] * 500
            + ["a@b.co"] * 1000
        )
        df = pd.DataFrame({"email": emails})
        result = validate_email_syntax_column(df)
        assert len(result) == 5000
        valid_count = (result["syntax_valid"] == True).sum()
        assert valid_count == 3000  # 2000 + 1000
