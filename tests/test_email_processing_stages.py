"""Subphase-3 engine-refactor tests: migrated email-processing stages.

Each stage is tested in isolation with a minimal DataFrame and an
engine-vs-inline equivalence test verifies that running the four stages
through ``PipelineEngine`` produces exactly the same frame as calling
the underlying functions in the previous inline order.
"""

from __future__ import annotations

import pandas as pd

from app.engine import ChunkPayload, PipelineContext, PipelineEngine
from app.engine.stages import (
    DomainComparisonStage,
    DomainExtractionStage,
    EmailSyntaxValidationStage,
    TypoCorrectionStage,
)


# ---------------------------------------------------------------------------
# Per-stage unit tests
# ---------------------------------------------------------------------------

class TestEmailSyntaxValidationStage:
    def test_adds_syntax_columns(self):
        df = pd.DataFrame({"email": ["ok@ok.com", "bad@", "no-at-sign"]})
        out = EmailSyntaxValidationStage().run(
            ChunkPayload(frame=df), PipelineContext()
        )
        assert "syntax_valid" in out.frame.columns
        assert "syntax_reason" in out.frame.columns
        assert out.frame["syntax_valid"].tolist() == [True, False, False]
        assert out.frame.iloc[0]["syntax_reason"] == "valid"


class TestDomainExtractionStage:
    def test_extracts_local_and_domain(self):
        df = pd.DataFrame(
            {
                "email": ["alice@gmail.com", "bad-email"],
                "syntax_valid": pd.array([True, False], dtype="boolean"),
            }
        )
        out = DomainExtractionStage().run(
            ChunkPayload(frame=df), PipelineContext()
        )
        assert out.frame.iloc[0]["local_part_from_email"] == "alice"
        assert out.frame.iloc[0]["domain_from_email"] == "gmail.com"
        assert out.frame.iloc[1]["domain_from_email"] is None


class TestTypoCorrectionStage:
    def test_detects_typo_suggestion_without_rewriting_original(self):
        # After the redesign the stage is purely non-destructive: it
        # never overwrites ``corrected_domain`` with the suggested value,
        # even when the legacy typo map has a direct mapping. The
        # suggestion is surfaced via the new ``typo_detected`` /
        # ``suggested_domain`` columns instead.
        df = pd.DataFrame(
            {
                "local_part_from_email": ["alice", "bob", None],
                "domain_from_email": ["gmial.com", "gmail.com", None],
            }
        )
        ctx = PipelineContext(typo_map={"gmial.com": "gmail.com"})
        out = TypoCorrectionStage().run(ChunkPayload(frame=df), ctx).frame

        # Row 0: typo of gmail.com → suggested but original preserved.
        assert bool(out.iloc[0]["typo_detected"]) is True
        assert bool(out.iloc[0]["typo_corrected"]) is True  # legacy mirror
        assert out.iloc[0]["original_domain"] == "gmial.com"
        assert out.iloc[0]["suggested_domain"] == "gmail.com"
        assert out.iloc[0]["suggested_email"] == "alice@gmail.com"
        # Critical safety invariant: the domain used by downstream DNS
        # and scoring is always the *original* domain, never the guess.
        assert out.iloc[0]["corrected_domain"] == "gmial.com"
        assert out.iloc[0]["typo_type"] in {
            "common_provider_typo",
            "keyboard_typo",
            "tld_typo",
        }

        # Row 1: already a whitelisted provider → no suggestion.
        assert bool(out.iloc[1]["typo_detected"]) is False
        assert out.iloc[1]["suggested_domain"] is None
        assert out.iloc[1]["corrected_domain"] == "gmail.com"

        # Row 2: no domain → all suggestion fields empty.
        assert pd.isna(out.iloc[2]["typo_detected"])
        assert out.iloc[2]["suggested_domain"] is None

    def test_empty_typo_map_still_uses_whitelist_distance(self):
        # The whitelist + edit-distance path must work even with no map.
        df = pd.DataFrame(
            {
                "local_part_from_email": ["alice"],
                "domain_from_email": ["gmal.com"],
            }
        )
        ctx = PipelineContext(typo_map={})
        out = TypoCorrectionStage().run(ChunkPayload(frame=df), ctx).frame
        assert bool(out.iloc[0]["typo_detected"]) is True
        assert out.iloc[0]["suggested_domain"] == "gmail.com"
        # Original never rewritten.
        assert out.iloc[0]["corrected_domain"] == "gmal.com"

    def test_unknown_domain_gets_no_suggestion(self):
        # Completely unrelated domains must not be re-written or even
        # proposed for re-writing.
        df = pd.DataFrame(
            {
                "local_part_from_email": ["sam"],
                "domain_from_email": ["acme-industries.co.uk"],
            }
        )
        ctx = PipelineContext(typo_map={})
        out = TypoCorrectionStage().run(ChunkPayload(frame=df), ctx).frame
        assert bool(out.iloc[0]["typo_detected"]) is False
        assert out.iloc[0]["suggested_domain"] is None
        assert out.iloc[0]["corrected_domain"] == "acme-industries.co.uk"


class TestDomainComparisonStage:
    def test_compares_corrected_domain_to_input(self):
        df = pd.DataFrame(
            {
                "corrected_domain": ["gmail.com", "gmail.com", None],
                "domain": ["gmail.com", "yahoo.com", "x.com"],
            }
        )
        out = DomainComparisonStage().run(
            ChunkPayload(frame=df), PipelineContext()
        )
        assert bool(out.frame.iloc[0]["domain_matches_input_column"]) is True
        assert bool(out.frame.iloc[1]["domain_matches_input_column"]) is False
        # corrected_domain is None → pd.NA
        assert pd.isna(out.frame.iloc[2]["domain_matches_input_column"])


# ---------------------------------------------------------------------------
# End-to-end equivalence: engine path == inline path
# ---------------------------------------------------------------------------

class TestEmailProcessingEnginePathMatchesInline:
    def test_four_stages_produce_same_result_as_inline_sequence(self):
        from app.normalizers import (
            apply_domain_typo_suggestion_column,
            compare_domain_with_input_column,
            extract_email_components,
        )
        from app.typo_suggestions import TypoDetectorConfig
        from app.validators import validate_email_syntax_column

        raw = pd.DataFrame(
            {
                "email": [
                    "alice@gmial.com",
                    "bob@yahoo.com",
                    "no-at-sign",
                    "carol@ gmail.com",  # space → invalid
                    None,
                ],
                "domain": ["gmial.com", "yahoo.com", None, "gmail.com", None],
            }
        )
        typo_map = {"gmial.com": "gmail.com"}
        detector_config = TypoDetectorConfig()

        # ---- Inline reference path (using the new non-destructive applier) ----
        ref = validate_email_syntax_column(raw)
        ref = extract_email_components(ref)
        ref = apply_domain_typo_suggestion_column(
            ref, detector_config=detector_config, typo_map=typo_map
        )
        ref = compare_domain_with_input_column(ref)

        # ---- Engine path ----
        engine = PipelineEngine(
            stages=[
                EmailSyntaxValidationStage(),
                DomainExtractionStage(),
                TypoCorrectionStage(),
                DomainComparisonStage(),
            ]
        )
        ctx = PipelineContext(typo_map=typo_map)
        out = engine.run(ChunkPayload(frame=raw.copy()), ctx).frame

        assert list(out.columns) == list(ref.columns)
        pd.testing.assert_frame_equal(
            out.reset_index(drop=True), ref.reset_index(drop=True)
        )

    def test_full_eight_stage_engine_produces_expected_columns(self):
        """Smoke: the extended eight-stage chunk engine (preprocess +
        email-processing) produces every column the later inline steps
        depend on (DNS/scoring/dedupe look up these columns by name)."""
        from pathlib import Path

        from app.engine.stages import (
            HeaderNormalizationStage,
            StructuralValidationStage,
            TechnicalMetadataStage,
            ValueNormalizationStage,
        )
        from app.models import ChunkContext, FileIngestionMetrics, InputFile

        raw = pd.DataFrame(
            {
                "E-Mail": ["alice@GMIAL.COM", "bob@yahoo.com"],
                "Domain": ["gmial.com", "yahoo.com"],
            }
        )
        input_file = InputFile(
            absolute_path=Path("/tmp/x.csv"),
            original_name="x.csv",
            file_type="csv",
        )
        chunk_context = ChunkContext(chunk_index=0, row_count=2, start_row_number=2)
        metrics = FileIngestionMetrics(source_file="x.csv", source_file_type="csv")

        engine = PipelineEngine(
            stages=[
                HeaderNormalizationStage(),
                StructuralValidationStage(),
                ValueNormalizationStage(),
                TechnicalMetadataStage(),
                EmailSyntaxValidationStage(),
                DomainExtractionStage(),
                TypoCorrectionStage(),
                DomainComparisonStage(),
            ]
        )
        payload = ChunkPayload(
            frame=raw,
            chunk_index=0,
            source_file="x.csv",
            metadata={
                "is_first_chunk": True,
                "file_metrics": metrics,
                "input_file": input_file,
                "chunk_context": chunk_context,
            },
        )
        out = engine.run(
            payload, PipelineContext(typo_map={"gmial.com": "gmail.com"})
        ).frame

        expected_columns = {
            "email",
            "domain",
            "source_file",
            "source_row_number",
            "source_file_type",
            "chunk_index",
            "syntax_valid",
            "syntax_reason",
            "has_single_at",
            "local_part_present",
            "domain_part_present",
            "domain_has_dot",
            "contains_spaces",
            "local_part_from_email",
            "domain_from_email",
            "typo_detected",
            "original_domain",
            "suggested_domain",
            "suggested_email",
            "typo_type",
            "typo_confidence",
            "typo_corrected",
            "typo_original_domain",
            "corrected_domain",
            "domain_matches_input_column",
        }
        missing = expected_columns - set(out.columns)
        assert not missing, f"missing columns after 8 stages: {missing}"
        # Row 0: suggestion emitted, original domain preserved.
        assert out.iloc[0]["corrected_domain"] == "gmial.com"
        assert out.iloc[0]["suggested_domain"] == "gmail.com"
        assert bool(out.iloc[0]["typo_detected"]) is True
        assert bool(out.iloc[0]["typo_corrected"]) is True
        # Row 0 was not rewritten → corrected_domain matches the input
        # domain column exactly (both say "gmial.com" here).
        assert bool(out.iloc[0]["domain_matches_input_column"]) is True
        # Row 1 needed no correction; input domain equals corrected domain.
        assert bool(out.iloc[1]["domain_matches_input_column"]) is True
