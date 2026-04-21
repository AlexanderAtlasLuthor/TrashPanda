"""Subphase-2 engine-refactor tests: migrated preprocessing stages.

These tests verify each of the four migrated stages in isolation, and
also verify that running them through ``PipelineEngine`` produces the
same DataFrame as the previous inline sequence. They do not re-test any
business rule — they only check that the stage wrappers preserve the
pre-refactor behavior.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from app.engine import ChunkPayload, PipelineContext, PipelineEngine
from app.engine.stages import (
    HeaderNormalizationStage,
    StructuralValidationStage,
    TechnicalMetadataStage,
    ValueNormalizationStage,
)
from app.models import ChunkContext, FileIngestionMetrics, InputFile


# ---------------------------------------------------------------------------
# HeaderNormalizationStage
# ---------------------------------------------------------------------------

class TestHeaderNormalizationStage:
    def test_renames_aliases_to_canonical(self):
        df = pd.DataFrame({"E-Mail": ["a@b.com"], "First Name": ["A"]})
        out = HeaderNormalizationStage().run(
            ChunkPayload(frame=df), PipelineContext()
        )
        assert list(out.frame.columns) == ["email", "fname"]

    def test_returns_new_payload_not_original(self):
        df = pd.DataFrame({"email": ["a@b.com"]})
        payload = ChunkPayload(frame=df)
        out = HeaderNormalizationStage().run(payload, PipelineContext())
        # Headers didn't need renaming, but the stage still returns via
        # with_frame, i.e. a fresh payload instance backed by a new frame.
        assert out is not payload
        assert out.frame is not df


# ---------------------------------------------------------------------------
# StructuralValidationStage
# ---------------------------------------------------------------------------

class TestStructuralValidationStage:
    @staticmethod
    def _payload(df: pd.DataFrame, *, first_chunk: bool, metrics=None) -> ChunkPayload:
        return ChunkPayload(
            frame=df,
            metadata={
                "is_first_chunk": first_chunk,
                "file_metrics": metrics,
            },
        )

    def test_first_chunk_records_normalized_columns(self):
        df = pd.DataFrame({"email": ["x@y.com"], "fname": ["A"]})
        metrics = FileIngestionMetrics(source_file="f.csv", source_file_type="csv")
        StructuralValidationStage().run(
            self._payload(df, first_chunk=True, metrics=metrics),
            PipelineContext(),
        )
        assert metrics.normalized_columns == ["email", "fname"]

    def test_first_chunk_missing_required_column_raises(self):
        df = pd.DataFrame({"fname": ["A"]})  # no email
        with pytest.raises(ValueError, match="Missing required"):
            StructuralValidationStage().run(
                self._payload(df, first_chunk=True),
                PipelineContext(),
            )

    def test_first_chunk_reserved_column_raises(self):
        df = pd.DataFrame({"email": ["a@b.com"], "source_file": ["x"]})
        with pytest.raises(ValueError, match="reserved technical"):
            StructuralValidationStage().run(
                self._payload(df, first_chunk=True),
                PipelineContext(),
            )

    def test_non_first_chunk_skips_validation_and_side_effects(self):
        df = pd.DataFrame({"fname": ["A"]})  # would fail required-column check
        metrics = FileIngestionMetrics(source_file="f.csv", source_file_type="csv")
        # Should NOT raise — validation only runs on the first chunk.
        StructuralValidationStage().run(
            self._payload(df, first_chunk=False, metrics=metrics),
            PipelineContext(),
        )
        # Side effect must not have been applied.
        assert metrics.normalized_columns == []

    def test_first_chunk_without_metrics_still_validates(self):
        df = pd.DataFrame({"email": ["x@y.com"]})
        # Should not raise even though file_metrics is None.
        StructuralValidationStage().run(
            self._payload(df, first_chunk=True, metrics=None),
            PipelineContext(),
        )

    def test_payload_is_returned_unchanged(self):
        df = pd.DataFrame({"email": ["x@y.com"]})
        payload = self._payload(df, first_chunk=True)
        out = StructuralValidationStage().run(payload, PipelineContext())
        # Structural validation never replaces the frame.
        assert out is payload
        assert out.frame is df


# ---------------------------------------------------------------------------
# ValueNormalizationStage
# ---------------------------------------------------------------------------

class TestValueNormalizationStage:
    def test_lowercases_and_strips_email(self):
        df = pd.DataFrame({"email": ["  X@Y.COM  "], "fname": [" Ann "]})
        out = ValueNormalizationStage().run(
            ChunkPayload(frame=df), PipelineContext()
        )
        assert out.frame.iloc[0]["email"] == "x@y.com"
        # fname is stripped but not lowercased (matches normalize_value spec).
        assert out.frame.iloc[0]["fname"] == "Ann"


# ---------------------------------------------------------------------------
# TechnicalMetadataStage
# ---------------------------------------------------------------------------

class TestTechnicalMetadataStage:
    def test_injects_source_and_chunk_columns(self):
        df = pd.DataFrame({"email": ["a@b.com", "c@d.com"]})
        input_file = InputFile(
            absolute_path=Path("/tmp/f.csv"),
            original_name="f.csv",
            file_type="csv",
        )
        chunk_context = ChunkContext(chunk_index=3, row_count=2, start_row_number=100)
        payload = ChunkPayload(
            frame=df,
            metadata={"input_file": input_file, "chunk_context": chunk_context},
        )
        out = TechnicalMetadataStage().run(payload, PipelineContext())
        assert list(out.frame["source_file"]) == ["f.csv", "f.csv"]
        assert list(out.frame["source_row_number"]) == [100, 101]
        assert list(out.frame["source_file_type"]) == ["csv", "csv"]
        assert list(out.frame["chunk_index"]) == [3, 3]


# ---------------------------------------------------------------------------
# End-to-end equivalence: engine path == inline path
# ---------------------------------------------------------------------------

class TestEnginePathMatchesInline:
    def test_four_stages_produce_same_result_as_inline_sequence(self):
        """Running the four stages through PipelineEngine must yield the
        same DataFrame (same columns, same values) as the previous inline
        sequence of normalize_headers → validate → normalize_values →
        add_technical_metadata."""

        from app.normalizers import (
            add_technical_metadata,
            normalize_headers,
            normalize_values,
        )
        from app.validators import (
            validate_duplicate_columns,
            validate_required_columns,
            validate_reserved_columns,
        )

        raw = pd.DataFrame(
            {
                "E-Mail": ["  FOO@BAR.COM  ", "baz@qux.com"],
                "First Name": [" Ann ", "Bob"],
                "Domain": ["BAR.COM", None],
            }
        )
        input_file = InputFile(
            absolute_path=Path("/tmp/x.csv"),
            original_name="x.csv",
            file_type="csv",
        )
        chunk_context = ChunkContext(chunk_index=0, row_count=2, start_row_number=2)

        # ---- Inline reference path (copy of the pre-refactor code) ----
        ref = normalize_headers(raw)
        validate_duplicate_columns(ref.columns)
        validate_reserved_columns(ref.columns)
        validate_required_columns(ref.columns)
        ref_metrics = FileIngestionMetrics(
            source_file="x.csv", source_file_type="csv"
        )
        ref_metrics.normalized_columns = ref.columns.tolist()
        ref = normalize_values(ref)
        ref = add_technical_metadata(
            ref, input_file=input_file, chunk_context=chunk_context
        )

        # ---- Engine path ----
        engine = PipelineEngine(
            stages=[
                HeaderNormalizationStage(),
                StructuralValidationStage(),
                ValueNormalizationStage(),
                TechnicalMetadataStage(),
            ]
        )
        metrics = FileIngestionMetrics(source_file="x.csv", source_file_type="csv")
        payload = ChunkPayload(
            frame=raw.copy(),
            chunk_index=0,
            source_file="x.csv",
            metadata={
                "is_first_chunk": True,
                "file_metrics": metrics,
                "input_file": input_file,
                "chunk_context": chunk_context,
            },
        )
        out = engine.run(payload, PipelineContext()).frame

        assert list(out.columns) == list(ref.columns)
        pd.testing.assert_frame_equal(
            out.reset_index(drop=True), ref.reset_index(drop=True)
        )
        assert metrics.normalized_columns == ref_metrics.normalized_columns

    def test_second_chunk_skips_validation_even_if_columns_would_fail(self):
        """Guards the first-chunk-only semantics at the engine level."""
        df_bad = pd.DataFrame({"fname": ["A"]})  # no email; would fail on first chunk

        engine = PipelineEngine(
            stages=[
                HeaderNormalizationStage(),
                StructuralValidationStage(),
                ValueNormalizationStage(),
                TechnicalMetadataStage(),
            ]
        )
        input_file = InputFile(
            absolute_path=Path("/tmp/x.csv"),
            original_name="x.csv",
            file_type="csv",
        )
        chunk_context = ChunkContext(chunk_index=1, row_count=1, start_row_number=3)
        payload = ChunkPayload(
            frame=df_bad,
            chunk_index=1,
            source_file="x.csv",
            metadata={
                "is_first_chunk": False,  # simulates a non-first chunk
                "file_metrics": None,
                "input_file": input_file,
                "chunk_context": chunk_context,
            },
        )
        # Must not raise.
        engine.run(payload, PipelineContext())
