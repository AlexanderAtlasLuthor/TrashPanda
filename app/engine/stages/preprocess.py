"""Preprocessing stages: the first four chunk-processing steps.

These stages are thin wrappers around existing functions in
``app.normalizers`` and ``app.validators``. They do not reimplement any
business rule — they only adapt the current inline pipeline code into the
``Stage`` interface so the engine can drive them.

Payload metadata contract for this batch of stages:

    is_first_chunk  (bool)       — set by the pipeline per file; drives the
                                   first-chunk-only structural validation.
    file_metrics    (FileIngestionMetrics | None) — target for the side
                                   effect ``metrics.normalized_columns =
                                   frame.columns.tolist()`` on first chunk.
    input_file      (InputFile)  — consumed by ``TechnicalMetadataStage``.
    chunk_context   (ChunkContext) — consumed by ``TechnicalMetadataStage``.

Nothing in this module reads from or writes to ``PipelineContext``; these
stages are pure-ish preprocessing and only need the payload. The context
parameter is accepted to satisfy the ``Stage`` interface.
"""

from __future__ import annotations

from ...normalizers import (
    add_technical_metadata,
    normalize_headers,
    normalize_values,
)
from ...validators import (
    validate_duplicate_columns,
    validate_required_columns,
    validate_reserved_columns,
)
from ..context import PipelineContext
from ..payload import ChunkPayload
from ..stage import Stage


class HeaderNormalizationStage(Stage):
    """Normalize header names to the canonical vocabulary."""

    name = "header_normalization"
    produces = ("__normalized_headers__",)

    def run(self, payload: ChunkPayload, context: PipelineContext) -> ChunkPayload:
        return payload.with_frame(normalize_headers(payload.frame))


class StructuralValidationStage(Stage):
    """Validate column structure on the first chunk of each file only.

    Mirrors the exact semantics of the previous inline block:
      * on the first chunk, run duplicate/reserved/required column checks
        and record ``metrics.normalized_columns``;
      * on subsequent chunks, do nothing.

    The "first chunk" flag is driven by the caller via payload metadata
    (``is_first_chunk``), so this stage never maintains cross-chunk state.
    """

    name = "structural_validation"

    def run(self, payload: ChunkPayload, context: PipelineContext) -> ChunkPayload:
        if not payload.metadata.get("is_first_chunk", False):
            return payload

        columns = payload.frame.columns
        validate_duplicate_columns(columns)
        validate_reserved_columns(columns)
        validate_required_columns(columns)

        file_metrics = payload.metadata.get("file_metrics")
        if file_metrics is not None:
            file_metrics.normalized_columns = columns.tolist()

        return payload


class ValueNormalizationStage(Stage):
    """Apply conservative string normalization to cell values."""

    name = "value_normalization"

    def run(self, payload: ChunkPayload, context: PipelineContext) -> ChunkPayload:
        return payload.with_frame(normalize_values(payload.frame))


class TechnicalMetadataStage(Stage):
    """Attach per-row technical metadata (source file, row numbers, etc.).

    Reads ``input_file`` and ``chunk_context`` from payload metadata —
    these are the same objects the pipeline already constructs for
    ``add_technical_metadata``. This stage does not invent new metadata
    columns; it delegates to the existing normalizer.
    """

    name = "technical_metadata"
    produces = (
        "source_file",
        "source_row_number",
        "source_file_type",
        "chunk_index",
    )

    def run(self, payload: ChunkPayload, context: PipelineContext) -> ChunkPayload:
        input_file = payload.metadata["input_file"]
        chunk_context = payload.metadata["chunk_context"]
        return payload.with_frame(
            add_technical_metadata(
                payload.frame,
                input_file=input_file,
                chunk_context=chunk_context,
            )
        )
