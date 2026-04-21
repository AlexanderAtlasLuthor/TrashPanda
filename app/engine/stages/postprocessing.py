"""Postprocessing stages: the final three chunk-processing steps.

These stages wrap existing functions in ``app.dedupe`` and the
``StagingDB`` append method. No business rule, dedupe tie-breaking,
canonical selection, or staging schema is reimplemented here — each
stage only adapts the pre-refactor inline call into the ``Stage``
interface.

Execution order assumed by downstream stages:

    EmailNormalizationStage   (needs: email)
      ↳ adds email_normalized (dedupe key)
    DedupeStage               (needs: email_normalized, hard_fail, score,
                                completeness_score, source_file,
                                source_row_number)
      ↳ adds is_canonical / duplicate_flag / duplicate_reason /
              global_ordinal
      ↳ SIDE EFFECT: mutates context.dedupe_index in place. The index
              is explicitly stateful across chunks and files — it lives
              for the duration of one pipeline run.
    StagingPersistenceStage   (needs: the fully-processed frame)
      ↳ SIDE EFFECT: appends the chunk to context.staging. Returns the
              payload unchanged; no transformation is performed.

Stage dependencies on ``PipelineContext``:

    EmailNormalizationStage → (none)
    DedupeStage             → context.dedupe_index
    StagingPersistenceStage → context.staging
"""

from __future__ import annotations

from ...dedupe import apply_dedupe_columns, apply_email_normalized_column
from ..context import PipelineContext
from ..payload import ChunkPayload
from ..stage import Stage


class EmailNormalizationStage(Stage):
    """Compute ``email_normalized`` — the immutable dedupe key column."""

    name = "email_normalization"
    requires = ("email",)
    produces = ("email_normalized",)

    def run(self, payload: ChunkPayload, context: PipelineContext) -> ChunkPayload:
        return payload.with_frame(apply_email_normalized_column(payload.frame))


class DedupeStage(Stage):
    """Flag duplicates against the shared run-wide ``DedupeIndex``.

    The index is instantiated once per pipeline run (in ``pipeline.run``)
    and lives on ``context.dedupe_index``. This stage mutates it in
    place as each chunk arrives — the index's cumulative counters
    (``new_canonicals``, ``duplicates_detected``, ``replaced_canonicals``,
    ``emails_seen``) reflect the state across ALL chunks and files
    processed so far, exactly as they did before the refactor.

    The stage never recreates, resets, or wraps the index; it passes the
    same object ``apply_dedupe_columns`` already expects.
    """

    name = "dedupe"
    requires = (
        "email_normalized",
        "hard_fail",
        "score",
        "completeness_score",
        "source_file",
        "source_row_number",
    )
    produces = (
        "is_canonical",
        "duplicate_flag",
        "duplicate_reason",
        "global_ordinal",
    )

    def run(self, payload: ChunkPayload, context: PipelineContext) -> ChunkPayload:
        return payload.with_frame(
            apply_dedupe_columns(payload.frame, context.dedupe_index)
        )


class StagingPersistenceStage(Stage):
    """Persist the fully-processed chunk to the shared staging database.

    Side-effect-only stage: it appends the frame's rows to
    ``context.staging`` and returns the payload unchanged. No column
    is added, no transformation is performed, no buffering or batching
    different from the pre-refactor behavior. The staging handle is
    instantiated once per run in ``pipeline.run`` and closed there;
    this stage neither opens nor closes the connection.
    """

    name = "staging_persistence"

    def run(self, payload: ChunkPayload, context: PipelineContext) -> ChunkPayload:
        context.staging.append_chunk(payload.frame)
        return payload
