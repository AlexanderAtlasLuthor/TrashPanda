"""Scoring V2 pipeline stage — parallel, observational scoring.

``ScoringV2Stage`` runs the V2 signal-based engine alongside the
existing V1 ``ScoringStage``. It reads the same upstream row columns
V2 evaluators expect (populated by prior stages), passes each row as a
plain dict to :class:`ScoringEngineV2`, and appends V2-specific output
columns to the frame. Crucially, it never reads, writes, or shadows any
V1 column — V1 and V2 coexist side-by-side in the frame.

This stage is strictly additive in this subphase:
  * It does NOT drive any bucket, dedupe, or materialization decision.
  * It does NOT replace, mask, or rename V1 columns.
  * It only appends the ``*_v2`` columns listed below.

Downstream stages (``DedupeStage``, ``StagingPersistenceStage``) and
the second-pass ``_materialize`` still use V1 columns exclusively.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pandas as pd

from ...scoring_v2 import (
    COMPARISON_COLUMNS,
    ScoringEngineV2,
    build_default_engine,
    compare_scoring,
)
from ..context import PipelineContext
from ..payload import ChunkPayload
from ..stage import Stage

if TYPE_CHECKING:
    from ...scoring_v2 import ScoreBreakdown


# Column names produced by this stage. Centralized so tests and future
# callers have a single source of truth and the stage cannot silently
# drift from the documented contract.
V2_OUTPUT_COLUMNS: tuple[str, ...] = (
    # Core V2 outputs
    "score_v2",
    "confidence_v2",
    "bucket_v2",
    "hard_stop_v2",
    "hard_stop_reason_v2",
    # Explainability outputs
    "reason_codes_v2",
    "explanation_v2",
    # Breakdown outputs
    "positive_total_v2",
    "negative_total_v2",
    "raw_score_v2",
    "score_breakdown_v2",
)


class ScoringV2Stage(Stage):
    """Append V2 scoring columns to the frame; leave V1 columns alone.

    The stage wraps a single ``ScoringEngineV2`` instance for the life
    of the stage object (so evaluators and the profile are built once
    per pipeline run, not per row). If no engine is provided at
    construction time, ``build_default_engine`` supplies the standard
    V2 stack: syntax / domain presence / typo / domain match / DNS
    evaluators with the default hard-stop policy
    (``syntax_invalid``, ``no_domain``, ``nxdomain``).

    Dependencies match the V1 ``ScoringStage``: the same upstream
    columns drive both engines, which keeps the two results directly
    comparable for this subphase.
    """

    name = "scoring_v2"
    requires = (
        "syntax_valid",
        "corrected_domain",
        "has_mx_record",
        "has_a_record",
        "domain_exists",
        "dns_error",
        "typo_corrected",
        "domain_matches_input_column",
    )
    produces = V2_OUTPUT_COLUMNS

    def __init__(self, engine: ScoringEngineV2 | None = None) -> None:
        self._engine: ScoringEngineV2 = engine or build_default_engine()

    @property
    def engine(self) -> ScoringEngineV2:
        """Expose the underlying V2 engine for tests and debugging."""
        return self._engine

    def run(self, payload: ChunkPayload, context: PipelineContext) -> ChunkPayload:
        frame = payload.frame
        logger = getattr(context, "logger", None)
        row_dicts = frame.to_dict(orient="records")
        breakdowns = [self._engine.evaluate_row(row) for row in row_dicts]
        new_columns = _columns_from_breakdowns(breakdowns, frame.index)

        out = frame.copy()
        for col_name, series in new_columns.items():
            # Defensive: do not silently clobber any pre-existing V1
            # column. V2 columns are suffixed ``_v2`` precisely so the
            # namespaces never collide.
            if col_name in out.columns:
                raise RuntimeError(
                    f"ScoringV2Stage refusing to overwrite existing column "
                    f"{col_name!r}"
                )
            out[col_name] = series

        if logger is not None:
            try:
                logger.debug(
                    "ScoringV2Stage processed %s rows (chunk_index=%s, file=%s)",
                    len(row_dicts),
                    payload.chunk_index,
                    payload.source_file,
                )
            except Exception:
                # Logging must never break the pipeline.
                pass

        return payload.with_frame(out)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class ScoringComparisonStage(Stage):
    """Append V1-vs-V2 comparison columns to the frame.

    Must run after both ``ScoringStage`` (V1) and ``ScoringV2Stage``
    (V2). The stage is a thin wrapper around
    :func:`app.scoring_v2.compare_scoring` — it does not alter any
    existing V1 or V2 column, does not influence any downstream
    decision, and does not emit a separate file. Columns it appends
    are observational and are carried through to staging alongside
    everything else.
    """

    name = "scoring_comparison"
    requires = (
        # V1
        "score",
        "preliminary_bucket",
        "hard_fail",
        # V2
        "score_v2",
        "bucket_v2",
        "hard_stop_v2",
        "confidence_v2",
        "reason_codes_v2",
    )
    produces = COMPARISON_COLUMNS

    def run(self, payload: ChunkPayload, context: PipelineContext) -> ChunkPayload:
        return payload.with_frame(compare_scoring(payload.frame))


def _columns_from_breakdowns(
    breakdowns: list["ScoreBreakdown"],
    index: pd.Index,
) -> dict[str, pd.Series]:
    """Convert a list of ``ScoreBreakdown`` into per-column Series.

    Column values are plain Python scalars / strings so the result
    plays nicely with CSV materialization. ``reason_codes_v2`` is a
    ``|``-joined string (matching V1's ``score_reasons`` convention)
    and ``score_breakdown_v2`` is a JSON string (``json.dumps``,
    no custom encoders).
    """
    score_v2: list[float] = []
    confidence_v2: list[float] = []
    bucket_v2: list[str] = []
    hard_stop_v2: list[bool] = []
    hard_stop_reason_v2: list[str | None] = []
    reason_codes_v2: list[str] = []
    explanation_v2: list[str] = []
    positive_total_v2: list[float] = []
    negative_total_v2: list[float] = []
    raw_score_v2: list[float] = []
    score_breakdown_v2: list[str] = []

    for bd in breakdowns:
        score_v2.append(float(bd.final_score))
        confidence_v2.append(float(bd.confidence))
        bucket_v2.append(str(bd.bucket))
        hard_stop_v2.append(bool(bd.hard_stop))
        hard_stop_reason_v2.append(bd.hard_stop_reason)
        reason_codes_v2.append("|".join(bd.reason_codes))
        explanation_v2.append(str(bd.explanation))
        positive_total_v2.append(float(bd.positive_total))
        negative_total_v2.append(float(bd.negative_total))
        raw_score_v2.append(float(bd.raw_score))
        score_breakdown_v2.append(json.dumps(bd.breakdown_dict))

    return {
        "score_v2": pd.Series(score_v2, index=index, dtype="float64"),
        "confidence_v2": pd.Series(confidence_v2, index=index, dtype="float64"),
        "bucket_v2": pd.Series(bucket_v2, index=index, dtype="object"),
        "hard_stop_v2": pd.Series(hard_stop_v2, index=index, dtype="bool"),
        "hard_stop_reason_v2": pd.Series(
            hard_stop_reason_v2, index=index, dtype="object"
        ),
        "reason_codes_v2": pd.Series(
            reason_codes_v2, index=index, dtype="object"
        ),
        "explanation_v2": pd.Series(explanation_v2, index=index, dtype="object"),
        "positive_total_v2": pd.Series(
            positive_total_v2, index=index, dtype="float64"
        ),
        "negative_total_v2": pd.Series(
            negative_total_v2, index=index, dtype="float64"
        ),
        "raw_score_v2": pd.Series(raw_score_v2, index=index, dtype="float64"),
        "score_breakdown_v2": pd.Series(
            score_breakdown_v2, index=index, dtype="object"
        ),
    }
