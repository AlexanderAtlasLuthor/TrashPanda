"""Subphase-5 engine-refactor tests: migrated postprocessing stages.

Focus areas for this final structural subphase:
  * each stage in isolation (email normalization, dedupe, staging)
  * dedupe state preservation across multiple chunks
  * staging append ordering preserved across multiple chunks
  * engine-vs-inline equivalence on a realistic frame
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd

from app.dedupe import (
    DedupeIndex,
    apply_completeness_column,
    apply_dedupe_columns,
    apply_email_normalized_column,
)
from app.engine import ChunkPayload, PipelineContext, PipelineEngine
from app.engine.stages import (
    DedupeStage,
    EmailNormalizationStage,
    StagingPersistenceStage,
)
from app.storage import StagingDB


# ---------------------------------------------------------------------------
# EmailNormalizationStage
# ---------------------------------------------------------------------------

class TestEmailNormalizationStage:
    def test_adds_email_normalized_column(self):
        df = pd.DataFrame({"email": ["  ALICE@X.COM  ", None, "bob@y.com"]})
        out = EmailNormalizationStage().run(
            ChunkPayload(frame=df), PipelineContext()
        )
        assert "email_normalized" in out.frame.columns
        assert out.frame.iloc[0]["email_normalized"] == "alice@x.com"
        # None input normalizes to a null-equivalent; pandas may store it
        # as either None or np.nan depending on column dtype, so check
        # via pd.isna rather than identity.
        assert pd.isna(out.frame.iloc[1]["email_normalized"])
        assert out.frame.iloc[2]["email_normalized"] == "bob@y.com"


# ---------------------------------------------------------------------------
# DedupeStage
# ---------------------------------------------------------------------------

class TestDedupeStage:
    @staticmethod
    def _make_frame(emails, source_file="f.csv", start_row=2, **extras):
        n = len(emails)
        df = pd.DataFrame(
            {
                "email": emails,
                "email_normalized": [
                    e.lower() if isinstance(e, str) else None for e in emails
                ],
                "hard_fail": pd.array([False] * n, dtype="boolean"),
                "score": [75] * n,
                "completeness_score": [3] * n,
                "source_file": [source_file] * n,
                "source_row_number": list(range(start_row, start_row + n)),
            }
        )
        for k, v in extras.items():
            df[k] = v
        return df

    def test_uses_context_index_and_produces_columns(self):
        idx = DedupeIndex()
        ctx = PipelineContext(dedupe_index=idx)
        df = self._make_frame(["a@x.com", "b@x.com", "a@x.com"])
        out = DedupeStage().run(ChunkPayload(frame=df), ctx)

        for col in (
            "is_canonical",
            "duplicate_flag",
            "duplicate_reason",
            "global_ordinal",
        ):
            assert col in out.frame.columns

        # First occurrence is canonical, second of "a@x.com" loses.
        assert bool(out.frame.iloc[0]["is_canonical"]) is True
        assert bool(out.frame.iloc[2]["duplicate_flag"]) is True

    def test_index_state_preserved_across_chunks(self):
        """Running DedupeStage over TWO separate chunks must accumulate
        the index state — no reset between calls."""
        idx = DedupeIndex()
        ctx = PipelineContext(dedupe_index=idx)

        # Chunk 1: two distinct emails.
        chunk1 = self._make_frame(["a@x.com", "b@x.com"], start_row=2)
        DedupeStage().run(ChunkPayload(frame=chunk1), ctx)
        assert idx.index_size == 2
        assert idx.new_canonicals == 2
        assert idx.duplicates_detected == 0

        # Chunk 2: one new email + a duplicate of one from chunk 1.
        chunk2 = self._make_frame(["c@x.com", "a@x.com"], start_row=4)
        DedupeStage().run(ChunkPayload(frame=chunk2), ctx)
        assert idx.index_size == 3  # new canonical for c@x.com only
        assert idx.new_canonicals == 3
        assert idx.duplicates_detected == 1  # second a@x.com lost
        # Same index instance survived across calls.
        assert ctx.dedupe_index is idx

    def test_index_state_preserved_across_files(self):
        """Running DedupeStage on chunks from two different files must
        continue to resolve cross-file duplicates via the same index."""
        idx = DedupeIndex()
        ctx = PipelineContext(dedupe_index=idx)

        f1 = self._make_frame(["alice@x.com"], source_file="file1.csv", start_row=2)
        f2 = self._make_frame(["alice@x.com"], source_file="file2.csv", start_row=2)

        DedupeStage().run(ChunkPayload(frame=f1, source_file="file1.csv"), ctx)
        out2 = DedupeStage().run(
            ChunkPayload(frame=f2, source_file="file2.csv"), ctx
        ).frame

        # Cross-file dedupe: file2's alice@x.com is flagged as duplicate.
        assert bool(out2.iloc[0]["duplicate_flag"]) is True
        assert idx.index_size == 1

    def test_replaced_canonicals_metric_preserved(self):
        """A later chunk can win over an earlier canonical; the
        replaced_canonicals counter on the shared index is the signal
        the second pass uses to reconcile stale is_canonical flags."""
        idx = DedupeIndex()
        ctx = PipelineContext(dedupe_index=idx)

        # Chunk 1: a low-completeness canonical.
        early = self._make_frame(["a@x.com"], start_row=2)
        DedupeStage().run(ChunkPayload(frame=early), ctx)
        assert idx.replaced_canonicals == 0

        # Chunk 2: a higher-completeness challenger.
        later = self._make_frame(
            ["a@x.com"], source_file="f2.csv", start_row=5
        )
        later["completeness_score"] = [9]  # better than 3
        DedupeStage().run(ChunkPayload(frame=later), ctx)
        assert idx.replaced_canonicals == 1


# ---------------------------------------------------------------------------
# StagingPersistenceStage
# ---------------------------------------------------------------------------

class TestStagingPersistenceStage:
    @staticmethod
    def _make_staging_frame(n, start_row=2, source="f.csv"):
        return pd.DataFrame(
            {
                "email": [f"u{i}@x.com" for i in range(n)],
                "email_normalized": [f"u{i}@x.com" for i in range(n)],
                "source_file": [source] * n,
                "source_row_number": list(range(start_row, start_row + n)),
                "chunk_index": [0] * n,
                "global_ordinal": list(range(n)),
                "hard_fail": [False] * n,
                "score": [75] * n,
                "preliminary_bucket": ["high_confidence"] * n,
                "completeness_score": [3] * n,
                "is_canonical": [True] * n,
                "duplicate_flag": [False] * n,
                "duplicate_reason": [None] * n,
            }
        )

    def test_appends_to_context_staging_and_returns_payload_unchanged(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "stg.sqlite3"
            staging = StagingDB(db_path)
            ctx = PipelineContext(staging=staging)

            df = self._make_staging_frame(3)
            payload = ChunkPayload(frame=df)
            out = StagingPersistenceStage().run(payload, ctx)

            # Returned payload carries the same frame (no transformation).
            assert out.frame is df
            assert staging.row_count() == 3

            staging.close()

    def test_append_ordering_preserved_across_chunks(self):
        """Two sequential appends must yield rows in arrival order in
        the SQLite primary-key sequence (row_id autoincrement)."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "stg.sqlite3"
            staging = StagingDB(db_path)
            ctx = PipelineContext(staging=staging)

            chunk1 = self._make_staging_frame(2, start_row=2, source="a.csv")
            chunk2 = self._make_staging_frame(3, start_row=2, source="b.csv")

            StagingPersistenceStage().run(ChunkPayload(frame=chunk1), ctx)
            StagingPersistenceStage().run(ChunkPayload(frame=chunk2), ctx)

            assert staging.row_count() == 5

            # Read back via the same iteration path _materialize uses and
            # confirm chunk1 rows come before chunk2 rows.
            all_rows: list[dict] = []
            for batch in staging.iter_all_rows(batch_size=10):
                all_rows.extend(batch)
            assert [r["source_file"] for r in all_rows] == [
                "a.csv",
                "a.csv",
                "b.csv",
                "b.csv",
                "b.csv",
            ]

            staging.close()


# ---------------------------------------------------------------------------
# End-to-end equivalence: engine path == inline path
# ---------------------------------------------------------------------------

class TestPostprocessingEnginePathMatchesInline:
    @staticmethod
    def _prepared_frame():
        """A frame with the columns the postprocessing stages consume
        (email + post-scoring columns). The preceding 11 stages are
        skipped because they're already covered by earlier test files."""
        return pd.DataFrame(
            {
                "email": [
                    "alice@x.com",
                    "alice@x.com",  # duplicate of row 0 but lower completeness
                    "bob@y.com",
                    "carol@z.com",
                    None,
                ],
                "hard_fail": pd.array(
                    [False, False, False, False, True], dtype="boolean"
                ),
                "score": [80, 80, 70, 60, 0],
                "source_file": ["f.csv"] * 5,
                "source_row_number": list(range(2, 7)),
            }
        )

    def test_three_stages_equivalent_to_inline_sequence(self):
        with tempfile.TemporaryDirectory() as tmp:
            # Inline reference path. The pre-Subphase-5 production order
            # was: completeness (via engine CompletenessStage at position
            # 11) → email_normalized (inline) → dedupe (inline) → staging
            # (inline). We mirror that column order here.
            ref_idx = DedupeIndex()
            ref_staging = StagingDB(Path(tmp) / "ref.sqlite3")
            ref = apply_completeness_column(self._prepared_frame())
            ref = apply_email_normalized_column(ref)
            ref = apply_dedupe_columns(ref, ref_idx)
            ref_staging.append_chunk(ref)
            ref_rowcount = ref_staging.row_count()
            ref_staging.close()

            # Engine path: the three new stages only; completeness is
            # run inline first so both paths start from the same frame.
            eng_idx = DedupeIndex()
            eng_staging = StagingDB(Path(tmp) / "eng.sqlite3")
            eng_ctx = PipelineContext(
                dedupe_index=eng_idx, staging=eng_staging
            )
            engine = PipelineEngine(
                stages=[
                    EmailNormalizationStage(),
                    DedupeStage(),
                    StagingPersistenceStage(),
                ]
            )
            frame = apply_completeness_column(self._prepared_frame())
            out = engine.run(ChunkPayload(frame=frame), eng_ctx).frame

            # Frame equality.
            assert list(out.columns) == list(ref.columns)
            pd.testing.assert_frame_equal(
                out.reset_index(drop=True), ref.reset_index(drop=True)
            )

            # DedupeIndex state equality.
            assert eng_idx.index_size == ref_idx.index_size
            assert eng_idx.new_canonicals == ref_idx.new_canonicals
            assert eng_idx.duplicates_detected == ref_idx.duplicates_detected
            assert eng_idx.replaced_canonicals == ref_idx.replaced_canonicals

            # Staging row count equality.
            assert eng_staging.row_count() == ref_rowcount
            eng_staging.close()

    def test_full_fourteen_stage_engine_processes_chunk_end_to_end(self):
        """Smoke: the full 14-stage chunk engine resolves a realistic
        frame, populates every downstream column, writes to staging, and
        accumulates dedupe state — with NO inline business logic used."""
        from unittest.mock import patch

        from app.config import load_config, resolve_project_paths
        from app.dns_utils import DnsCache, DnsResult
        from app.engine.stages import (
            CompletenessStage,
            DNSEnrichmentStage,
            DomainComparisonStage,
            DomainExtractionStage,
            EmailSyntaxValidationStage,
            HeaderNormalizationStage,
            ScoringStage,
            StructuralValidationStage,
            TechnicalMetadataStage,
            TypoCorrectionStage,
            ValueNormalizationStage,
        )
        from app.models import ChunkContext, FileIngestionMetrics, InputFile

        def fake_resolve(domain, timeout_seconds=4.0, fallback_to_a_record=True):
            return DnsResult(
                dns_check_performed=True,
                domain_exists=True,
                has_mx_record=True,
                has_a_record=False,
                dns_error=None,
            )

        raw = pd.DataFrame(
            {
                "email": [
                    "alice@gmail.com",
                    "alice@gmail.com",  # duplicate
                    "bob@yahoo.com",
                ],
                "domain": ["gmail.com", "gmail.com", "yahoo.com"],
                "fname": ["Alice", None, "Bob"],
            }
        )

        cfg = load_config(base_dir=resolve_project_paths().project_root)
        dns_cache = DnsCache()
        dedupe_index = DedupeIndex()

        with tempfile.TemporaryDirectory() as tmp:
            staging = StagingDB(Path(tmp) / "stg.sqlite3")
            ctx = PipelineContext(
                config=cfg,
                dns_cache=dns_cache,
                dedupe_index=dedupe_index,
                staging=staging,
                typo_map={},
            )

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
                    DNSEnrichmentStage(),
                    ScoringStage(),
                    CompletenessStage(),
                    EmailNormalizationStage(),
                    DedupeStage(),
                    StagingPersistenceStage(),
                ]
            )

            input_file = InputFile(
                absolute_path=Path("/tmp/x.csv"),
                original_name="x.csv",
                file_type="csv",
            )
            chunk_context = ChunkContext(
                chunk_index=0, row_count=3, start_row_number=2
            )
            metrics = FileIngestionMetrics(
                source_file="x.csv", source_file_type="csv"
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

            with patch(
                "app.dns_utils.resolve_domain_dns", side_effect=fake_resolve
            ):
                out = engine.run(payload, ctx).frame

            # All downstream columns present.
            for col in (
                "email_normalized",
                "is_canonical",
                "duplicate_flag",
                "duplicate_reason",
                "global_ordinal",
                "completeness_score",
                "hard_fail",
                "score",
                "preliminary_bucket",
            ):
                assert col in out.columns

            # Cross-cutting verifications:
            # - Dedupe index saw 3 rows, 2 canonicals (alice once, bob once),
            #   1 duplicate (second alice).
            assert dedupe_index.index_size == 2
            assert dedupe_index.new_canonicals == 2
            assert dedupe_index.duplicates_detected == 1
            # - All three rows made it to staging.
            assert staging.row_count() == 3
            staging.close()
