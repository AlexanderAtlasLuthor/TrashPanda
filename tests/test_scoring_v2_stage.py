"""Integration tests for ``ScoringV2Stage`` and pipeline coexistence.

Exercises:
  * ScoringV2Stage appends all expected columns
  * stage output is deterministic and does not mutate V1 columns
  * V1 and V2 coexist when run in the chunk engine together
  * downstream DedupeStage still relies on V1 columns only
  * end-to-end engine produces both V1 and V2 columns
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from app.config import load_config, resolve_project_paths
from app.dedupe import DedupeIndex
from app.dns_utils import DnsCache, DnsResult
from app.engine import ChunkPayload, PipelineContext, PipelineEngine
from app.engine.stages import (
    CompletenessStage,
    DNSEnrichmentStage,
    DedupeStage,
    DomainComparisonStage,
    DomainExtractionStage,
    EmailNormalizationStage,
    EmailSyntaxValidationStage,
    HeaderNormalizationStage,
    ScoringStage,
    ScoringV2Stage,
    StructuralValidationStage,
    TechnicalMetadataStage,
    TypoCorrectionStage,
    ValueNormalizationStage,
)
from app.engine.stages.scoring_v2 import V2_OUTPUT_COLUMNS
from app.models import ChunkContext, FileIngestionMetrics, InputFile
from app.scoring_v2 import ScoringEngineV2, build_default_engine


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_MX_RESULT = DnsResult(
    dns_check_performed=True,
    domain_exists=True,
    has_mx_record=True,
    has_a_record=False,
    dns_error=None,
)
_A_ONLY_RESULT = DnsResult(
    dns_check_performed=True,
    domain_exists=True,
    has_mx_record=False,
    has_a_record=True,
    dns_error=None,
)
_NXDOMAIN_RESULT = DnsResult(
    dns_check_performed=True,
    domain_exists=False,
    has_mx_record=False,
    has_a_record=False,
    dns_error="nxdomain",
)
_DNS_MAP = {
    "gmail.com": _MX_RESULT,
    "yahoo.com": _MX_RESULT,
    "example.net": _A_ONLY_RESULT,
    "bad.invalid": _NXDOMAIN_RESULT,
    "nxdomain.invalid": _NXDOMAIN_RESULT,
}


def _fake_resolve(domain, timeout_seconds=4.0, fallback_to_a_record=True):
    return _DNS_MAP.get(domain, _NXDOMAIN_RESULT)


def _build_config():
    project_paths = resolve_project_paths()
    return load_config(base_dir=project_paths.project_root)


def _post_scoring_frame() -> pd.DataFrame:
    """A realistic frame as it would look after V1 ScoringStage has run."""
    return pd.DataFrame(
        {
            "email": [
                "alice@gmail.com",
                "bob@example.net",
                "carol@bad.invalid",
                "no-at-sign",
            ],
            "domain": ["gmail.com", "example.net", "bad.invalid", None],
            "syntax_valid": pd.array(
                [True, True, True, False], dtype="boolean"
            ),
            "corrected_domain": [
                "gmail.com",
                "example.net",
                "bad.invalid",
                None,
            ],
            "typo_corrected": pd.array(
                [False, False, False, False], dtype="boolean"
            ),
            "domain_matches_input_column": pd.array(
                [True, True, True, pd.NA], dtype="boolean"
            ),
            "has_mx_record": pd.array(
                [True, False, False, False], dtype="boolean"
            ),
            "has_a_record": pd.array(
                [False, True, False, False], dtype="boolean"
            ),
            "domain_exists": pd.array(
                [True, True, False, False], dtype="boolean"
            ),
            "dns_error": [None, None, "nxdomain", None],
            # V1 scoring outputs already present.
            "hard_fail": pd.array(
                [False, False, True, True], dtype="boolean"
            ),
            "score": [75, 45, 0, 0],
            "score_reasons": [
                "syntax_valid|mx_present|domain_match",
                "syntax_valid|a_fallback|domain_match",
                "nxdomain",
                "syntax_invalid",
            ],
            "preliminary_bucket": [
                "high_confidence",
                "review",
                "invalid",
                "invalid",
            ],
        }
    )


# ---------------------------------------------------------------------------
# ScoringV2Stage unit behavior
# ---------------------------------------------------------------------------


class TestScoringV2StageColumns:
    def test_adds_all_expected_v2_columns(self):
        df = _post_scoring_frame()
        out = ScoringV2Stage().run(
            ChunkPayload(frame=df.copy()), PipelineContext()
        )
        for col in V2_OUTPUT_COLUMNS:
            assert col in out.frame.columns, f"missing column: {col}"

    def test_all_v1_columns_preserved_unchanged(self):
        df = _post_scoring_frame()
        before_v1 = df[
            [
                "hard_fail",
                "score",
                "score_reasons",
                "preliminary_bucket",
            ]
        ].copy()
        out = ScoringV2Stage().run(
            ChunkPayload(frame=df.copy()), PipelineContext()
        )
        after_v1 = out.frame[
            [
                "hard_fail",
                "score",
                "score_reasons",
                "preliminary_bucket",
            ]
        ]
        pd.testing.assert_frame_equal(
            before_v1.reset_index(drop=True),
            after_v1.reset_index(drop=True),
        )

    def test_input_frame_is_not_mutated(self):
        """The stage must not add columns to the caller's frame.

        Callers may still depend on the original frame; ``run`` must
        return a new frame via ``with_frame`` rather than mutating
        in-place."""
        df = _post_scoring_frame()
        snapshot_cols = list(df.columns)
        ScoringV2Stage().run(ChunkPayload(frame=df), PipelineContext())
        assert list(df.columns) == snapshot_cols

    def test_refuses_to_overwrite_pre_existing_v2_column(self):
        """Defense-in-depth: if somehow a ``*_v2`` column already
        exists the stage raises rather than silently clobbering."""
        df = _post_scoring_frame()
        df["score_v2"] = 0.5  # planted value
        import pytest

        with pytest.raises(RuntimeError, match="score_v2"):
            ScoringV2Stage().run(ChunkPayload(frame=df), PipelineContext())


class TestScoringV2StageValues:
    def test_populates_realistic_values(self):
        df = _post_scoring_frame()
        out = ScoringV2Stage().run(
            ChunkPayload(frame=df.copy()), PipelineContext()
        ).frame

        # alice@gmail.com: syntax + domain_present + domain_match + mx → clean
        row0 = out.iloc[0]
        assert 0.0 <= float(row0["score_v2"]) <= 1.0
        assert float(row0["score_v2"]) > 0.5
        assert bool(row0["hard_stop_v2"]) is False
        assert row0["hard_stop_reason_v2"] is None
        assert row0["bucket_v2"] in {"high_confidence", "review"}
        assert "mx_present" in row0["reason_codes_v2"]
        assert row0["explanation_v2"]

        # carol@bad.invalid: nxdomain → hard stop, invalid bucket
        row2 = out.iloc[2]
        assert bool(row2["hard_stop_v2"]) is True
        assert row2["hard_stop_reason_v2"] == "nxdomain"
        assert row2["bucket_v2"] == "invalid"
        assert "nxdomain" in row2["reason_codes_v2"]
        assert "Hard stop triggered: nxdomain." in row2["explanation_v2"]

        # no-at-sign: syntax_invalid → hard stop
        row3 = out.iloc[3]
        assert bool(row3["hard_stop_v2"]) is True
        assert row3["hard_stop_reason_v2"] == "syntax_invalid"
        assert row3["bucket_v2"] == "invalid"

    def test_score_breakdown_is_valid_json(self):
        df = _post_scoring_frame()
        out = ScoringV2Stage().run(
            ChunkPayload(frame=df.copy()), PipelineContext()
        ).frame
        for raw in out["score_breakdown_v2"]:
            parsed = json.loads(raw)
            # Minimum contract from Subphase 3.
            for key in (
                "signals",
                "positive_total",
                "negative_total",
                "raw_score",
                "final_score",
                "confidence",
                "hard_stop",
                "hard_stop_reason",
                "bucket",
                "reason_codes",
            ):
                assert key in parsed

    def test_reason_codes_v2_is_pipe_joined_string(self):
        df = _post_scoring_frame()
        out = ScoringV2Stage().run(
            ChunkPayload(frame=df.copy()), PipelineContext()
        ).frame
        # Alice's row should contain multiple tokens separated by '|'.
        tokens = out.iloc[0]["reason_codes_v2"].split("|")
        assert len(tokens) >= 2
        # No leading/trailing whitespace in tokens.
        for t in tokens:
            assert t == t.strip()

    def test_raw_score_matches_positive_minus_negative(self):
        df = _post_scoring_frame()
        out = ScoringV2Stage().run(
            ChunkPayload(frame=df.copy()), PipelineContext()
        ).frame
        for i in range(len(out)):
            row = out.iloc[i]
            assert float(row["raw_score_v2"]) == (
                float(row["positive_total_v2"])
                - float(row["negative_total_v2"])
            )


class TestScoringV2StageDeterminism:
    def test_repeat_evaluation_is_identical(self):
        df = _post_scoring_frame()
        stage = ScoringV2Stage()
        first = stage.run(
            ChunkPayload(frame=df.copy()), PipelineContext()
        ).frame
        second = stage.run(
            ChunkPayload(frame=df.copy()), PipelineContext()
        ).frame
        for col in V2_OUTPUT_COLUMNS:
            pd.testing.assert_series_equal(
                first[col].reset_index(drop=True),
                second[col].reset_index(drop=True),
                check_names=False,
            )

    def test_fresh_stage_instances_agree(self):
        """Two separately-constructed stages produce the same columns."""
        df = _post_scoring_frame()
        a = ScoringV2Stage().run(
            ChunkPayload(frame=df.copy()), PipelineContext()
        ).frame
        b = ScoringV2Stage().run(
            ChunkPayload(frame=df.copy()), PipelineContext()
        ).frame
        for col in V2_OUTPUT_COLUMNS:
            pd.testing.assert_series_equal(
                a[col].reset_index(drop=True),
                b[col].reset_index(drop=True),
                check_names=False,
            )


class TestScoringV2StageEngineInjection:
    def test_accepts_custom_engine_and_reuses_it(self):
        custom = build_default_engine()
        stage = ScoringV2Stage(engine=custom)
        assert stage.engine is custom

    def test_defaults_to_build_default_engine(self):
        stage = ScoringV2Stage()
        assert isinstance(stage.engine, ScoringEngineV2)
        # Should have 5 evaluators (syntax, domain presence, typo,
        # domain match, DNS).
        assert len(stage.engine.evaluators) == 5

    def test_engine_is_built_once_per_stage(self):
        """Reuse: engine identity is stable across calls."""
        stage = ScoringV2Stage()
        first_engine = stage.engine
        df = _post_scoring_frame()
        stage.run(ChunkPayload(frame=df.copy()), PipelineContext())
        stage.run(ChunkPayload(frame=df.copy()), PipelineContext())
        assert stage.engine is first_engine


# ---------------------------------------------------------------------------
# Pipeline coexistence — V1 + V2 in a single engine pass
# ---------------------------------------------------------------------------


class TestPipelineCoexistence:
    def test_v1_and_v2_both_populated_after_engine_pass(self):
        raw = pd.DataFrame(
            {
                "E-Mail": [
                    "alice@gmail.com",
                    "bob@yahoo.com",
                    "bad@nxdomain.invalid",
                ],
                "Domain": ["gmail.com", "yahoo.com", "nxdomain.invalid"],
                "First Name": ["Alice", "Bob", "Bad"],
            }
        )
        cfg = _build_config()
        cache = DnsCache()
        ctx = PipelineContext(config=cfg, dns_cache=cache, typo_map={})
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
                ScoringV2Stage(),
                CompletenessStage(),
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
        metrics = FileIngestionMetrics(source_file="x.csv", source_file_type="csv")
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
            "app.dns_utils.resolve_domain_dns", side_effect=_fake_resolve
        ):
            out = engine.run(payload, ctx).frame

        # V1 columns still present and populated.
        for col in ("hard_fail", "score", "score_reasons", "preliminary_bucket"):
            assert col in out.columns
        # V2 columns present and populated.
        for col in V2_OUTPUT_COLUMNS:
            assert col in out.columns
        # Alice: V1 high_confidence, V2 not hard-stopped.
        assert out.iloc[0]["preliminary_bucket"] == "high_confidence"
        assert bool(out.iloc[0]["hard_stop_v2"]) is False
        # NXDOMAIN row: V1 hard_fail and V2 hard_stop both fire.
        assert bool(out.iloc[2]["hard_fail"]) is True
        assert bool(out.iloc[2]["hard_stop_v2"]) is True
        assert out.iloc[2]["hard_stop_reason_v2"] == "nxdomain"

    def test_adding_v2_does_not_change_v1_columns(self):
        """Running with ScoringV2Stage inserted yields identical V1
        columns to the reference path without it."""
        raw = pd.DataFrame(
            {
                "E-Mail": [
                    "alice@gmail.com",
                    "bob@yahoo.com",
                    "bad@nxdomain.invalid",
                ],
                "Domain": ["gmail.com", "yahoo.com", "nxdomain.invalid"],
                "First Name": ["Alice", "Bob", "Bad"],
            }
        )
        cfg = _build_config()
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

        def _payload(frame):
            return ChunkPayload(
                frame=frame,
                chunk_index=0,
                source_file="x.csv",
                metadata={
                    "is_first_chunk": True,
                    "file_metrics": metrics,
                    "input_file": input_file,
                    "chunk_context": chunk_context,
                },
            )

        base_stages = [
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
        ]
        ref_stages = base_stages + [CompletenessStage()]
        aug_stages = base_stages + [ScoringV2Stage(), CompletenessStage()]

        ctx_ref = PipelineContext(
            config=cfg, dns_cache=DnsCache(), typo_map={}
        )
        ctx_aug = PipelineContext(
            config=cfg, dns_cache=DnsCache(), typo_map={}
        )

        with patch(
            "app.dns_utils.resolve_domain_dns", side_effect=_fake_resolve
        ):
            ref = PipelineEngine(stages=ref_stages).run(
                _payload(raw.copy()), ctx_ref
            ).frame
            aug = PipelineEngine(stages=aug_stages).run(
                _payload(raw.copy()), ctx_aug
            ).frame

        # Every column present in ref must match exactly in aug.
        for col in ref.columns:
            pd.testing.assert_series_equal(
                ref[col].reset_index(drop=True),
                aug[col].reset_index(drop=True),
                check_names=False,
                check_dtype=False,
            )

        # And aug must additionally carry every V2 column.
        for col in V2_OUTPUT_COLUMNS:
            assert col in aug.columns
            assert col not in ref.columns

    def test_dedupe_still_uses_v1_columns_only(self):
        """Downstream DedupeStage must continue to read V1 fields.
        We verify by removing every V2 column before DedupeStage runs
        and confirming the dedupe outputs are identical."""
        raw = pd.DataFrame(
            {
                "E-Mail": [
                    "alice@gmail.com",
                    "alice@GMAIL.com",  # duplicate normalized
                    "bob@yahoo.com",
                ],
                "Domain": ["gmail.com", "gmail.com", "yahoo.com"],
                "First Name": ["Alice", "Alice2", "Bob"],
            }
        )
        cfg = _build_config()
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

        def _payload(frame):
            return ChunkPayload(
                frame=frame,
                chunk_index=0,
                source_file="x.csv",
                metadata={
                    "is_first_chunk": True,
                    "file_metrics": metrics,
                    "input_file": input_file,
                    "chunk_context": chunk_context,
                },
            )

        base_stages = [
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
        ]
        # Reference path: no V2 stage; full downstream chain.
        ref_stages = base_stages + [
            CompletenessStage(),
            EmailNormalizationStage(),
            DedupeStage(),
        ]
        # Augmented path: V2 stage present; full downstream chain.
        aug_stages = base_stages + [
            ScoringV2Stage(),
            CompletenessStage(),
            EmailNormalizationStage(),
            DedupeStage(),
        ]

        ctx_ref = PipelineContext(
            config=cfg,
            dns_cache=DnsCache(),
            dedupe_index=DedupeIndex(),
            typo_map={},
        )
        ctx_aug = PipelineContext(
            config=cfg,
            dns_cache=DnsCache(),
            dedupe_index=DedupeIndex(),
            typo_map={},
        )

        with patch(
            "app.dns_utils.resolve_domain_dns", side_effect=_fake_resolve
        ):
            ref = PipelineEngine(stages=ref_stages).run(
                _payload(raw.copy()), ctx_ref
            ).frame
            aug = PipelineEngine(stages=aug_stages).run(
                _payload(raw.copy()), ctx_aug
            ).frame

        # Dedupe outputs identical between the two paths.
        for col in (
            "is_canonical",
            "duplicate_flag",
            "duplicate_reason",
            "global_ordinal",
        ):
            pd.testing.assert_series_equal(
                ref[col].reset_index(drop=True),
                aug[col].reset_index(drop=True),
                check_names=False,
                check_dtype=False,
            )
        # And the shared dedupe-index mutations match.
        assert ctx_ref.dedupe_index.new_canonicals == ctx_aug.dedupe_index.new_canonicals
        assert (
            ctx_ref.dedupe_index.duplicates_detected
            == ctx_aug.dedupe_index.duplicates_detected
        )


# ---------------------------------------------------------------------------
# V1 untouched
# ---------------------------------------------------------------------------


class TestV1Untouched:
    def test_v1_scoring_module_has_no_v2_imports(self):
        """Sanity: app.scoring must not depend on scoring_v2."""
        from pathlib import Path as _P

        source = _P(__file__).resolve().parent.parent / "app" / "scoring.py"
        text = source.read_text()
        assert "scoring_v2" not in text
        assert "ScoringEngineV2" not in text
