"""Tests for V1-vs-V2 scoring comparison utilities + stage.

Covers:
  * ``compare_scoring`` column-level semantics (bucket ordering,
    hard-stop classification, score delta, low-confidence flag)
  * ``summarize_comparison`` aggregates, percentages, JSON
    serialization, and the empty-frame edge case
  * ``write_comparison_report`` end-to-end JSON write
  * ``ScoringComparisonStage`` + integration in a pipeline that
    still leaves V1 and V2 columns unchanged
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from app.config import load_config, resolve_project_paths
from app.dns_utils import DnsCache, DnsResult
from app.engine import ChunkPayload, PipelineContext, PipelineEngine
from app.engine.stages import (
    CompletenessStage,
    DNSEnrichmentStage,
    DomainComparisonStage,
    DomainExtractionStage,
    EmailSyntaxValidationStage,
    HeaderNormalizationStage,
    ScoringComparisonStage,
    ScoringStage,
    ScoringV2Stage,
    StructuralValidationStage,
    TechnicalMetadataStage,
    TypoCorrectionStage,
    ValueNormalizationStage,
)
from app.engine.stages.scoring_v2 import V2_OUTPUT_COLUMNS
from app.models import ChunkContext, FileIngestionMetrics, InputFile
from app.scoring_v2 import (
    COMPARISON_COLUMNS,
    compare_scoring,
    summarize_comparison,
    write_comparison_report,
)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_MX_RESULT = DnsResult(True, True, True, False, None)
_A_ONLY_RESULT = DnsResult(True, True, False, True, None)
_NXDOMAIN_RESULT = DnsResult(True, False, False, False, "nxdomain")

_DNS_MAP = {
    "gmail.com": _MX_RESULT,
    "yahoo.com": _MX_RESULT,
    "example.net": _A_ONLY_RESULT,
    "nxdomain.invalid": _NXDOMAIN_RESULT,
}


def _fake_resolve(domain, timeout_seconds=4.0, fallback_to_a_record=True):
    return _DNS_MAP.get(domain, _NXDOMAIN_RESULT)


def _build_config():
    paths = resolve_project_paths()
    return load_config(base_dir=paths.project_root)


def _base_row(**overrides):
    """Default row with both V1 and V2 scoring columns populated."""
    row = {
        "score": 75,
        "preliminary_bucket": "high_confidence",
        "hard_fail": False,
        "score_v2": 0.8,
        "bucket_v2": "high_confidence",
        "hard_stop_v2": False,
        "confidence_v2": 0.9,
        "reason_codes_v2": "syntax_valid|mx_present|domain_match",
    }
    row.update(overrides)
    return row


def _make_frame(rows):
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# compare_scoring — row-level semantics
# ---------------------------------------------------------------------------


class TestCompareScoringColumns:
    def test_appends_all_expected_columns(self):
        df = _make_frame([_base_row()])
        out = compare_scoring(df)
        for col in COMPARISON_COLUMNS:
            assert col in out.columns

    def test_does_not_mutate_input(self):
        df = _make_frame([_base_row()])
        snapshot_cols = list(df.columns)
        snapshot_vals = df.copy()
        compare_scoring(df)
        assert list(df.columns) == snapshot_cols
        pd.testing.assert_frame_equal(df, snapshot_vals)

    def test_score_delta_is_v2_minus_v1(self):
        df = _make_frame(
            [
                _base_row(score=75, score_v2=0.8),
                _base_row(score=40, score_v2=0.2),
                _base_row(score=0, score_v2=0.0),
            ]
        )
        out = compare_scoring(df)
        assert out["score_delta"].tolist() == pytest.approx([0.8 - 75, 0.2 - 40, 0.0])
        assert out["abs_score_delta"].tolist() == pytest.approx(
            [abs(0.8 - 75), abs(0.2 - 40), 0.0]
        )


class TestCompareScoringBuckets:
    @pytest.mark.parametrize(
        "v1, v2, expected_changed, expected_higher, expected_lower",
        [
            ("invalid", "invalid", False, False, False),
            ("invalid", "review", True, True, False),
            ("invalid", "high_confidence", True, True, False),
            ("review", "invalid", True, False, True),
            ("review", "review", False, False, False),
            ("review", "high_confidence", True, True, False),
            ("high_confidence", "invalid", True, False, True),
            ("high_confidence", "review", True, False, True),
            ("high_confidence", "high_confidence", False, False, False),
        ],
    )
    def test_bucket_ordering(
        self, v1, v2, expected_changed, expected_higher, expected_lower
    ):
        df = _make_frame(
            [_base_row(preliminary_bucket=v1, bucket_v2=v2)]
        )
        out = compare_scoring(df)
        assert bool(out.iloc[0]["bucket_changed"]) is expected_changed
        assert bool(out.iloc[0]["v2_higher_bucket"]) is expected_higher
        assert bool(out.iloc[0]["v2_lower_bucket"]) is expected_lower

    def test_unknown_bucket_label_is_below_invalid(self):
        """Unknown V2 label (rank -1) vs known 'invalid' (rank 0) is
        classified as 'v2_lower_bucket' — the ordering is defined, not
        silent."""
        df = _make_frame(
            [_base_row(preliminary_bucket="invalid", bucket_v2="something_new")]
        )
        out = compare_scoring(df)
        assert bool(out.iloc[0]["bucket_changed"]) is True
        assert bool(out.iloc[0]["v2_lower_bucket"]) is True
        assert bool(out.iloc[0]["v2_higher_bucket"]) is False


class TestCompareScoringHardStop:
    @pytest.mark.parametrize(
        "v1_hf, v2_hs, changed, strict, permissive",
        [
            (False, False, False, False, False),
            (True, True, False, False, False),
            (False, True, True, True, False),  # V2 adds a hard stop
            (True, False, True, False, True),  # V2 relaxes a hard fail
        ],
    )
    def test_hard_stop_classification(
        self, v1_hf, v2_hs, changed, strict, permissive
    ):
        df = _make_frame(
            [_base_row(hard_fail=v1_hf, hard_stop_v2=v2_hs)]
        )
        out = compare_scoring(df)
        assert bool(out.iloc[0]["hard_decision_changed"]) is changed
        assert bool(out.iloc[0]["v2_more_strict"]) is strict
        assert bool(out.iloc[0]["v2_more_permissive"]) is permissive

    def test_nullable_bool_hard_fail_is_handled(self):
        """``hard_fail`` arrives from V1 as a pandas nullable boolean —
        NA must be treated as ``False`` (not raise, not bleed through
        as NA)."""
        df = pd.DataFrame(
            [
                _base_row(hard_fail=False, hard_stop_v2=False),
                _base_row(hard_fail=True, hard_stop_v2=False),
            ]
        )
        df["hard_fail"] = pd.array(df["hard_fail"].tolist(), dtype="boolean")
        out = compare_scoring(df)
        assert bool(out.iloc[0]["hard_decision_changed"]) is False
        assert bool(out.iloc[1]["v2_more_permissive"]) is True


class TestCompareScoringLowConfidence:
    def test_low_confidence_threshold_is_0_5(self):
        df = _make_frame(
            [
                _base_row(confidence_v2=0.49),
                _base_row(confidence_v2=0.50),  # boundary: NOT low
                _base_row(confidence_v2=0.75),
            ]
        )
        out = compare_scoring(df)
        assert out["low_confidence_v2"].tolist() == [True, False, False]


# ---------------------------------------------------------------------------
# summarize_comparison — aggregates
# ---------------------------------------------------------------------------


class TestSummarizeComparison:
    def test_empty_frame_yields_well_formed_summary(self):
        empty = pd.DataFrame(
            columns=[
                "score",
                "preliminary_bucket",
                "hard_fail",
                "score_v2",
                "bucket_v2",
                "hard_stop_v2",
                "confidence_v2",
                "reason_codes_v2",
            ]
        )
        comp = compare_scoring(empty)
        summary = summarize_comparison(comp)
        assert summary["total_rows"] == 0
        assert summary["bucket_change_count"] == 0
        assert summary["bucket_change_pct"] == 0.0
        assert summary["avg_score_delta"] == 0.0
        assert summary["avg_confidence_v2"] == 0.0
        assert summary["low_confidence_rate"] == 0.0
        # JSON serializable even when empty.
        json.dumps(summary)

    def test_counts_and_percentages(self):
        # 4 rows: 2 bucket changes (one up, one down), 1 hard-stop change.
        rows = [
            _base_row(
                preliminary_bucket="invalid",
                bucket_v2="review",  # UPGRADE
                hard_fail=False,
                hard_stop_v2=False,
                score=0,
                score_v2=0.6,
                confidence_v2=0.9,
                reason_codes_v2="mx_present|domain_match",
            ),
            _base_row(
                preliminary_bucket="high_confidence",
                bucket_v2="review",  # DOWNGRADE
                hard_fail=False,
                hard_stop_v2=False,
                score=75,
                score_v2=0.55,
                confidence_v2=0.3,  # low confidence
                reason_codes_v2="a_fallback|domain_match",
            ),
            _base_row(
                preliminary_bucket="high_confidence",
                bucket_v2="high_confidence",  # unchanged
                hard_fail=False,
                hard_stop_v2=False,
                score=80,
                score_v2=0.9,
                confidence_v2=0.95,
                reason_codes_v2="mx_present|domain_match",
            ),
            _base_row(
                preliminary_bucket="high_confidence",
                bucket_v2="invalid",  # DOWNGRADE via hard stop
                hard_fail=False,
                hard_stop_v2=True,  # V2 is stricter
                score=70,
                score_v2=0.0,
                confidence_v2=0.8,
                reason_codes_v2="nxdomain",
            ),
        ]
        df = _make_frame(rows)
        summary = summarize_comparison(compare_scoring(df))

        assert summary["total_rows"] == 4
        assert summary["bucket_change_count"] == 3
        assert summary["bucket_change_pct"] == pytest.approx(75.0)
        assert summary["v2_higher_bucket_count"] == 1
        assert summary["v2_lower_bucket_count"] == 2
        assert summary["hard_decision_changed_count"] == 1
        assert summary["v2_more_strict_count"] == 1
        assert summary["v2_more_permissive_count"] == 0
        # low confidence flag: 0.3 < 0.5 → 1 of 4 rows
        assert summary["low_confidence_rate"] == pytest.approx(0.25)

    def test_score_delta_aggregates(self):
        rows = [
            _base_row(score=70, score_v2=0.7),   # delta = -69.3
            _base_row(score=50, score_v2=0.5),   # delta = -49.5
            _base_row(score=10, score_v2=0.9),   # delta = -9.1
        ]
        df = _make_frame(rows)
        summary = summarize_comparison(compare_scoring(df))
        # max_score_increase is the largest (least-negative) delta.
        assert summary["max_score_increase"] == pytest.approx(-9.1)
        assert summary["max_score_decrease"] == pytest.approx(-69.3)
        assert summary["median_score_delta"] == pytest.approx(-49.5)

    def test_top_reason_codes_split_on_pipes(self):
        rows = [
            _base_row(
                preliminary_bucket="high_confidence",
                bucket_v2="invalid",
                hard_stop_v2=True,
                reason_codes_v2="nxdomain|domain_match",
            ),
            _base_row(
                preliminary_bucket="high_confidence",
                bucket_v2="invalid",
                hard_stop_v2=True,
                reason_codes_v2="nxdomain",
            ),
            _base_row(
                preliminary_bucket="high_confidence",
                bucket_v2="review",
                reason_codes_v2="a_fallback|domain_match",
            ),
        ]
        df = _make_frame(rows)
        summary = summarize_comparison(compare_scoring(df))
        # All 3 downgraded.
        downgrade_codes = {
            entry["reason_code"]: entry["count"]
            for entry in summary["top_reason_codes_when_v2_downgraded"]
        }
        assert downgrade_codes.get("nxdomain") == 2
        assert downgrade_codes.get("domain_match") == 2
        assert downgrade_codes.get("a_fallback") == 1

    def test_summary_is_json_serializable_with_strict_parser(self):
        rows = [_base_row(), _base_row(bucket_v2="review", score_v2=0.5)]
        summary = summarize_comparison(compare_scoring(_make_frame(rows)))
        encoded = json.dumps(summary)  # strict JSON (no NaN tokens)
        roundtrip = json.loads(encoded)
        assert roundtrip["total_rows"] == 2
        assert "bucket_v2_distribution" in roundtrip

    def test_bucket_and_confidence_distributions(self):
        rows = [
            _base_row(bucket_v2="high_confidence", confidence_v2=0.9),
            _base_row(bucket_v2="review", confidence_v2=0.6),
            _base_row(bucket_v2="invalid", confidence_v2=0.2),
            _base_row(bucket_v2="invalid", confidence_v2=0.1),
        ]
        summary = summarize_comparison(compare_scoring(_make_frame(rows)))
        assert summary["bucket_v2_distribution"] == {
            "invalid": 2,
            "review": 1,
            "high_confidence": 1,
        }
        conf = summary["confidence_v2_distribution"]
        assert conf["lt_0_25"] == 2
        assert conf["0_5_to_0_75"] == 1
        assert conf["gte_0_75"] == 1


# ---------------------------------------------------------------------------
# write_comparison_report
# ---------------------------------------------------------------------------


class TestWriteComparisonReport:
    def test_writes_valid_json_and_returns_summary(self, tmp_path):
        rows = [_base_row()]
        out_path = tmp_path / "reports" / "scoring_v2_comparison.json"
        summary = write_comparison_report(_make_frame(rows), out_path)

        assert out_path.exists()
        on_disk = json.loads(out_path.read_text())
        assert on_disk == summary
        assert on_disk["total_rows"] == 1

    def test_accepts_prepared_or_raw_frame(self, tmp_path):
        rows = [_base_row(), _base_row(bucket_v2="review")]
        raw = _make_frame(rows)
        prepared = compare_scoring(raw)

        raw_summary = write_comparison_report(raw, tmp_path / "raw.json")
        prep_summary = write_comparison_report(
            prepared, tmp_path / "prep.json"
        )
        assert raw_summary == prep_summary


# ---------------------------------------------------------------------------
# ScoringComparisonStage — unit + integration
# ---------------------------------------------------------------------------


def _post_v2_frame():
    return pd.DataFrame(
        [
            _base_row(
                score=75,
                preliminary_bucket="high_confidence",
                hard_fail=False,
                score_v2=0.9,
                bucket_v2="high_confidence",
                hard_stop_v2=False,
                confidence_v2=0.95,
            ),
            _base_row(
                score=0,
                preliminary_bucket="invalid",
                hard_fail=True,
                score_v2=0.0,
                bucket_v2="invalid",
                hard_stop_v2=True,
                confidence_v2=0.7,
                reason_codes_v2="nxdomain",
            ),
        ]
    )


class TestScoringComparisonStage:
    def test_appends_all_comparison_columns(self):
        df = _post_v2_frame()
        out = ScoringComparisonStage().run(
            ChunkPayload(frame=df.copy()), PipelineContext()
        ).frame
        for col in COMPARISON_COLUMNS:
            assert col in out.columns

    def test_does_not_alter_v1_or_v2_columns(self):
        df = _post_v2_frame()
        before = df.copy()
        out = ScoringComparisonStage().run(
            ChunkPayload(frame=df.copy()), PipelineContext()
        ).frame
        # Every input column preserved byte-for-byte.
        for col in before.columns:
            pd.testing.assert_series_equal(
                before[col].reset_index(drop=True),
                out[col].reset_index(drop=True),
                check_names=False,
            )


class TestFullPipelineWithComparison:
    def test_pipeline_produces_v1_v2_and_comparison_columns(self):
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
        ctx = PipelineContext(config=cfg, dns_cache=DnsCache(), typo_map={})
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
                ScoringComparisonStage(),
                CompletenessStage(),
            ]
        )
        payload = ChunkPayload(
            frame=raw,
            chunk_index=0,
            source_file="x.csv",
            metadata={
                "is_first_chunk": True,
                "file_metrics": FileIngestionMetrics(
                    source_file="x.csv", source_file_type="csv"
                ),
                "input_file": InputFile(
                    absolute_path=Path("/tmp/x.csv"),
                    original_name="x.csv",
                    file_type="csv",
                ),
                "chunk_context": ChunkContext(
                    chunk_index=0, row_count=3, start_row_number=2
                ),
            },
        )

        with patch(
            "app.dns_utils.resolve_domain_dns", side_effect=_fake_resolve
        ):
            out = engine.run(payload, ctx).frame

        # V1 columns
        for col in ("hard_fail", "score", "preliminary_bucket"):
            assert col in out.columns
        # V2 columns
        for col in V2_OUTPUT_COLUMNS:
            assert col in out.columns
        # Comparison columns
        for col in COMPARISON_COLUMNS:
            assert col in out.columns

    def test_adding_comparison_stage_does_not_change_v1_or_v2_columns(self):
        """End-to-end: running the pipeline with and without
        ScoringComparisonStage yields identical V1 AND V2 columns."""
        raw = pd.DataFrame(
            {
                "E-Mail": [
                    "alice@gmail.com",
                    "bad@nxdomain.invalid",
                ],
                "Domain": ["gmail.com", "nxdomain.invalid"],
                "First Name": ["Alice", "Bad"],
            }
        )
        cfg = _build_config()

        def _payload(frame):
            return ChunkPayload(
                frame=frame,
                chunk_index=0,
                source_file="x.csv",
                metadata={
                    "is_first_chunk": True,
                    "file_metrics": FileIngestionMetrics(
                        source_file="x.csv", source_file_type="csv"
                    ),
                    "input_file": InputFile(
                        absolute_path=Path("/tmp/x.csv"),
                        original_name="x.csv",
                        file_type="csv",
                    ),
                    "chunk_context": ChunkContext(
                        chunk_index=0, row_count=2, start_row_number=2
                    ),
                },
            )

        base = [
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
        ]

        ctx_ref = PipelineContext(
            config=cfg, dns_cache=DnsCache(), typo_map={}
        )
        ctx_aug = PipelineContext(
            config=cfg, dns_cache=DnsCache(), typo_map={}
        )

        with patch(
            "app.dns_utils.resolve_domain_dns", side_effect=_fake_resolve
        ):
            ref = PipelineEngine(stages=base + [CompletenessStage()]).run(
                _payload(raw.copy()), ctx_ref
            ).frame
            aug = PipelineEngine(
                stages=base
                + [ScoringComparisonStage(), CompletenessStage()]
            ).run(_payload(raw.copy()), ctx_aug).frame

        # Every V1 + V2 column the reference path has must match exactly.
        for col in ref.columns:
            pd.testing.assert_series_equal(
                ref[col].reset_index(drop=True),
                aug[col].reset_index(drop=True),
                check_names=False,
                check_dtype=False,
            )

        # And the aug path has the additional comparison columns.
        for col in COMPARISON_COLUMNS:
            assert col in aug.columns
            assert col not in ref.columns


# ---------------------------------------------------------------------------
# V1 untouched
# ---------------------------------------------------------------------------


class TestV1Untouched:
    def test_scoring_module_has_no_scoring_v2_or_comparison_imports(self):
        source = Path(__file__).resolve().parent.parent / "app" / "scoring.py"
        text = source.read_text()
        assert "scoring_v2" not in text
        assert "compare_scoring" not in text
