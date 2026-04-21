"""Subphase-4 engine-refactor tests: migrated enrichment stages.

Each stage is tested in isolation with a minimal DataFrame, and an
engine-vs-inline equivalence test verifies that running the three
stages through ``PipelineEngine`` produces exactly the same frame as
calling the underlying functions in the previous inline order.

DNS resolution is patched with a deterministic mock so the test does
not hit the network and the outputs are predictable.
"""

from __future__ import annotations

import dataclasses
from unittest.mock import patch

import pandas as pd

from app.config import load_config, resolve_project_paths
from app.dedupe import apply_completeness_column
from app.dns_utils import DnsCache, DnsResult, apply_dns_enrichment_column
from app.engine import ChunkPayload, PipelineContext, PipelineEngine
from app.engine.stages import (
    CompletenessStage,
    DNSEnrichmentStage,
    ScoringStage,
)
from app.scoring import apply_scoring_column


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
}


def _fake_resolve(domain, timeout_seconds=4.0, fallback_to_a_record=True):
    return _DNS_MAP.get(domain, _NXDOMAIN_RESULT)


def _build_config():
    project_paths = resolve_project_paths()
    return load_config(base_dir=project_paths.project_root)


def _build_pre_dns_frame() -> pd.DataFrame:
    """A frame with the columns every row needs for the three enrichment
    stages: email processing outputs + the ``domain`` input column."""
    return pd.DataFrame(
        {
            "email": [
                "alice@gmail.com",
                "bob@yahoo.com",
                "carol@example.net",
                "dan@bad.invalid",
                "no-at-sign",
            ],
            "domain": [
                "gmail.com",
                "yahoo.com",
                "example.net",
                "bad.invalid",
                None,
            ],
            "fname": ["Alice", "Bob", "Carol", "Dan", None],
            "lname": ["A", "B", "C", "D", None],
            "syntax_valid": pd.array(
                [True, True, True, True, False], dtype="boolean"
            ),
            "corrected_domain": [
                "gmail.com",
                "yahoo.com",
                "example.net",
                "bad.invalid",
                None,
            ],
            "typo_corrected": pd.array(
                [False, False, False, False, pd.NA], dtype="boolean"
            ),
            "domain_matches_input_column": pd.array(
                [True, True, True, True, pd.NA], dtype="boolean"
            ),
        }
    )


# ---------------------------------------------------------------------------
# Per-stage unit tests
# ---------------------------------------------------------------------------

class TestDNSEnrichmentStage:
    def test_uses_context_cache_and_produces_dns_columns(self):
        cfg = _build_config()
        cache = DnsCache()
        ctx = PipelineContext(config=cfg, dns_cache=cache)
        df = _build_pre_dns_frame()

        with patch(
            "app.dns_utils.resolve_domain_dns", side_effect=_fake_resolve
        ):
            out = DNSEnrichmentStage().run(
                ChunkPayload(frame=df), ctx
            )

        # Produces the expected columns.
        for col in (
            "dns_check_performed",
            "domain_exists",
            "has_mx_record",
            "has_a_record",
            "dns_error",
        ):
            assert col in out.frame.columns

        # Cache populated with exactly the unique corrected_domains we
        # had on eligible rows.
        assert cache.domains_queried == 4  # gmail, yahoo, example.net, bad
        assert cache.cache_hits == 0  # first time seeing each

        # MX vs A-only vs nxdomain is faithfully mapped back.
        assert bool(out.frame.iloc[0]["has_mx_record"]) is True
        assert bool(out.frame.iloc[2]["has_a_record"]) is True
        assert bool(out.frame.iloc[3]["domain_exists"]) is False
        assert out.frame.iloc[3]["dns_error"] == "nxdomain"

    def test_reuses_cache_on_second_call(self):
        cfg = _build_config()
        cache = DnsCache()
        ctx = PipelineContext(config=cfg, dns_cache=cache)
        df = _build_pre_dns_frame()

        with patch(
            "app.dns_utils.resolve_domain_dns", side_effect=_fake_resolve
        ):
            DNSEnrichmentStage().run(ChunkPayload(frame=df.copy()), ctx)
            # Second call: everything should be a cache hit.
            DNSEnrichmentStage().run(ChunkPayload(frame=df.copy()), ctx)

        assert cache.domains_queried == 4
        assert cache.cache_hits == 4  # 4 unique domains re-seen


class TestScoringStage:
    def test_uses_context_thresholds(self):
        cfg = _build_config()
        ctx = PipelineContext(config=cfg)

        # Row is valid + MX → gets 25 + 50 = 75 points → high_confidence
        # (default threshold 70).
        df = pd.DataFrame(
            {
                "syntax_valid": pd.array([True], dtype="boolean"),
                "corrected_domain": ["gmail.com"],
                "has_mx_record": pd.array([True], dtype="boolean"),
                "has_a_record": pd.array([False], dtype="boolean"),
                "domain_exists": pd.array([True], dtype="boolean"),
                "dns_error": [None],
                "typo_corrected": pd.array([False], dtype="boolean"),
                "domain_matches_input_column": pd.array([True], dtype="boolean"),
            }
        )
        out = ScoringStage().run(ChunkPayload(frame=df), ctx)
        assert int(out.frame.iloc[0]["score"]) == 75
        assert out.frame.iloc[0]["preliminary_bucket"] == "high_confidence"
        assert bool(out.frame.iloc[0]["hard_fail"]) is False

    def test_nxdomain_row_is_hard_fail(self):
        cfg = _build_config()
        ctx = PipelineContext(config=cfg)

        df = pd.DataFrame(
            {
                "syntax_valid": pd.array([True], dtype="boolean"),
                "corrected_domain": ["bad.invalid"],
                "has_mx_record": pd.array([False], dtype="boolean"),
                "has_a_record": pd.array([False], dtype="boolean"),
                "domain_exists": pd.array([False], dtype="boolean"),
                "dns_error": ["nxdomain"],
                "typo_corrected": pd.array([False], dtype="boolean"),
                "domain_matches_input_column": pd.array([True], dtype="boolean"),
            }
        )
        out = ScoringStage().run(ChunkPayload(frame=df), ctx)
        assert bool(out.frame.iloc[0]["hard_fail"]) is True
        assert int(out.frame.iloc[0]["score"]) == 0
        assert out.frame.iloc[0]["preliminary_bucket"] == "invalid"

    def test_custom_thresholds_propagated(self):
        """If the config's thresholds change, the stage honors them —
        proving the stage reads from context.config rather than hard-
        coding defaults."""
        cfg = dataclasses.replace(
            _build_config(),
            high_confidence_threshold=200,  # unreachable
            review_threshold=200,
        )
        ctx = PipelineContext(config=cfg)

        df = pd.DataFrame(
            {
                "syntax_valid": pd.array([True], dtype="boolean"),
                "corrected_domain": ["gmail.com"],
                "has_mx_record": pd.array([True], dtype="boolean"),
                "has_a_record": pd.array([False], dtype="boolean"),
                "domain_exists": pd.array([True], dtype="boolean"),
                "dns_error": [None],
                "typo_corrected": pd.array([False], dtype="boolean"),
                "domain_matches_input_column": pd.array([True], dtype="boolean"),
            }
        )
        out = ScoringStage().run(ChunkPayload(frame=df), ctx)
        # Score is clamped to 100 (below the unreachable threshold).
        assert int(out.frame.iloc[0]["score"]) == 75
        # With thresholds of 200/200 no row can reach high_confidence or review.
        assert out.frame.iloc[0]["preliminary_bucket"] == "invalid"


class TestCompletenessStage:
    def test_counts_non_null_business_columns(self):
        # 5 business columns populated → completeness_score = 5.
        df = pd.DataFrame(
            {
                "email": ["alice@x.com"],
                "domain": ["x.com"],
                "fname": ["Alice"],
                "lname": [None],
                "state": ["CA"],
                "city": ["SF"],
                # technical columns shouldn't count
                "source_file": ["f.csv"],
                "score": [75],
            }
        )
        out = CompletenessStage().run(
            ChunkPayload(frame=df), PipelineContext()
        )
        assert int(out.frame.iloc[0]["completeness_score"]) == 5


# ---------------------------------------------------------------------------
# End-to-end equivalence: engine path == inline path
# ---------------------------------------------------------------------------

class TestEnrichmentEnginePathMatchesInline:
    def test_three_stages_produce_same_result_as_inline_sequence(self):
        cfg = _build_config()
        df = _build_pre_dns_frame()

        # ---- Inline reference path (copy of pre-refactor code) ----
        cache_a = DnsCache()
        with patch(
            "app.dns_utils.resolve_domain_dns", side_effect=_fake_resolve
        ):
            ref = apply_dns_enrichment_column(
                df.copy(),
                cache=cache_a,
                timeout_seconds=cfg.dns_timeout_seconds,
                fallback_to_a_record=cfg.fallback_to_a_record,
                max_workers=cfg.max_workers,
            )
        ref = apply_scoring_column(
            ref,
            high_confidence_threshold=cfg.high_confidence_threshold,
            review_threshold=cfg.review_threshold,
        )
        ref = apply_completeness_column(ref)

        # ---- Engine path ----
        cache_b = DnsCache()
        ctx = PipelineContext(config=cfg, dns_cache=cache_b)
        engine = PipelineEngine(
            stages=[DNSEnrichmentStage(), ScoringStage(), CompletenessStage()]
        )
        with patch(
            "app.dns_utils.resolve_domain_dns", side_effect=_fake_resolve
        ):
            out = engine.run(ChunkPayload(frame=df.copy()), ctx).frame

        assert list(out.columns) == list(ref.columns)
        pd.testing.assert_frame_equal(
            out.reset_index(drop=True), ref.reset_index(drop=True)
        )
        # Cache counters match between the two paths.
        assert cache_a.domains_queried == cache_b.domains_queried
        assert cache_a.cache_hits == cache_b.cache_hits

    def test_full_eleven_stage_engine_produces_expected_columns(self):
        """Smoke: the extended 11-stage chunk engine (preprocess + email
        processing + enrichment) produces every column the inline dedupe
        code downstream depends on (completeness_score, hard_fail, score,
        preliminary_bucket)."""
        from pathlib import Path

        from app.engine.stages import (
            DomainComparisonStage,
            DomainExtractionStage,
            EmailSyntaxValidationStage,
            HeaderNormalizationStage,
            StructuralValidationStage,
            TechnicalMetadataStage,
            TypoCorrectionStage,
            ValueNormalizationStage,
        )
        from app.models import ChunkContext, FileIngestionMetrics, InputFile

        raw = pd.DataFrame(
            {
                "E-Mail": ["alice@gmail.com", "bob@yahoo.com", "bad@nxdomain.invalid"],
                "Domain": ["gmail.com", "yahoo.com", "nxdomain.invalid"],
                "First Name": ["Alice", "Bob", "Bad"],
            }
        )
        input_file = InputFile(
            absolute_path=Path("/tmp/x.csv"),
            original_name="x.csv",
            file_type="csv",
        )
        chunk_context = ChunkContext(chunk_index=0, row_count=3, start_row_number=2)
        metrics = FileIngestionMetrics(source_file="x.csv", source_file_type="csv")

        cfg = _build_config()
        cache = DnsCache()
        ctx = PipelineContext(
            config=cfg,
            dns_cache=cache,
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

        with patch(
            "app.dns_utils.resolve_domain_dns", side_effect=_fake_resolve
        ):
            out = engine.run(payload, ctx).frame

        for col in (
            "syntax_valid",
            "corrected_domain",
            "domain_matches_input_column",
            "dns_check_performed",
            "domain_exists",
            "has_mx_record",
            "has_a_record",
            "dns_error",
            "hard_fail",
            "score",
            "score_reasons",
            "preliminary_bucket",
            "completeness_score",
        ):
            assert col in out.columns, f"missing column: {col}"

        # gmail row is valid + MX; expect non-hard-fail and high_confidence.
        assert bool(out.iloc[0]["hard_fail"]) is False
        assert out.iloc[0]["preliminary_bucket"] == "high_confidence"
        # nxdomain row is hard-fail.
        assert bool(out.iloc[2]["hard_fail"]) is True
        assert out.iloc[2]["preliminary_bucket"] == "invalid"
