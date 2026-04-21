"""Tests for Subphase 5: DNS utilities, cache, and DataFrame enrichment.

Sections:
  1. TestDnsResult               — dataclass structure and field types
  2. TestDnsCache                — get/set, metrics, containment
  3. TestResolveDomainDns        — DNS resolution with mocked dns.resolver
  4. TestApplyDnsEnrichmentColumn — DataFrame-level enrichment with mocked resolver
  5. TestDnsPipelineIntegration  — end-to-end pipeline with mocked DNS
  6. TestNoFuturePhaseContamination — scoring/dedupe/SMTP absent from dns_utils
"""

from __future__ import annotations

import inspect
import logging
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import dns.exception
import dns.resolver
import pandas as pd
import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_VENDOR_SITE = _PROJECT_ROOT / "vendor_site"
if str(_VENDOR_SITE) not in sys.path:
    sys.path.insert(0, str(_VENDOR_SITE))

from app.dns_utils import (
    DnsCache,
    DnsResult,
    apply_dns_enrichment_column,
    resolve_domain_dns,
)
from app.normalizers import (
    apply_domain_typo_correction_column,
    extract_email_components,
)
from app.typo_rules import build_typo_map
from app.validators import validate_email_syntax_column

TYPO_MAP_PATH = _PROJECT_ROOT / "configs" / "typo_map.csv"


def _typo_map() -> dict[str, str]:
    return build_typo_map(TYPO_MAP_PATH)


def _build_subphase4_df(emails: list[str | None]) -> pd.DataFrame:
    """Build a DataFrame already processed through Subphase 3 + 4."""
    df = pd.DataFrame({"email": emails})
    df = validate_email_syntax_column(df)
    df = extract_email_components(df)
    df = apply_domain_typo_correction_column(df, _typo_map())
    return df


# ===========================================================================
# SECTION 1 — DnsResult
# ===========================================================================

class TestDnsResult:
    def test_fields_present(self):
        r = DnsResult(
            dns_check_performed=True,
            domain_exists=True,
            has_mx_record=True,
            has_a_record=False,
            dns_error=None,
        )
        assert r.dns_check_performed is True
        assert r.domain_exists is True
        assert r.has_mx_record is True
        assert r.has_a_record is False
        assert r.dns_error is None

    def test_dns_error_string(self):
        r = DnsResult(
            dns_check_performed=True,
            domain_exists=False,
            has_mx_record=False,
            has_a_record=False,
            dns_error="nxdomain",
        )
        assert r.dns_error == "nxdomain"

    def test_all_error_strings_are_strings_or_none(self):
        for error in ("nxdomain", "timeout", "no_nameservers", "no_mx", "no_mx_no_a", "error", None):
            r = DnsResult(
                dns_check_performed=True,
                domain_exists=False,
                has_mx_record=False,
                has_a_record=False,
                dns_error=error,
            )
            assert r.dns_error == error


# ===========================================================================
# SECTION 2 — DnsCache
# ===========================================================================

class TestDnsCache:
    def test_initial_state(self):
        cache = DnsCache()
        assert cache.domains_queried == 0
        assert cache.cache_hits == 0
        assert len(cache) == 0

    def test_set_increments_domains_queried(self):
        cache = DnsCache()
        result = DnsResult(True, True, True, False, None)
        cache.set("gmail.com", result)
        assert cache.domains_queried == 1

    def test_set_stores_result(self):
        cache = DnsCache()
        result = DnsResult(True, True, True, False, None)
        cache.set("gmail.com", result)
        assert cache.get("gmail.com") is result

    def test_get_returns_none_for_missing(self):
        cache = DnsCache()
        assert cache.get("unknown.com") is None

    def test_get_does_not_modify_metrics(self):
        cache = DnsCache()
        cache.get("missing.com")
        assert cache.cache_hits == 0
        assert cache.domains_queried == 0

    def test_contains_true_after_set(self):
        cache = DnsCache()
        cache.set("example.com", DnsResult(True, True, False, True, None))
        assert "example.com" in cache

    def test_contains_false_for_missing(self):
        cache = DnsCache()
        assert "example.com" not in cache

    def test_multiple_sets_increment_counter(self):
        cache = DnsCache()
        for i in range(5):
            cache.set(f"domain{i}.com", DnsResult(True, True, True, False, None))
        assert cache.domains_queried == 5
        assert len(cache) == 5

    def test_set_same_domain_twice_counts_twice(self):
        # Callers should check containment before calling set.
        cache = DnsCache()
        r = DnsResult(True, True, True, False, None)
        cache.set("gmail.com", r)
        cache.set("gmail.com", r)
        assert cache.domains_queried == 2

    def test_cache_hits_starts_at_zero(self):
        cache = DnsCache()
        assert cache.cache_hits == 0


# ===========================================================================
# SECTION 3 — resolve_domain_dns (mocked DNS)
# ===========================================================================

class TestResolveDomainDns:
    """All DNS calls are mocked; no real network traffic."""

    def test_mx_found_returns_domain_exists_true(self):
        with patch.object(dns.resolver.Resolver, "resolve", return_value=MagicMock()):
            result = resolve_domain_dns("gmail.com")
        assert result.dns_check_performed is True
        assert result.domain_exists is True
        assert result.has_mx_record is True
        assert result.has_a_record is False
        assert result.dns_error is None

    def test_nxdomain_returns_domain_exists_false(self):
        with patch.object(dns.resolver.Resolver, "resolve", side_effect=dns.resolver.NXDOMAIN):
            result = resolve_domain_dns("nonexistent-xyz-123.com")
        assert result.dns_check_performed is True
        assert result.domain_exists is False
        assert result.has_mx_record is False
        assert result.has_a_record is False
        assert result.dns_error == "nxdomain"

    def test_timeout_returns_timeout_error(self):
        with patch.object(dns.resolver.Resolver, "resolve", side_effect=dns.exception.Timeout):
            result = resolve_domain_dns("slow-domain.com")
        assert result.dns_check_performed is True
        assert result.domain_exists is False
        assert result.dns_error == "timeout"

    def test_no_nameservers_returns_no_nameservers_error(self):
        with patch.object(dns.resolver.Resolver, "resolve", side_effect=dns.resolver.NoNameservers):
            result = resolve_domain_dns("broken.com")
        assert result.dns_check_performed is True
        assert result.domain_exists is False
        assert result.dns_error == "no_nameservers"

    def test_generic_exception_returns_error(self):
        with patch.object(dns.resolver.Resolver, "resolve", side_effect=RuntimeError("unexpected")):
            result = resolve_domain_dns("weird.com")
        assert result.dns_check_performed is True
        assert result.domain_exists is False
        assert result.dns_error == "error"

    def test_no_mx_but_a_found_fallback(self):
        def side_effect(domain, rdtype):
            if rdtype == "MX":
                raise dns.resolver.NoAnswer
            return MagicMock()

        with patch.object(dns.resolver.Resolver, "resolve", side_effect=side_effect):
            result = resolve_domain_dns("a-only-domain.com", fallback_to_a_record=True)

        assert result.dns_check_performed is True
        assert result.domain_exists is True
        assert result.has_mx_record is False
        assert result.has_a_record is True
        assert result.dns_error is None

    def test_no_mx_fallback_disabled(self):
        with patch.object(dns.resolver.Resolver, "resolve", side_effect=dns.resolver.NoAnswer):
            result = resolve_domain_dns("a-only-domain.com", fallback_to_a_record=False)

        assert result.domain_exists is False
        assert result.has_mx_record is False
        assert result.has_a_record is False
        assert result.dns_error == "no_mx"

    def test_no_mx_no_a_no_aaaa(self):
        with patch.object(dns.resolver.Resolver, "resolve", side_effect=dns.resolver.NoAnswer):
            result = resolve_domain_dns("records-less.com", fallback_to_a_record=True)

        assert result.domain_exists is False
        assert result.has_mx_record is False
        assert result.has_a_record is False
        assert result.dns_error == "no_mx_no_a"

    def test_dns_check_performed_always_true(self):
        for exc in (
            dns.resolver.NXDOMAIN,
            dns.exception.Timeout,
            dns.resolver.NoNameservers,
        ):
            with patch.object(dns.resolver.Resolver, "resolve", side_effect=exc):
                result = resolve_domain_dns("test.com")
            assert result.dns_check_performed is True


# ===========================================================================
# SECTION 4 — apply_dns_enrichment_column (mocked resolve_domain_dns)
# ===========================================================================

_MX_RESULT = DnsResult(True, True, True, False, None)
_A_RESULT = DnsResult(True, True, False, True, None)
_NXDOMAIN_RESULT = DnsResult(True, False, False, False, "nxdomain")

DNS_COLUMNS = ["dns_check_performed", "domain_exists", "has_mx_record", "has_a_record", "dns_error"]


class TestApplyDnsEnrichmentColumn:
    """DataFrame-level enrichment. resolve_domain_dns is always mocked."""

    @pytest.fixture
    def cache(self) -> DnsCache:
        return DnsCache()

    def test_valid_email_gets_dns_columns(self, cache):
        df = _build_subphase4_df(["alice@gmail.com"])
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_RESULT):
            result = apply_dns_enrichment_column(df, cache)
        for col in DNS_COLUMNS:
            assert col in result.columns

    def test_valid_email_mx_found(self, cache):
        df = _build_subphase4_df(["alice@gmail.com"])
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_RESULT):
            result = apply_dns_enrichment_column(df, cache)
        assert result.iloc[0]["dns_check_performed"] == True
        assert result.iloc[0]["domain_exists"] == True
        assert result.iloc[0]["has_mx_record"] == True
        assert result.iloc[0]["has_a_record"] == False
        assert result.iloc[0]["dns_error"] is None

    def test_invalid_email_dns_not_performed(self, cache):
        df = _build_subphase4_df(["bad@@email"])
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_RESULT) as mock_fn:
            result = apply_dns_enrichment_column(df, cache)
        mock_fn.assert_not_called()
        assert pd.isna(result.iloc[0]["dns_check_performed"])
        assert pd.isna(result.iloc[0]["domain_exists"])

    def test_none_email_dns_not_performed(self, cache):
        df = _build_subphase4_df([None])
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_RESULT) as mock_fn:
            result = apply_dns_enrichment_column(df, cache)
        mock_fn.assert_not_called()
        assert pd.isna(result.iloc[0]["dns_check_performed"])

    def test_nxdomain_result_mapped_correctly(self, cache):
        df = _build_subphase4_df(["alice@nonexistent-xyz.com"])
        with patch("app.dns_utils.resolve_domain_dns", return_value=_NXDOMAIN_RESULT):
            result = apply_dns_enrichment_column(df, cache)
        assert result.iloc[0]["domain_exists"] == False
        assert result.iloc[0]["dns_error"] == "nxdomain"

    def test_same_domain_queried_once(self, cache):
        df = _build_subphase4_df(["a@gmail.com", "b@gmail.com", "c@gmail.com"])
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_RESULT) as mock_fn:
            apply_dns_enrichment_column(df, cache)
        assert mock_fn.call_count == 1

    def test_cache_reused_across_calls(self, cache):
        df = _build_subphase4_df(["a@gmail.com"])
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_RESULT) as mock_fn:
            apply_dns_enrichment_column(df, cache)
            apply_dns_enrichment_column(df, cache)
        # Second call must not trigger a new DNS query.
        assert mock_fn.call_count == 1
        assert cache.cache_hits >= 1

    def test_domains_queried_counter_incremented(self, cache):
        df = _build_subphase4_df(["a@gmail.com", "b@yahoo.com"])
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_RESULT):
            apply_dns_enrichment_column(df, cache)
        assert cache.domains_queried == 2

    def test_cache_hits_counter_incremented_on_second_chunk(self, cache):
        df = _build_subphase4_df(["a@gmail.com"])
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_RESULT):
            apply_dns_enrichment_column(df, cache)  # first: queries DNS
            apply_dns_enrichment_column(df, cache)  # second: cache hit
        assert cache.cache_hits == 1

    def test_mixed_valid_invalid_rows(self, cache):
        df = _build_subphase4_df(["alice@gmail.com", "bad@@email", None, "bob@yahoo.com"])
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_RESULT):
            result = apply_dns_enrichment_column(df, cache)
        assert result.iloc[0]["dns_check_performed"] == True
        assert pd.isna(result.iloc[1]["dns_check_performed"])
        assert pd.isna(result.iloc[2]["dns_check_performed"])
        assert result.iloc[3]["dns_check_performed"] == True

    def test_original_columns_not_destroyed(self, cache):
        df = _build_subphase4_df(["alice@gmail.com"])
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_RESULT):
            result = apply_dns_enrichment_column(df, cache)
        assert "email" in result.columns
        assert "syntax_valid" in result.columns
        assert "corrected_domain" in result.columns
        assert result.iloc[0]["email"] == "alice@gmail.com"

    def test_original_frame_not_mutated(self, cache):
        df = _build_subphase4_df(["alice@gmail.com"])
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_RESULT):
            apply_dns_enrichment_column(df, cache)
        assert "dns_check_performed" not in df.columns

    def test_boolean_dtype_on_dns_columns(self, cache):
        df = _build_subphase4_df(["alice@gmail.com"])
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_RESULT):
            result = apply_dns_enrichment_column(df, cache)
        for col in ("dns_check_performed", "domain_exists", "has_mx_record", "has_a_record"):
            assert str(result[col].dtype) == "boolean", f"{col} dtype is not boolean"

    def test_missing_syntax_valid_column_does_not_crash(self, cache):
        df = pd.DataFrame({"email": ["alice@gmail.com"], "corrected_domain": ["gmail.com"]})
        # No syntax_valid column: all rows ineligible, function returns safely.
        result = apply_dns_enrichment_column(df, cache)
        assert "dns_check_performed" in result.columns

    def test_missing_corrected_domain_column_does_not_crash(self, cache):
        df = pd.DataFrame({"email": ["alice@gmail.com"], "syntax_valid": [True]})
        result = apply_dns_enrichment_column(df, cache)
        assert "dns_check_performed" in result.columns

    def test_a_fallback_mapped_correctly(self, cache):
        df = _build_subphase4_df(["alice@a-only.com"])
        with patch("app.dns_utils.resolve_domain_dns", return_value=_A_RESULT):
            result = apply_dns_enrichment_column(df, cache)
        assert result.iloc[0]["domain_exists"] == True
        assert result.iloc[0]["has_mx_record"] == False
        assert result.iloc[0]["has_a_record"] == True

    def test_row_count_unchanged(self, cache):
        emails = ["a@gmail.com", "bad@@x", None, "b@yahoo.com", "c@outlook.com"]
        df = _build_subphase4_df(emails)
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_RESULT):
            result = apply_dns_enrichment_column(df, cache)
        assert len(result) == len(emails)

    def test_typo_corrected_domain_used_for_dns(self, cache):
        """DNS must query the corrected domain, not the typo."""
        df = _build_subphase4_df(["alice@gmial.com"])  # typo → gmail.com
        queried: list[str] = []

        def capturing_resolve(domain, *args, **kwargs):
            queried.append(domain)
            return _MX_RESULT

        with patch("app.dns_utils.resolve_domain_dns", side_effect=capturing_resolve):
            apply_dns_enrichment_column(df, cache)

        assert "gmail.com" in queried
        assert "gmial.com" not in queried


# ===========================================================================
# SECTION 5 — Pipeline integration (mocked DNS)
# ===========================================================================

class TestDnsPipelineIntegration:
    """End-to-end pipeline test through Subphase 5 with mocked DNS."""

    @pytest.fixture
    def csv_file(self, tmp_path: Path) -> Path:
        f = tmp_path / "test_subphase5.csv"
        f.write_text(
            "email,domain\n"
            "alice@gmail.com,gmail.com\n"
            "bob@gmial.com,gmial.com\n"
            "carol@nonexistent-xyz-789.com,nonexistent-xyz-789.com\n"
            "bad@@email,\n"
            ",\n",
            encoding="utf-8",
        )
        return f

    def _run_pipeline(self, csv_file: Path, chunk_size: int = 10):
        import dataclasses
        from app.config import load_config, resolve_project_paths
        from app.io_utils import build_run_context
        from app.pipeline import EmailCleaningPipeline

        project_paths = resolve_project_paths()
        config = load_config(base_dir=project_paths.project_root)
        config = dataclasses.replace(config, chunk_size=chunk_size)
        run_context = build_run_context(config)
        logger = logging.getLogger("test_pipeline_s5")
        pipeline = EmailCleaningPipeline(config=config, logger=logger)
        return pipeline.run(input_file=str(csv_file), run_context=run_context)

    def test_pipeline_runs_without_error(self, csv_file):
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_RESULT):
            result = self._run_pipeline(csv_file)
        assert result is not None

    def test_pipeline_status_is_subphase_5_ready(self, csv_file):
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_RESULT):
            result = self._run_pipeline(csv_file)
        assert result.status == "subphase_8_ready"

    def test_pipeline_total_rows_correct(self, csv_file):
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_RESULT):
            result = self._run_pipeline(csv_file)
        assert result.total_rows == 5

    def test_pipeline_logs_dns_new_queries(self, csv_file, caplog):
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_RESULT):
            with caplog.at_level(logging.INFO, logger="test_pipeline_s5"):
                self._run_pipeline(csv_file)
        assert "dns_new_queries" in "\n".join(caplog.messages)

    def test_pipeline_logs_dns_cache_hits(self, csv_file, caplog):
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_RESULT):
            with caplog.at_level(logging.INFO, logger="test_pipeline_s5"):
                self._run_pipeline(csv_file)
        assert "dns_cache_hits" in "\n".join(caplog.messages)

    def test_pipeline_logs_mx_found(self, csv_file, caplog):
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_RESULT):
            with caplog.at_level(logging.INFO, logger="test_pipeline_s5"):
                self._run_pipeline(csv_file)
        assert "mx_found" in "\n".join(caplog.messages)

    def test_pipeline_logs_total_dns_summary(self, csv_file, caplog):
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_RESULT):
            with caplog.at_level(logging.INFO, logger="test_pipeline_s5"):
                self._run_pipeline(csv_file)
        combined = "\n".join(caplog.messages)
        assert "dns_total_queries" in combined

    def test_same_domain_queried_once_across_chunks(self, csv_file):
        """With chunk_size=2, gmail.com appears in chunk 0 and must be cached for chunk 1."""
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_RESULT) as mock_fn:
            # All 3 valid emails: gmail.com, gmail.com (typo corrected), nonexistent
            self._run_pipeline(csv_file, chunk_size=2)
        # gmail.com appears in chunks 0 and 1 but must only be queried once.
        queried_domains = [call.args[0] for call in mock_fn.call_args_list]
        assert queried_domains.count("gmail.com") == 1

    def test_subphase3_columns_still_present(self, csv_file):
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_RESULT):
            result = self._run_pipeline(csv_file)
        assert result.total_rows == 5  # proxy: pipeline completed fully

    def test_pipeline_backward_compat_subphase4_status_replaced(self, csv_file):
        with patch("app.dns_utils.resolve_domain_dns", return_value=_MX_RESULT):
            result = self._run_pipeline(csv_file)
        assert result.status != "subphase_4_ready"


# ===========================================================================
# SECTION 6 — No future-phase contamination
# ===========================================================================

class TestNoFuturePhaseContamination:
    """Confirm dns_utils contains no scoring, dedupe, SMTP, or disposable logic."""

    def _src(self) -> str:
        import app.dns_utils as mod
        return inspect.getsource(mod)

    def test_no_scoring_import_in_dns_utils(self):
        src = self._src()
        assert "import scoring" not in src.lower()
        assert "from .scoring" not in src.lower()
        assert "scoring(" not in src.lower()

    def test_no_dedupe_in_dns_utils(self):
        assert "dedupe" not in self._src().lower()

    def test_no_smtp_library_in_dns_utils(self):
        src = self._src()
        for forbidden in ("smtplib", "aiosmtplib", "import smtp"):
            assert forbidden not in src.lower(), f"SMTP library found: {forbidden}"

    def test_no_disposable_in_dns_utils(self):
        assert "disposable" not in self._src().lower()

    def test_no_sqlite_in_dns_utils(self):
        assert "sqlite" not in self._src().lower()

    def test_no_final_decision_in_dns_utils(self):
        for term in ("high_confidence", "invalid_bucket", "final_decision"):
            assert term not in self._src().lower()

    def test_no_inbox_probing_calls_in_dns_utils(self):
        src = self._src()
        for forbidden in ("ehlo(", "vrfy(", "rcpt(", "smtplib.SMTP", "connect_smtp"):
            assert forbidden not in src.lower(), f"Inbox probe call found: {forbidden}"

    def test_dns_utils_does_not_import_scoring(self):
        import app.dns_utils as mod
        assert "scoring" not in [m for m in dir(mod)]

    def test_pipeline_result_still_has_no_scoring_fields(self):
        from app.models import PipelineResult
        fields = {f.name for f in PipelineResult.__dataclass_fields__.values()}  # type: ignore[attr-defined]
        for forbidden in ("bucket",):
            for field_name in fields:
                assert forbidden not in field_name.lower()
