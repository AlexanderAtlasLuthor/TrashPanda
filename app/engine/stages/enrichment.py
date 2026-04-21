"""Enrichment stages: Subphases 5, 6, and part of 7 of the email cleaner.

These stages wrap the existing functions in ``app.dns_utils``,
``app.scoring``, and ``app.dedupe``. No business rule, DNS logic, scoring
weight, threshold, or completeness definition is reimplemented here —
each stage only adapts the inline call into the ``Stage`` interface.

Execution order assumed by downstream stages:

    DNSEnrichmentStage   (needs: syntax_valid, corrected_domain)
      ↳ adds dns_check_performed / domain_exists / has_mx_record /
              has_a_record / dns_error
    ScoringStage         (needs: all DNS + email-processing outputs)
      ↳ adds hard_fail / score / score_reasons / preliminary_bucket
    CompletenessStage    (needs: the business columns counted by
                          app.dedupe.BUSINESS_COLUMNS)
      ↳ adds completeness_score

Stage dependencies on ``PipelineContext``:

    DNSEnrichmentStage  → context.dns_cache, context.config
    ScoringStage        → context.config
    CompletenessStage   → (none)
"""

from __future__ import annotations

from ...dedupe import apply_completeness_column
from ...dns_utils import apply_dns_enrichment_column
from ...scoring import apply_scoring_column
from ..context import PipelineContext
from ..payload import ChunkPayload
from ..stage import Stage


class DNSEnrichmentStage(Stage):
    """Resolve MX/A records per corrected domain using the shared cache.

    Delegates entirely to ``apply_dns_enrichment_column``. The shared
    ``DnsCache`` lives on ``context.dns_cache`` and is mutated in place
    by the underlying function (adding newly resolved domains and
    incrementing ``cache_hits`` on reuse). DNS timeout, A-record
    fallback, and worker-count settings are read from ``context.config``.
    """

    name = "dns_enrichment"
    requires = ("syntax_valid", "corrected_domain")
    produces = (
        "dns_check_performed",
        "domain_exists",
        "has_mx_record",
        "has_a_record",
        "dns_error",
    )

    def run(self, payload: ChunkPayload, context: PipelineContext) -> ChunkPayload:
        cfg = context.config
        return payload.with_frame(
            apply_dns_enrichment_column(
                payload.frame,
                cache=context.dns_cache,
                timeout_seconds=cfg.dns_timeout_seconds,
                fallback_to_a_record=cfg.fallback_to_a_record,
                max_workers=cfg.max_workers,
            )
        )


class ScoringStage(Stage):
    """Compute hard-fail, score, reasons, and preliminary bucket per row.

    Thresholds are read from ``context.config`` (same values the inline
    pipeline passed directly). Weights, penalty tables, and bucket rules
    are entirely defined in ``app.scoring`` and are not touched here.
    """

    name = "scoring"
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
    produces = ("hard_fail", "score", "score_reasons", "preliminary_bucket")

    def run(self, payload: ChunkPayload, context: PipelineContext) -> ChunkPayload:
        cfg = context.config
        return payload.with_frame(
            apply_scoring_column(
                payload.frame,
                high_confidence_threshold=cfg.high_confidence_threshold,
                review_threshold=cfg.review_threshold,
            )
        )


class CompletenessStage(Stage):
    """Count non-null business columns per row (``completeness_score``).

    Delegates to ``apply_completeness_column`` in ``app.dedupe``. The
    set of columns that count toward completeness (``BUSINESS_COLUMNS``)
    is defined there and not duplicated.
    """

    name = "completeness"
    produces = ("completeness_score",)

    def run(self, payload: ChunkPayload, context: PipelineContext) -> ChunkPayload:
        return payload.with_frame(apply_completeness_column(payload.frame))
