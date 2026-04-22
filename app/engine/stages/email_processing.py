"""Email-processing stages: Subphases 3 and 4 of the email cleaner.

These stages are thin wrappers around existing functions in
``app.validators`` and ``app.normalizers``. They do not reimplement any
business rule — they only adapt the inline pipeline code into the
``Stage`` interface so the engine can drive them.

Execution order assumed by downstream stages:

    EmailSyntaxValidationStage  (needs: email)
      ↳ adds syntax_valid / syntax_reason / has_single_at / …
    DomainExtractionStage       (needs: email, syntax_valid)
      ↳ adds local_part_from_email / domain_from_email
    TypoCorrectionStage         (needs: domain_from_email)
      ↳ adds typo_detected / original_domain / suggested_domain /
             suggested_email / typo_type / typo_confidence
             plus legacy mirrors typo_corrected / typo_original_domain /
             corrected_domain (always equal to the original domain in
             suggest-only mode — we never auto-rewrite the user's input).
    DomainComparisonStage       (needs: corrected_domain, domain)
      ↳ adds domain_matches_input_column

A second, post-DNS safety pass lives in ``enrichment`` as
``TypoSuggestionValidationStage``: once DNS has resolved the original
domain we suppress any suggestion for domains that already have a valid
MX record, so live domains are never "corrected" on top of real data.

The only dependency any of these stages has on ``PipelineContext`` is
``TypoCorrectionStage``, which reads ``context.typo_map`` and, when
available, ``context.config.typo_correction``.
"""

from __future__ import annotations

from ...normalizers import (
    apply_domain_typo_suggestion_column,
    compare_domain_with_input_column,
    extract_email_components,
)
from ...typo_suggestions import DEFAULT_PROVIDER_WHITELIST, TypoDetectorConfig
from ...validators import validate_email_syntax_column
from ..context import PipelineContext
from ..payload import ChunkPayload
from ..stage import Stage


def _detector_config_from_context(context: PipelineContext) -> TypoDetectorConfig:
    """Build a :class:`TypoDetectorConfig` from the run-wide context.

    Falls back to safe defaults when the context was constructed without
    an :class:`AppConfig` (e.g. in unit tests that exercise stages in
    isolation). Those defaults match the shipped ``configs/default.yaml``.
    """

    cfg = getattr(context, "config", None)
    typo_cfg = getattr(cfg, "typo_correction", None) if cfg is not None else None
    if typo_cfg is None:
        return TypoDetectorConfig(
            mode="suggest_only",
            max_edit_distance=2,
            whitelist=DEFAULT_PROVIDER_WHITELIST,
            require_original_no_mx=True,
        )

    whitelist = getattr(typo_cfg, "whitelist", None) or DEFAULT_PROVIDER_WHITELIST
    return TypoDetectorConfig(
        mode=str(getattr(typo_cfg, "mode", "suggest_only")),
        max_edit_distance=int(getattr(typo_cfg, "max_edit_distance", 2)),
        whitelist=frozenset(whitelist),
        require_original_no_mx=bool(getattr(typo_cfg, "require_original_no_mx", True)),
    )


class EmailSyntaxValidationStage(Stage):
    """Validate email syntax per row and add syntax_* columns."""

    name = "email_syntax_validation"
    requires = ("email",)
    produces = (
        "syntax_valid",
        "syntax_reason",
        "has_single_at",
        "local_part_present",
        "domain_part_present",
        "domain_has_dot",
        "contains_spaces",
    )

    def run(self, payload: ChunkPayload, context: PipelineContext) -> ChunkPayload:
        return payload.with_frame(validate_email_syntax_column(payload.frame))


class DomainExtractionStage(Stage):
    """Extract local_part_from_email and domain_from_email for valid rows."""

    name = "domain_extraction"
    requires = ("email", "syntax_valid")
    produces = ("local_part_from_email", "domain_from_email")

    def run(self, payload: ChunkPayload, context: PipelineContext) -> ChunkPayload:
        return payload.with_frame(extract_email_components(payload.frame))


class TypoCorrectionStage(Stage):
    """Detect *possible* domain typos non-destructively.

    Reads the legacy typo map from ``context.typo_map`` (populated once
    per run in ``pipeline.run``) as a curated candidate source, and the
    detector configuration (mode, whitelist, max edit distance) from
    ``context.config.typo_correction`` when available. Produces the full
    set of suggestion columns plus legacy mirrors so every downstream
    consumer keeps working. **The original email and its domain are
    never modified here** — see ``TypoSuggestionValidationStage`` for the
    post-DNS safety check.
    """

    name = "typo_correction"
    requires = ("domain_from_email",)
    produces = (
        "typo_detected",
        "original_domain",
        "suggested_domain",
        "suggested_email",
        "typo_type",
        "typo_confidence",
        "typo_corrected",
        "typo_original_domain",
        "corrected_domain",
    )

    def run(self, payload: ChunkPayload, context: PipelineContext) -> ChunkPayload:
        detector_config = _detector_config_from_context(context)
        return payload.with_frame(
            apply_domain_typo_suggestion_column(
                payload.frame,
                detector_config=detector_config,
                typo_map=context.typo_map or {},
            )
        )


class DomainComparisonStage(Stage):
    """Compare corrected_domain against the input ``domain`` column."""

    name = "domain_comparison"
    requires = ("corrected_domain",)
    produces = ("domain_matches_input_column",)

    def run(self, payload: ChunkPayload, context: PipelineContext) -> ChunkPayload:
        return payload.with_frame(compare_domain_with_input_column(payload.frame))
