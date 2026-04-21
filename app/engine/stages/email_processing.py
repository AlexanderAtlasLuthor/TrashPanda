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
      ↳ adds typo_corrected / typo_original_domain / corrected_domain
    DomainComparisonStage       (needs: corrected_domain, domain)
      ↳ adds domain_matches_input_column

The only dependency any of these stages has on ``PipelineContext`` is
``TypoCorrectionStage``, which reads ``context.typo_map``. Nothing is
stashed on the payload metadata in this batch.
"""

from __future__ import annotations

from ...normalizers import (
    apply_domain_typo_correction_column,
    compare_domain_with_input_column,
    extract_email_components,
)
from ...validators import validate_email_syntax_column
from ..context import PipelineContext
from ..payload import ChunkPayload
from ..stage import Stage


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
    """Apply the closed typo map to the extracted domain column.

    Reads the typo map from ``context.typo_map`` (populated once per run
    in ``pipeline.run``). No fallback is applied — if the context was
    configured without a typo map, the underlying function will raise,
    matching the pre-refactor inline behavior.
    """

    name = "typo_correction"
    requires = ("domain_from_email",)
    produces = ("typo_corrected", "typo_original_domain", "corrected_domain")

    def run(self, payload: ChunkPayload, context: PipelineContext) -> ChunkPayload:
        return payload.with_frame(
            apply_domain_typo_correction_column(payload.frame, context.typo_map)
        )


class DomainComparisonStage(Stage):
    """Compare corrected_domain against the input ``domain`` column."""

    name = "domain_comparison"
    requires = ("corrected_domain",)
    produces = ("domain_matches_input_column",)

    def run(self, payload: ChunkPayload, context: PipelineContext) -> ChunkPayload:
        return payload.with_frame(compare_domain_with_input_column(payload.frame))
