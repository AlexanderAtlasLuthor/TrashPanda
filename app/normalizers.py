"""Header, value, and domain normalization for Subphase 2 and 4."""

from __future__ import annotations

import logging
import re
import unicodedata
from typing import Any

import pandas as pd

from .models import ChunkContext, InputFile
from .rules import COLUMN_ALIASES
from .typo_rules import DomainTypoCorrectionResult, apply_domain_typo_correction
from .typo_suggestions import (
    TypoDetectorConfig,
    TypoSuggestion,
    detect_typo_suggestion,
)


_WHITESPACE_REGEX = re.compile(r"\s+")
_HEADER_LOGGER = logging.getLogger("app.normalizers.headers")


def _strip_accents(text: str) -> str:
    """Remove combining diacritical marks (á→a, ñ→n, ü→u, etc.)."""
    decomposed = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in decomposed if not unicodedata.combining(ch))


def normalize_header_name(name: Any) -> str:
    """Normalize a raw header into a canonical, comparable name.

    Steps: strip → lowercase → strip accents → dashes/whitespace to
    underscores → alias lookup. Accent stripping makes Spanish headers
    like ``correo electrónico``, ``teléfono``, ``compañía`` resolve to
    the same alias keys as their ASCII equivalents.
    """
    normalized = str(name).strip().lower()
    normalized = _strip_accents(normalized)
    normalized = normalized.replace("-", "_")
    normalized = _WHITESPACE_REGEX.sub("_", normalized)
    return COLUMN_ALIASES.get(normalized, normalized)


def normalize_headers(frame: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of the frame with normalized header names.

    Emits an INFO log line for each header whose canonical form differs
    from the raw input, so operators can see how aliases were applied.
    """
    renamed_columns: dict[str, str] = {}
    for column in frame.columns:
        raw = str(column)
        normalized = normalize_header_name(raw)
        renamed_columns[raw] = normalized
        if raw != normalized:
            _HEADER_LOGGER.info("Mapped column %r -> %r", raw, normalized)
    return frame.rename(columns=renamed_columns).copy()


def normalize_value(value: Any, lowercase: bool = False) -> Any:
    """Apply conservative string normalization to one scalar value."""

    if value is None or pd.isna(value):
        return None
    if not isinstance(value, str):
        value = str(value)
    cleaned = value.strip()
    if cleaned == "":
        return None
    if lowercase:
        return cleaned.lower()
    return cleaned


def normalize_values(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize string values conservatively without semantic transformations."""

    normalized = frame.copy()
    for column in normalized.columns:
        lowercase = column in {"email", "domain"}
        normalized[column] = normalized[column].map(lambda value: normalize_value(value, lowercase=lowercase))
    normalized = normalized.astype(object)
    normalized = normalized.replace({"": None})
    normalized = normalized.where(pd.notnull(normalized), None)
    return normalized


def add_technical_metadata(frame: pd.DataFrame, input_file: InputFile, chunk_context: ChunkContext) -> pd.DataFrame:
    """Attach source-level technical metadata to each row in the chunk."""

    enriched = frame.copy()
    row_numbers = [chunk_context.start_row_number + offset for offset in range(chunk_context.row_count)]
    enriched["source_file"] = input_file.original_name
    enriched["source_row_number"] = row_numbers
    enriched["source_file_type"] = input_file.file_type
    enriched["chunk_index"] = chunk_context.chunk_index
    return enriched


# ---------------------------------------------------------------------------
# Subphase 4: Domain extraction, typo correction, domain column comparison
# ---------------------------------------------------------------------------

def extract_email_components(frame: pd.DataFrame) -> pd.DataFrame:
    """Extract local_part_from_email and domain_from_email for syntactically valid rows.

    Runs only when syntax_valid == True. Invalid or null emails produce None
    in both derived columns. The email column itself is never modified.
    """

    result = frame.copy()
    result["local_part_from_email"] = None
    result["domain_from_email"] = None

    for idx in result.index:
        if result.loc[idx, "syntax_valid"] is not True and result.loc[idx, "syntax_valid"] != True:
            continue
        email = result.loc[idx, "email"]
        if not isinstance(email, str) or "@" not in email:
            continue
        local, domain = email.rsplit("@", 1)
        result.loc[idx, "local_part_from_email"] = local
        result.loc[idx, "domain_from_email"] = domain

    return result


def apply_domain_typo_correction_column(
    frame: pd.DataFrame,
    typo_map: dict[str, str],
) -> pd.DataFrame:
    """Apply the closed typo map to the domain_from_email column.

    .. deprecated::
        Kept for backward compatibility with callers and tests that rely on
        the pre-redesign *destructive* semantics (``corrected_domain`` being
        overwritten with the mapped target). New code should call
        :func:`apply_domain_typo_suggestion_column`, which is
        non-destructive: it never modifies ``corrected_domain`` and
        instead populates the new ``typo_detected`` / ``suggested_*``
        columns.

    Adds three columns:
    - typo_corrected: True if a correction was applied, False if not, pd.NA if no domain.
    - typo_original_domain: the domain before correction (or None if no domain).
    - corrected_domain: the final domain after correction (equals original if no correction).

    The local_part_from_email is never touched.
    """

    result = frame.copy()
    result["typo_corrected"] = pd.NA
    result["typo_original_domain"] = None
    result["corrected_domain"] = None

    for idx in result.index:
        domain = result.loc[idx, "domain_from_email"]

        if domain is None or (isinstance(domain, float) and pd.isna(domain)):
            # No domain available (email was syntactically invalid)
            continue

        correction: DomainTypoCorrectionResult = apply_domain_typo_correction(domain, typo_map)
        result.loc[idx, "typo_corrected"] = correction.typo_corrected
        result.loc[idx, "typo_original_domain"] = correction.typo_original_domain
        result.loc[idx, "corrected_domain"] = correction.corrected_domain

    result["typo_corrected"] = result["typo_corrected"].astype("boolean")
    return result


# ---------------------------------------------------------------------------
# Non-destructive typo *suggestion* column applier (new safe default).
# ---------------------------------------------------------------------------


# Columns added by ``apply_domain_typo_suggestion_column``. Exported so
# tests and downstream reporting can reference the exact output schema.
TYPO_SUGGESTION_COLUMNS: tuple[str, ...] = (
    "typo_detected",
    "original_domain",
    "suggested_domain",
    "suggested_email",
    "typo_type",
    "typo_confidence",
    # Backward-compatible mirrors (populated so pre-redesign consumers keep
    # working; they carry the *suggestion* semantics now, not a rewrite).
    "typo_corrected",
    "typo_original_domain",
    "corrected_domain",
)


def apply_domain_typo_suggestion_column(
    frame: pd.DataFrame,
    detector_config: TypoDetectorConfig,
    typo_map: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Populate non-destructive typo-suggestion columns on ``frame``.

    This function is the safe, redesigned replacement for
    :func:`apply_domain_typo_correction_column`. It never modifies the
    original email or its domain. For every row it writes:

    * ``typo_detected`` — bool, ``pd.NA`` when no domain was extracted.
    * ``original_domain`` — the domain as extracted by
      :func:`extract_email_components` (mirrors ``domain_from_email``).
    * ``suggested_domain`` — the safe correction candidate, or ``None``.
    * ``suggested_email`` — ``local_part @ suggested_domain`` or ``None``.
    * ``typo_type`` — classification token
      (``common_provider_typo`` / ``tld_typo`` / ``keyboard_typo`` /
      ``unknown``).
    * ``typo_confidence`` — numeric confidence in ``[0, 1]``.

    The legacy columns ``typo_corrected`` / ``typo_original_domain`` /
    ``corrected_domain`` are also populated so downstream validation
    and CSV consumers continue to work, but with *safe* semantics:

    * ``typo_corrected`` mirrors ``typo_detected`` (i.e. "a suggestion
      exists"), it does **not** mean the domain was rewritten.
    * ``corrected_domain`` is always equal to the *original* domain when
      the detector runs in ``suggest_only`` mode, so every downstream
      stage validates the real user input, not a guess.
    """

    result = frame.copy()

    # Initialise columns so downstream stages can rely on their presence
    # even when every row is a no-op.
    result["typo_detected"] = pd.NA
    result["original_domain"] = None
    result["suggested_domain"] = None
    result["suggested_email"] = None
    result["typo_type"] = None
    result["typo_confidence"] = pd.NA

    result["typo_corrected"] = pd.NA
    result["typo_original_domain"] = None
    result["corrected_domain"] = None

    has_local = "local_part_from_email" in result.columns

    for idx in result.index:
        domain = result.loc[idx, "domain_from_email"]

        if domain is None or (isinstance(domain, float) and pd.isna(domain)):
            # No domain available (email was syntactically invalid).
            continue

        local_part = None
        if has_local:
            raw_local = result.loc[idx, "local_part_from_email"]
            if isinstance(raw_local, str) and raw_local:
                local_part = raw_local

        suggestion: TypoSuggestion = detect_typo_suggestion(
            local_part=local_part,
            domain=domain,
            config=detector_config,
            typo_map=typo_map or {},
        )

        # Non-destructive: ``corrected_domain`` always mirrors the original
        # domain in suggest-only mode. Even in ``auto_apply_safe`` we keep
        # this invariant until a dedicated post-DNS applier flips it
        # safely; that stage is intentionally out of scope of this module.
        result.loc[idx, "original_domain"] = domain
        result.loc[idx, "typo_original_domain"] = domain
        result.loc[idx, "corrected_domain"] = domain

        result.loc[idx, "typo_detected"] = bool(suggestion.detected)
        result.loc[idx, "typo_corrected"] = bool(suggestion.detected)

        if suggestion.detected:
            result.loc[idx, "suggested_domain"] = suggestion.suggested_domain
            result.loc[idx, "suggested_email"] = suggestion.suggested_email
            result.loc[idx, "typo_type"] = suggestion.typo_type
            if suggestion.confidence is not None:
                result.loc[idx, "typo_confidence"] = float(suggestion.confidence)

    result["typo_detected"] = result["typo_detected"].astype("boolean")
    result["typo_corrected"] = result["typo_corrected"].astype("boolean")
    # ``typo_confidence`` is left as object/Float so pd.NA is preserved
    # without forcing NaN float semantics on consumers that check for None.
    return result


# The post-DNS safety pass (previously defined here) now lives in
# ``app.typo_suggestions`` so this module remains free of any
# deliverability-record concerns (enforced by contamination tests).


def compare_domain_with_input_column(frame: pd.DataFrame) -> pd.DataFrame:
    """Compare corrected_domain against the input domain column, row by row.

    Adds domain_matches_input_column:
    - True  if corrected_domain == normalized domain column value
    - False if they differ
    - pd.NA if corrected_domain is None, or domain column absent, or domain value is null

    Comparison is against the already-normalized domain column (lowercased by normalize_values).
    """

    result = frame.copy()
    result["domain_matches_input_column"] = pd.NA

    has_domain_column = "domain" in result.columns

    for idx in result.index:
        corrected = result.loc[idx, "corrected_domain"]
        if corrected is None or (isinstance(corrected, float) and pd.isna(corrected)):
            continue

        if not has_domain_column:
            continue

        input_domain = result.loc[idx, "domain"]
        if input_domain is None or (isinstance(input_domain, float) and pd.isna(input_domain)):
            continue

        result.loc[idx, "domain_matches_input_column"] = corrected == input_domain

    result["domain_matches_input_column"] = result["domain_matches_input_column"].astype("boolean")
    return result
