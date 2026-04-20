"""Header, value, and domain normalization for Subphase 2 and 4."""

from __future__ import annotations

import re
from typing import Any

import pandas as pd

from .models import ChunkContext, InputFile
from .rules import COLUMN_ALIASES
from .typo_rules import DomainTypoCorrectionResult, apply_domain_typo_correction


_WHITESPACE_REGEX = re.compile(r"\s+")


def normalize_header_name(name: Any) -> str:
    """Normalize a raw header into a canonical, comparable name."""

    normalized = str(name).strip().lower()
    normalized = normalized.replace("-", "_")
    normalized = _WHITESPACE_REGEX.sub("_", normalized)
    return COLUMN_ALIASES.get(normalized, normalized)


def normalize_headers(frame: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of the frame with normalized header names."""

    renamed_columns = {str(column): normalize_header_name(column) for column in frame.columns}
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
