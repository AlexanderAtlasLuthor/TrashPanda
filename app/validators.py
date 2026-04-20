"""Structural and syntactic input validators for Subphase 2 and 3.

Subphase 2: Structural validation of columns.
Subphase 3: Syntactic validation of email column.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable

import pandas as pd

from .email_rules import check_email_syntax
from .rules import MINIMUM_REQUIRED_COLUMNS, TECHNICAL_METADATA_COLUMNS


def validate_required_columns(columns: Iterable[str]) -> None:
    """Ensure the minimum required canonical columns are present."""

    column_list = list(columns)
    missing = [column for column in MINIMUM_REQUIRED_COLUMNS if column not in column_list]
    if missing:
        raise ValueError(f"Missing required column(s): {', '.join(missing)}")


def validate_reserved_columns(columns: Iterable[str]) -> None:
    """Ensure input columns do not collide with reserved technical metadata names."""

    column_list = list(columns)
    reserved_present = [column for column in column_list if column in TECHNICAL_METADATA_COLUMNS]
    if reserved_present:
        raise ValueError(
            "Input contains reserved technical column(s): "
            + ", ".join(sorted(set(reserved_present)))
        )


def validate_duplicate_columns(columns: Iterable[str]) -> None:
    """Ensure normalized column names are unique."""

    counts = Counter(columns)
    duplicates = [column for column, count in counts.items() if count > 1]
    if duplicates:
        raise ValueError(f"Duplicate columns after normalization: {', '.join(sorted(duplicates))}")


def validate_email_syntax_column(frame: pd.DataFrame) -> pd.DataFrame:
    """
    Add email syntax validation columns to a chunk.

    This function does not modify the email column itself. It adds:
    - syntax_valid: boolean indicating if email has valid syntax
    - syntax_reason: primary reason for validity/invalidity
    - has_single_at: boolean indicating presence of exactly one @
    - local_part_present: boolean indicating non-empty local part
    - domain_part_present: boolean indicating non-empty domain part
    - domain_has_dot: boolean indicating at least one dot in domain
    - contains_spaces: boolean indicating presence of spaces

    Returns a copy of the frame with new validation columns.
    """

    result = frame.copy()

    # Initialize validation columns with None to preserve type consistency
    result["syntax_valid"] = pd.NA
    result["syntax_reason"] = pd.NA
    result["has_single_at"] = pd.NA
    result["local_part_present"] = pd.NA
    result["domain_part_present"] = pd.NA
    result["domain_has_dot"] = pd.NA
    result["contains_spaces"] = pd.NA

    # Apply validation to each email cell
    for idx in result.index:
        email_value = result.loc[idx, "email"]
        check_result = check_email_syntax(email_value)

        result.loc[idx, "syntax_valid"] = check_result.syntax_valid
        result.loc[idx, "syntax_reason"] = check_result.syntax_reason
        result.loc[idx, "has_single_at"] = check_result.has_single_at
        result.loc[idx, "local_part_present"] = check_result.local_part_present
        result.loc[idx, "domain_part_present"] = check_result.domain_part_present
        result.loc[idx, "domain_has_dot"] = check_result.domain_has_dot
        result.loc[idx, "contains_spaces"] = check_result.contains_spaces

    # Convert boolean columns to boolean type (pandas handles NA properly)
    result["syntax_valid"] = result["syntax_valid"].astype("boolean")
    result["has_single_at"] = result["has_single_at"].astype("boolean")
    result["local_part_present"] = result["local_part_present"].astype("boolean")
    result["domain_part_present"] = result["domain_part_present"].astype("boolean")
    result["domain_has_dot"] = result["domain_has_dot"].astype("boolean")
    result["contains_spaces"] = result["contains_spaces"].astype("boolean")

    return result

