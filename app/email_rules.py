"""Email syntax validation rules for Subphase 3.

This module defines explicit, conservative email syntax rules without semantic or
DNS validation. Rules follow RFC-adjacent principles but are intentionally not
exhaustive for corner cases.
"""

from __future__ import annotations

import re
from dataclasses import dataclass


# Basic character class patterns for email parts
VALID_LOCAL_CHARS_REGEX = re.compile(r"^[a-zA-Z0-9._+-]+$")
VALID_DOMAIN_CHARS_REGEX = re.compile(r"^[a-zA-Z0-9.-]+$")


@dataclass(slots=True)
class EmailSyntaxCheckResult:
    """Detailed result of email syntax validation."""

    syntax_valid: bool
    syntax_reason: str
    has_single_at: bool
    local_part_present: bool
    domain_part_present: bool
    domain_has_dot: bool
    contains_spaces: bool
    local_part: str | None = None
    domain_part: str | None = None


def check_email_syntax(email: str | None) -> EmailSyntaxCheckResult:
    """
    Validate email syntax using explicit rules.

    Returns a detailed result object with flags and the primary reason for validity/invalidity.
    """

    # Null/empty handling (including NaN from pandas)
    if email is None or (isinstance(email, str) and email.strip() == ""):
        return EmailSyntaxCheckResult(
            syntax_valid=False,
            syntax_reason="email_is_empty",
            has_single_at=False,
            local_part_present=False,
            domain_part_present=False,
            domain_has_dot=False,
            contains_spaces=False,
        )

    # Handle pandas NaN (float)
    if not isinstance(email, str):
        return EmailSyntaxCheckResult(
            syntax_valid=False,
            syntax_reason="email_is_empty",
            has_single_at=False,
            local_part_present=False,
            domain_part_present=False,
            domain_has_dot=False,
            contains_spaces=False,
        )

    email = email.strip()

    # Check for spaces (early exit)
    if " " in email:
        return EmailSyntaxCheckResult(
            syntax_valid=False,
            syntax_reason="contains_spaces",
            has_single_at=False,
            local_part_present=False,
            domain_part_present=False,
            domain_has_dot=False,
            contains_spaces=True,
        )

    # Check @ count
    at_count = email.count("@")
    if at_count != 1:
        return EmailSyntaxCheckResult(
            syntax_valid=False,
            syntax_reason="not_exactly_one_at" if at_count > 1 else "no_at_sign",
            has_single_at=False,
            local_part_present=False,
            domain_part_present=False,
            domain_has_dot=False,
            contains_spaces=False,
        )

    # Split into local and domain parts
    local_part, domain_part = email.split("@")

    # Check if parts are non-empty
    if not local_part:
        return EmailSyntaxCheckResult(
            syntax_valid=False,
            syntax_reason="local_part_empty",
            has_single_at=True,
            local_part_present=False,
            domain_part_present=bool(domain_part),
            domain_has_dot=False,
            contains_spaces=False,
            local_part=local_part if local_part else None,
            domain_part=domain_part if domain_part else None,
        )

    if not domain_part:
        return EmailSyntaxCheckResult(
            syntax_valid=False,
            syntax_reason="domain_part_empty",
            has_single_at=True,
            local_part_present=True,
            domain_part_present=False,
            domain_has_dot=False,
            contains_spaces=False,
            local_part=local_part,
            domain_part=domain_part if domain_part else None,
        )

    # Check local part rules
    local_reason = _check_local_part(local_part)
    if local_reason:
        domain_has_dot = "." in domain_part
        return EmailSyntaxCheckResult(
            syntax_valid=False,
            syntax_reason=local_reason,
            has_single_at=True,
            local_part_present=True,
            domain_part_present=True,
            domain_has_dot=domain_has_dot,
            contains_spaces=False,
            local_part=local_part,
            domain_part=domain_part,
        )

    # Check domain part rules
    domain_reason = _check_domain_part(domain_part)
    if domain_reason:
        return EmailSyntaxCheckResult(
            syntax_valid=False,
            syntax_reason=domain_reason,
            has_single_at=True,
            local_part_present=True,
            domain_part_present=True,
            domain_has_dot="." in domain_part,
            contains_spaces=False,
            local_part=local_part,
            domain_part=domain_part,
        )

    # All checks passed
    return EmailSyntaxCheckResult(
        syntax_valid=True,
        syntax_reason="valid",
        has_single_at=True,
        local_part_present=True,
        domain_part_present=True,
        domain_has_dot="." in domain_part,
        contains_spaces=False,
        local_part=local_part,
        domain_part=domain_part,
    )


def _check_local_part(local_part: str) -> str | None:
    """Check local part rules. Return reason if invalid, None if valid."""

    # Check for invalid characters
    if not VALID_LOCAL_CHARS_REGEX.match(local_part):
        return "local_part_has_invalid_chars"

    # Check for leading/trailing dots
    if local_part.startswith("."):
        return "local_part_starts_with_dot"
    if local_part.endswith("."):
        return "local_part_ends_with_dot"

    # Check for consecutive dots
    if ".." in local_part:
        return "local_part_has_consecutive_dots"

    return None


def _check_domain_part(domain_part: str) -> str | None:
    """Check domain part rules. Return reason if invalid, None if valid."""

    # Must have at least one dot
    if "." not in domain_part:
        return "domain_missing_dot"

    # Check for invalid characters
    if not VALID_DOMAIN_CHARS_REGEX.match(domain_part):
        return "domain_has_invalid_chars"

    # Check for consecutive dots
    if ".." in domain_part:
        return "domain_has_consecutive_dots"

    # Check each label (parts between dots)
    labels = domain_part.split(".")
    for label in labels:
        if not label:
            # Empty label between dots
            return "domain_has_empty_label"
        if label.startswith("-"):
            return "domain_label_starts_with_hyphen"
        if label.endswith("-"):
            return "domain_label_ends_with_hyphen"

    return None
