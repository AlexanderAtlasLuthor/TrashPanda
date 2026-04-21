"""Scoring engine for Subphase 6: hard fail detection, score calculation,
and preliminary bucket assignment.

Uses only signals already computed by Subphases 3, 4, and 5:
  Subphase 3: syntax_valid
  Subphase 4: corrected_domain, typo_corrected, domain_matches_input_column
  Subphase 5: domain_exists, has_mx_record, has_a_record, dns_error

No SMTP. No deduplication. No export. No final decisions.
preliminary_bucket is provisional until Subphase 7 (dedupe).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


# ---------------------------------------------------------------------------
# Scoring weights (all values represent points added to score)
# ---------------------------------------------------------------------------

SCORE_SYNTAX_VALID: int = 25    # email passes syntax check
SCORE_MX_PRESENT: int = 50      # domain has MX records (strong email signal)
SCORE_A_FALLBACK: int = 20      # domain has A/AAAA but no MX (weaker signal)

PENALTY_DOMAIN_MISMATCH: int = -5   # corrected_domain != input domain column (explicit mismatch)
PENALTY_TYPO_CORRECTED: int = -3    # domain needed correction (mild, not structural)
PENALTY_DNS_TRANSIENT: int = -15    # timeout / no_nameservers / generic error (uncertain, not definitive)
PENALTY_DNS_NO_RECORDS: int = -10   # no_mx / no_mx_no_a (domain exists but no useful records)

# Transient DNS errors: uncertain, domain may still exist.
DNS_TRANSIENT_ERRORS: frozenset[str] = frozenset({"timeout", "no_nameservers", "error"})
# No-record DNS errors: domain present in DNS but no email-useful records found.
DNS_NO_RECORDS_ERRORS: frozenset[str] = frozenset({"no_mx", "no_mx_no_a"})


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class ScoringResult:
    """Structured output of the scoring pass for one email row."""

    hard_fail: bool
    score: int
    score_reasons: str
    preliminary_bucket: str


# ---------------------------------------------------------------------------
# Core scoring function (pure, no side effects, fully unit-testable)
# ---------------------------------------------------------------------------

def score_row(
    *,
    syntax_valid: bool | None,
    corrected_domain: str | None,
    has_mx_record: bool | None,
    has_a_record: bool | None,
    domain_exists: bool | None,
    dns_error: str | None,
    typo_corrected: bool | None,
    domain_matches_input_column: bool | None,
    high_confidence_threshold: int = 70,
    review_threshold: int = 40,
) -> ScoringResult:
    """Compute hard_fail, score, score_reasons, and preliminary_bucket for one row.

    Hard fail conditions (mutually exclusive to scoring — forces score=0, bucket=invalid):
      1. syntax_valid is not True          → reason: "syntax_invalid"
      2. corrected_domain is None/empty    → reason: "no_domain"
      3. domain_exists==False AND
         dns_error=="nxdomain"             → reason: "nxdomain"

    NOT hard fails: DNS timeout, A-only fallback, typo correction, domain mismatch.

    Score is clamped to [0, 100].
    Buckets use the configured thresholds (default 70 / 40).
    score_reasons is a stable "|"-separated string of reason tokens in fixed order.
    """
    hard_fail_reason = _check_hard_fail(syntax_valid, corrected_domain, domain_exists, dns_error)
    if hard_fail_reason:
        return ScoringResult(
            hard_fail=True,
            score=0,
            score_reasons=hard_fail_reason,
            preliminary_bucket="invalid",
        )

    score = 0
    reasons: list[str] = []

    # --- Positive signals ---
    if syntax_valid is True:
        score += SCORE_SYNTAX_VALID
        reasons.append("syntax_valid")

    if has_mx_record is True:
        score += SCORE_MX_PRESENT
        reasons.append("mx_present")
    elif has_a_record is True:
        score += SCORE_A_FALLBACK
        reasons.append("a_fallback")

    # --- Penalties (applied in fixed order for stable reasons string) ---
    if dns_error in DNS_TRANSIENT_ERRORS:
        score += PENALTY_DNS_TRANSIENT
        reasons.append("dns_error")
    elif dns_error in DNS_NO_RECORDS_ERRORS:
        score += PENALTY_DNS_NO_RECORDS
        reasons.append("dns_no_records")

    if typo_corrected is True:
        score += PENALTY_TYPO_CORRECTED
        reasons.append("typo_corrected")

    if domain_matches_input_column is False:
        score += PENALTY_DOMAIN_MISMATCH
        reasons.append("domain_mismatch")

    score = max(0, min(100, score))

    if score >= high_confidence_threshold:
        bucket = "high_confidence"
    elif score >= review_threshold:
        bucket = "review"
    else:
        bucket = "invalid"

    return ScoringResult(
        hard_fail=False,
        score=score,
        score_reasons="|".join(reasons),
        preliminary_bucket=bucket,
    )


def _check_hard_fail(
    syntax_valid: bool | None,
    corrected_domain: str | None,
    domain_exists: bool | None,
    dns_error: str | None,
) -> str | None:
    """Return the hard fail reason token if any hard fail condition applies, else None."""
    if syntax_valid is not True:
        return "syntax_invalid"
    if not corrected_domain:
        return "no_domain"
    if domain_exists is False and dns_error == "nxdomain":
        return "nxdomain"
    return None


# ---------------------------------------------------------------------------
# DataFrame-level application
# ---------------------------------------------------------------------------

def _coerce_bool(val: Any) -> bool | None:
    """Map pandas boolean-compatible values to Python bool or None.

    Must handle np.True_/np.False_ from pandas 3.x nullable boolean columns
    accessed via .loc — identity checks (is True) fail for numpy booleans.
    """
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    try:
        return bool(val)
    except (TypeError, ValueError):
        return None


def _coerce_str(val: Any) -> str | None:
    """Map pandas string-compatible values to Python str or None."""
    if isinstance(val, str) and val:
        return val
    return None


def apply_scoring_column(
    frame: pd.DataFrame,
    high_confidence_threshold: int = 70,
    review_threshold: int = 40,
) -> pd.DataFrame:
    """Add hard_fail, score, score_reasons, and preliminary_bucket columns to a chunk.

    Reads signals from columns produced by Subphases 3, 4, and 5.
    Missing columns are treated as None (safe default, equivalent to no signal).
    Returns a copy; the original frame is never mutated.
    """
    result = frame.copy()

    hard_fails: list[bool] = []
    scores: list[int] = []
    reasons_list: list[str] = []
    buckets: list[str] = []

    def _get_bool(col: str) -> bool | None:
        if col not in result.columns:
            return None
        return _coerce_bool(result.loc[idx, col])

    def _get_str(col: str) -> str | None:
        if col not in result.columns:
            return None
        return _coerce_str(result.loc[idx, col])

    for idx in result.index:
        sr = score_row(
            syntax_valid=_get_bool("syntax_valid"),
            corrected_domain=_get_str("corrected_domain"),
            has_mx_record=_get_bool("has_mx_record"),
            has_a_record=_get_bool("has_a_record"),
            domain_exists=_get_bool("domain_exists"),
            dns_error=_get_str("dns_error"),
            typo_corrected=_get_bool("typo_corrected"),
            domain_matches_input_column=_get_bool("domain_matches_input_column"),
            high_confidence_threshold=high_confidence_threshold,
            review_threshold=review_threshold,
        )
        hard_fails.append(sr.hard_fail)
        scores.append(sr.score)
        reasons_list.append(sr.score_reasons)
        buckets.append(sr.preliminary_bucket)

    result["hard_fail"] = pd.array(hard_fails, dtype="boolean")
    result["score"] = scores
    result["score_reasons"] = reasons_list
    result["preliminary_bucket"] = buckets

    return result
