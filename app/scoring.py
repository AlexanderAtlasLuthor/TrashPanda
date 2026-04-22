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
PENALTY_TYPO_CORRECTED: int = 0     # typo DETECTION no longer contributes score (kept at 0 for backward-compat exports).
PENALTY_DNS_TRANSIENT: int = -15    # timeout / no_nameservers / generic error (uncertain, not definitive)
PENALTY_DNS_NO_RECORDS: int = -10   # no_mx / no_mx_no_a (domain exists but no useful records)

# Transient DNS errors: uncertain, domain may still exist.
DNS_TRANSIENT_ERRORS: frozenset[str] = frozenset({"timeout", "no_nameservers", "error"})
# No-record DNS errors: domain present in DNS but no email-useful records found.
DNS_NO_RECORDS_ERRORS: frozenset[str] = frozenset({"no_mx", "no_mx_no_a"})

# ---------------------------------------------------------------------------
# Business rule sets for role account and placeholder detection
# ---------------------------------------------------------------------------

# Local parts that indicate a role/functional account (→ REVIEW, not invalid).
ROLE_ACCOUNT_LOCAL_PARTS: frozenset[str] = frozenset({
    "info", "admin", "support", "sales", "contact",
    "noreply", "no-reply", "help", "billing",
    "postmaster", "webmaster", "hostmaster", "abuse",
    "marketing", "newsletter", "hr", "jobs", "careers",
    "legal", "privacy", "security", "ops", "team",
    "hello", "hi", "service", "services", "office",
})

# Local parts that suggest a fake/placeholder email (→ INVALID hard fail).
PLACEHOLDER_LOCAL_PARTS: frozenset[str] = frozenset({
    "test", "asdf", "abc", "qwerty", "demo", "sample",
    "fake", "noemail", "none", "null", "a",
    "test1", "test2", "testing", "temp", "temporary",
    "placeholder", "dummy", "invalid", "notreal",
    "user", "example", "noreply",
})

# Domains that indicate a placeholder/test email (→ INVALID hard fail).
PLACEHOLDER_DOMAINS: frozenset[str] = frozenset({
    "test.com", "example.com", "email.com", "none.com",
    "invalid.com", "example.org", "example.net",
    "test.org", "test.net", "placeholder.com",
    "domain.tld", "email.test",
})

# ---------------------------------------------------------------------------
# Client-facing (human-readable) reason mapping
# ---------------------------------------------------------------------------

CLIENT_REASON_MAP: dict[str, str] = {
    "syntax_invalid": "Invalid email format",
    "no_domain": "Domain does not exist",
    "nxdomain": "Domain does not exist",
    "disposable": "Temporary/disposable email",
    "placeholder": "Fake or placeholder email",
    "role_account": "Role-based email",
    "dns_no_records": "No mail server (MX) found",
    "dns_error": "Mail server check inconclusive",
    "domain_mismatch": "Domain does not match input",
    "typo_corrected": "Possible domain typo",
    "typo_suggested": "Possible domain typo",
}

# Priority order used to select the primary client-facing reason.
_CLIENT_REASON_PRIORITY: tuple[str, ...] = (
    "placeholder",
    "disposable",
    "syntax_invalid",
    "nxdomain",
    "no_domain",
    "role_account",
    "dns_no_records",
    "dns_error",
    "domain_mismatch",
    "typo_suggested",
    "typo_corrected",
)


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
    client_reason: str = ""


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
    local_part: str | None = None,
    disposable_domains: frozenset[str] | None = None,
    invalid_if_disposable: bool = True,
    high_confidence_threshold: int = 70,
    review_threshold: int = 40,
) -> ScoringResult:
    """Compute hard_fail, score, score_reasons, preliminary_bucket, and client_reason for one row.

    Hard fail conditions (mutually exclusive to scoring — forces score=0, bucket=invalid):
      1. syntax_valid is not True                         → reason: "syntax_invalid"
      2. corrected_domain is None/empty                   → reason: "no_domain"
      3. domain_exists==False AND dns_error=="nxdomain"   → reason: "nxdomain"
      4. local_part or corrected_domain is a placeholder  → reason: "placeholder"
      5. corrected_domain is disposable (if configured)   → reason: "disposable"

    Role accounts (info@, admin@, …) are not hard fails — they are forced to
    bucket="review" and tagged with reason token "role_account".

    NOT hard fails: DNS timeout, A-only fallback, typo correction, domain mismatch.

    Score is clamped to [0, 100].
    Buckets use the configured thresholds (default 70 / 40).
    score_reasons is a stable "|"-separated string of reason tokens in fixed order.
    """
    hard_fail_reason = _check_hard_fail(
        syntax_valid,
        corrected_domain,
        domain_exists,
        dns_error,
        local_part=local_part,
        disposable_domains=disposable_domains,
        invalid_if_disposable=invalid_if_disposable,
    )
    if hard_fail_reason:
        # Safety net for the redesigned typo-suggestion engine: when the
        # original domain looks like a plausible typo of a trusted
        # provider *and* the only thing killing the row is the original
        # domain not resolving (nxdomain / no_domain), we downgrade the
        # hard-fail to a manual REVIEW so a human can approve the
        # suggestion instead of silently discarding the contact.
        # Syntax/placeholder/disposable hard fails are never downgraded.
        if (
            typo_corrected is True
            and hard_fail_reason in ("nxdomain", "no_domain")
        ):
            return ScoringResult(
                hard_fail=False,
                score=0,
                score_reasons="typo_suggested",
                preliminary_bucket="review",
                client_reason=CLIENT_REASON_MAP.get("typo_suggested", ""),
            )
        return ScoringResult(
            hard_fail=True,
            score=0,
            score_reasons=hard_fail_reason,
            preliminary_bucket="invalid",
            client_reason=CLIENT_REASON_MAP.get(hard_fail_reason, ""),
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
        # The redesigned engine treats ``typo_corrected`` as a
        # *suggestion-detected* flag, not a silent rewrite. We no longer
        # add a numeric penalty for it (PENALTY_TYPO_CORRECTED is 0); we
        # emit a ``typo_suggested`` token and force the row to REVIEW so
        # a human can confirm the correction before any data is changed.
        reasons.append("typo_suggested")

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

    # --- Typo suggestion override: always force REVIEW when detected ---
    # (even if the score would otherwise land in high_confidence or
    # invalid) so a reviewer can decide whether to accept the
    # suggestion. This is the "→ enviar a REVIEW" rule from the
    # redesign spec.
    if typo_corrected is True:
        bucket = "review"

    # --- Role account detection: always force to "review" and tag reason ---
    if local_part is not None and local_part.lower() in ROLE_ACCOUNT_LOCAL_PARTS:
        bucket = "review"
        reasons.append("role_account")

    score_reasons_str = "|".join(reasons)
    return ScoringResult(
        hard_fail=False,
        score=score,
        score_reasons=score_reasons_str,
        preliminary_bucket=bucket,
        client_reason=_compute_client_reason(score_reasons_str),
    )


def _check_hard_fail(
    syntax_valid: bool | None,
    corrected_domain: str | None,
    domain_exists: bool | None,
    dns_error: str | None,
    local_part: str | None = None,
    disposable_domains: frozenset[str] | None = None,
    invalid_if_disposable: bool = True,
) -> str | None:
    """Return the hard fail reason token if any hard fail condition applies, else None."""
    if syntax_valid is not True:
        return "syntax_invalid"
    if not corrected_domain:
        return "no_domain"
    if domain_exists is False and dns_error == "nxdomain":
        return "nxdomain"
    # Placeholder detection: fake/test local parts or known placeholder domains.
    if local_part is not None and local_part.lower() in PLACEHOLDER_LOCAL_PARTS:
        return "placeholder"
    if corrected_domain is not None and corrected_domain.lower() in PLACEHOLDER_DOMAINS:
        return "placeholder"
    # Disposable domain detection.
    if (
        invalid_if_disposable
        and disposable_domains is not None
        and corrected_domain is not None
        and corrected_domain.lower() in disposable_domains
    ):
        return "disposable"
    return None


def _compute_client_reason(score_reasons: str) -> str:
    """Return the primary human-readable reason derived from the internal reason tokens."""
    if not score_reasons:
        return ""
    token_set = set(score_reasons.split("|"))
    for token in _CLIENT_REASON_PRIORITY:
        if token in token_set:
            return CLIENT_REASON_MAP.get(token, "")
    return ""


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
    disposable_domains: frozenset[str] | None = None,
    invalid_if_disposable: bool = True,
) -> pd.DataFrame:
    """Add hard_fail, score, score_reasons, preliminary_bucket, and client_reason columns.

    Reads signals from columns produced by Subphases 3, 4, and 5.
    Also reads local_part_from_email (Subphase 4) for role/placeholder detection.
    Missing columns are treated as None (safe default, equivalent to no signal).
    Returns a copy; the original frame is never mutated.
    """
    result = frame.copy()

    hard_fails: list[bool] = []
    scores: list[int] = []
    reasons_list: list[str] = []
    buckets: list[str] = []
    client_reasons: list[str] = []

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
            local_part=_get_str("local_part_from_email"),
            disposable_domains=disposable_domains,
            invalid_if_disposable=invalid_if_disposable,
            high_confidence_threshold=high_confidence_threshold,
            review_threshold=review_threshold,
        )
        hard_fails.append(sr.hard_fail)
        scores.append(sr.score)
        reasons_list.append(sr.score_reasons)
        buckets.append(sr.preliminary_bucket)
        client_reasons.append(sr.client_reason)

    result["hard_fail"] = pd.array(hard_fails, dtype="boolean")
    result["score"] = scores
    result["score_reasons"] = reasons_list
    result["preliminary_bucket"] = buckets
    result["client_reason"] = client_reasons

    return result
