"""V2-authoritative output bucket mapping for the materialization step.

Single source of truth for the rule activated in Subphase V2.1:

  * V2 ``final_action`` controls clean / review / invalid placement.
  * V1 hard-fail and dedupe duplicate routing remain terminal.
  * Missing or unrecognised V2 ``final_action`` falls back to *review*,
    never *clean*.

The mapping is intentionally pure: callers feed it the row signals they
already have and receive one of the output-bucket constants below. No
side effects, no I/O, no reading of the surrounding row.
"""

from __future__ import annotations


# Output bucket vocabulary returned by :func:`map_v2_decision_to_output_bucket`.
# Kept in this module so callers do not hard-code string literals.
OUTPUT_CLEAN = "clean"
OUTPUT_REVIEW = "review"
OUTPUT_INVALID = "invalid"
OUTPUT_DUPLICATE = "duplicate"
OUTPUT_HARD_FAIL = "hard_fail"


# V2 ``final_action`` vocabulary, mirroring
# :class:`app.validation_v2.decision.policy.FinalAction`. Sets are kept
# explicit so a typo in the engine vocabulary surfaces as a routing
# inconsistency in tests, rather than a silent fall-through to review.
_V2_ACCEPT_ACTIONS: frozenset[str] = frozenset({"auto_approve"})
_V2_REVIEW_ACTIONS: frozenset[str] = frozenset({"manual_review"})
_V2_REJECT_ACTIONS: frozenset[str] = frozenset({"auto_reject"})


def map_v2_decision_to_output_bucket(
    *,
    is_canonical: bool,
    v1_hard_fail: bool,
    final_action: str | None,
) -> str:
    """Resolve the final output bucket for one row.

    Priority (first match wins):

      1. Non-canonical row → ``duplicate`` (dedupe is terminal).
      2. V1 ``hard_fail`` → ``hard_fail`` (structural failure is terminal;
         V2 cannot override in this subphase).
      3. V2 ``final_action == "auto_approve"`` → ``clean``.
      4. V2 ``final_action == "manual_review"`` → ``review``.
      5. V2 ``final_action == "auto_reject"`` → ``invalid``.
      6. Missing / unknown V2 ``final_action`` → ``review`` (conservative).

    The conservative fallback is the whole point of V2.1: the system must
    never ship an email to ``clean`` based on V1 alone.
    """
    if not is_canonical:
        return OUTPUT_DUPLICATE
    if v1_hard_fail:
        return OUTPUT_HARD_FAIL

    action = (final_action or "").strip()
    if action in _V2_ACCEPT_ACTIONS:
        return OUTPUT_CLEAN
    if action in _V2_REVIEW_ACTIONS:
        return OUTPUT_REVIEW
    if action in _V2_REJECT_ACTIONS:
        return OUTPUT_INVALID

    # Unknown / missing V2 output — never clean.
    return OUTPUT_REVIEW


def output_reason_from_bucket(
    bucket: str,
    decision_reason: str | None = None,
) -> str:
    """Translate an output bucket back to a legacy ``final_output_reason``.

    The reason tokens are kept compatible with the pre-V2.1 vocabulary so
    downstream consumers (reports, client exports, tests) keep working
    without a vocabulary change. ``invalid`` carries the V2 decision
    reason when present so the audit trail can distinguish a V2 active
    rejection from the legacy "score below threshold" case.
    """
    if bucket == OUTPUT_DUPLICATE:
        return "removed_duplicate"
    if bucket == OUTPUT_HARD_FAIL:
        return "removed_hard_fail"
    if bucket == OUTPUT_CLEAN:
        return "kept_high_confidence"
    if bucket == OUTPUT_REVIEW:
        return "kept_review"
    if bucket == OUTPUT_INVALID:
        normalized = (decision_reason or "").strip()
        # V2.1 vocab: probability- and structural-driven rejections.
        if normalized in {"low_probability", "hard_fail", "duplicate"}:
            return f"removed_v2_{normalized}"
        # V2.2 vocab: SMTP-driven rejections (e.g. ``smtp_invalid``).
        if normalized.startswith("smtp_"):
            return f"removed_v2_{normalized}"
        # V2.6 vocab: domain-intelligence-driven rejections. Catch-all
        # caps go to review, not invalid, so they don't appear here —
        # but we surface domain_high_risk / cold_start tokens for the
        # rare path where future policy might reject on these.
        if normalized in {"domain_high_risk", "cold_start_no_smtp_valid"}:
            return f"removed_v2_{normalized}"
        return "removed_low_score"
    return "removed_low_score"


__all__ = [
    "OUTPUT_CLEAN",
    "OUTPUT_REVIEW",
    "OUTPUT_INVALID",
    "OUTPUT_DUPLICATE",
    "OUTPUT_HARD_FAIL",
    "map_v2_decision_to_output_bucket",
    "output_reason_from_bucket",
]
