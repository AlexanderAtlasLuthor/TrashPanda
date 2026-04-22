"""V2 Phase 6 — decision engine (core).

Pure, deterministic function: in goes a :class:`DecisionInputs` payload
built from a CSV row, out comes a :class:`DecisionResult` containing
the action, a short reason code, the confidence (= Phase-5 probability),
and — optionally — an overridden bucket name.

Guardrails
----------
* **Hard-fails and duplicates always auto-reject.** No bucket override
  either. These classifications are terminal from V1's perspective and
  the Decision Engine respects that.
* **Bucket override is opt-in.** When
  :attr:`DecisionPolicy.enable_bucket_override` is False the
  ``overridden_bucket`` field is always ``""``, so consumers can simply
  check "is it non-empty?" to know whether to relocate a row.
* **Cross-tier moves are allowed but transparent.** When override is
  enabled, an ``auto_reject`` on a ``ready``-bucketed row WILL target
  ``invalid``. The original bucket stays on the row so downstream code
  can audit the change.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .policy import (
    DEFAULT_DECISION_POLICY,
    DecisionPolicy,
    DecisionReason,
    FinalAction,
    OverrideBucket,
)


# --------------------------------------------------------------------------- #
# Inputs / outputs                                                            #
# --------------------------------------------------------------------------- #


@dataclass(slots=True, frozen=True)
class DecisionInputs:
    """Everything the engine needs from a single CSV row."""

    deliverability_probability: float
    v2_final_bucket: str            # "ready" | "review" | "invalid" | "hard_fail" | "duplicate" | "unknown"
    hard_fail: bool
    smtp_result: str = "not_tested"  # optional, used by explanation layer only


@dataclass(slots=True, frozen=True)
class DecisionResult:
    """Output of :func:`apply_decision_policy`."""

    final_action: str              # one of FinalAction.ALL
    decision_reason: str           # one of DecisionReason.ALL
    decision_confidence: float     # echoes Phase-5 probability, clamped to [0,1]
    overridden_bucket: str         # "" when no override applies


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #


def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, value))


def _truthy(value: Any) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() in ("1", "true", "t", "yes", "y")


def _float_or(value: Any, default: float) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


# --------------------------------------------------------------------------- #
# Building DecisionInputs from a CSV row                                      #
# --------------------------------------------------------------------------- #


def inputs_from_row(row: dict[str, str]) -> DecisionInputs:
    """Best-effort extraction from a CSV row dict."""
    return DecisionInputs(
        deliverability_probability=_clamp(
            _float_or(row.get("deliverability_probability"), 0.0)
        ),
        v2_final_bucket=(row.get("v2_final_bucket") or "").strip() or "unknown",
        hard_fail=_truthy(row.get("hard_fail")),
        smtp_result=(row.get("smtp_result") or "").strip() or "not_tested",
    )


# --------------------------------------------------------------------------- #
# Core policy application                                                     #
# --------------------------------------------------------------------------- #


def _overridden_bucket_for(
    action: str, current_bucket: str, policy: DecisionPolicy,
) -> str:
    """Return the new bucket name when override fires, else empty string.

    Never moves rows already at their target bucket — that's a no-op and
    would muddy the audit trail.
    """
    if not policy.enable_bucket_override:
        return ""
    if action == FinalAction.AUTO_APPROVE and current_bucket != OverrideBucket.READY:
        return OverrideBucket.READY
    if action == FinalAction.AUTO_REJECT and current_bucket != OverrideBucket.INVALID:
        return OverrideBucket.INVALID
    return ""


def apply_decision_policy(
    inputs: DecisionInputs,
    policy: DecisionPolicy = DEFAULT_DECISION_POLICY,
) -> DecisionResult:
    """Collapse Phase-5 signals into one of three actions.

    Evaluation order (first match wins):
      1. Hard-fail (flag or bucket) → ``auto_reject`` with reason
         ``hard_fail``. No override.
      2. Duplicate bucket → ``auto_reject`` with reason ``duplicate``.
         No override.
      3. Probability-based branch: ``auto_approve`` / ``manual_review``
         / ``auto_reject``. Bucket override applies if enabled.
    """
    probability = _clamp(inputs.deliverability_probability)

    # ── Hard guards ──────────────────────────────────────────────── #
    if inputs.hard_fail or inputs.v2_final_bucket == "hard_fail":
        return DecisionResult(
            final_action=FinalAction.AUTO_REJECT,
            decision_reason=DecisionReason.HARD_FAIL,
            decision_confidence=0.0,
            overridden_bucket="",
        )
    if inputs.v2_final_bucket == "duplicate":
        return DecisionResult(
            final_action=FinalAction.AUTO_REJECT,
            decision_reason=DecisionReason.DUPLICATE,
            decision_confidence=0.0,
            overridden_bucket="",
        )

    # ── Probability branch ──────────────────────────────────────── #
    if probability >= policy.approve_threshold:
        action = FinalAction.AUTO_APPROVE
        reason = DecisionReason.HIGH_PROBABILITY
    elif probability >= policy.review_threshold:
        action = FinalAction.MANUAL_REVIEW
        reason = DecisionReason.MEDIUM_PROBABILITY
    else:
        action = FinalAction.AUTO_REJECT
        reason = DecisionReason.LOW_PROBABILITY

    override = _overridden_bucket_for(action, inputs.v2_final_bucket, policy)
    return DecisionResult(
        final_action=action,
        decision_reason=reason,
        decision_confidence=probability,
        overridden_bucket=override,
    )


__all__ = [
    "DecisionInputs",
    "DecisionResult",
    "apply_decision_policy",
    "inputs_from_row",
]
