"""V2 Phase 6 — decision-level natural-language explanations.

Given a :class:`DecisionResult` (and optionally the :class:`DecisionInputs`
that produced it), returns one short sentence describing the action
and the driver behind it.

Deterministic: every (action, reason) pair maps to a specific template.
"""

from __future__ import annotations

from .decision_engine import DecisionInputs, DecisionResult
from .policy import DecisionReason, FinalAction


def explain_decision(
    result: DecisionResult,
    inputs: DecisionInputs | None = None,
) -> str:
    """Return one sentence describing the decision and its main reason."""
    reason = result.decision_reason
    action = result.final_action
    p = result.decision_confidence

    # ── Terminal overrides ──────────────────────────────────────── #
    if reason == DecisionReason.HARD_FAIL:
        return "Auto-rejected: row was hard-failed by validation."
    if reason == DecisionReason.DUPLICATE:
        return "Auto-rejected: row was removed as a duplicate."

    # ── Probability-driven branches ─────────────────────────────── #
    if action == FinalAction.AUTO_APPROVE:
        # If SMTP confirmed the address, mention it — that's the strongest
        # possible corroboration for an auto-approval.
        if inputs is not None and inputs.smtp_result == "deliverable":
            return (
                f"Auto-approved due to high deliverability probability "
                f"({p:.2f}) and confirmed SMTP delivery."
            )
        return f"Auto-approved due to high deliverability probability ({p:.2f})."

    if action == FinalAction.MANUAL_REVIEW:
        return (
            f"Flagged for manual review — medium deliverability probability "
            f"({p:.2f})."
        )

    # action == FinalAction.AUTO_REJECT, probability-driven
    if inputs is not None and inputs.smtp_result == "undeliverable":
        return (
            f"Auto-rejected due to low deliverability probability ({p:.2f}) "
            f"and SMTP rejection."
        )
    return f"Auto-rejected due to low deliverability probability ({p:.2f})."


__all__ = ["explain_decision"]
