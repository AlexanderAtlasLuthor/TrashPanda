"""V2.4 — Centralized deliverability-probability decision policy.

Single source of truth for the V2 final-action contract:

    deliverability_probability + safety overrides → final_action

V2.1 made V2 authoritative, V2.2 added SMTP signals, V2.3 added catch-all
signals. V2.4 collapses the override priority chain into one
deterministic, unit-testable function so threshold logic, hard
overrides, and reason vocabulary live in one place — not scattered
across :class:`DecisionStage`'s helpers.

Public API
----------

* :data:`REASON_*` — canonical reason vocabulary used in
  ``decision_reason`` and downstream materialization audit tokens.
* :func:`clamp_probability` — bounds any input to ``[0, 1]``.
* :func:`probability_to_final_action` — pure mapping
  ``probability -> (action, reason)``. No I/O, no signals other than
  the probability and the two thresholds.
* :func:`apply_v2_decision_policy` — the full priority chain. Takes
  the probability + all V2 signals (SMTP, catch-all, V1 hard fail,
  duplicate flag) and returns a fully-populated
  :class:`~app.validation_v2.decision.decision_engine.DecisionResult`.

Priority order (first match wins)
---------------------------------

1. V1 ``hard_fail`` (or ``v2_final_bucket=="hard_fail"``)
   → ``auto_reject`` / ``hard_fail``.
2. ``v2_final_bucket=="duplicate"`` → ``auto_reject`` / ``duplicate``.
3. ``smtp_status=="invalid"`` → ``auto_reject`` / ``smtp_invalid``.
4. Probability mapping
   (``high`` ≥ approve_threshold ≥ ``medium`` ≥ review_threshold > ``low``).
5. If the probability mapping produced ``auto_approve``, apply the
   safety caps in this order:
   a. SMTP inconclusive (``blocked / timeout / temp_fail / error /
      catch_all_possible``) → cap to ``manual_review``.
   b. Catch-all risk (``catch_all_flag=True`` with status in
      :data:`CATCH_ALL_RISK_STATUSES`) → cap to ``manual_review``.
   c. SMTP candidate without a confirmed-valid mailbox → cap to
      ``manual_review`` with reason
      ``smtp_unconfirmed_for_candidate``.
6. Otherwise the probability mapping stands.

The function never *upgrades* an action — it can only force a reject
(rule 3) or cap a would-be approval (rule 5).
"""

from __future__ import annotations

import math

from .validation_v2.decision.decision_engine import DecisionResult
from .validation_v2.decision.policy import (
    DEFAULT_DECISION_POLICY,
    DecisionPolicy,
    FinalAction,
    OverrideBucket,
)


# --------------------------------------------------------------------------- #
# Reason vocabulary                                                           #
# --------------------------------------------------------------------------- #


# Probability-driven reasons (mirror DecisionReason values verbatim so
# the existing decision-engine vocabulary stays in lock-step).
REASON_HIGH_PROBABILITY = "high_probability"
REASON_MEDIUM_PROBABILITY = "medium_probability"
REASON_LOW_PROBABILITY = "low_probability"

# Terminal reasons.
REASON_HARD_FAIL = "hard_fail"
REASON_DUPLICATE = "duplicate"

# SMTP-driven reasons.
REASON_SMTP_INVALID = "smtp_invalid"
REASON_SMTP_UNCONFIRMED_FOR_CANDIDATE = "smtp_unconfirmed_for_candidate"

# V2.6 — Domain-intelligence reasons.
REASON_DOMAIN_HIGH_RISK = "domain_high_risk"
REASON_COLD_START_NO_SMTP_VALID = "cold_start_no_smtp_valid"

# V2.10.11 — external validator consensus rejection. Only fires when
# every registered validator (or a confident-enough subset, per the
# aggregator) signals ``invalid``. The policy never *upgrades* a row
# based on external consensus — they're a second opinion, not the
# source of truth.
REASON_EXTERNAL_VALIDATORS_INVALID = "external_validators_invalid"


# --------------------------------------------------------------------------- #
# SMTP / catch-all status sets                                                #
# --------------------------------------------------------------------------- #


# Canonical SMTP statuses are imported lazily inside the policy
# function to avoid an import cycle with the engine stages package.
# The strings are stable across releases; we hard-code them here as a
# defensive measure so changes in the SMTP module always require a
# matching policy update.
_SMTP_INVALID = "invalid"
_SMTP_VALID = "valid"
_SMTP_INCONCLUSIVE_SET: frozenset[str] = frozenset({
    "blocked",
    "timeout",
    "temp_fail",
    "error",
    "catch_all_possible",
})

_CATCH_ALL_RISK_SET: frozenset[str] = frozenset({
    "confirmed_catch_all",
    "possible_catch_all",
})


# V2.6 — domain risk levels that block ``auto_approve`` regardless of
# probability or SMTP outcome. Hard-coded mirror of
# ``app.engine.stages.domain_intelligence.DOMAIN_RISK_BLOCKING`` so a
# rename in one place fails this constant out of sync (and the V2.6
# tests catch the drift).
_DOMAIN_HIGH_RISK_SET: frozenset[str] = frozenset({"high"})


# --------------------------------------------------------------------------- #
# Pure helpers                                                                #
# --------------------------------------------------------------------------- #


def clamp_probability(value: object) -> float:
    """Bound any probability input to ``[0.0, 1.0]``.

    Used at every entry point so a malformed input never propagates
    into routing logic. ``None``, ``NaN``, non-numeric strings, and
    out-of-range values all collapse to ``0.0`` — the conservative
    default keeps malformed rows out of clean output.
    """
    if value is None:
        return 0.0
    try:
        v = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0
    if math.isnan(v) or math.isinf(v):
        return 0.0
    if v < 0.0:
        return 0.0
    if v > 1.0:
        return 1.0
    return v


def probability_to_final_action(
    probability: float,
    *,
    approve_threshold: float = DEFAULT_DECISION_POLICY.approve_threshold,
    review_threshold: float = DEFAULT_DECISION_POLICY.review_threshold,
) -> tuple[str, str]:
    """Pure deterministic mapping ``probability → (action, reason)``.

    Rules:

      * ``probability >= approve_threshold`` →
        ``(auto_approve, high_probability)``.
      * ``review_threshold <= probability < approve_threshold`` →
        ``(manual_review, medium_probability)``.
      * ``probability < review_threshold`` →
        ``(auto_reject, low_probability)``.

    Probability is clamped first; the function does not validate the
    threshold ordering (the :class:`DecisionPolicy` constructor
    already does that).
    """
    p = clamp_probability(probability)
    if p >= approve_threshold:
        return (FinalAction.AUTO_APPROVE, REASON_HIGH_PROBABILITY)
    if p >= review_threshold:
        return (FinalAction.MANUAL_REVIEW, REASON_MEDIUM_PROBABILITY)
    return (FinalAction.AUTO_REJECT, REASON_LOW_PROBABILITY)


# --------------------------------------------------------------------------- #
# Override-bucket helper                                                      #
# --------------------------------------------------------------------------- #


def overridden_bucket_for(
    action: str,
    current_bucket: str,
    policy: DecisionPolicy,
) -> str:
    """Return the new bucket name when override fires, else empty string.

    Mirrors the helper in :mod:`app.validation_v2.decision.decision_engine`
    so V2.4-driven overrides keep the same override-bucket semantics
    as probability-driven ones in the underlying decision engine. Kept
    here so the entire V2.4 policy lives in one module.
    """
    if not policy.enable_bucket_override:
        return ""
    if action == FinalAction.AUTO_APPROVE and current_bucket != OverrideBucket.READY:
        return OverrideBucket.READY
    if action == FinalAction.AUTO_REJECT and current_bucket != OverrideBucket.INVALID:
        return OverrideBucket.INVALID
    return ""


# --------------------------------------------------------------------------- #
# Centralized policy                                                          #
# --------------------------------------------------------------------------- #


def apply_v2_decision_policy(
    *,
    probability: float,
    smtp_status: str,
    smtp_was_candidate: bool,
    catch_all_status: str,
    catch_all_flag: bool,
    hard_fail: bool,
    v2_final_bucket: str,
    policy: DecisionPolicy = DEFAULT_DECISION_POLICY,
    # V2.6 — domain-intelligence inputs. Default to ``unknown`` /
    # ``False`` so callers that don't yet emit these fields keep their
    # old behaviour unchanged.
    domain_risk_level: str = "unknown",
    domain_cold_start: bool = False,
    # V2.10.10 — domain-intelligence safety-cap toggles. The
    # corresponding YAML flags
    # (``domain_intelligence.high_risk_blocks_auto_approve`` and
    # ``domain_intelligence.cold_start_requires_smtp_valid``) used to be
    # loaded into AppConfig but never consulted here, so rules 5d/5e
    # were unconditional regardless of operator preference. Surfacing
    # them as kwargs lets per-job postures (strict / balanced /
    # permissive) tune the cold-start cap without forking the policy.
    high_risk_blocks_auto_approve: bool = True,
    cold_start_requires_smtp_valid: bool = True,
    # V2.10.11 — consensus from registered external validators. Empty
    # / ``not_run`` is the safe default for jobs without external
    # validation. Rule 5f rejects only on ``invalid``; never escalates.
    external_consensus: str = "not_run",
) -> DecisionResult:
    """The single source of truth for V2.4 + V2.6 final action.

    Combines the probability mapping with every V2 safety override into
    one deterministic, unit-testable function. Replaces the per-override
    helpers from V2.2 / V2.3 / V2.6 — those still exist as private
    utilities for back-compat but the chunk-time pipeline flows through
    this one entry point.

    V2.6 caps (rule 5d / 5e):

      5d. ``domain_risk_level == "high"`` caps ``auto_approve`` →
          ``manual_review`` with reason ``domain_high_risk``.
      5e. ``domain_cold_start AND smtp_status != "valid"`` caps
          approval with reason ``cold_start_no_smtp_valid``. (Mostly
          redundant with V2.4 rule 5c for SMTP candidates, but
          explicit at the domain level for audit clarity.)

    See module docstring for the full priority chain.
    """
    p = clamp_probability(probability)

    # ── 1. Terminal V1 outcomes. ────────────────────────────────────── #
    if hard_fail or v2_final_bucket == "hard_fail":
        return DecisionResult(
            final_action=FinalAction.AUTO_REJECT,
            decision_reason=REASON_HARD_FAIL,
            decision_confidence=0.0,
            overridden_bucket="",
        )

    # ── 2. Duplicate is terminal. ─────────────────────────────────── #
    if v2_final_bucket == "duplicate":
        return DecisionResult(
            final_action=FinalAction.AUTO_REJECT,
            decision_reason=REASON_DUPLICATE,
            decision_confidence=0.0,
            overridden_bucket="",
        )

    # ── 3. SMTP invalid is terminal. ──────────────────────────────── #
    if smtp_status == _SMTP_INVALID:
        return DecisionResult(
            final_action=FinalAction.AUTO_REJECT,
            decision_reason=REASON_SMTP_INVALID,
            decision_confidence=p,
            overridden_bucket=overridden_bucket_for(
                FinalAction.AUTO_REJECT, v2_final_bucket, policy
            ),
        )

    # ── 3b. V2.10.11 — external validator consensus invalid. ────────── #
    # A unanimous (or aggregator-confident) "this address bounces"
    # from registered third-party validators is treated like an
    # SMTP-invalid. Lives between rules 3 and 4 so probability /
    # catch-all caps do NOT veto a vendor-confirmed rejection.
    if external_consensus == "invalid":
        return DecisionResult(
            final_action=FinalAction.AUTO_REJECT,
            decision_reason=REASON_EXTERNAL_VALIDATORS_INVALID,
            decision_confidence=p,
            overridden_bucket=overridden_bucket_for(
                FinalAction.AUTO_REJECT, v2_final_bucket, policy
            ),
        )

    # ── 4. Probability mapping. ───────────────────────────────────── #
    action, reason = probability_to_final_action(
        p,
        approve_threshold=policy.approve_threshold,
        review_threshold=policy.review_threshold,
    )

    # ── 5. Safety caps applied only to a would-be approval. ───────── #
    if action == FinalAction.AUTO_APPROVE:
        # 5a. SMTP inconclusive caps at review.
        if smtp_status in _SMTP_INCONCLUSIVE_SET:
            return DecisionResult(
                final_action=FinalAction.MANUAL_REVIEW,
                decision_reason=f"smtp_{smtp_status}",
                decision_confidence=p,
                overridden_bucket=overridden_bucket_for(
                    FinalAction.MANUAL_REVIEW, v2_final_bucket, policy
                ),
            )

        # 5b. Catch-all risk caps at review.
        if catch_all_flag and catch_all_status in _CATCH_ALL_RISK_SET:
            ca_reason = (
                "catch_all_confirmed"
                if catch_all_status == "confirmed_catch_all"
                else "catch_all_possible"
            )
            return DecisionResult(
                final_action=FinalAction.MANUAL_REVIEW,
                decision_reason=ca_reason,
                decision_confidence=p,
                overridden_bucket=overridden_bucket_for(
                    FinalAction.MANUAL_REVIEW, v2_final_bucket, policy
                ),
            )

        # 5c. SMTP candidate without a confirmed-valid mailbox.
        if smtp_was_candidate and smtp_status != _SMTP_VALID:
            return DecisionResult(
                final_action=FinalAction.MANUAL_REVIEW,
                decision_reason=REASON_SMTP_UNCONFIRMED_FOR_CANDIDATE,
                decision_confidence=p,
                overridden_bucket=overridden_bucket_for(
                    FinalAction.MANUAL_REVIEW, v2_final_bucket, policy
                ),
            )

        # 5d. V2.6 — high-risk domain caps approval. A known-bad
        # domain (disposable, suspicious-shape, future history-driven
        # signals) cannot reach clean even with valid SMTP. Gated by
        # ``high_risk_blocks_auto_approve`` so a permissive posture
        # can opt out (the disposable list is still independently
        # capped by V1 hard_fail / probability scoring).
        if (
            high_risk_blocks_auto_approve
            and domain_risk_level in _DOMAIN_HIGH_RISK_SET
        ):
            return DecisionResult(
                final_action=FinalAction.MANUAL_REVIEW,
                decision_reason=REASON_DOMAIN_HIGH_RISK,
                decision_confidence=p,
                overridden_bucket=overridden_bucket_for(
                    FinalAction.MANUAL_REVIEW, v2_final_bucket, policy
                ),
            )

        # 5e. V2.6 — cold-start without SMTP valid caps approval.
        # Mostly redundant with rule 5c for SMTP candidates (which
        # already covers ``smtp_was_candidate AND smtp != valid``),
        # but explicit at the domain level so the audit trail says
        # *why* the row was capped (``cold_start_no_smtp_valid``).
        # Gated by ``cold_start_requires_smtp_valid`` so a permissive
        # posture can deliver high-probability cold-start domains
        # without an SMTP confirmation.
        if (
            cold_start_requires_smtp_valid
            and domain_cold_start
            and smtp_status != _SMTP_VALID
        ):
            return DecisionResult(
                final_action=FinalAction.MANUAL_REVIEW,
                decision_reason=REASON_COLD_START_NO_SMTP_VALID,
                decision_confidence=p,
                overridden_bucket=overridden_bucket_for(
                    FinalAction.MANUAL_REVIEW, v2_final_bucket, policy
                ),
            )

    # ── 6. Probability mapping stands. ────────────────────────────── #
    return DecisionResult(
        final_action=action,
        decision_reason=reason,
        decision_confidence=p,
        overridden_bucket=overridden_bucket_for(action, v2_final_bucket, policy),
    )


__all__ = [
    "REASON_DUPLICATE",
    "REASON_HARD_FAIL",
    "REASON_HIGH_PROBABILITY",
    "REASON_LOW_PROBABILITY",
    "REASON_MEDIUM_PROBABILITY",
    "REASON_SMTP_INVALID",
    "REASON_SMTP_UNCONFIRMED_FOR_CANDIDATE",
    "apply_v2_decision_policy",
    "clamp_probability",
    "overridden_bucket_for",
    "probability_to_final_action",
]
