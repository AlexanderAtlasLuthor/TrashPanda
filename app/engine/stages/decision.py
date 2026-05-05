"""V2 Decision stage — Subphase V2.1.

Computes the deliverability probability and the V2 ``final_action``
inside the chunk pipeline so the V2 decision is available at
materialization time, not after. This stage promotes V2 from
annotation-only to bucket-authoritative.

Position in the chunk engine:

    ScoringComparisonStage → **DecisionStage** → CompletenessStage

At chunk time, history-based and SMTP signals are not yet available
(those are populated by the post-pipeline passes in
``app.api_boundary``). The stage therefore computes a *cold-start*
deliverability probability using only the signals available in the
chunk: V1 hard fail, DNS, domain match, typo, and the V2 bucket.

The post-pipeline passes (``_maybe_run_probability_model`` /
``_maybe_run_decision_engine`` in ``app.api_boundary``) still run
afterwards and may refresh these columns with history- and SMTP-aware
values; the row's *placement* in the output CSV is locked in here and
is not relocated.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from ...v2_decision_policy import apply_v2_decision_policy
from ...validation_v2.decision.decision_engine import (
    DecisionInputs,
    DecisionResult,
)
from ...validation_v2.decision.decision_explanation import explain_decision
from ...validation_v2.decision.policy import (
    DEFAULT_DECISION_POLICY,
    DecisionPolicy,
    FinalAction,
)
from ...validation_v2.probability.row_explanation import explain_deliverability
from ...validation_v2.probability.row_model import (
    DEFAULT_PROBABILITY_THRESHOLDS,
    DeliverabilityInputs,
    ProbabilityThresholds,
    compute_deliverability_probability,
)
from ..context import PipelineContext
from ..payload import ChunkPayload
from ..stage import Stage
from .catch_all_detection import (
    CATCH_ALL_RISK_STATUSES,
    CATCH_ALL_STATUS_CONFIRMED,
    CATCH_ALL_STATUS_NOT_TESTED,
    CATCH_ALL_STATUS_POSSIBLE,
)
from .smtp_verification import (
    SMTP_STATUS_BLOCKED,
    SMTP_STATUS_CATCH_ALL_POSSIBLE,
    SMTP_STATUS_ERROR,
    SMTP_STATUS_INVALID,
    SMTP_STATUS_NOT_TESTED,
    SMTP_STATUS_TEMP_FAIL,
    SMTP_STATUS_TIMEOUT,
    SMTP_STATUS_VALID,
    smtp_status_to_model_smtp_result,
)


# Mapping from V2 ``bucket_v2`` (ScoringV2Stage) onto the
# ``v2_final_bucket`` vocabulary the Decision Engine consumes.
_BUCKET_V2_TO_FINAL: dict[str, str] = {
    "high_confidence": "ready",
    "review": "review",
    "invalid": "invalid",
}


# Columns this stage appends to the chunk frame. Kept central so tests
# and downstream code reference one source of truth.
DECISION_STAGE_OUTPUT_COLUMNS: tuple[str, ...] = (
    "v2_final_bucket",
    "deliverability_probability",
    "deliverability_label",
    "deliverability_factors",
    "deliverability_note",
    "final_action",
    "decision_reason",
    "decision_confidence",
    "overridden_bucket",
    "decision_note",
)


# --------------------------------------------------------------------------- #
# V2.2 — SMTP-aware decision overrides                                        #
# --------------------------------------------------------------------------- #


# Statuses that prove the mailbox does not exist or that the row should
# be actively rejected.
_SMTP_REJECT_STATUSES: frozenset[str] = frozenset({SMTP_STATUS_INVALID})


# Statuses that mean "we don't know yet — never auto-approve, always
# review at most." Catch-all is included here in V2.2 (its real handling
# is V2.3); blocked / timeout / temp_fail / error all map here.
_SMTP_REVIEW_STATUSES: frozenset[str] = frozenset({
    SMTP_STATUS_BLOCKED,
    SMTP_STATUS_TIMEOUT,
    SMTP_STATUS_TEMP_FAIL,
    SMTP_STATUS_ERROR,
    SMTP_STATUS_CATCH_ALL_POSSIBLE,
})


def _smtp_reason_token(status: str) -> str:
    """Translate an SMTP status to a stable ``decision_reason`` token.

    Tokens are read by ``app.v2_classification.output_reason_from_bucket``
    to produce the legacy ``final_output_reason`` column on disk, so they
    must not collide with existing reason vocabulary.
    """
    return f"smtp_{status}"


def _bucket_for_action(
    action: str,
    current_bucket: str,
    policy: DecisionPolicy,
) -> str:
    """Re-compute ``overridden_bucket`` for a forced action.

    Mirrors the helper in ``decision_engine._overridden_bucket_for`` so
    SMTP-driven overrides keep the same override-bucket semantics as
    probability-driven ones.
    """
    if not policy.enable_bucket_override:
        return ""
    if action == FinalAction.AUTO_APPROVE and current_bucket != "ready":
        return "ready"
    if action == FinalAction.AUTO_REJECT and current_bucket != "invalid":
        return "invalid"
    return ""


def _apply_smtp_overrides(
    *,
    result: DecisionResult,
    smtp_status: str,
    smtp_was_candidate: bool,
    hard_fail: bool,
    v2_final_bucket: str,
    policy: DecisionPolicy,
) -> DecisionResult:
    """Apply V2.2 SMTP-aware overrides on top of the probability decision.

    Override priority:

      1. V1 hard fail / duplicate already returned a terminal
         ``auto_reject`` from ``apply_decision_policy``; we never
         override those (preserves V2.1 invariants).
      2. ``smtp_status == invalid``  → ``auto_reject`` (mailbox confirmed
         dead). This is the headline V2.2 behaviour.
      3. ``smtp_status`` in the review-only set
         (``blocked / timeout / temp_fail / error / catch_all_possible``)
         → cap at ``manual_review`` (downgrade ``auto_approve`` only).
      4. SMTP candidate but status is not ``valid`` → cap at
         ``manual_review`` (the conservative-fallback rule from the
         prompt).

    The helper never *upgrades* an action; it only forces a reject for
    confirmed-invalid mailboxes or downgrades to review when SMTP is
    inconclusive.
    """
    # Rule 1 — terminal V1 outcomes are sacrosanct.
    if hard_fail or v2_final_bucket in ("hard_fail", "duplicate"):
        return result

    # Rule 2 — confirmed permanent rejection forces auto_reject.
    if smtp_status in _SMTP_REJECT_STATUSES:
        return DecisionResult(
            final_action=FinalAction.AUTO_REJECT,
            decision_reason=_smtp_reason_token(smtp_status),
            decision_confidence=result.decision_confidence,
            overridden_bucket=_bucket_for_action(
                FinalAction.AUTO_REJECT, v2_final_bucket, policy
            ),
        )

    # Rule 3 — inconclusive SMTP caps at review.
    if smtp_status in _SMTP_REVIEW_STATUSES:
        if result.final_action == FinalAction.AUTO_APPROVE:
            return DecisionResult(
                final_action=FinalAction.MANUAL_REVIEW,
                decision_reason=_smtp_reason_token(smtp_status),
                decision_confidence=result.decision_confidence,
                overridden_bucket=_bucket_for_action(
                    FinalAction.MANUAL_REVIEW, v2_final_bucket, policy
                ),
            )
        return result

    # Rule 4 — candidate without a positive signal cannot auto-approve.
    if (
        smtp_was_candidate
        and smtp_status != SMTP_STATUS_VALID
        and result.final_action == FinalAction.AUTO_APPROVE
    ):
        return DecisionResult(
            final_action=FinalAction.MANUAL_REVIEW,
            decision_reason="smtp_unconfirmed_for_candidate",
            decision_confidence=result.decision_confidence,
            overridden_bucket=_bucket_for_action(
                FinalAction.MANUAL_REVIEW, v2_final_bucket, policy
            ),
        )

    return result


# --------------------------------------------------------------------------- #
# V2.3 — Catch-all-aware decision overrides                                   #
# --------------------------------------------------------------------------- #


def _catch_all_reason_token(status: str) -> str:
    """Return a stable ``decision_reason`` for a catch-all-driven cap.

    Maps directly onto :data:`CATCH_ALL_STATUS_*` so the audit trail in
    the materialized CSV (``final_output_reason``) is unambiguous.
    """
    if status == CATCH_ALL_STATUS_CONFIRMED:
        return "catch_all_confirmed"
    if status == CATCH_ALL_STATUS_POSSIBLE:
        return "catch_all_possible"
    return "catch_all_risk"


def _apply_catch_all_overrides(
    *,
    result: DecisionResult,
    catch_all_status: str,
    catch_all_flag: bool,
    hard_fail: bool,
    v2_final_bucket: str,
    smtp_status: str,
    policy: DecisionPolicy,
) -> DecisionResult:
    """Cap ``auto_approve`` whenever catch-all evidence is positive.

    Override priority:

      1. V1 hard-fail / duplicate / SMTP-invalid outcomes are terminal
         (already returned ``auto_reject`` from earlier overrides) —
         we never override an active reject.
      2. ``catch_all_flag=True`` (status in
         :data:`CATCH_ALL_RISK_STATUSES`) → cap ``auto_approve`` →
         ``manual_review`` with the appropriate
         ``catch_all_confirmed`` / ``catch_all_possible`` reason.

    The override does *not* change ``manual_review`` or ``auto_reject``
    outcomes — it only blocks the approval path. ``not_catch_all``
    actively allows approval (the row passed the random-RCPT test).
    """
    # Rule 1 — preserve V2.1 + V2.2 terminal outcomes.
    if hard_fail or v2_final_bucket in ("hard_fail", "duplicate"):
        return result
    if smtp_status == SMTP_STATUS_INVALID:
        return result
    if result.final_action == FinalAction.AUTO_REJECT:
        return result

    # Rule 2 — catch-all risk caps approval.
    if catch_all_flag and catch_all_status in CATCH_ALL_RISK_STATUSES:
        if result.final_action == FinalAction.AUTO_APPROVE:
            return DecisionResult(
                final_action=FinalAction.MANUAL_REVIEW,
                decision_reason=_catch_all_reason_token(catch_all_status),
                decision_confidence=result.decision_confidence,
                overridden_bucket=_bucket_for_action(
                    FinalAction.MANUAL_REVIEW, v2_final_bucket, policy
                ),
            )

    return result


def _coerce_bool(val: Any) -> bool:
    if val is None:
        return False
    try:
        if pd.isna(val):
            return False
    except (TypeError, ValueError):
        pass
    try:
        return bool(val)
    except (TypeError, ValueError):
        return False


def _coerce_str(val: Any) -> str:
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except (TypeError, ValueError):
        pass
    s = str(val)
    return s if s.lower() not in {"nan", "none"} else ""


def _coerce_int(val: Any, default: int = 0) -> int:
    if val is None:
        return default
    try:
        if pd.isna(val):
            return default
    except (TypeError, ValueError):
        pass
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


def _derive_v2_final_bucket(row: dict[str, Any]) -> str:
    """Derive ``v2_final_bucket`` from the V2 chunk-time outputs.

    Returns one of: ``ready`` | ``review`` | ``invalid`` | ``hard_fail``
    | ``unknown``. ``duplicate`` is not produced here because dedupe runs
    *after* this stage; duplicates are routed by ``_materialize`` based
    on the canonical flag, not by V2.
    """
    if _coerce_bool(row.get("hard_fail")):
        return "hard_fail"
    if _coerce_bool(row.get("hard_stop_v2")):
        return "hard_fail"
    bucket = _coerce_str(row.get("bucket_v2"))
    if bucket:
        return _BUCKET_V2_TO_FINAL.get(bucket, "unknown")
    return "unknown"


def _policy_from_context(
    context: PipelineContext, default: DecisionPolicy
) -> DecisionPolicy:
    """Build a :class:`DecisionPolicy` from ``context.config.decision``.

    Falls back to ``default`` whenever the config is missing or
    contains unparseable values. The point is to honour YAML edits
    (``decision.enable_bucket_override``, thresholds) without a code
    change.
    """
    cfg = getattr(context, "config", None)
    decision_cfg = getattr(cfg, "decision", None) if cfg is not None else None
    if decision_cfg is None:
        return default
    try:
        return DecisionPolicy(
            approve_threshold=float(
                getattr(decision_cfg, "approve_threshold", default.approve_threshold)
            ),
            review_threshold=float(
                getattr(decision_cfg, "review_threshold", default.review_threshold)
            ),
            enable_bucket_override=bool(
                getattr(
                    decision_cfg,
                    "enable_bucket_override",
                    default.enable_bucket_override,
                )
            ),
        )
    except (TypeError, ValueError):
        return default


def _domain_intelligence_caps_from_context(
    context: PipelineContext,
) -> tuple[bool, bool]:
    """Read V2.6 safety-cap toggles from ``context.config.domain_intelligence``.

    Returns ``(high_risk_blocks_auto_approve,
    cold_start_requires_smtp_valid)`` with the policy defaults
    (``True``, ``True``) when config is missing or unparseable. These
    flags exist on :class:`DomainIntelligenceConfig` and used to be
    loaded but never consulted by ``apply_v2_decision_policy`` — the
    V2.10.10 audit identified that as the gap that locked every
    cold-start row into review regardless of operator preference.
    """
    cfg = getattr(context, "config", None)
    intel_cfg = getattr(cfg, "domain_intelligence", None) if cfg is not None else None
    if intel_cfg is None:
        return True, True
    try:
        high_risk_blocks = bool(
            getattr(intel_cfg, "high_risk_blocks_auto_approve", True)
        )
        cold_start_requires_smtp_valid = bool(
            getattr(intel_cfg, "cold_start_requires_smtp_valid", True)
        )
    except (TypeError, ValueError):
        return True, True
    return high_risk_blocks, cold_start_requires_smtp_valid


class DecisionStage(Stage):
    """Compute V2 deliverability probability + final action per row.

    Runs in the chunk engine after V1 + V2 scoring so every input the
    decision needs is already on the frame. Outputs are appended to the
    frame and propagate into staging via ``StagingPersistenceStage``;
    ``_materialize`` reads them back as the V2-authoritative routing
    signal.
    """

    name = "decision"
    requires = (
        "syntax_valid",
        "corrected_domain",
        "has_mx_record",
        "has_a_record",
        "domain_exists",
        "dns_error",
        "hard_fail",
        "score",
        "preliminary_bucket",
        "bucket_v2",
        "hard_stop_v2",
        # V2.2 — SMTP fields populated by SMTPVerificationStage. Values
        # default to ``not_tested`` for non-candidates, so the column is
        # always present even on a fully-disabled SMTP run.
        "smtp_status",
        "smtp_was_candidate",
        # V2.3 — Catch-all fields populated by CatchAllDetectionStage.
        # ``catch_all_flag=True`` is a hard rule: the row cannot
        # auto-approve regardless of probability or SMTP outcome.
        "catch_all_status",
        "catch_all_flag",
        # V2.6 — Domain-intelligence fields populated by
        # DomainIntelligenceStage. ``domain_risk_level=high`` and
        # ``domain_cold_start AND smtp_status!=valid`` cap approval.
        "domain_risk_level",
        "domain_cold_start",
    )
    produces = DECISION_STAGE_OUTPUT_COLUMNS

    def __init__(
        self,
        thresholds: ProbabilityThresholds | None = None,
        policy: DecisionPolicy | None = None,
    ) -> None:
        self._thresholds = thresholds or DEFAULT_PROBABILITY_THRESHOLDS
        self._policy = policy or DEFAULT_DECISION_POLICY

    @property
    def thresholds(self) -> ProbabilityThresholds:
        return self._thresholds

    @property
    def policy(self) -> DecisionPolicy:
        return self._policy

    def run(self, payload: ChunkPayload, context: PipelineContext) -> ChunkPayload:
        thresholds = self._thresholds
        policy = _policy_from_context(context, self._policy)
        (
            high_risk_blocks_auto_approve,
            cold_start_requires_smtp_valid,
        ) = _domain_intelligence_caps_from_context(context)

        frame = payload.frame
        rows = frame.to_dict(orient="records")

        out: dict[str, list[Any]] = {
            col: [] for col in DECISION_STAGE_OUTPUT_COLUMNS
        }

        for row in rows:
            v2_final_bucket = _derive_v2_final_bucket(row)
            hard_fail = _coerce_bool(row.get("hard_fail")) or _coerce_bool(
                row.get("hard_stop_v2")
            )

            # V2.2 — SMTP signals from upstream SMTPVerificationStage.
            smtp_status_raw = _coerce_str(row.get("smtp_status"))
            smtp_status = smtp_status_raw or SMTP_STATUS_NOT_TESTED
            smtp_was_candidate = _coerce_bool(row.get("smtp_was_candidate"))
            smtp_confidence_raw = row.get("smtp_confidence")
            try:
                smtp_confidence = float(smtp_confidence_raw or 0.0)
            except (TypeError, ValueError):
                smtp_confidence = 0.0
            model_smtp_result = smtp_status_to_model_smtp_result(smtp_status)

            # V2.3 — Catch-all signals from upstream CatchAllDetectionStage.
            catch_all_status = (
                _coerce_str(row.get("catch_all_status"))
                or CATCH_ALL_STATUS_NOT_TESTED
            )
            catch_all_flag = _coerce_bool(row.get("catch_all_flag"))

            # V2.6 — Domain-intelligence signals from upstream
            # DomainIntelligenceStage. Defaults to ``unknown`` /
            # ``False`` so the policy treats missing intel as
            # informational, not as positive evidence.
            domain_risk_level = (
                _coerce_str(row.get("domain_risk_level")) or "unknown"
            )
            domain_cold_start = _coerce_bool(row.get("domain_cold_start"))

            prob_inputs = DeliverabilityInputs(
                score_post_history=_coerce_int(row.get("score"), 0),
                historical_label="neutral",
                confidence_adjustment_applied=0,
                catch_all_confidence=0.0,
                possible_catch_all=False,
                smtp_result=model_smtp_result,
                smtp_confidence=smtp_confidence,
                has_mx_record=_coerce_bool(row.get("has_mx_record")),
                has_a_record=_coerce_bool(row.get("has_a_record")),
                domain_match=_coerce_bool(row.get("domain_matches_input_column")),
                typo_detected=_coerce_bool(row.get("typo_detected")),
                hard_fail=hard_fail,
                v2_final_bucket=v2_final_bucket,
                email=_coerce_str(row.get("email")),
                domain=_coerce_str(
                    row.get("corrected_domain") or row.get("domain")
                ),
            )
            comp = compute_deliverability_probability(prob_inputs, thresholds)

            # V2.4 — single centralized policy. All probability mapping,
            # SMTP/catch-all caps, and V1/duplicate terminals live in
            # ``apply_v2_decision_policy``. The DecisionStage no longer
            # owns any threshold or override logic — it only orchestrates
            # the inputs and persists the outputs.
            decision_inputs = DecisionInputs(
                deliverability_probability=comp.probability,
                v2_final_bucket=v2_final_bucket,
                hard_fail=hard_fail,
                smtp_result=model_smtp_result,
            )
            result = apply_v2_decision_policy(
                probability=comp.probability,
                smtp_status=smtp_status,
                smtp_was_candidate=smtp_was_candidate,
                catch_all_status=catch_all_status,
                catch_all_flag=catch_all_flag,
                hard_fail=hard_fail,
                v2_final_bucket=v2_final_bucket,
                policy=policy,
                # V2.6 — domain-intelligence inputs.
                domain_risk_level=domain_risk_level,
                domain_cold_start=domain_cold_start,
                # V2.10.10 — operator-tunable safety-cap toggles
                # sourced from ``domain_intelligence.*`` config.
                high_risk_blocks_auto_approve=high_risk_blocks_auto_approve,
                cold_start_requires_smtp_valid=cold_start_requires_smtp_valid,
            )

            factor_names = "|".join(f.name for f in comp.factors)
            if comp.override_reason:
                factor_names = f"override:{comp.override_reason}"

            out["v2_final_bucket"].append(v2_final_bucket)
            out["deliverability_probability"].append(round(comp.probability, 3))
            out["deliverability_label"].append(comp.label)
            out["deliverability_factors"].append(factor_names)
            out["deliverability_note"].append(explain_deliverability(comp))
            out["final_action"].append(result.final_action)
            out["decision_reason"].append(result.decision_reason)
            out["decision_confidence"].append(round(result.decision_confidence, 3))
            out["overridden_bucket"].append(result.overridden_bucket)
            out["decision_note"].append(explain_decision(result, decision_inputs))

        new_frame = frame.copy()
        for col, values in out.items():
            new_frame[col] = values
        return payload.with_frame(new_frame)


__all__ = [
    "DecisionStage",
    "DECISION_STAGE_OUTPUT_COLUMNS",
]
