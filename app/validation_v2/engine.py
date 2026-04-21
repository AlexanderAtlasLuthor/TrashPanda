"""ValidationEngineV2: orchestration for the passive-intelligence layer.

The engine wires together the pluggable services defined in
:mod:`app.validation_v2.interfaces` under the rules encoded in a
:class:`~app.validation_v2.policy.ValidationPolicy`. It is the
single orchestration surface: subphases add logic behind the
interfaces, the engine itself changes minimally.

Current surface (Subphase 3 — telemetry + control plane):

    * Domain intelligence + provider reputation run first. Their
      outputs are collected into the result breakdown and mirrored
      into a compact metadata block. They run on every path,
      including the exclusion and candidate-skip short-circuits,
      so downstream consumers always see the same structured
      evidence.

    * Exclusion gate (short-circuit). If an ``ExclusionService`` is
      present and flags the request, the engine returns a canonical
      ``excluded_by_policy`` result with the collected intel
      attached.

    * Candidate-selection gate (short-circuit). If a
      ``ValidationCandidateSelector`` declines the request, the
      engine returns a ``deliverable_uncertain`` result with a
      ``not_a_candidate`` / ``low_priority_candidate`` reason.

    * Rate limit gate. If a :class:`RateLimiter` is attached the
      engine consults it; a blocked request falls through to the
      execution-policy step, which stamps ``rate_limited`` on the
      result.

    * Execution-policy gate. The engine calls
      :func:`is_validation_allowed` with the network policy plus
      the upstream decisions and attaches the resulting
      ``execution_decision`` to the result.

    * Every step appends an entry to a
      :class:`ProbeDecisionTrace` that is attached to the result
      — so every decision the engine made is explainable without
      re-running it.

    * Structured telemetry events are emitted at every gate
      through a pluggable :class:`TelemetrySink`. Emission is
      best-effort; a failing sink cannot affect the validation
      outcome.

The constructor signature is stable. Future subphases (SMTP,
catch-all, retries) will plug in without changing it. The
control-plane collaborators
(:attr:`telemetry_sink`, :attr:`rate_limiter`,
:attr:`execution_policy`) are configured by assigning the
instance attributes after construction — this keeps the
constructor frozen while still giving callers full control.
"""

from __future__ import annotations

import time
from typing import Any

from .control.decision_trace import (
    ProbeDecisionTrace,
    STAGE_CANDIDATE,
    STAGE_DOMAIN_INTELLIGENCE,
    STAGE_EXCLUSION,
    STAGE_EXECUTION_POLICY,
    STAGE_HISTORICAL_INTELLIGENCE,
    STAGE_PROVIDER_REPUTATION,
    STAGE_PROBABILITY,
    STAGE_RATE_LIMIT,
    STAGE_CATCH_ALL,
    STAGE_SMTP_PROBE,
    STAGE_SMTP_RETRY,
)
from .control.execution_policy import (
    EXECUTION_REASON_ALLOWED,
    EXECUTION_REASON_RATE_LIMITED,
    NetworkExecutionPolicy,
    is_validation_allowed,
)
from .control.rate_limit import RateLimiter
from .control.telemetry import (
    EVENT_CANDIDATE_SKIPPED,
    EVENT_EXCLUDED,
    EVENT_VALIDATION_ALLOWED,
    EVENT_VALIDATION_BLOCKED_BY_POLICY,
    EVENT_VALIDATION_STARTED,
    EVENT_SMTP_PROBE_COMPLETED,
    EVENT_SMTP_PROBE_FAILED,
    EVENT_SMTP_PROBE_STARTED,
    EVENT_SMTP_RETRY_ATTEMPTED,
    EVENT_SMTP_RETRY_SKIPPED,
    EVENT_CATCH_ALL_CLASSIFIED,
    TelemetryEvent,
    TelemetrySink as ControlTelemetrySink,
)
from .interfaces import (
    CatchAllAnalyzer,
    DomainIntelligenceService,
    ExclusionService,
    ProviderReputationService,
    RetryStrategy,
    SMTPProbeClient,
    TelemetrySink,
    ValidationCandidateSelector,
)
from .policy import ValidationPolicy
from .request import ValidationRequest
from .result import ValidationResult
from .types import ReasonCode, SmtpProbeStatus, ValidationStatus
from .history.read_service import HistoricalIntelligence, HistoricalIntelligenceService
from .history.write_service import ReputationLearningService
from .history.integration import retry_support
from .network.smtp_classifier import (
    SMTP_STATUS_NOT_ATTEMPTED,
    SMTP_STATUS_UNCERTAIN,
    SMTPResultClassifier,
)
from .network.smtp_result import SMTPProbeResult
from .network.catch_all import CatchAllAssessment
from .network.retry import RetryDecision
from .probability import (
    DeliverabilityAggregator,
    DeliverabilityResult,
    DeliverabilitySignal,
    ExplanationBuilder,
    ValidationDecisionPolicy,
)


# Reason-code tokens emitted by the engine in addition to the
# vocabulary in :class:`ReasonCode`. Keeping them here (rather
# than in ``types.py``) scopes them to the engine — other modules
# have no business emitting them directly.
REASON_EXCLUDED_DOMAIN = "excluded_domain"
REASON_LOW_PRIORITY_CANDIDATE = "low_priority_candidate"
SMTP_SKIP_EXCLUDED = "excluded"
SMTP_SKIP_NOT_CANDIDATE = "not_candidate"
SMTP_SKIP_NETWORK_BLOCKED = "network_blocked"
SMTP_SKIP_DISABLED_BY_POLICY = "smtp_disabled_by_policy"
SMTP_SKIP_CLIENT_MISSING = "smtp_client_missing"
SMTP_REASON_ALLOWED_BY_POLICY = "allowed_by_policy"
RETRY_SKIP_NO_RESULT = "smtp_not_attempted"
RETRY_SKIP_STRATEGY_MISSING = "retry_strategy_missing"
CATCH_ALL_SKIP_NO_RESULT = "smtp_not_attempted"
CATCH_ALL_SKIP_ANALYZER_MISSING = "catch_all_analyzer_missing"


class ValidationEngineV2:
    """Orchestrator for V2 validation.

    Construct with a policy plus any combination of optional
    collaborator services. Missing services degrade gracefully:
    if no ``ExclusionService`` is wired, exclusion is never
    applied; if no ``ValidationCandidateSelector`` is wired, every
    non-excluded request proceeds.

    The constructor signature is additive-only across subphases —
    new services will appear as additional keyword-only
    parameters, existing callers are never broken.

    Subphase 3 adds three optional *instance attributes* that
    configure the control plane without touching the constructor:

        * :attr:`telemetry_sink` — a
          :class:`~app.validation_v2.control.TelemetrySink`
          receiving structured events.
        * :attr:`rate_limiter` — a
          :class:`~app.validation_v2.control.RateLimiter` that
          decides whether the request is allowed through the
          per-domain / global caps.
        * :attr:`execution_policy` — a
          :class:`~app.validation_v2.control.NetworkExecutionPolicy`
          that governs whether network-bearing work may proceed.

    All three default to ``None``; a ``None`` collaborator is a
    no-op at the relevant gate.
    """

    def __init__(
        self,
        policy: ValidationPolicy,
        *,
        domain_intel: DomainIntelligenceService | None = None,
        provider_reputation: ProviderReputationService | None = None,
        exclusion_service: ExclusionService | None = None,
        candidate_selector: ValidationCandidateSelector | None = None,
        smtp_client: SMTPProbeClient | None = None,
        catch_all_analyzer: CatchAllAnalyzer | None = None,
        retry_strategy: RetryStrategy | None = None,
        telemetry: TelemetrySink | None = None,
    ) -> None:
        self._policy = policy
        self._domain_intel = domain_intel
        self._provider_reputation = provider_reputation
        self._exclusion_service = exclusion_service
        self._candidate_selector = candidate_selector
        # smtp / catch-all / retry are held but never invoked in
        # this subphase. Storing them here locks the constructor
        # shape so future subphases can land implementations
        # without API churn.
        self._smtp_client = smtp_client
        self._catch_all_analyzer = catch_all_analyzer
        self._retry_strategy = retry_strategy
        self._telemetry = telemetry

        # -- Subphase 3 control plane -------------------------------
        # Public attributes so callers can swap them in after
        # construction. The constructor signature stays frozen.
        self.telemetry_sink: ControlTelemetrySink | None = None
        self.rate_limiter: RateLimiter | None = None
        self.execution_policy: NetworkExecutionPolicy | None = None
        self.historical_intelligence_service: HistoricalIntelligenceService | None = None
        self.reputation_learning_service: ReputationLearningService | None = None

    # ------------------------------------------------------------------
    # Read-only accessors (handy for tests / introspection)
    # ------------------------------------------------------------------

    @property
    def policy(self) -> ValidationPolicy:
        return self._policy

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def validate(self, request: ValidationRequest) -> ValidationResult:
        """Run the passive-intelligence validation flow for ``request``.

        Steps (Subphase 3):

          1. Start a :class:`ProbeDecisionTrace`.
          2. Emit ``validation_started`` telemetry.
          3. Collect domain intelligence (trace).
          4. Collect provider reputation (trace).
          5. Exclusion gate (trace + telemetry + short-circuit).
          6. Candidate gate (trace + telemetry + short-circuit).
          7. Rate limit (trace).
          8. Execution policy (trace + telemetry).
          9. Attach the trace + execution decision to the result.

        The method does not mutate ``request`` and does not make
        any network calls.
        """
        trace = ProbeDecisionTrace()

        # -- 2. Emit "validation_started" --------------------------------
        self._emit_event(
            EVENT_VALIDATION_STARTED,
            domain=request.domain,
            metadata={
                "email": request.email,
                "bucket_v2": request.bucket_v2,
            },
        )

        # -- 3-4. Collect passive intelligence ---------------------------
        intel = self._run_domain_intel(request)
        trace.add_step(
            stage=STAGE_DOMAIN_INTELLIGENCE,
            decision="collected" if intel is not None else "skipped",
            reason=(
                "domain_intelligence_service_missing"
                if intel is None
                else "domain_analyzed"
            ),
            inputs={"domain": request.domain},
        )

        reputation = self._run_provider_reputation(request)
        trace.add_step(
            stage=STAGE_PROVIDER_REPUTATION,
            decision="collected" if reputation is not None else "skipped",
            reason=(
                "provider_reputation_service_missing"
                if reputation is None
                else "provider_classified"
            ),
            inputs={"domain": request.domain},
        )

        provider_label = _pluck_provider_label(reputation)
        provider_key = _pluck_provider_key(reputation)
        historical, historical_reason = self._fetch_historical_intelligence(
            request.domain,
            provider_key,
        )
        trace.add_step(
            stage=STAGE_HISTORICAL_INTELLIGENCE,
            decision="fetched" if historical.history_cache_hit else "missing",
            reason=historical_reason,
            inputs={
                "domain": request.domain,
                "provider_key": provider_key,
            },
        )

        breakdown = self._build_base_breakdown(intel, reputation, historical)
        base_metadata = _build_base_metadata(
            request=request,
            intel=intel,
            reputation=reputation,
            historical=historical,
        )

        # -- 5. Exclusion ------------------------------------------------
        exclusion_reason = self._check_exclusion(request)
        trace.add_step(
            stage=STAGE_EXCLUSION,
            decision="excluded" if exclusion_reason else "pass",
            reason=exclusion_reason or "not_excluded",
            inputs={
                "domain": request.domain,
                "excluded_domains_size": len(self._policy.excluded_domains),
                "syntax_valid": request.syntax_valid,
            },
        )
        if exclusion_reason is not None:
            self._add_smtp_skipped_step(
                trace,
                reason=SMTP_SKIP_EXCLUDED,
                request=request,
                extra_inputs={"exclusion_reason": exclusion_reason},
            )
            self._emit_event(
                EVENT_EXCLUDED,
                domain=request.domain,
                metadata={"reason": exclusion_reason},
            )
            result = self._build_excluded_result(
                request=request,
                breakdown=breakdown,
                metadata=base_metadata,
                exclusion_reason=exclusion_reason,
                trace=trace,
            )
            self._record_historical_write(
                request=request,
                result=result,
                trace=trace,
                smtp_summary=None,
                catch_all_summary=None,
                historical=historical,
            )
            return result

        # -- 6. Candidate selection --------------------------------------
        candidate_accepted, candidate_reason = self._check_candidate(request)
        trace.add_step(
            stage=STAGE_CANDIDATE,
            decision="accepted" if candidate_accepted else "rejected",
            reason=candidate_reason,
            inputs={
                "bucket_v2": request.bucket_v2,
                "score_v2": request.score_v2,
                "confidence_v2": request.confidence_v2,
                "syntax_valid": request.syntax_valid,
            },
        )
        if not candidate_accepted:
            self._add_smtp_skipped_step(
                trace,
                reason=SMTP_SKIP_NOT_CANDIDATE,
                request=request,
                extra_inputs={"candidate_reason": candidate_reason},
            )
            self._emit_event(
                EVENT_CANDIDATE_SKIPPED,
                domain=request.domain,
                metadata={"reason": candidate_reason},
            )
            result = self._build_skipped_result(
                request=request,
                breakdown=breakdown,
                metadata=base_metadata,
                candidate_reason=candidate_reason,
                trace=trace,
            )
            self._record_historical_write(
                request=request,
                result=result,
                trace=trace,
                smtp_summary=None,
                catch_all_summary=None,
                historical=historical,
            )
            return result

        # -- 7. Rate limit ------------------------------------------------
        rate_limit_blocked = False
        if self.rate_limiter is not None:
            allowed_by_rate = self.rate_limiter.allow(request.domain)
            rate_limit_blocked = not allowed_by_rate
            trace.add_step(
                stage=STAGE_RATE_LIMIT,
                decision="allowed" if allowed_by_rate else "blocked",
                reason=(
                    "within_limits"
                    if allowed_by_rate
                    else EXECUTION_REASON_RATE_LIMITED
                ),
                inputs={"domain": request.domain},
            )

        # -- 8. Execution policy -----------------------------------------
        if rate_limit_blocked:
            execution_allowed = False
            execution_reason = EXECUTION_REASON_RATE_LIMITED
        else:
            execution_allowed, execution_reason = is_validation_allowed(
                self.execution_policy,
                candidate_accepted,
                exclusion_reason,
            )
        trace.add_step(
            stage=STAGE_EXECUTION_POLICY,
            decision="allowed" if execution_allowed else "blocked",
            reason=execution_reason,
            inputs={
                "allow_network": (
                    None
                    if self.execution_policy is None
                    else self.execution_policy.allow_network
                ),
                "candidate_accepted": bool(candidate_accepted),
                "exclusion_reason": exclusion_reason,
                "rate_limiter_wired": self.rate_limiter is not None,
            },
        )

        if execution_allowed:
            self._emit_event(
                EVENT_VALIDATION_ALLOWED,
                domain=request.domain,
                metadata={"reason": execution_reason},
            )
        else:
            self._emit_event(
                EVENT_VALIDATION_BLOCKED_BY_POLICY,
                domain=request.domain,
                metadata={"reason": execution_reason},
            )

        smtp_result, smtp_classification, smtp_skip_reason = self._maybe_run_smtp_probe(
            request=request,
            trace=trace,
            execution_allowed=execution_allowed,
            execution_reason=execution_reason,
        )
        retry_result, retry_attempted, retry_outcome = self._maybe_retry_smtp_probe(
            request=request,
            trace=trace,
            smtp_result=smtp_result,
            historical=historical,
        )
        if retry_result is not None:
            smtp_result = retry_result
            smtp_classification = SMTPResultClassifier().classify(smtp_result)

        catch_all = self._maybe_assess_catch_all(
            request=request,
            trace=trace,
            smtp_result=smtp_result,
            smtp_classification=smtp_classification,
            historical=historical,
        )
        signals = _build_deliverability_signals(
            request=request,
            intel=intel,
            reputation=reputation,
            smtp_status=(
                str(smtp_classification["smtp_status"])
                if smtp_classification is not None
                else SMTP_STATUS_NOT_ATTEMPTED
            ),
            catch_all=_catch_all_for_probability(catch_all),
        )
        probability_result = DeliverabilityAggregator().compute(
            signals,
            historical=historical.to_dict(),
        )
        historical_influence = dict(probability_result.historical_influence or {})
        trace.add_step(
            stage="probability_history_adjustment",
            decision="applied" if historical_influence.get("applied") else "skipped",
            reason=str(
                historical_influence.get("reason", "no_history_or_low_confidence")
            ),
            inputs={
                "base_probability": probability_result.base_probability,
                "adjustment": historical_influence.get("adjustment", 0.0),
                "base_confidence": probability_result.base_confidence,
                "confidence_delta": historical_influence.get(
                    "confidence_delta", 0.0
                ),
            },
        )
        decision = ValidationDecisionPolicy().decide(
            probability_result.probability
        )
        explanation = ExplanationBuilder().build(
            probability_result.signals,
            probability_result,
            decision,
        )
        trace.add_step(
            stage=STAGE_PROBABILITY,
            decision="computed",
            reason="aggregation_complete",
            inputs={
                "signal_count": len(probability_result.signals),
                "probability": probability_result.probability,
                "confidence": probability_result.confidence,
            },
        )

        # -- 9. Happy-path placeholder result ----------------------------
        metadata = dict(base_metadata)
        metadata["candidate_decision"] = {
            "accepted": True,
            "reason": candidate_reason,
        }
        if smtp_classification is not None:
            metadata["smtp_classification"] = dict(smtp_classification)
        elif smtp_skip_reason is not None:
            metadata["smtp_skip_reason"] = smtp_skip_reason
        if catch_all is not None:
            metadata["catch_all"] = {
                "classification": catch_all.classification,
                "confidence": catch_all.confidence,
                "signals": dict(catch_all.signals),
            }
            if "historical_catch_all_support" in catch_all.signals:
                metadata["historical_catch_all_influence"] = catch_all.signals[
                    "historical_catch_all_support"
                ]
        retry_steps = [s for s in trace.steps if s["stage"] == STAGE_SMTP_RETRY]
        if retry_steps:
            metadata["historical_retry_influence"] = retry_steps[-1]["inputs"].get(
                "historical_retry_support"
            )
            metadata["historical_retry_recommended"] = bool(retry_attempted)
        metadata["action_recommendation"] = decision.action
        metadata["historical_probability_influence"] = historical_influence

        smtp_status = (
            str(smtp_classification["smtp_status"])
            if smtp_classification is not None
            else SMTP_STATUS_NOT_ATTEMPTED
        )
        smtp_code = smtp_result.code if smtp_result is not None else None
        smtp_latency = (
            smtp_result.latency_ms if smtp_result is not None else None
        )
        smtp_error_type = (
            smtp_result.error_type if smtp_result is not None else None
        )
        result = ValidationResult(
            validation_status=decision.status,
            deliverability_probability=probability_result.probability,
            smtp_probe_status=_smtp_probe_status_from_signal(
                smtp_result,
                smtp_status,
            ),
            catch_all_status=(
                catch_all.classification if catch_all is not None else None
            ),
            provider_reputation=provider_label,
            retry_recommended=False,
            validation_reason_codes=(
                *tuple(s.name for s in probability_result.signals),
                decision.status,
            ),
            validation_explanation=str(explanation["explanation_text"]),
            breakdown=breakdown,
            metadata=metadata,
            decision_trace=trace.to_dict(),
            execution_decision={
                "allowed": bool(execution_allowed),
                "reason": execution_reason,
            },
            smtp_status=smtp_status,
            smtp_code=smtp_code,
            smtp_latency=smtp_latency,
            smtp_error_type=smtp_error_type,
            catch_all_confidence=(
                catch_all.confidence if catch_all is not None else None
            ),
            retry_attempted=retry_attempted,
            retry_outcome=retry_outcome,
            deliverability_confidence=probability_result.confidence,
            action_recommendation=decision.action,
            validation_breakdown={
                "probability": probability_result.probability,
                "confidence": probability_result.confidence,
                "base_probability": probability_result.base_probability,
                "base_confidence": probability_result.base_confidence,
                "historical_influence": historical_influence,
                "decision": {
                    "status": decision.status,
                    "action": decision.action,
                },
                "signals": [_signal_to_dict(s) for s in probability_result.signals],
                "explanation": explanation,
            },
        )
        self._record_historical_write(
            request=request,
            result=result,
            trace=trace,
            smtp_summary=_smtp_learning_summary(
                smtp_result=smtp_result,
                smtp_classification=smtp_classification,
                retry_attempted=retry_attempted,
                retry_outcome=retry_outcome,
            ),
            catch_all_summary=_catch_all_learning_summary(catch_all),
            historical=historical,
        )
        self._emit({"event": "skeleton_completed", "domain": request.domain})
        return result

    # ------------------------------------------------------------------
    # Step helpers
    # ------------------------------------------------------------------

    def _run_domain_intel(
        self, request: ValidationRequest
    ) -> dict[str, Any] | None:
        """Call ``domain_intel.analyze`` if wired; return a dict or None.

        Returning ``None`` (rather than an empty dict) keeps the
        "no service wired" and "service returned nothing" cases
        distinguishable for downstream consumers of the breakdown.
        """
        if self._domain_intel is None:
            return None
        intel = self._domain_intel.analyze(request.domain)
        return dict(intel) if intel else {}

    def _run_provider_reputation(
        self, request: ValidationRequest
    ) -> dict[str, Any] | None:
        if self._provider_reputation is None:
            return None
        reputation = self._provider_reputation.classify(request.domain)
        return dict(reputation) if reputation else {}

    def _check_exclusion(self, request: ValidationRequest) -> str | None:
        """Return the fine-grained exclusion reason, or ``None``.

        Uses the richer ``check(request, policy)`` API when the
        wired service provides it (duck-typed) and falls back to
        ``is_excluded`` otherwise. The fallback loses the specific
        reason but still produces a correct boolean decision.
        """
        if self._exclusion_service is None:
            return None
        check = getattr(self._exclusion_service, "check", None)
        if callable(check):
            return check(request, self._policy)
        if self._exclusion_service.is_excluded(request, self._policy):
            return REASON_EXCLUDED_DOMAIN
        return None

    def _check_candidate(
        self, request: ValidationRequest
    ) -> tuple[bool, str]:
        """Return (accepted, reason_code) for the candidate selector.

        When no selector is wired the engine accepts unconditionally
        — the reason code ``"no_selector_wired"`` records that for
        telemetry.
        """
        if self._candidate_selector is None:
            return True, "no_selector_wired"
        explain = getattr(self._candidate_selector, "explain", None)
        if callable(explain):
            decision = explain(request, self._policy)
            # The concrete selector returns a NamedTuple; we only
            # need the two fields.
            return bool(decision.accepted), str(decision.reason)
        accepted = self._candidate_selector.should_validate(
            request, self._policy
        )
        return bool(accepted), (
            "accepted" if accepted else "not_accepted"
        )

    def _maybe_run_smtp_probe(
        self,
        *,
        request: ValidationRequest,
        trace: ProbeDecisionTrace,
        execution_allowed: bool,
        execution_reason: str,
    ) -> tuple[SMTPProbeResult | None, dict[str, Any] | None, str | None]:
        """Run the controlled SMTP sampler only after every gate passes."""
        if not execution_allowed:
            self._add_smtp_skipped_step(
                trace,
                reason=SMTP_SKIP_NETWORK_BLOCKED,
                request=request,
                extra_inputs={"execution_reason": execution_reason},
            )
            return None, None, SMTP_SKIP_NETWORK_BLOCKED

        if not _smtp_policy_enabled(self._policy):
            self._add_smtp_skipped_step(
                trace,
                reason=SMTP_SKIP_DISABLED_BY_POLICY,
                request=request,
            )
            return None, None, SMTP_SKIP_DISABLED_BY_POLICY

        if self._smtp_client is None:
            self._add_smtp_skipped_step(
                trace,
                reason=SMTP_SKIP_CLIENT_MISSING,
                request=request,
            )
            return None, None, SMTP_SKIP_CLIENT_MISSING

        trace.add_step(
            stage=STAGE_SMTP_PROBE,
            decision="executed",
            reason=SMTP_REASON_ALLOWED_BY_POLICY,
            inputs={
                "email": request.email,
                "domain": request.domain,
                "allow_smtp": True,
            },
        )
        self._emit_event(
            EVENT_SMTP_PROBE_STARTED,
            domain=request.domain,
            metadata={"email": request.email},
        )

        try:
            result = self._smtp_client.probe(request)
        except Exception as exc:
            result = SMTPProbeResult(
                success=False,
                code=None,
                message=str(exc) or None,
                latency_ms=None,
                error_type="protocol_error",
            )

        classification = SMTPResultClassifier().classify(result)
        metadata = {
            "code": result.code,
            "smtp_status": classification["smtp_status"],
            "classification_reason": classification[
                "classification_reason"
            ],
            "error_type": result.error_type,
        }
        if result.error_type:
            self._emit_event(
                EVENT_SMTP_PROBE_FAILED,
                domain=request.domain,
                metadata=metadata,
            )
        else:
            self._emit_event(
                EVENT_SMTP_PROBE_COMPLETED,
                domain=request.domain,
                metadata=metadata,
            )
        return result, classification, None

    def _maybe_retry_smtp_probe(
        self,
        *,
        request: ValidationRequest,
        trace: ProbeDecisionTrace,
        smtp_result: SMTPProbeResult | None,
        historical: HistoricalIntelligence,
    ) -> tuple[SMTPProbeResult | None, bool, str]:
        if smtp_result is None:
            return None, False, "none"

        if self._retry_strategy is None:
            self._add_retry_step(
                trace, "skipped", RETRY_SKIP_STRATEGY_MISSING, request
            )
            self._emit_event(
                EVENT_SMTP_RETRY_SKIPPED,
                domain=request.domain,
                metadata={"reason": RETRY_SKIP_STRATEGY_MISSING},
            )
            return None, False, "none"

        historical_payload = historical.to_dict()
        decision = _evaluate_retry(
            self._retry_strategy,
            smtp_result,
            historical_payload,
        )
        if not decision.should_retry:
            self._add_retry_step(
                trace,
                "skipped",
                decision.reason,
                request,
                extra_inputs={
                    "historical_retry_support": _retry_support_for_trace(
                        historical_payload
                    )
                },
            )
            self._emit_event(
                EVENT_SMTP_RETRY_SKIPPED,
                domain=request.domain,
                metadata={"reason": decision.reason},
            )
            return None, False, "none"

        self._add_retry_step(
            trace,
            "executed",
            decision.reason,
            request,
            extra_inputs={
                "delay_ms": decision.delay_ms,
                "historical_retry_support": _retry_support_for_trace(
                    historical_payload
                ),
            },
        )
        self._emit_event(
            EVENT_SMTP_RETRY_ATTEMPTED,
            domain=request.domain,
            metadata={"reason": decision.reason, "delay_ms": decision.delay_ms},
        )
        try:
            retry_result = self._smtp_client.probe(request)  # type: ignore[union-attr]
        except Exception as exc:
            retry_result = SMTPProbeResult(
                success=False,
                code=None,
                message=str(exc) or None,
                latency_ms=None,
                error_type="protocol_error",
            )
        return retry_result, True, "fail" if retry_result.error_type else "success"

    def _maybe_assess_catch_all(
        self,
        *,
        request: ValidationRequest,
        trace: ProbeDecisionTrace,
        smtp_result: SMTPProbeResult | None,
        smtp_classification: dict[str, Any] | None,
        historical: HistoricalIntelligence,
    ) -> CatchAllAssessment | None:
        if smtp_result is None or smtp_classification is None:
            return None

        if self._catch_all_analyzer is None:
            trace.add_step(
                stage=STAGE_CATCH_ALL,
                decision="skipped",
                reason=CATCH_ALL_SKIP_ANALYZER_MISSING,
                inputs={"domain": request.domain},
            )
            return None

        try:
            try:
                assessment = self._catch_all_analyzer.assess(
                    request.domain,
                    smtp_result,
                    smtp_classification,
                    _cache_from_domain_intel(self._domain_intel),
                    historical.to_dict(),
                )
            except TypeError:
                assessment = self._catch_all_analyzer.assess(
                    request.domain,
                    smtp_result,
                    smtp_classification,
                    _cache_from_domain_intel(self._domain_intel),
                )
            if isinstance(assessment, dict):
                assessment = CatchAllAssessment(
                    classification=str(assessment.get("classification", "unknown")),
                    confidence=float(assessment.get("confidence", 0.0)),
                    signals=dict(assessment.get("signals") or {}),
                )
        except Exception:
            assessment = CatchAllAssessment(
                classification="unknown",
                confidence=0.0,
                signals={"reason": "catch_all_analyzer_error"},
            )

        trace.add_step(
            stage=STAGE_CATCH_ALL,
            decision="classified",
            reason=str(assessment.signals.get("reason", assessment.classification)),
            inputs={
                "domain": request.domain,
                "confidence": assessment.confidence,
                "historical_catch_all_support": assessment.signals.get(
                    "historical_catch_all_support"
                ),
            },
        )
        self._emit_event(
            EVENT_CATCH_ALL_CLASSIFIED,
            domain=request.domain,
            metadata={
                "classification": assessment.classification,
                "confidence": assessment.confidence,
            },
        )
        return assessment

    @staticmethod
    def _add_retry_step(
        trace: ProbeDecisionTrace,
        decision: str,
        reason: str,
        request: ValidationRequest,
        extra_inputs: dict[str, Any] | None = None,
    ) -> None:
        inputs: dict[str, Any] = {
            "email": request.email,
            "domain": request.domain,
        }
        if extra_inputs:
            inputs.update(extra_inputs)
        trace.add_step(
            stage=STAGE_SMTP_RETRY,
            decision=decision,
            reason=reason,
            inputs=inputs,
        )

    @staticmethod
    def _add_smtp_skipped_step(
        trace: ProbeDecisionTrace,
        *,
        reason: str,
        request: ValidationRequest,
        extra_inputs: dict[str, Any] | None = None,
    ) -> None:
        inputs: dict[str, Any] = {
            "email": request.email,
            "domain": request.domain,
        }
        if extra_inputs:
            inputs.update(extra_inputs)
        trace.add_step(
            stage=STAGE_SMTP_PROBE,
            decision="skipped",
            reason=reason,
            inputs=inputs,
        )

    # ------------------------------------------------------------------
    # Result constructors
    # ------------------------------------------------------------------

    @staticmethod
    def _build_base_breakdown(
        intel: dict[str, Any] | None,
        reputation: dict[str, Any] | None,
        historical: HistoricalIntelligence,
    ) -> dict[str, Any]:
        breakdown: dict[str, Any] = {}
        if intel is not None:
            breakdown["domain_intelligence"] = intel
        if reputation is not None:
            breakdown["provider_reputation"] = reputation
        breakdown["historical_intelligence"] = {
            "history_cache_hit": historical.history_cache_hit,
            "domain": historical.domain,
            "provider_key": historical.provider_key,
            "domain_observation_count": historical.domain_observation_count,
            "provider_observation_count": historical.provider_observation_count,
            "historical_domain_reputation": (
                historical.historical_domain_reputation
            ),
            "historical_provider_reputation": (
                historical.historical_provider_reputation
            ),
            "historical_smtp_valid_rate": historical.historical_smtp_valid_rate,
            "historical_smtp_invalid_rate": (
                historical.historical_smtp_invalid_rate
            ),
            "historical_timeout_rate": historical.historical_timeout_rate,
            "historical_catch_all_risk": historical.historical_catch_all_risk,
            "domain_history_stale": historical.domain_history_stale,
            "provider_history_stale": historical.provider_history_stale,
        }
        return breakdown

    def _build_excluded_result(
        self,
        *,
        request: ValidationRequest,
        breakdown: dict[str, Any],
        metadata: dict[str, Any],
        exclusion_reason: str,
        trace: ProbeDecisionTrace,
    ) -> ValidationResult:
        metadata = dict(metadata)
        metadata["excluded"] = True
        metadata["exclusion_reason"] = exclusion_reason
        breakdown = dict(breakdown)
        breakdown["excluded"] = True
        breakdown["exclusion_reason"] = exclusion_reason

        reason_codes: tuple[str, ...] = (
            ReasonCode.EXCLUDED_BY_POLICY.value,
            exclusion_reason,
        )
        result = ValidationResult(
            validation_status=ValidationStatus.EXCLUDED_BY_POLICY.value,
            deliverability_probability=0.0,
            smtp_probe_status=SmtpProbeStatus.SKIPPED.value,
            catch_all_status=None,
            provider_reputation=_pluck_provider_label_from_breakdown(breakdown),
            retry_recommended=False,
            validation_reason_codes=reason_codes,
            validation_explanation=(
                f"Request excluded by policy ({exclusion_reason}): "
                f"domain={request.domain!r}."
            ),
            breakdown=breakdown,
            metadata=metadata,
            decision_trace=trace.to_dict(),
            execution_decision={
                "allowed": False,
                "reason": "excluded",
            },
        )
        self._emit(
            {
                "event": "excluded",
                "domain": request.domain,
                "reason": exclusion_reason,
            }
        )
        return result

    def _build_skipped_result(
        self,
        *,
        request: ValidationRequest,
        breakdown: dict[str, Any],
        metadata: dict[str, Any],
        candidate_reason: str,
        trace: ProbeDecisionTrace,
    ) -> ValidationResult:
        metadata = dict(metadata)
        metadata["candidate_decision"] = {
            "accepted": False,
            "reason": candidate_reason,
        }
        breakdown = dict(breakdown)
        breakdown["candidate"] = False
        breakdown["candidate_reason"] = candidate_reason

        reason_codes: tuple[str, ...] = (
            ReasonCode.NOT_A_CANDIDATE.value,
            REASON_LOW_PRIORITY_CANDIDATE,
            ReasonCode.VALIDATION_SKIPPED.value,
        )
        result = ValidationResult(
            validation_status=ValidationStatus.DELIVERABLE_UNCERTAIN.value,
            deliverability_probability=0.0,
            smtp_probe_status=SmtpProbeStatus.SKIPPED.value,
            catch_all_status=None,
            provider_reputation=_pluck_provider_label_from_breakdown(breakdown),
            retry_recommended=False,
            validation_reason_codes=reason_codes,
            validation_explanation=(
                "Request is not a validation candidate under current "
                f"policy ({candidate_reason})."
            ),
            breakdown=breakdown,
            metadata=metadata,
            decision_trace=trace.to_dict(),
            execution_decision={
                "allowed": False,
                "reason": "not_candidate",
            },
        )
        self._emit(
            {
                "event": "skipped_not_candidate",
                "domain": request.domain,
                "reason": candidate_reason,
            }
        )
        return result

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _emit(self, event: dict[str, Any]) -> None:
        """Emit a legacy dict-shaped telemetry event, swallowing any error.

        Telemetry failures must never affect validation outcomes.
        The engine is deterministic from the caller's perspective
        regardless of sink behaviour.
        """
        if self._telemetry is None:
            return
        try:
            self._telemetry.emit(event)
        except Exception:
            # Best-effort — sink resilience is the sink's job.
            pass

    def _emit_event(
        self,
        event_type: str,
        *,
        domain: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Emit a structured :class:`TelemetryEvent` to the control sink.

        Separate from :meth:`_emit` because the control-plane sink
        uses the :class:`TelemetryEvent` dataclass while the
        legacy sink uses plain dicts. Both live side by side so
        callers can migrate without the engine making the choice.
        """
        if self.telemetry_sink is None:
            return
        try:
            event = TelemetryEvent(
                event_type=event_type,
                timestamp=time.time(),
                domain=domain,
                metadata=dict(metadata) if metadata else {},
            )
            self.telemetry_sink.emit(event)
        except Exception:
            # Best-effort — sink failure must not affect
            # validation.
            pass


    def _fetch_historical_intelligence(
        self, domain: str, provider_key: str | None
    ) -> tuple[HistoricalIntelligence, str]:
        if self.historical_intelligence_service is None:
            historical = HistoricalIntelligenceService().fetch(
                domain,
                provider_key,
            )
            return historical, "service_unavailable"

        historical = self.historical_intelligence_service.fetch(
            domain,
            provider_key,
        )
        return (
            historical,
            "cache_hit" if historical.history_cache_hit else "no_history",
        )

    def _record_historical_write(
        self,
        *,
        request: ValidationRequest,
        result: ValidationResult,
        trace: ProbeDecisionTrace,
        smtp_summary: dict[str, Any] | None,
        catch_all_summary: dict[str, Any] | None,
        historical: HistoricalIntelligence,
    ) -> None:
        if self.reputation_learning_service is None:
            result.metadata["historical_write_recorded"] = False
            trace.add_step(
                stage="historical_write",
                decision="skipped",
                reason="service_unavailable",
                inputs={"domain": request.domain},
            )
            result.decision_trace = trace.to_dict()
            return

        try:
            self.reputation_learning_service.record_validation(
                request=request,
                result=result,
                smtp_result=smtp_summary,
                catch_all=catch_all_summary,
                historical=historical.to_dict(),
            )
        except Exception:
            result.metadata["historical_write_recorded"] = False
            trace.add_step(
                stage="historical_write",
                decision="failed",
                reason="learning_service_error",
                inputs={"domain": request.domain},
            )
            result.decision_trace = trace.to_dict()
            return

        result.metadata["historical_write_recorded"] = True
        trace.add_step(
            stage="historical_write",
            decision="recorded",
            reason="learning_recorded",
            inputs={"domain": request.domain},
        )
        result.decision_trace = trace.to_dict()


# ---------------------------------------------------------------------------
# Free helpers
# ---------------------------------------------------------------------------


def _pluck_provider_label(reputation: dict[str, Any] | None) -> str | None:
    if not reputation:
        return None
    value = reputation.get("provider")
    return value if isinstance(value, str) else None


def _pluck_provider_key(reputation: dict[str, Any] | None) -> str | None:
    if not reputation:
        return None
    value = reputation.get("provider_key")
    if isinstance(value, str) and value:
        return value
    return _pluck_provider_label(reputation)


def _pluck_provider_label_from_breakdown(
    breakdown: dict[str, Any],
) -> str | None:
    return _pluck_provider_label(breakdown.get("provider_reputation"))


def _build_base_metadata(
    *,
    request: ValidationRequest,
    intel: dict[str, Any] | None,
    reputation: dict[str, Any] | None,
    historical: HistoricalIntelligence,
) -> dict[str, Any]:
    """Compose the compact per-request metadata summary.

    Pulls a small set of hand-picked fields out of the richer
    breakdown so callers that only want the headline signals
    (provider_type, reputation_score, cache_hit) do not have to
    walk the nested dicts themselves.
    """
    metadata: dict[str, Any] = {
        "email": request.email,
        "domain": request.domain,
    }
    cache_hit = False
    if intel:
        if "provider_hint" in intel:
            metadata["provider_hint"] = intel["provider_hint"]
        if "is_common_provider" in intel:
            metadata["is_common_provider"] = intel["is_common_provider"]
        if "is_suspicious_pattern" in intel:
            metadata["is_suspicious_pattern"] = intel["is_suspicious_pattern"]
        if intel.get("historical_score") is not None:
            metadata["historical_score"] = intel["historical_score"]
        cache_hit = cache_hit or bool(intel.get("cache_hit"))
    if reputation:
        if "provider_type" in reputation:
            metadata["provider_type"] = reputation["provider_type"]
        if "reputation_score" in reputation:
            metadata["reputation_score"] = reputation["reputation_score"]
        if "trust_level" in reputation:
            metadata["trust_level"] = reputation["trust_level"]
        cache_hit = cache_hit or bool(reputation.get("cache_hit"))
    metadata["cache_hit"] = cache_hit
    metadata["historical_intelligence"] = historical.to_dict()
    return metadata


def _smtp_policy_enabled(policy: ValidationPolicy) -> bool:
    """Return True when SMTP sampling is explicitly enabled."""
    allow_smtp = getattr(policy, "allow_smtp", None)
    if allow_smtp is not None:
        return bool(allow_smtp)
    return bool(policy.enable_smtp_probing)


def _smtp_probe_status_from_signal(
    smtp_result: SMTPProbeResult | None,
    smtp_status: str,
) -> str:
    if smtp_result is None:
        return SmtpProbeStatus.NOT_ATTEMPTED.value
    if smtp_result.error_type:
        return SmtpProbeStatus.ATTEMPTED_TEMP_FAIL.value
    if smtp_status == "invalid":
        return SmtpProbeStatus.ATTEMPTED_REJECT.value
    return SmtpProbeStatus.ATTEMPTED_SUCCESS.value


def _build_deliverability_signals(
    *,
    request: ValidationRequest,
    intel: dict[str, Any] | None,
    reputation: dict[str, Any] | None,
    smtp_status: str,
    catch_all: CatchAllAssessment | None,
) -> list[DeliverabilitySignal]:
    signals = [
        DeliverabilitySignal(
            name="syntax_valid",
            value=1.0 if request.syntax_valid else 0.0,
            weight=1.0,
            source="structural",
        ),
        DeliverabilitySignal(
            name="domain_present",
            value=1.0 if request.domain_present else 0.0,
            weight=0.8,
            source="dns",
        ),
    ]

    if intel is not None:
        is_suspicious = bool(intel.get("is_suspicious_pattern"))
        signals.append(
            DeliverabilitySignal(
                name="domain_pattern",
                value=0.3 if is_suspicious else 0.8,
                weight=0.8,
                source="dns",
            )
        )

    if reputation is not None:
        score = reputation.get("reputation_score")
        if score is None:
            score = reputation.get("score")
        if score is not None:
            signals.append(
                DeliverabilitySignal(
                    name="provider_reputation",
                    value=_clamp_float(score),
                    weight=1.2,
                    source="reputation",
                )
            )

    smtp_value = {
        "valid": 1.0,
        "invalid": 0.0,
        "uncertain": 0.5,
    }.get(smtp_status)
    if smtp_value is not None:
        signals.append(
            DeliverabilitySignal(
                name="smtp_result",
                value=smtp_value,
                weight=2.0,
                source="smtp",
            )
        )

    if catch_all is not None:
        catch_all_value = {
            "confirmed": 0.4,
            "likely": 0.5,
            "unlikely": 0.8,
            "unknown": 0.6,
        }.get(catch_all.classification, 0.6)
        signals.append(
            DeliverabilitySignal(
                name="catch_all",
                value=catch_all_value,
                weight=0.9,
                source="catch_all",
            )
        )

    return signals


def _signal_to_dict(signal: DeliverabilitySignal) -> dict[str, object]:
    return {
        "name": signal.name,
        "value": signal.value,
        "weight": signal.weight,
        "source": signal.source,
    }


def _smtp_learning_summary(
    *,
    smtp_result: SMTPProbeResult | None,
    smtp_classification: dict[str, Any] | None,
    retry_attempted: bool,
    retry_outcome: str,
) -> dict[str, Any] | None:
    if smtp_result is None and smtp_classification is None and not retry_attempted:
        return None
    return {
        "smtp_status": (
            str(smtp_classification["smtp_status"])
            if smtp_classification is not None
            else SMTP_STATUS_NOT_ATTEMPTED
        ),
        "smtp_code": smtp_result.code if smtp_result is not None else None,
        "smtp_error_type": (
            smtp_result.error_type if smtp_result is not None else None
        ),
        "retry_attempted": bool(retry_attempted),
        "retry_outcome": retry_outcome,
    }


def _catch_all_learning_summary(
    catch_all: CatchAllAssessment | None,
) -> dict[str, Any] | None:
    if catch_all is None:
        return None
    return {
        "catch_all_status": catch_all.classification,
        "confidence": catch_all.confidence,
    }


def _catch_all_for_probability(
    catch_all: CatchAllAssessment | None,
) -> CatchAllAssessment | None:
    if catch_all is None:
        return None
    original = catch_all.signals.get("pre_history_classification")
    if not isinstance(original, str):
        return catch_all
    return CatchAllAssessment(
        classification=original,
        confidence=float(catch_all.signals.get("pre_history_confidence") or 0.0),
        signals=dict(catch_all.signals),
    )


def _clamp_float(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.5
    return max(0.0, min(1.0, number))


def _evaluate_retry(
    strategy: RetryStrategy,
    result: SMTPProbeResult,
    historical: dict[str, Any] | None = None,
) -> RetryDecision:
    evaluate = getattr(strategy, "evaluate", None)
    if callable(evaluate):
        try:
            decision = evaluate(result, historical)
        except TypeError:
            decision = evaluate(result)
        if isinstance(decision, RetryDecision):
            return decision
        return RetryDecision(
            should_retry=bool(getattr(decision, "should_retry", False)),
            delay_ms=getattr(decision, "delay_ms", None),
            reason=str(getattr(decision, "reason", "not_retryable")),
        )

    decide = getattr(strategy, "decide")
    raw = decide(
        {
            "success": result.success,
            "code": result.code,
            "message": result.message,
            "latency_ms": result.latency_ms,
            "error_type": result.error_type,
            "historical": historical or {},
        }
    )
    return RetryDecision(
        should_retry=bool(raw.get("retry")),
        delay_ms=raw.get("delay_ms"),  # type: ignore[arg-type]
        reason=str(raw.get("reason", "not_retryable")),
    )


def _retry_support_for_trace(historical: dict[str, Any]) -> dict[str, Any]:
    return retry_support(historical)


def _cache_from_domain_intel(domain_intel: Any) -> Any:
    return getattr(domain_intel, "_cache", None)


__all__ = [
    "ValidationEngineV2",
    "REASON_EXCLUDED_DOMAIN",
    "REASON_LOW_PRIORITY_CANDIDATE",
]
