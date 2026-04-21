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
    STAGE_PROVIDER_REPUTATION,
    STAGE_RATE_LIMIT,
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


# Reason-code tokens emitted by the engine in addition to the
# vocabulary in :class:`ReasonCode`. Keeping them here (rather
# than in ``types.py``) scopes them to the engine — other modules
# have no business emitting them directly.
REASON_EXCLUDED_DOMAIN = "excluded_domain"
REASON_LOW_PRIORITY_CANDIDATE = "low_priority_candidate"


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
        breakdown = self._build_base_breakdown(intel, reputation)
        base_metadata = _build_base_metadata(
            request=request,
            intel=intel,
            reputation=reputation,
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
            self._emit_event(
                EVENT_EXCLUDED,
                domain=request.domain,
                metadata={"reason": exclusion_reason},
            )
            return self._build_excluded_result(
                request=request,
                breakdown=breakdown,
                metadata=base_metadata,
                exclusion_reason=exclusion_reason,
                trace=trace,
            )

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
            self._emit_event(
                EVENT_CANDIDATE_SKIPPED,
                domain=request.domain,
                metadata={"reason": candidate_reason},
            )
            return self._build_skipped_result(
                request=request,
                breakdown=breakdown,
                metadata=base_metadata,
                candidate_reason=candidate_reason,
                trace=trace,
            )

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

        # -- 9. Happy-path placeholder result ----------------------------
        metadata = dict(base_metadata)
        metadata["candidate_decision"] = {
            "accepted": True,
            "reason": candidate_reason,
        }
        result = ValidationResult(
            validation_status=ValidationStatus.DELIVERABLE_UNCERTAIN.value,
            deliverability_probability=0.0,
            smtp_probe_status=SmtpProbeStatus.NOT_ATTEMPTED.value,
            catch_all_status=None,
            provider_reputation=provider_label,
            retry_recommended=False,
            validation_reason_codes=(
                ReasonCode.VALIDATION_SKIPPED.value,
                ReasonCode.NO_ACTIVE_PROBE.value,
            ),
            validation_explanation=(
                "Passive-intel engine: no active probe performed. "
                f"domain={request.domain!r}, bucket_v2={request.bucket_v2!r}."
            ),
            breakdown=breakdown,
            metadata=metadata,
            decision_trace=trace.to_dict(),
            execution_decision={
                "allowed": bool(execution_allowed),
                "reason": execution_reason,
            },
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

    # ------------------------------------------------------------------
    # Result constructors
    # ------------------------------------------------------------------

    @staticmethod
    def _build_base_breakdown(
        intel: dict[str, Any] | None,
        reputation: dict[str, Any] | None,
    ) -> dict[str, Any]:
        breakdown: dict[str, Any] = {}
        if intel is not None:
            breakdown["domain_intelligence"] = intel
        if reputation is not None:
            breakdown["provider_reputation"] = reputation
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


# ---------------------------------------------------------------------------
# Free helpers
# ---------------------------------------------------------------------------


def _pluck_provider_label(reputation: dict[str, Any] | None) -> str | None:
    if not reputation:
        return None
    value = reputation.get("provider")
    return value if isinstance(value, str) else None


def _pluck_provider_label_from_breakdown(
    breakdown: dict[str, Any],
) -> str | None:
    return _pluck_provider_label(breakdown.get("provider_reputation"))


def _build_base_metadata(
    *,
    request: ValidationRequest,
    intel: dict[str, Any] | None,
    reputation: dict[str, Any] | None,
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
    return metadata


__all__ = [
    "ValidationEngineV2",
    "REASON_EXCLUDED_DOMAIN",
    "REASON_LOW_PRIORITY_CANDIDATE",
]
