"""ValidationEngineV2: orchestration for the passive-intelligence layer.

The engine wires together the pluggable services defined in
:mod:`app.validation_v2.interfaces` under the rules encoded in a
:class:`~app.validation_v2.policy.ValidationPolicy`. It is the
single orchestration surface: subphases add logic behind the
interfaces, the engine itself changes minimally.

Current surface (Subphase 2 — passive intelligence):

    * Domain intelligence + provider reputation run first. Their
      outputs are collected into the result breakdown and mirrored
      into a compact metadata block. They run on every path,
      including the exclusion and candidate-skip short-circuits,
      so downstream consumers always see the same structured
      evidence.

    * Exclusion gate (short-circuit). If an ``ExclusionService`` is
      present and flags the request, the engine returns a canonical
      ``excluded_by_policy`` result with the collected intel
      attached. When the service exposes the richer ``check`` API
      (see :class:`~app.validation_v2.services.exclusion.\
DefaultExclusionService`) the engine surfaces the matching
      fine-grained reason (e.g. ``excluded_domain``).

    * Candidate-selection gate (short-circuit). If a
      ``ValidationCandidateSelector`` declines the request, the
      engine returns a ``deliverable_uncertain`` result with a
      ``not_a_candidate`` / ``low_priority_candidate`` reason.
      When the selector exposes the richer ``explain`` API the
      fine-grained reason is surfaced.

    * Happy path returns a structured placeholder result. The
      engine never calls SMTP / catch-all / retry / telemetry
      services beyond best-effort observability emission, and it
      never mutates its input.

The constructor signature is stable. Future subphases (SMTP,
catch-all, retries) will plug in without changing it.
"""

from __future__ import annotations

from typing import Any

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

        Steps:

          1. Collect domain intelligence (if wired).
          2. Collect provider reputation (if wired).
          3. Exclusion gate — short-circuit on hit.
          4. Candidate selection gate — short-circuit on skip.
          5. Build a structured placeholder result carrying every
             collected signal.

        The method does not mutate ``request`` and does not make
        network calls.
        """
        # -- 1-2. Collect passive intelligence ---------------------------
        intel = self._run_domain_intel(request)
        reputation = self._run_provider_reputation(request)
        provider_label = _pluck_provider_label(reputation)
        breakdown = self._build_base_breakdown(intel, reputation)
        base_metadata = _build_base_metadata(
            request=request,
            intel=intel,
            reputation=reputation,
        )

        # -- 3. Exclusion -------------------------------------------------
        exclusion_reason = self._check_exclusion(request)
        if exclusion_reason is not None:
            return self._build_excluded_result(
                request=request,
                breakdown=breakdown,
                metadata=base_metadata,
                exclusion_reason=exclusion_reason,
            )

        # -- 4. Candidate selection --------------------------------------
        candidate_accepted, candidate_reason = self._check_candidate(request)
        if not candidate_accepted:
            return self._build_skipped_result(
                request=request,
                breakdown=breakdown,
                metadata=base_metadata,
                candidate_reason=candidate_reason,
            )

        # -- 5. Happy path placeholder -----------------------------------
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
        """Emit a telemetry event, swallowing any error.

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
