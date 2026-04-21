"""ValidationEngineV2: orchestration skeleton.

The engine wires together the pluggable services defined in
:mod:`app.validation_v2.interfaces` under the rules encoded in a
:class:`~app.validation_v2.policy.ValidationPolicy`. It is the
single orchestration surface: subphases add logic behind the
interfaces, the engine itself changes minimally.

This subphase implements the skeleton only:

    * Exclusion gate — if an ``ExclusionService`` is present and it
      excludes the request, return a canonical
      ``excluded_by_policy`` result immediately.

    * Candidate selection — if a ``ValidationCandidateSelector`` is
      present and declines the request, return a lightweight
      ``deliverable_uncertain`` result with a ``not_a_candidate``
      reason code.

    * Domain intelligence + provider reputation — if provided,
      their outputs land in the result breakdown. No validation
      decisions are made from them yet.

    * SMTP / catch-all / retry / telemetry — NEVER invoked in this
      subphase. Telemetry is emitted best-effort for observability
      only; failures are swallowed.

The engine never mutates its input and never performs network
calls.
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


class ValidationEngineV2:
    """Orchestration skeleton for V2 validation.

    Construct with a policy plus any combination of optional
    collaborator services. Missing services degrade gracefully:
    if no ``ExclusionService`` is wired, exclusion is never
    applied; if no ``ValidationCandidateSelector`` is wired, every
    non-excluded request proceeds.
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
        """Run the current skeleton validation flow for ``request``.

        Steps:

          1. Exclusion gate.
          2. Candidate selection gate.
          3. Collect domain intelligence + provider reputation
             (purely observational in this subphase).
          4. Return a structured placeholder result.

        The method does not mutate ``request`` and does not make
        network calls.
        """
        # -- 1. Exclusion -------------------------------------------------
        if self._exclusion_service is not None and self._exclusion_service.is_excluded(
            request, self._policy
        ):
            result = ValidationResult(
                validation_status=ValidationStatus.EXCLUDED_BY_POLICY.value,
                deliverability_probability=0.0,
                smtp_probe_status=SmtpProbeStatus.SKIPPED.value,
                catch_all_status=None,
                provider_reputation=None,
                retry_recommended=False,
                validation_reason_codes=(ReasonCode.EXCLUDED_BY_POLICY.value,),
                validation_explanation=(
                    f"Request excluded by policy: domain={request.domain!r}."
                ),
                breakdown={"excluded": True, "domain": request.domain},
                metadata={"email": request.email},
            )
            self._emit({"event": "excluded", "domain": request.domain})
            return result

        # -- 2. Candidate selection --------------------------------------
        if (
            self._candidate_selector is not None
            and not self._candidate_selector.should_validate(request, self._policy)
        ):
            result = ValidationResult(
                validation_status=ValidationStatus.DELIVERABLE_UNCERTAIN.value,
                deliverability_probability=0.0,
                smtp_probe_status=SmtpProbeStatus.SKIPPED.value,
                catch_all_status=None,
                provider_reputation=None,
                retry_recommended=False,
                validation_reason_codes=(
                    ReasonCode.NOT_A_CANDIDATE.value,
                    ReasonCode.VALIDATION_SKIPPED.value,
                ),
                validation_explanation=(
                    "Request is not a validation candidate under current policy."
                ),
                breakdown={"candidate": False, "domain": request.domain},
                metadata={"email": request.email},
            )
            self._emit({"event": "skipped_not_candidate", "domain": request.domain})
            return result

        # -- 3. Domain intelligence + provider reputation ----------------
        breakdown: dict[str, Any] = {}
        provider_label: str | None = None

        if self._domain_intel is not None:
            intel = self._domain_intel.analyze(request.domain)
            breakdown["domain_intelligence"] = dict(intel) if intel else {}

        if self._provider_reputation is not None:
            reputation = self._provider_reputation.classify(request.domain)
            if reputation:
                breakdown["provider_reputation"] = dict(reputation)
                # Pull a ``provider`` key out if the service
                # provided one — convenient for downstream
                # consumers without forcing them to parse the
                # opaque dict.
                pv = reputation.get("provider")
                if isinstance(pv, str):
                    provider_label = pv
            else:
                breakdown["provider_reputation"] = {}

        # -- 4. Placeholder structured result ----------------------------
        # The skeleton does not (yet) alter status or probability
        # based on the collected intel. Future subphases will read
        # the breakdown and decide.
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
                "Skeleton engine: no active probe performed. "
                f"domain={request.domain!r}, bucket_v2={request.bucket_v2!r}."
            ),
            breakdown=breakdown,
            metadata={"email": request.email, "domain": request.domain},
        )
        self._emit({"event": "skeleton_completed", "domain": request.domain})
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


__all__ = ["ValidationEngineV2"]
