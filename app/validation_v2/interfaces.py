"""Abstract contracts for the pluggable services used by ValidationEngineV2.

Every collaborator the engine talks to — domain intelligence,
provider reputation, exclusion, candidate selection, SMTP probing,
catch-all analysis, retry strategy, telemetry — is expressed here as
a pure ABC. Concrete implementations land in later subphases.

Keeping the contracts in one file makes the plug surface small and
auditable: the engine depends on these abstractions, not on any
particular implementation, so the test suite can inject
fully-isolated fakes without mocking library magic.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from .policy import ValidationPolicy
from .request import ValidationRequest


class DomainIntelligenceService(ABC):
    """Produces structured facts about a domain.

    Implementations may consult DNS caches, MX records, IP
    reputation lists, or any other source. The contract is
    deliberately loose — the return value is an opaque dict the
    engine stores in the result breakdown.
    """

    @abstractmethod
    def analyze(self, domain: str) -> dict[str, Any]:
        """Return a dict of domain-level facts for ``domain``.

        Implementations must be deterministic for a given domain
        during a single run (the engine may call ``analyze`` once
        and pass the result to downstream services).
        """
        raise NotImplementedError


class ProviderReputationService(ABC):
    """Classifies a domain against known-provider reputation data."""

    @abstractmethod
    def classify(self, domain: str) -> dict[str, Any]:
        """Return a reputation classification dict for ``domain``.

        Return shape is implementation-defined; the engine stores
        it verbatim in the result breakdown and may pull a
        top-level ``provider`` key out if present.
        """
        raise NotImplementedError


class ExclusionService(ABC):
    """Decides whether a request must be skipped by policy.

    Distinct from candidate selection: exclusion is a hard policy
    gate (e.g. blacklisted domain, opt-out list) that short-circuits
    before any scoring or probing. Candidate selection is a
    *soft* gate that considers score, confidence, and bucket.
    """

    @abstractmethod
    def is_excluded(
        self, request: ValidationRequest, policy: ValidationPolicy
    ) -> bool:
        """Return True if ``request`` must not be validated."""
        raise NotImplementedError


class ValidationCandidateSelector(ABC):
    """Decides whether a request is worth validating.

    A selector returns ``True`` to indicate the engine should
    proceed with domain intelligence / probing, or ``False`` to
    short-circuit with a lightweight result.
    """

    @abstractmethod
    def should_validate(
        self, request: ValidationRequest, policy: ValidationPolicy
    ) -> bool:
        """Return True if the request is a validation candidate."""
        raise NotImplementedError


class SMTPProbeClient(ABC):
    """Performs an SMTP probe against a mailbox.

    Implementations will open a network connection; the contract
    is defined here but no implementation exists yet in this
    subphase. The engine in this subphase never calls
    :meth:`probe`.
    """

    @abstractmethod
    def probe(self, request: ValidationRequest) -> dict[str, Any]:
        """Return an opaque probe-result dict for ``request``."""
        raise NotImplementedError


class CatchAllAnalyzer(ABC):
    """Classifies a domain's catch-all behaviour from a probe result."""

    @abstractmethod
    def assess(
        self, domain: str, probe_result: dict[str, Any]
    ) -> dict[str, Any]:
        """Return a catch-all classification dict."""
        raise NotImplementedError


class RetryStrategy(ABC):
    """Decides whether and how to retry a probe after an outcome."""

    @abstractmethod
    def decide(self, probe_result: dict[str, Any]) -> dict[str, Any]:
        """Return a retry decision dict.

        Expected keys (contract only; not enforced here):
          * ``retry`` — bool
          * ``delay_seconds`` — non-negative float
          * ``attempts_remaining`` — non-negative int
        """
        raise NotImplementedError


class TelemetrySink(ABC):
    """Receives structured telemetry events from the engine.

    The engine MUST tolerate missing telemetry: a null sink, a
    failing sink, or a sink that raises must not interrupt
    validation. Concrete sinks are responsible for their own
    resilience.
    """

    @abstractmethod
    def emit(self, event: dict[str, Any]) -> None:
        """Emit a single structured event."""
        raise NotImplementedError


__all__ = [
    "DomainIntelligenceService",
    "ProviderReputationService",
    "ExclusionService",
    "ValidationCandidateSelector",
    "SMTPProbeClient",
    "CatchAllAnalyzer",
    "RetryStrategy",
    "TelemetrySink",
]
