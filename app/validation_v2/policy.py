"""ValidationPolicy: read-only configuration for ValidationEngineV2.

A policy is a declarative bundle of operational limits and gates:
whether SMTP probing is enabled, how many probes per domain/run,
retry behaviour, cache TTL, excluded domains, and the criteria a
request must meet before it is considered a candidate for deeper
validation.

The policy carries no logic. Services consult it, but they do not
mutate it. Instances are frozen so accidental mutation is caught
at construction time rather than leaking between requests.
"""

from __future__ import annotations

from dataclasses import dataclass, field


def _default_allowed_buckets() -> frozenset[str]:
    # V2 scoring's ``review`` bucket is the primary validation
    # target: high_confidence rows already have strong evidence,
    # invalid rows are dead. ``high_confidence`` is included too so
    # operators can opt into re-verifying strong candidates before
    # an important send. Treat as read-only: frozenset is
    # immutable so downstream callers cannot accidentally mutate
    # the default.
    return frozenset({"review", "high_confidence"})


@dataclass(frozen=True, slots=True)
class ValidationPolicy:
    """Configuration bundle for ValidationEngineV2.

    Attributes:
        enable_smtp_probing: Master switch for the SMTP probe step.
            The skeleton engine never probes; this flag exists so
            future subphases and operators have a single kill
            switch.
        max_probes_per_domain: Upper bound on how many SMTP probes
            the engine may perform against a single domain in one
            run. Prevents a single noisy domain from dominating the
            probe budget.
        max_probes_per_run: Global cap on SMTP probes for a single
            run (CLI invocation, pipeline execution, etc.). Keeps
            runs within a predictable cost envelope.
        retry_enabled: Whether the retry strategy may schedule
            retries for transient failures.
        max_retries: Upper bound on retry attempts per request.
        cache_ttl_seconds: TTL (seconds) for cached validation
            artifacts (e.g. catch-all classifications, reputation
            lookups). Zero disables caching.
        excluded_domains: Set of domains that must never be
            validated (regardless of score). Stored as a
            ``frozenset`` so the policy remains truly immutable.
            Callers may pass a plain ``set`` to the constructor —
            it is frozen in ``__post_init__``.
        strong_candidate_min_score: Minimum V2 score a request must
            have to count as a strong candidate for deeper
            validation. Used by the default candidate-selector
            heuristic; custom selectors may ignore it.
        strong_candidate_min_confidence: Minimum V2 confidence
            required alongside ``strong_candidate_min_score``.
        allow_validation_for_buckets: Set of V2 bucket labels the
            engine is permitted to validate. Stored as a
            ``frozenset`` for immutability. Defaults to
            ``{"review", "high_confidence"}``.
    """

    enable_smtp_probing: bool = False
    max_probes_per_domain: int = 3
    max_probes_per_run: int = 500
    retry_enabled: bool = True
    max_retries: int = 2
    cache_ttl_seconds: int = 86_400
    excluded_domains: frozenset[str] = field(default_factory=frozenset)
    strong_candidate_min_score: float = 0.40
    strong_candidate_min_confidence: float = 0.60
    allow_validation_for_buckets: frozenset[str] = field(
        default_factory=_default_allowed_buckets
    )

    def __post_init__(self) -> None:
        # Coerce any iterable set-like container to frozenset. The
        # public API documents ``set[str]`` for ergonomics; we
        # enforce immutability underneath. ``object.__setattr__``
        # is required on a frozen dataclass.
        if not isinstance(self.excluded_domains, frozenset):
            object.__setattr__(
                self, "excluded_domains", frozenset(self.excluded_domains)
            )
        if not isinstance(self.allow_validation_for_buckets, frozenset):
            object.__setattr__(
                self,
                "allow_validation_for_buckets",
                frozenset(self.allow_validation_for_buckets),
            )

        # Basic sanity checks. Cheap to run, prevents
        # silently-bad policies from entering production.
        if self.max_probes_per_domain < 0:
            raise ValueError("max_probes_per_domain must be >= 0")
        if self.max_probes_per_run < 0:
            raise ValueError("max_probes_per_run must be >= 0")
        if self.max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if self.cache_ttl_seconds < 0:
            raise ValueError("cache_ttl_seconds must be >= 0")
        if not 0.0 <= self.strong_candidate_min_score <= 1.0:
            raise ValueError("strong_candidate_min_score must be in [0.0, 1.0]")
        if not 0.0 <= self.strong_candidate_min_confidence <= 1.0:
            raise ValueError(
                "strong_candidate_min_confidence must be in [0.0, 1.0]"
            )

    def with_overrides(self, **overrides: object) -> "ValidationPolicy":
        """Return a new policy with ``overrides`` applied.

        Convenience for tests and operator scripts that need a
        one-field tweak without writing out the full constructor.
        Because the policy is frozen, mutation has to go through
        reconstruction anyway — this method just makes the intent
        explicit.
        """
        current: dict[str, object] = {
            "enable_smtp_probing": self.enable_smtp_probing,
            "max_probes_per_domain": self.max_probes_per_domain,
            "max_probes_per_run": self.max_probes_per_run,
            "retry_enabled": self.retry_enabled,
            "max_retries": self.max_retries,
            "cache_ttl_seconds": self.cache_ttl_seconds,
            "excluded_domains": self.excluded_domains,
            "strong_candidate_min_score": self.strong_candidate_min_score,
            "strong_candidate_min_confidence": self.strong_candidate_min_confidence,
            "allow_validation_for_buckets": self.allow_validation_for_buckets,
        }
        current.update(overrides)
        return ValidationPolicy(**current)  # type: ignore[arg-type]


__all__ = ["ValidationPolicy"]
