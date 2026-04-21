"""ValidationRequest: immutable input bundle for ValidationEngineV2.

A request is the shape the engine accepts — already-normalized row
data plus the V2 scoring output. Making the request frozen and
typed means the engine cannot accidentally mutate caller state,
and every downstream service (exclusion, candidate selection,
future SMTP probe, telemetry) sees exactly the same immutable view.

This module has no dependencies on other V2 modules and no side
effects. It is safe to import from anywhere in the package.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Mapping


@dataclass(frozen=True, slots=True)
class ValidationRequest:
    """Frozen input to :meth:`ValidationEngineV2.validate`.

    Attributes:
        email: Normalized email address (lowercased, trimmed). The
            engine trusts this is already normalized — upstream
            stages are responsible for cleanup.
        domain: Normalized domain portion of ``email``.
        corrected_domain: If the syntax/typo stage rewrote the
            domain (e.g. ``gmial.com`` → ``gmail.com``) the
            corrected value lives here. ``None`` when no correction
            was applied.
        syntax_valid: Whether the syntax-validation stage accepted
            the address.
        domain_present: Whether a non-empty domain portion was
            present after normalization.
        score_v2: Final V2 score in [0.0, 1.0].
        confidence_v2: V2 confidence aggregate in [0.0, 1.0].
        bucket_v2: V2 bucket label (e.g. ``"high_confidence"``,
            ``"review"``, ``"invalid"``). The request does not
            validate this against a fixed vocabulary — V2 scoring
            owns that set.
        reason_codes_v2: Tuple of reason codes the V2 engine emitted,
            in arrival order. Tuple (not list) so the request stays
            hashable and genuinely immutable.
        metadata: Extension dict for upstream stages to pass
            arbitrary context forward (e.g. DNS cache hits,
            normalizer diagnostics) without forcing a schema
            change. Stored as a read-only mapping so even the
            metadata cannot be mutated after construction.
    """

    email: str
    domain: str
    corrected_domain: str | None
    syntax_valid: bool
    domain_present: bool
    score_v2: float
    confidence_v2: float
    bucket_v2: str
    reason_codes_v2: tuple[str, ...]
    metadata: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))

    def __post_init__(self) -> None:
        # Type validation. We use explicit isinstance checks rather
        # than typing-runtime tricks: the request is a public
        # boundary and constructor-time errors should be loud.
        _require_type("email", self.email, str)
        _require_type("domain", self.domain, str)
        if self.corrected_domain is not None:
            _require_type("corrected_domain", self.corrected_domain, str)
        _require_type("syntax_valid", self.syntax_valid, bool)
        _require_type("domain_present", self.domain_present, bool)
        _require_number("score_v2", self.score_v2)
        _require_number("confidence_v2", self.confidence_v2)
        _require_type("bucket_v2", self.bucket_v2, str)

        if not isinstance(self.reason_codes_v2, tuple):
            raise TypeError(
                "ValidationRequest.reason_codes_v2 must be a tuple, got "
                f"{type(self.reason_codes_v2).__name__}"
            )
        for i, rc in enumerate(self.reason_codes_v2):
            if not isinstance(rc, str):
                raise TypeError(
                    f"ValidationRequest.reason_codes_v2[{i}] must be str, "
                    f"got {type(rc).__name__}"
                )

        if not isinstance(self.metadata, Mapping):
            raise TypeError(
                "ValidationRequest.metadata must be a Mapping, got "
                f"{type(self.metadata).__name__}"
            )
        # Wrap the metadata in a read-only view so the request is
        # genuinely immutable end-to-end. ``object.__setattr__`` is
        # required because the dataclass is frozen.
        if not isinstance(self.metadata, MappingProxyType):
            object.__setattr__(
                self, "metadata", MappingProxyType(dict(self.metadata))
            )


def _require_type(field_name: str, value: Any, expected: type) -> None:
    if expected is bool:
        if not isinstance(value, bool):
            raise TypeError(
                f"ValidationRequest.{field_name} must be bool, "
                f"got {type(value).__name__}"
            )
        return
    if not isinstance(value, expected):
        raise TypeError(
            f"ValidationRequest.{field_name} must be {expected.__name__}, "
            f"got {type(value).__name__}"
        )


def _require_number(field_name: str, value: Any) -> None:
    # Reject bool explicitly — ``True`` is an ``int`` in Python and
    # would silently pass a numeric check.
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(
            f"ValidationRequest.{field_name} must be a number, "
            f"got {type(value).__name__}"
        )


__all__ = ["ValidationRequest"]
