"""Persistent history records for Validation Engine V2.

These dataclasses are deliberately small transport records. They do
not infer reputation, mutate counters, or call the validation engine;
they only validate the shape of persisted history rows.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


def _validate_non_negative(name: str, value: int) -> None:
    if value < 0:
        raise ValueError(f"{name} must be >= 0")


def _validate_probability(name: str, value: float | None) -> None:
    if value is None:
        return
    if not 0.0 <= value <= 1.0:
        raise ValueError(f"{name} must be between 0 and 1")


@dataclass
class DomainHistoryRecord:
    domain: str
    provider_type: str | None
    provider_hint: str | None
    first_seen_at: float
    last_seen_at: float
    ttl_expires_at: float | None
    total_observations: int
    smtp_attempt_count: int
    smtp_valid_count: int
    smtp_invalid_count: int
    smtp_uncertain_count: int
    timeout_count: int
    retry_count: int
    catch_all_confirmed_count: int
    catch_all_likely_count: int
    catch_all_unlikely_count: int
    last_smtp_status: str | None
    last_catch_all_status: str | None
    last_deliverability_probability: float | None
    last_validation_status: str | None
    domain_reputation_score: float | None
    domain_reputation_confidence: float | None

    def __post_init__(self) -> None:
        if not self.domain:
            raise ValueError("domain must not be empty")
        for name in _DOMAIN_COUNT_FIELDS:
            _validate_non_negative(name, int(getattr(self, name)))
        _validate_probability(
            "last_deliverability_probability", self.last_deliverability_probability
        )
        _validate_probability("domain_reputation_score", self.domain_reputation_score)
        _validate_probability(
            "domain_reputation_confidence", self.domain_reputation_confidence
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ProviderHistoryRecord:
    provider_key: str
    provider_type: str | None
    first_seen_at: float
    last_seen_at: float
    ttl_expires_at: float | None
    total_domains_seen: int
    total_observations: int
    smtp_valid_count: int
    smtp_invalid_count: int
    smtp_uncertain_count: int
    timeout_count: int
    catch_all_confirmed_count: int
    catch_all_likely_count: int
    catch_all_unlikely_count: int
    provider_reputation_score: float | None
    provider_reputation_confidence: float | None

    def __post_init__(self) -> None:
        if not self.provider_key:
            raise ValueError("provider_key must not be empty")
        for name in _PROVIDER_COUNT_FIELDS:
            _validate_non_negative(name, int(getattr(self, name)))
        _validate_probability(
            "provider_reputation_score", self.provider_reputation_score
        )
        _validate_probability(
            "provider_reputation_confidence", self.provider_reputation_confidence
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ProbeEventRecord:
    event_id: str
    timestamp: float
    domain: str
    provider_key: str | None
    smtp_status: str | None
    smtp_code: int | None
    smtp_error_type: str | None
    catch_all_status: str | None
    retry_attempted: bool
    retry_outcome: str | None
    deliverability_probability: float | None
    validation_status: str | None

    def __post_init__(self) -> None:
        if not self.event_id:
            raise ValueError("event_id must not be empty")
        if not self.domain:
            raise ValueError("domain must not be empty")
        if self.smtp_code is not None and self.smtp_code < 0:
            raise ValueError("smtp_code must be >= 0")
        _validate_probability(
            "deliverability_probability", self.deliverability_probability
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


_DOMAIN_COUNT_FIELDS = (
    "total_observations",
    "smtp_attempt_count",
    "smtp_valid_count",
    "smtp_invalid_count",
    "smtp_uncertain_count",
    "timeout_count",
    "retry_count",
    "catch_all_confirmed_count",
    "catch_all_likely_count",
    "catch_all_unlikely_count",
)

_PROVIDER_COUNT_FIELDS = (
    "total_domains_seen",
    "total_observations",
    "smtp_valid_count",
    "smtp_invalid_count",
    "smtp_uncertain_count",
    "timeout_count",
    "catch_all_confirmed_count",
    "catch_all_likely_count",
    "catch_all_unlikely_count",
)


__all__ = [
    "DomainHistoryRecord",
    "ProviderHistoryRecord",
    "ProbeEventRecord",
]
