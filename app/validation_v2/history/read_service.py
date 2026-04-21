"""Read-only historical intelligence for Validation Engine V2."""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass
from typing import Any

from .domain_store import DomainHistoryStore
from .models import DomainHistoryRecord, ProviderHistoryRecord
from .provider_store import ProviderHistoryStore
from .reputation import (
    compute_domain_reputation_confidence,
    compute_domain_reputation_score,
    compute_provider_reputation_confidence,
    compute_provider_reputation_score,
)


DEFAULT_STALE_AFTER_SECONDS = 30 * 24 * 3600


@dataclass(frozen=True)
class HistoricalIntelligence:
    domain: str
    provider_key: str | None
    history_cache_hit: bool
    historical_domain_reputation: float | None
    historical_domain_reputation_confidence: float | None
    historical_smtp_valid_rate: float | None
    historical_smtp_invalid_rate: float | None
    historical_smtp_uncertain_rate: float | None
    historical_timeout_rate: float | None
    historical_catch_all_risk: float | None
    domain_observation_count: int
    domain_last_seen_at: float | None
    historical_provider_reputation: float | None
    historical_provider_reputation_confidence: float | None
    provider_observation_count: int
    provider_last_seen_at: float | None
    domain_history_stale: bool
    provider_history_stale: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class HistoricalIntelligenceService:
    """Read-only summarizer over persistent domain/provider history."""

    def __init__(
        self,
        domain_store: DomainHistoryStore | None = None,
        provider_store: ProviderHistoryStore | None = None,
        stale_after_seconds: int = DEFAULT_STALE_AFTER_SECONDS,
    ) -> None:
        if stale_after_seconds < 0:
            raise ValueError("stale_after_seconds must be >= 0")
        self._domain_store = domain_store
        self._provider_store = provider_store
        self._stale_after_seconds = int(stale_after_seconds)

    def fetch(
        self,
        domain: str,
        provider_key: str | None = None,
        now: float | None = None,
    ) -> HistoricalIntelligence:
        current_time = time.time() if now is None else float(now)
        domain_record = self._get_domain_record(domain)
        provider_record = self._get_provider_record(provider_key)

        return HistoricalIntelligence(
            domain=domain,
            provider_key=provider_key,
            history_cache_hit=domain_record is not None or provider_record is not None,
            historical_domain_reputation=(
                _domain_reputation_score(domain_record)
            ),
            historical_domain_reputation_confidence=(
                _domain_reputation_confidence(domain_record)
            ),
            historical_smtp_valid_rate=_domain_smtp_rate(
                domain_record, "smtp_valid_count"
            ),
            historical_smtp_invalid_rate=_domain_smtp_rate(
                domain_record, "smtp_invalid_count"
            ),
            historical_smtp_uncertain_rate=_domain_smtp_rate(
                domain_record, "smtp_uncertain_count"
            ),
            historical_timeout_rate=_domain_smtp_rate(
                domain_record, "timeout_count"
            ),
            historical_catch_all_risk=_catch_all_risk(domain_record),
            domain_observation_count=(
                domain_record.total_observations if domain_record else 0
            ),
            domain_last_seen_at=domain_record.last_seen_at if domain_record else None,
            historical_provider_reputation=(
                _provider_reputation_score(provider_record)
            ),
            historical_provider_reputation_confidence=(
                _provider_reputation_confidence(provider_record)
            ),
            provider_observation_count=(
                provider_record.total_observations if provider_record else 0
            ),
            provider_last_seen_at=(
                provider_record.last_seen_at if provider_record else None
            ),
            domain_history_stale=_is_stale(
                domain_record.last_seen_at if domain_record else None,
                self._stale_after_seconds,
                current_time,
            ),
            provider_history_stale=_is_stale(
                provider_record.last_seen_at if provider_record else None,
                self._stale_after_seconds,
                current_time,
            ),
        )

    def _get_domain_record(self, domain: str) -> DomainHistoryRecord | None:
        if self._domain_store is None:
            return None
        return self._domain_store.get(domain)

    def _get_provider_record(
        self, provider_key: str | None
    ) -> ProviderHistoryRecord | None:
        if self._provider_store is None or provider_key is None:
            return None
        return self._provider_store.get(provider_key)


def _safe_rate(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return float(numerator) / float(denominator)


def _is_stale(
    last_seen_at: float | None, stale_after_seconds: int, now: float
) -> bool:
    if last_seen_at is None:
        return False
    return float(now) - float(last_seen_at) > float(stale_after_seconds)


def _domain_smtp_rate(
    record: DomainHistoryRecord | None, count_field: str
) -> float | None:
    if record is None:
        return None
    return _safe_rate(int(getattr(record, count_field)), record.smtp_attempt_count)


def _catch_all_risk(record: DomainHistoryRecord | None) -> float | None:
    if record is None:
        return None
    risky = record.catch_all_confirmed_count + record.catch_all_likely_count
    total = risky + record.catch_all_unlikely_count
    return _safe_rate(risky, total)


def _domain_reputation_score(record: DomainHistoryRecord | None) -> float | None:
    if record is None:
        return None
    if record.domain_reputation_score is not None:
        return record.domain_reputation_score
    return compute_domain_reputation_score(record)


def _domain_reputation_confidence(
    record: DomainHistoryRecord | None,
) -> float | None:
    if record is None:
        return None
    if record.domain_reputation_confidence is not None:
        return record.domain_reputation_confidence
    return compute_domain_reputation_confidence(record)


def _provider_reputation_score(
    record: ProviderHistoryRecord | None,
) -> float | None:
    if record is None:
        return None
    if record.provider_reputation_score is not None:
        return record.provider_reputation_score
    return compute_provider_reputation_score(record)


def _provider_reputation_confidence(
    record: ProviderHistoryRecord | None,
) -> float | None:
    if record is None:
        return None
    if record.provider_reputation_confidence is not None:
        return record.provider_reputation_confidence
    return compute_provider_reputation_confidence(record)


__all__ = [
    "HistoricalIntelligence",
    "HistoricalIntelligenceService",
    "_safe_rate",
    "_is_stale",
]
