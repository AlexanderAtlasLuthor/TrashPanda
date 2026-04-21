"""Deterministic reputation formulas for Validation Engine V2 history.

Scores are bounded quality estimates. Confidence is separate and
answers "how much should a future phase trust this historical score?"

Confidence formula:
    * Base: min(total_observations / 20, 1.0).
    * Staleness penalty: multiply by 0.6 when ttl_expires_at is present
      and older than last_seen_at. This keeps the function pure and
      deterministic without a wall-clock dependency.
    * Contradiction penalty: multiply by 0.7 when both valid and invalid
      SMTP rates are at least 30%, meaning the history is mixed enough to
      be less reliable.
"""

from __future__ import annotations

from .models import DomainHistoryRecord, ProviderHistoryRecord


def compute_domain_reputation_score(record: DomainHistoryRecord) -> float | None:
    if record.smtp_attempt_count <= 0:
        return None

    valid_rate = _safe_rate(record.smtp_valid_count, record.smtp_attempt_count)
    invalid_rate = _safe_rate(record.smtp_invalid_count, record.smtp_attempt_count)
    uncertain_rate = _safe_rate(record.smtp_uncertain_count, record.smtp_attempt_count)
    timeout_rate = _safe_rate(record.timeout_count, record.smtp_attempt_count)
    catch_all_risk = _domain_catch_all_risk(record)

    score = (
        0.55 * valid_rate
        + 0.15 * (1.0 - invalid_rate)
        + 0.10 * (1.0 - uncertain_rate)
        + 0.10 * (1.0 - timeout_rate)
        + 0.10 * (1.0 - catch_all_risk)
    )
    return _clamp(score)


def compute_domain_reputation_confidence(record: DomainHistoryRecord) -> float | None:
    if record.total_observations <= 0:
        return None

    confidence = min(float(record.total_observations) / 20.0, 1.0)
    if _stored_record_is_stale(record.last_seen_at, record.ttl_expires_at):
        confidence *= 0.6
    if _has_contradictory_smtp(record.smtp_valid_count, record.smtp_invalid_count, record.smtp_attempt_count):
        confidence *= 0.7
    return _clamp(confidence)


def compute_provider_reputation_score(record: ProviderHistoryRecord) -> float | None:
    attempts = (
        record.smtp_valid_count
        + record.smtp_invalid_count
        + record.smtp_uncertain_count
    )
    if attempts <= 0:
        return None

    valid_rate = _safe_rate(record.smtp_valid_count, attempts)
    invalid_rate = _safe_rate(record.smtp_invalid_count, attempts)
    uncertain_rate = _safe_rate(record.smtp_uncertain_count, attempts)
    timeout_rate = _safe_rate(record.timeout_count, attempts)
    catch_all_risk = _provider_catch_all_risk(record)

    score = (
        0.55 * valid_rate
        + 0.15 * (1.0 - invalid_rate)
        + 0.10 * (1.0 - uncertain_rate)
        + 0.10 * (1.0 - timeout_rate)
        + 0.10 * (1.0 - catch_all_risk)
    )
    return _clamp(score)


def compute_provider_reputation_confidence(
    record: ProviderHistoryRecord,
) -> float | None:
    if record.total_observations <= 0:
        return None

    attempts = (
        record.smtp_valid_count
        + record.smtp_invalid_count
        + record.smtp_uncertain_count
    )
    confidence = min(float(record.total_observations) / 20.0, 1.0)
    if _stored_record_is_stale(record.last_seen_at, record.ttl_expires_at):
        confidence *= 0.6
    if _has_contradictory_smtp(record.smtp_valid_count, record.smtp_invalid_count, attempts):
        confidence *= 0.7
    return _clamp(confidence)


def _domain_catch_all_risk(record: DomainHistoryRecord) -> float:
    total = (
        record.catch_all_confirmed_count
        + record.catch_all_likely_count
        + record.catch_all_unlikely_count
    )
    if total <= 0:
        return 0.0
    risky = record.catch_all_confirmed_count + 0.5 * record.catch_all_likely_count
    return _clamp(risky / float(total))


def _provider_catch_all_risk(record: ProviderHistoryRecord) -> float:
    total = (
        record.catch_all_confirmed_count
        + record.catch_all_likely_count
        + record.catch_all_unlikely_count
    )
    if total <= 0:
        return 0.0
    risky = record.catch_all_confirmed_count + 0.5 * record.catch_all_likely_count
    return _clamp(risky / float(total))


def _has_contradictory_smtp(valid_count: int, invalid_count: int, attempts: int) -> bool:
    if attempts <= 0:
        return False
    return (
        _safe_rate(valid_count, attempts) >= 0.3
        and _safe_rate(invalid_count, attempts) >= 0.3
    )


def _stored_record_is_stale(last_seen_at: float, ttl_expires_at: float | None) -> bool:
    return ttl_expires_at is not None and ttl_expires_at <= last_seen_at


def _safe_rate(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator) / float(denominator)


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


__all__ = [
    "compute_domain_reputation_score",
    "compute_domain_reputation_confidence",
    "compute_provider_reputation_score",
    "compute_provider_reputation_confidence",
]
