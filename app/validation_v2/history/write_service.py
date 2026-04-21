"""Post-validation learning writes for Validation Engine V2 history."""

from __future__ import annotations

import time
from typing import Any, Callable

from ..request import ValidationRequest
from ..result import ValidationResult
from .domain_store import DomainHistoryStore
from .models import DomainHistoryRecord, ProbeEventRecord, ProviderHistoryRecord
from .probe_event_store import ProbeEventStore
from .provider_store import ProviderHistoryStore
from .reputation import (
    compute_domain_reputation_confidence,
    compute_domain_reputation_score,
    compute_provider_reputation_confidence,
    compute_provider_reputation_score,
)
from .ttl import compute_ttl_expiry


DEFAULT_HISTORY_TTL_SECONDS = 30 * 24 * 3600
SMTP_NOT_ATTEMPTED = "not_attempted"


class ReputationLearningService:
    """Write completed validation outcomes into persistent history stores.

    This service is intentionally post-facto. It computes counters from
    already-produced request/result data and never returns signals to the
    validation decision path.
    """

    def __init__(
        self,
        domain_store: DomainHistoryStore | None = None,
        provider_store: ProviderHistoryStore | None = None,
        probe_event_store: ProbeEventStore | None = None,
        ttl_seconds: int | None = DEFAULT_HISTORY_TTL_SECONDS,
        time_fn: Callable[[], float] = time.time,
    ) -> None:
        if ttl_seconds is not None and ttl_seconds < 0:
            raise ValueError("ttl_seconds must be >= 0")
        self._domain_store = domain_store
        self._provider_store = provider_store
        self._probe_event_store = probe_event_store
        self._ttl_seconds = ttl_seconds
        self._time_fn = time_fn
        self._event_counter = 0

    def record_validation(
        self,
        request: ValidationRequest,
        result: ValidationResult,
        smtp_result: dict | None = None,
        catch_all: dict | None = None,
        historical: dict | None = None,
    ) -> None:
        """Persist learning from a completed validation, swallowing failures."""
        try:
            self._record_validation(
                request=request,
                result=result,
                smtp_result=smtp_result,
                catch_all=catch_all,
                historical=historical,
            )
        except Exception:
            return

    def _record_validation(
        self,
        *,
        request: ValidationRequest,
        result: ValidationResult,
        smtp_result: dict | None,
        catch_all: dict | None,
        historical: dict | None,
    ) -> None:
        now = float(self._time_fn())
        expires_at = compute_ttl_expiry(now, self._ttl_seconds)
        smtp = _smtp_summary_from(result, smtp_result)
        catch_all_summary = _catch_all_summary_from(result, catch_all)
        provider_key = _provider_key_from(result, historical)
        provider_type = _provider_type_from(result)

        existing_domain = self._get_domain(request.domain)
        if self._domain_store is not None:
            domain_record = _with_domain_reputation(
                _updated_domain_record(
                    existing=existing_domain,
                    request=request,
                    result=result,
                    smtp=smtp,
                    catch_all=catch_all_summary,
                    provider_key=provider_key,
                    provider_type=provider_type,
                    now=now,
                    expires_at=expires_at,
                )
            )
            self._domain_store.upsert(domain_record)

        if provider_key is not None and self._provider_store is not None:
            existing_provider = self._get_provider(provider_key)
            provider_record = _with_provider_reputation(
                _updated_provider_record(
                    existing=existing_provider,
                    existing_domain=existing_domain,
                    request=request,
                    provider_key=provider_key,
                    provider_type=provider_type,
                    smtp=smtp,
                    catch_all=catch_all_summary,
                    now=now,
                    expires_at=expires_at,
                )
            )
            self._provider_store.upsert(provider_record)

        if self._probe_event_store is not None and _has_probe_signal(
            smtp, catch_all_summary, result
        ):
            self._probe_event_store.append(
                ProbeEventRecord(
                    event_id=self._next_event_id(request.domain, now),
                    timestamp=now,
                    domain=request.domain,
                    provider_key=provider_key,
                    smtp_status=smtp["smtp_status"],
                    smtp_code=smtp["smtp_code"],
                    smtp_error_type=smtp["smtp_error_type"],
                    catch_all_status=catch_all_summary["catch_all_status"],
                    retry_attempted=bool(smtp["retry_attempted"]),
                    retry_outcome=smtp["retry_outcome"],
                    deliverability_probability=result.deliverability_probability,
                    validation_status=result.validation_status,
                )
            )

    def _get_domain(self, domain: str) -> DomainHistoryRecord | None:
        if self._domain_store is None:
            return None
        return self._domain_store.get(domain)

    def _get_provider(self, provider_key: str) -> ProviderHistoryRecord | None:
        if self._provider_store is None:
            return None
        return self._provider_store.get(provider_key)

    def _next_event_id(self, domain: str, timestamp: float) -> str:
        self._event_counter += 1
        safe_domain = domain.replace(":", "_")
        return f"{safe_domain}:{timestamp:.6f}:{self._event_counter}"


def _updated_domain_record(
    *,
    existing: DomainHistoryRecord | None,
    request: ValidationRequest,
    result: ValidationResult,
    smtp: dict[str, Any],
    catch_all: dict[str, Any],
    provider_key: str | None,
    provider_type: str | None,
    now: float,
    expires_at: float | None,
) -> DomainHistoryRecord:
    record = existing or DomainHistoryRecord(
        domain=request.domain,
        provider_type=None,
        provider_hint=None,
        first_seen_at=now,
        last_seen_at=now,
        ttl_expires_at=expires_at,
        total_observations=0,
        smtp_attempt_count=0,
        smtp_valid_count=0,
        smtp_invalid_count=0,
        smtp_uncertain_count=0,
        timeout_count=0,
        retry_count=0,
        catch_all_confirmed_count=0,
        catch_all_likely_count=0,
        catch_all_unlikely_count=0,
        last_smtp_status=None,
        last_catch_all_status=None,
        last_deliverability_probability=None,
        last_validation_status=None,
        domain_reputation_score=None,
        domain_reputation_confidence=None,
    )

    smtp_status = smtp["smtp_status"]
    catch_all_status = catch_all["catch_all_status"]
    return DomainHistoryRecord(
        domain=record.domain,
        provider_type=provider_type or record.provider_type,
        provider_hint=provider_key or record.provider_hint,
        first_seen_at=record.first_seen_at,
        last_seen_at=now,
        ttl_expires_at=expires_at,
        total_observations=record.total_observations + 1,
        smtp_attempt_count=record.smtp_attempt_count + int(_smtp_attempted(smtp)),
        smtp_valid_count=record.smtp_valid_count + int(smtp_status == "valid"),
        smtp_invalid_count=record.smtp_invalid_count + int(smtp_status == "invalid"),
        smtp_uncertain_count=(
            record.smtp_uncertain_count + int(_smtp_uncertain(smtp_status))
        ),
        timeout_count=record.timeout_count + int(_is_timeout_like(smtp)),
        retry_count=record.retry_count + int(bool(smtp["retry_attempted"])),
        catch_all_confirmed_count=(
            record.catch_all_confirmed_count + int(catch_all_status == "confirmed")
        ),
        catch_all_likely_count=(
            record.catch_all_likely_count + int(catch_all_status == "likely")
        ),
        catch_all_unlikely_count=(
            record.catch_all_unlikely_count + int(catch_all_status == "unlikely")
        ),
        last_smtp_status=smtp_status or record.last_smtp_status,
        last_catch_all_status=catch_all_status or record.last_catch_all_status,
        last_deliverability_probability=result.deliverability_probability,
        last_validation_status=result.validation_status,
        domain_reputation_score=record.domain_reputation_score,
        domain_reputation_confidence=record.domain_reputation_confidence,
    )


def _updated_provider_record(
    *,
    existing: ProviderHistoryRecord | None,
    existing_domain: DomainHistoryRecord | None,
    request: ValidationRequest,
    provider_key: str,
    provider_type: str | None,
    smtp: dict[str, Any],
    catch_all: dict[str, Any],
    now: float,
    expires_at: float | None,
) -> ProviderHistoryRecord:
    record = existing or ProviderHistoryRecord(
        provider_key=provider_key,
        provider_type=provider_type,
        first_seen_at=now,
        last_seen_at=now,
        ttl_expires_at=expires_at,
        total_domains_seen=0,
        total_observations=0,
        smtp_valid_count=0,
        smtp_invalid_count=0,
        smtp_uncertain_count=0,
        timeout_count=0,
        catch_all_confirmed_count=0,
        catch_all_likely_count=0,
        catch_all_unlikely_count=0,
        provider_reputation_score=None,
        provider_reputation_confidence=None,
    )

    smtp_status = smtp["smtp_status"]
    catch_all_status = catch_all["catch_all_status"]
    # Best-effort unique-domain tracking for the first SQLite schema:
    # without a provider-domain join table, a missing domain record is the
    # safest signal that this provider is seeing the domain for the first time.
    new_domain_for_provider = existing_domain is None or (
        existing_domain.provider_hint not in (None, provider_key)
    )
    return ProviderHistoryRecord(
        provider_key=record.provider_key,
        provider_type=provider_type or record.provider_type,
        first_seen_at=record.first_seen_at,
        last_seen_at=now,
        ttl_expires_at=expires_at,
        total_domains_seen=record.total_domains_seen
        + int(new_domain_for_provider),
        total_observations=record.total_observations + 1,
        smtp_valid_count=record.smtp_valid_count + int(smtp_status == "valid"),
        smtp_invalid_count=record.smtp_invalid_count + int(smtp_status == "invalid"),
        smtp_uncertain_count=(
            record.smtp_uncertain_count + int(_smtp_uncertain(smtp_status))
        ),
        timeout_count=record.timeout_count + int(_is_timeout_like(smtp)),
        catch_all_confirmed_count=(
            record.catch_all_confirmed_count + int(catch_all_status == "confirmed")
        ),
        catch_all_likely_count=(
            record.catch_all_likely_count + int(catch_all_status == "likely")
        ),
        catch_all_unlikely_count=(
            record.catch_all_unlikely_count + int(catch_all_status == "unlikely")
        ),
        provider_reputation_score=record.provider_reputation_score,
        provider_reputation_confidence=record.provider_reputation_confidence,
    )


def _with_domain_reputation(record: DomainHistoryRecord) -> DomainHistoryRecord:
    return DomainHistoryRecord(
        **{
            **record.to_dict(),
            "domain_reputation_score": compute_domain_reputation_score(record),
            "domain_reputation_confidence": (
                compute_domain_reputation_confidence(record)
            ),
        }
    )


def _with_provider_reputation(record: ProviderHistoryRecord) -> ProviderHistoryRecord:
    return ProviderHistoryRecord(
        **{
            **record.to_dict(),
            "provider_reputation_score": compute_provider_reputation_score(record),
            "provider_reputation_confidence": (
                compute_provider_reputation_confidence(record)
            ),
        }
    )


def _smtp_summary_from(
    result: ValidationResult, smtp_result: dict | None
) -> dict[str, Any]:
    source = dict(smtp_result or {})
    retry_outcome = source.get("retry_outcome", result.retry_outcome)
    return {
        "smtp_status": source.get("smtp_status", result.smtp_status),
        "smtp_code": source.get("smtp_code", result.smtp_code),
        "smtp_error_type": source.get("smtp_error_type", result.smtp_error_type),
        "retry_attempted": source.get(
            "retry_attempted", result.retry_attempted
        ),
        "retry_outcome": None if retry_outcome == "none" else retry_outcome,
    }


def _catch_all_summary_from(
    result: ValidationResult, catch_all: dict | None
) -> dict[str, Any]:
    source = dict(catch_all or {})
    return {
        "catch_all_status": source.get("catch_all_status")
        or source.get("classification")
        or result.catch_all_status,
    }


def _provider_key_from(
    result: ValidationResult, historical: dict | None
) -> str | None:
    metadata = result.metadata
    history = dict(historical or metadata.get("historical_intelligence") or {})
    value = (
        metadata.get("provider_key")
        or history.get("provider_key")
        or result.provider_reputation
        or metadata.get("provider_hint")
    )
    return value if isinstance(value, str) and value else None


def _provider_type_from(result: ValidationResult) -> str | None:
    value = result.metadata.get("provider_type")
    return value if isinstance(value, str) and value else None


def _smtp_attempted(smtp: dict[str, Any]) -> bool:
    status = smtp["smtp_status"]
    return status is not None and status != SMTP_NOT_ATTEMPTED


def _smtp_uncertain(status: str | None) -> bool:
    return status not in (None, SMTP_NOT_ATTEMPTED, "valid", "invalid")


def _is_timeout_like(smtp: dict[str, Any]) -> bool:
    status = smtp["smtp_status"]
    error_type = smtp["smtp_error_type"]
    return status == "timeout" or (
        isinstance(error_type, str) and "timeout" in error_type.lower()
    )


def _has_probe_signal(
    smtp: dict[str, Any],
    catch_all: dict[str, Any],
    result: ValidationResult,
) -> bool:
    return (
        _smtp_attempted(smtp)
        or smtp["smtp_code"] is not None
        or smtp["smtp_error_type"] is not None
        or catch_all["catch_all_status"] is not None
        or bool(result.retry_attempted)
    )


__all__ = ["ReputationLearningService"]
