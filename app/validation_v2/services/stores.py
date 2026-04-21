"""In-memory persistent stores for the passive intelligence layer.

Both stores live inside the process: the "persistent" qualifier in
the subphase spec is about lifetime across repeated ``validate()``
calls within a single run, not across process boundaries. A real
external cache (Redis, SQLite, Parquet) can replace these later by
re-implementing the same ``get`` / ``set`` surface.

Design choices:

    * TTL semantics are explicit: ``get`` returns ``None`` for
      expired keys and lazily evicts them. Callers do not need a
      separate eviction pass.

    * Time source is injectable. Tests pass a ``time_source``
      callable so TTL behaviour is deterministic without sleeps.

    * ``default_ttl_seconds`` <= 0 disables expiration. Zero is a
      useful sentinel for tests that want cache hits to stick for
      the duration of the suite.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class _Entry:
    """Cache entry: the stored value plus its expiry timestamp.

    ``expires_at`` is an absolute POSIX timestamp. ``None`` means
    "never expires". Storing absolute times (rather than relative
    TTLs) keeps expiry computation in one place.
    """

    value: Any
    expires_at: Optional[float]


class _TTLCache:
    """Shared TTL-cache implementation for the two public stores.

    Kept private: callers use the wrapping classes below, which add
    domain- and pattern-specific conveniences (``record_domain``,
    ``record_pattern``). Splitting the implementation here keeps
    the concerns separated from the wrappers without duplicating
    core TTL logic.
    """

    def __init__(
        self,
        default_ttl_seconds: float = 86_400.0,
        time_source: Callable[[], float] = time.time,
    ) -> None:
        if default_ttl_seconds < 0:
            raise ValueError("default_ttl_seconds must be >= 0")
        self._default_ttl_seconds = float(default_ttl_seconds)
        self._time_source = time_source
        self._entries: dict[str, _Entry] = {}

    @property
    def default_ttl_seconds(self) -> float:
        return self._default_ttl_seconds

    def get(self, key: str) -> Any | None:
        """Return the stored value for ``key`` or ``None``.

        Expired entries are evicted on access (read-through lazy
        eviction). Missing keys return ``None`` — callers must not
        rely on ``None`` being a valid value for presence checks.
        """
        entry = self._entries.get(key)
        if entry is None:
            return None
        if entry.expires_at is not None and self._time_source() >= entry.expires_at:
            self._entries.pop(key, None)
            return None
        return entry.value

    def has(self, key: str) -> bool:
        return self.get(key) is not None

    def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: Optional[float] = None,
    ) -> None:
        """Store ``value`` under ``key``.

        ``ttl_seconds=None`` uses the store's default; a negative
        value is rejected; ``0`` means "no expiration for this
        entry" (useful for pinning a hot key).
        """
        ttl = self._default_ttl_seconds if ttl_seconds is None else float(ttl_seconds)
        if ttl < 0:
            raise ValueError("ttl_seconds must be >= 0")
        if ttl == 0:
            expires_at: Optional[float] = None
        else:
            expires_at = self._time_source() + ttl
        self._entries[key] = _Entry(value=value, expires_at=expires_at)

    def delete(self, key: str) -> None:
        self._entries.pop(key, None)

    def clear(self) -> None:
        self._entries.clear()

    def __len__(self) -> int:
        return len(self._entries)

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        return self.has(key)


# ---------------------------------------------------------------------------
# Public stores
# ---------------------------------------------------------------------------


@dataclass
class DomainRecord:
    """Typed record persisted by :class:`DomainCacheStore`.

    A plain dataclass rather than a raw dict: the fields accessed
    by the passive-intel services are small and stable, and making
    them explicit means a typo at a call site is a static-check
    error, not a silent ``KeyError`` at runtime.
    """

    domain: str
    last_seen: float
    provider_type: str | None = None
    reputation_score: float | None = None
    counters: dict[str, int] = field(default_factory=dict)
    catch_all_classification: str | None = None
    catch_all_confidence: float | None = None


class DomainCacheStore:
    """TTL-bounded cache keyed by domain.

    Used by :class:`SimpleDomainIntelligenceService` as a cheap
    memory across repeated validations within a run. Every record
    carries the last-seen timestamp and an open-ended counters
    dict so future subphases can accumulate per-domain telemetry
    (probe attempts, catch-all confirmations, etc.) without a
    schema change.
    """

    def __init__(
        self,
        default_ttl_seconds: float = 86_400.0,
        time_source: Callable[[], float] = time.time,
    ) -> None:
        self._cache = _TTLCache(
            default_ttl_seconds=default_ttl_seconds, time_source=time_source
        )
        self._time_source = time_source

    def get(self, domain: str) -> DomainRecord | None:
        value = self._cache.get(domain)
        if value is None:
            return None
        # Defensive: accept dicts too so external callers can seed
        # the cache from serialized state.
        if isinstance(value, DomainRecord):
            return value
        if isinstance(value, dict):
            return DomainRecord(
                domain=str(value.get("domain", domain)),
                last_seen=float(value.get("last_seen", self._time_source())),
                provider_type=value.get("provider_type"),
                reputation_score=value.get("reputation_score"),
                counters=dict(value.get("counters") or {}),
                catch_all_classification=value.get("catch_all_classification"),
                catch_all_confidence=value.get("catch_all_confidence"),
            )
        return None

    def set(
        self,
        domain: str,
        record: DomainRecord,
        ttl_seconds: Optional[float] = None,
    ) -> None:
        self._cache.set(domain, record, ttl_seconds=ttl_seconds)

    def record_domain(
        self,
        domain: str,
        *,
        provider_type: str | None = None,
        reputation_score: float | None = None,
        ttl_seconds: Optional[float] = None,
    ) -> DomainRecord:
        """Idempotent upsert — returns the new record.

        Ergonomic convenience for services that want to record "I
        just classified this domain" without constructing a
        :class:`DomainRecord` explicitly. Preserves existing
        counters so the helper is safe to call on every request.
        """
        existing = self.get(domain)
        counters = dict(existing.counters) if existing else {}
        record = DomainRecord(
            domain=domain,
            last_seen=self._time_source(),
            provider_type=provider_type
            if provider_type is not None
            else (existing.provider_type if existing else None),
            reputation_score=reputation_score
            if reputation_score is not None
            else (existing.reputation_score if existing else None),
            counters=counters,
            catch_all_classification=(
                existing.catch_all_classification if existing else None
            ),
            catch_all_confidence=(
                existing.catch_all_confidence if existing else None
            ),
        )
        self.set(domain, record, ttl_seconds=ttl_seconds)
        return record

    def increment(self, domain: str, counter: str, delta: int = 1) -> int:
        """Increment a named counter on the domain's record.

        Creates a minimal record if none exists. Returns the new
        counter value. Kept small on purpose: future telemetry
        work will expand this, but the contract stays stable.
        """
        existing = self.get(domain) or DomainRecord(
            domain=domain, last_seen=self._time_source()
        )
        existing.counters[counter] = existing.counters.get(counter, 0) + int(delta)
        existing.last_seen = self._time_source()
        self.set(domain, existing)
        return existing.counters[counter]

    def __len__(self) -> int:
        return len(self._cache)

    def __contains__(self, key: object) -> bool:
        return key in self._cache


class PatternCacheStore:
    """TTL-bounded cache keyed by an arbitrary pattern string.

    Reason-code tallies, suspicious-pattern memoization, provider
    heuristic results, etc. Kept intentionally simple: raw string
    keys, raw value storage, basic TTL. A richer schema can land
    in a future subphase without breaking callers of ``get`` /
    ``set``.
    """

    def __init__(
        self,
        default_ttl_seconds: float = 86_400.0,
        time_source: Callable[[], float] = time.time,
    ) -> None:
        self._cache = _TTLCache(
            default_ttl_seconds=default_ttl_seconds, time_source=time_source
        )

    def get(self, pattern: str) -> Any | None:
        return self._cache.get(pattern)

    def set(
        self,
        pattern: str,
        value: Any,
        ttl_seconds: Optional[float] = None,
    ) -> None:
        self._cache.set(pattern, value, ttl_seconds=ttl_seconds)

    def record_pattern(
        self,
        pattern: str,
        value: Any,
        ttl_seconds: Optional[float] = None,
    ) -> None:
        self.set(pattern, value, ttl_seconds=ttl_seconds)

    def delete(self, pattern: str) -> None:
        self._cache.delete(pattern)

    def clear(self) -> None:
        self._cache.clear()

    def __len__(self) -> int:
        return len(self._cache)

    def __contains__(self, key: object) -> bool:
        return key in self._cache


__all__ = [
    "DomainRecord",
    "DomainCacheStore",
    "PatternCacheStore",
]
