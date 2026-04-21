"""Rate-limiting policy and implementations for the V2 control plane.

Rate limits are expressed as two sliding-window caps:

    * ``max_per_domain_per_minute`` — per-domain ceiling.
    * ``max_global_per_minute``     — global ceiling across all
      domains.

The limiter is deterministic: a caller-supplied clock drives the
sliding window, so tests can advance time without sleeping.

The implementation is intentionally simple. We store the
timestamps of allowed calls in two lists (global + per-domain),
evict anything older than 60 seconds on every :meth:`allow`
call, and compare the surviving counts to the policy. This keeps
the code auditable and allocates no background threads — Subphase
3 does not need a high-throughput rate limiter.
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable


# The sliding window is fixed at 60 seconds to match the policy
# field names. Keeping it a constant (rather than configurable)
# avoids a combinatorial explosion of test cases for a Subphase
# that only needs a per-minute ceiling.
WINDOW_SECONDS: float = 60.0


@dataclass(frozen=True)
class RateLimitPolicy:
    """Declarative rate-limit caps.

    Attributes:
        max_per_domain_per_minute: Per-domain allowance. Zero
            effectively disables the domain — no requests are
            ever allowed.
        max_global_per_minute: Global allowance across every
            domain. Zero blocks every request.
    """

    max_per_domain_per_minute: int
    max_global_per_minute: int

    def __post_init__(self) -> None:
        if self.max_per_domain_per_minute < 0:
            raise ValueError("max_per_domain_per_minute must be >= 0")
        if self.max_global_per_minute < 0:
            raise ValueError("max_global_per_minute must be >= 0")


class RateLimiter(ABC):
    """Abstract rate limiter: ``allow(domain) -> bool``."""

    @abstractmethod
    def allow(self, domain: str) -> bool:
        """Return True if a request for ``domain`` may proceed.

        The act of calling :meth:`allow` is itself the rate-limited
        event — a True result consumes one slot in the per-domain
        and global windows. False leaves the windows untouched.
        """
        raise NotImplementedError


class InMemoryRateLimiter(RateLimiter):
    """Sliding-window limiter backed by in-memory timestamp lists.

    Single-threaded by design; no locking. Suitable for the
    engine's synchronous path and for deterministic tests. A
    multi-process deployment will need a different backend — that
    is a future subphase concern.
    """

    def __init__(
        self,
        policy: RateLimitPolicy,
        *,
        time_source: Callable[[], float] | None = None,
    ) -> None:
        self._policy = policy
        # ``time.monotonic`` is the production default because it
        # is immune to wall-clock adjustments. Tests inject a
        # ``_ManualClock``-style callable to get deterministic
        # behaviour without sleeping.
        self._time: Callable[[], float] = time_source or time.monotonic
        self._per_domain: dict[str, list[float]] = {}
        self._global: list[float] = []

    @property
    def policy(self) -> RateLimitPolicy:
        return self._policy

    def allow(self, domain: str) -> bool:
        now = self._time()
        cutoff = now - WINDOW_SECONDS

        # Evict stale timestamps. We rebuild the list rather than
        # mutating in place because the window is small and the
        # code is easier to audit this way.
        self._global = [t for t in self._global if t > cutoff]
        domain_list = [
            t for t in self._per_domain.get(domain, []) if t > cutoff
        ]

        # Check caps first so we don't record a blocked call.
        if len(self._global) >= self._policy.max_global_per_minute:
            self._per_domain[domain] = domain_list
            return False
        if len(domain_list) >= self._policy.max_per_domain_per_minute:
            self._per_domain[domain] = domain_list
            return False

        # Record the allowed call in both windows.
        domain_list.append(now)
        self._per_domain[domain] = domain_list
        self._global.append(now)
        return True

    # Introspection helpers. Not part of the ABC — tests and
    # future dashboards can use them without depending on
    # ``_per_domain`` internals.

    def domain_count(self, domain: str) -> int:
        """Number of surviving timestamps for ``domain`` (after eviction)."""
        now = self._time()
        cutoff = now - WINDOW_SECONDS
        return sum(1 for t in self._per_domain.get(domain, []) if t > cutoff)

    def global_count(self) -> int:
        """Number of surviving timestamps in the global window."""
        now = self._time()
        cutoff = now - WINDOW_SECONDS
        return sum(1 for t in self._global if t > cutoff)


__all__ = [
    "RateLimitPolicy",
    "RateLimiter",
    "InMemoryRateLimiter",
    "WINDOW_SECONDS",
]
