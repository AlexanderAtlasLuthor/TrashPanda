"""SimpleProviderReputationService: tiered reputation from the domain string.

Maps a domain into one of four provider types:

    * ``"tier_1"`` — major consumer providers (gmail, outlook, …)
    * ``"enterprise"`` — domains that look like business mail
      (TLDs like ``.com`` / ``.io`` / ``.co`` etc., not common
      consumer providers, not suspicious-shape)
    * ``"suspicious"`` — domains whose shape triggers a suspicious
      heuristic (very long, many hyphens, numeric-heavy)
    * ``"unknown"`` — everything else

Each type is paired with a fixed reputation score and trust level.
The service is fully offline and deterministic — no DNS, no
WHOIS, no external lookups.
"""

from __future__ import annotations

from typing import Any

from ..interfaces import ProviderReputationService
from .domain_intelligence import (
    COMMON_PROVIDERS,
    _detect_suspicious_reasons,
)
from .stores import DomainCacheStore


ProviderType = str  # One of "tier_1" | "enterprise" | "unknown" | "suspicious"


# Reputation scores per provider type. The scale is [0.0, 1.0]
# so the layer above (candidate selection, scoring, etc.) can
# treat the value uniformly. tier_1 is not 1.0 — even big
# providers host abusive mailboxes, the score reflects the
# *domain*, not the specific address.
REPUTATION_SCORES: dict[ProviderType, float] = {
    "tier_1": 0.90,
    "enterprise": 0.70,
    "unknown": 0.50,
    "suspicious": 0.20,
}

TRUST_LEVELS: dict[ProviderType, str] = {
    "tier_1": "high",
    "enterprise": "medium",
    "unknown": "medium",
    "suspicious": "low",
}

# TLDs that bump an otherwise-unknown domain up to "enterprise"
# tier. Conservative list — adding a TLD here is an opinion
# about its typical occupants, so we keep it narrow.
_ENTERPRISE_TLDS: frozenset[str] = frozenset(
    {
        "com",
        "net",
        "org",
        "io",
        "co",
        "ai",
        "dev",
        "app",
        "biz",
        "us",
        "eu",
        "uk",
    }
)


class SimpleProviderReputationService(ProviderReputationService):
    """Offline provider reputation lookup.

    Parameters:
        cache: Optional :class:`DomainCacheStore` used to persist
            reputation classifications across calls. When present,
            a repeated classification returns the cached value
            *and* refreshes ``last_seen`` so downstream telemetry
            can tell active from stale domains.
    """

    def __init__(self, cache: DomainCacheStore | None = None) -> None:
        self._cache = cache

    # ------------------------------------------------------------------
    # ProviderReputationService contract
    # ------------------------------------------------------------------

    def classify(self, domain: str) -> dict[str, Any]:
        """Return a reputation-classification dict for ``domain``.

        Keys:
          * ``provider`` — human-readable provider hint (e.g.
            ``"gmail"``, ``"microsoft"``, ``"other"``). Matches
            :class:`SimpleDomainIntelligenceService` so the two
            services stay in sync.
          * ``provider_type`` — tier label (see module docstring)
          * ``reputation_score`` — float in [0.0, 1.0]
          * ``trust_level`` — ``"low"`` / ``"medium"`` / ``"high"``
          * ``cache_hit`` — True iff a prior classification was
            served from the cache
        """
        normalized = (domain or "").strip().lower()

        cache_hit = False
        if self._cache is not None and normalized:
            cached = self._cache.get(normalized)
            if (
                cached is not None
                and cached.provider_type is not None
                and cached.reputation_score is not None
            ):
                cache_hit = True
                provider_type = cached.provider_type
                score = cached.reputation_score
                trust = TRUST_LEVELS.get(provider_type, "medium")
                # Refresh last_seen without clobbering the
                # cached values.
                self._cache.record_domain(
                    normalized,
                    provider_type=provider_type,
                    reputation_score=score,
                )
                return {
                    "provider": _provider_label_for(normalized),
                    "provider_type": provider_type,
                    "reputation_score": score,
                    "trust_level": trust,
                    "cache_hit": cache_hit,
                }

        provider_type = _classify_provider_type(normalized)
        score = REPUTATION_SCORES[provider_type]
        trust = TRUST_LEVELS[provider_type]

        if self._cache is not None and normalized:
            self._cache.record_domain(
                normalized,
                provider_type=provider_type,
                reputation_score=score,
            )

        return {
            "provider": _provider_label_for(normalized),
            "provider_type": provider_type,
            "reputation_score": score,
            "trust_level": trust,
            "cache_hit": cache_hit,
        }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _classify_provider_type(domain: str) -> ProviderType:
    """Return the tier label for ``domain``.

    Order matters:
      1. Empty / malformed → ``"suspicious"`` (treated as
         shape-suspicious; the exclusion service handles the hard
         rejection separately).
      2. Common provider membership → ``"tier_1"``.
      3. Any suspicious shape → ``"suspicious"``.
      4. Known enterprise TLD → ``"enterprise"``.
      5. Otherwise → ``"unknown"``.
    """
    if not domain or "." not in domain:
        return "suspicious"

    if domain in COMMON_PROVIDERS:
        return "tier_1"

    suspicious = _detect_suspicious_reasons(domain)
    if suspicious:
        return "suspicious"

    tld = domain.rsplit(".", 1)[-1]
    if tld in _ENTERPRISE_TLDS:
        return "enterprise"

    return "unknown"


def _provider_label_for(domain: str) -> str:
    """Friendly label shown in the result's ``provider`` field.

    Kept in sync with :mod:`..services.domain_intelligence` so the
    two services produce the same label for the same domain. The
    narrow duplication is intentional: classification rules can
    diverge without pulling intel into the reputation service's
    import graph.
    """
    from .domain_intelligence import _PROVIDER_HINTS  # local import: see above

    return _PROVIDER_HINTS.get(domain, "other")


__all__ = [
    "SimpleProviderReputationService",
    "REPUTATION_SCORES",
    "TRUST_LEVELS",
]
