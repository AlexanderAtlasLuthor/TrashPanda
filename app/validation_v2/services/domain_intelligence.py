"""SimpleDomainIntelligenceService: offline domain heuristics.

Looks at the domain string alone and reports a small set of
deterministic facts: which common consumer provider it matches
(if any), whether its shape looks suspicious (very long, many
hyphens, numeric-heavy), and — when a cache is wired — any
historical reputation score captured by prior validations.

No DNS. No network. No randomness. Repeated calls with the same
input must return the same output, which is what makes the service
safe to use inside the deterministic engine skeleton.
"""

from __future__ import annotations

from typing import Any

from ..interfaces import DomainIntelligenceService
from .stores import DomainCacheStore


# Common consumer providers (lowercased). Kept narrow on purpose:
# this is not a provider directory, it is the set V2 wants to
# short-circuit into tier_1 without further checks. Extending the
# set is a single-line change but each new entry should be a
# genuinely common mailbox provider.
COMMON_PROVIDERS: frozenset[str] = frozenset(
    {
        "gmail.com",
        "googlemail.com",
        "yahoo.com",
        "outlook.com",
        "hotmail.com",
        "live.com",
        "icloud.com",
        "me.com",
        "mac.com",
        "aol.com",
        "proton.me",
        "protonmail.com",
    }
)

# Provider labels returned via ``provider_hint``. "other" is the
# catch-all for any domain we don't recognize. Downstream services
# may refine this — the intel service only reports what's
# cheaply derivable from the string.
_PROVIDER_HINTS: dict[str, str] = {
    "gmail.com": "gmail",
    "googlemail.com": "gmail",
    "yahoo.com": "yahoo",
    "outlook.com": "microsoft",
    "hotmail.com": "microsoft",
    "live.com": "microsoft",
    "icloud.com": "apple",
    "me.com": "apple",
    "mac.com": "apple",
    "aol.com": "aol",
    "proton.me": "proton",
    "protonmail.com": "proton",
}


# Suspicious-shape thresholds. Deliberately conservative: the
# current subphase uses "suspicious" only as a signal stored in
# metadata, not as a hard-reject gate. Making the thresholds
# module-level constants lets tests tune them with monkeypatching
# if the calibration needs to shift later.
SUSPICIOUS_LENGTH_THRESHOLD: int = 40
SUSPICIOUS_HYPHEN_THRESHOLD: int = 3
SUSPICIOUS_DIGIT_RATIO_THRESHOLD: float = 0.5


class SimpleDomainIntelligenceService(DomainIntelligenceService):
    """Deterministic, offline domain-level intelligence.

    The service never performs I/O. Passing a
    :class:`DomainCacheStore` enables cache-hit reporting and
    historical-score carryover. Without a cache, every call reports
    ``cache_hit=False`` and ``historical_score=None``.
    """

    def __init__(self, cache: DomainCacheStore | None = None) -> None:
        self._cache = cache

    # ------------------------------------------------------------------
    # DomainIntelligenceService contract
    # ------------------------------------------------------------------

    def analyze(self, domain: str) -> dict[str, Any]:
        """Return a deterministic intel dict for ``domain``.

        Keys:
          * ``domain`` — the input (lowercased for consistency)
          * ``provider_hint`` — label (``"gmail"``, ``"microsoft"``,
            …, or ``"other"``)
          * ``is_common_provider`` — True iff ``domain`` is in
            :data:`COMMON_PROVIDERS`
          * ``is_suspicious_pattern`` — True iff any of the
            suspicious-shape rules fire
          * ``suspicious_reasons`` — list of reason-code strings
            for the rules that fired; empty list when none fired
          * ``historical_score`` — cached reputation score or
            ``None``
          * ``cache_hit`` — True iff a cache entry existed for
            this domain
        """
        normalized = (domain or "").strip().lower()
        provider_hint = _PROVIDER_HINTS.get(normalized, "other")
        is_common = normalized in COMMON_PROVIDERS
        suspicious_reasons = _detect_suspicious_reasons(normalized)
        is_suspicious = bool(suspicious_reasons)

        historical_score: float | None = None
        cache_hit = False
        if self._cache is not None and normalized:
            cached = self._cache.get(normalized)
            if cached is not None:
                cache_hit = True
                historical_score = cached.reputation_score
            # Record the observation for future validations. This
            # does not overwrite existing cached data — the
            # ``record_domain`` helper preserves counters and will
            # only update provider_type when a non-None value is
            # supplied.
            self._cache.record_domain(
                normalized,
                provider_type=provider_hint if provider_hint != "other" else None,
            )

        return {
            "domain": normalized,
            "provider_hint": provider_hint,
            "is_common_provider": is_common,
            "is_suspicious_pattern": is_suspicious,
            "suspicious_reasons": list(suspicious_reasons),
            "historical_score": historical_score,
            "cache_hit": cache_hit,
        }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _detect_suspicious_reasons(domain: str) -> list[str]:
    """Return reason-code strings for suspicious-shape rules that fire.

    Separated from ``analyze`` so the rules can be unit-tested
    without standing up a service. Rules are ordered so the output
    list is deterministic.
    """
    if not domain:
        return ["empty_domain"]

    reasons: list[str] = []
    if len(domain) >= SUSPICIOUS_LENGTH_THRESHOLD:
        reasons.append("very_long_domain")

    hyphens = domain.count("-")
    if hyphens >= SUSPICIOUS_HYPHEN_THRESHOLD:
        reasons.append("many_hyphens")

    # Digit ratio is over the whole domain string including the
    # TLD. A legitimate domain (``example.com``) has ratio 0;
    # a numeric-heavy one (``mail-123456.xyz``) easily clears the
    # default threshold. ``.` and `-` are not counted as digits.
    alnum = [c for c in domain if c.isalnum()]
    if alnum:
        digit_count = sum(1 for c in alnum if c.isdigit())
        digit_ratio = digit_count / len(alnum)
        if digit_ratio >= SUSPICIOUS_DIGIT_RATIO_THRESHOLD:
            reasons.append("numeric_heavy")

    return reasons


__all__ = [
    "SimpleDomainIntelligenceService",
    "COMMON_PROVIDERS",
    "SUSPICIOUS_LENGTH_THRESHOLD",
    "SUSPICIOUS_HYPHEN_THRESHOLD",
    "SUSPICIOUS_DIGIT_RATIO_THRESHOLD",
]
