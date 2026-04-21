"""TTL helpers for Validation Engine V2 history storage."""

from __future__ import annotations


def compute_ttl_expiry(now: float, ttl_seconds: int | None) -> float | None:
    """Return an absolute expiry timestamp, or None for no expiration."""
    if ttl_seconds is None:
        return None
    if ttl_seconds < 0:
        raise ValueError("ttl_seconds must be >= 0")
    return float(now) + float(ttl_seconds)


def is_expired(expires_at: float | None, now: float) -> bool:
    """Return True when ``expires_at`` is present and at or before ``now``."""
    return expires_at is not None and float(expires_at) <= float(now)


__all__ = ["compute_ttl_expiry", "is_expired"]
