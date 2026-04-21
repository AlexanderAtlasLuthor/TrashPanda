"""Feature flags for safe Validation Engine V2 rollout."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RolloutConfig:
    enabled: bool
    strategy: str
    percentage: float
    allow_domains: set[str] | None = None
    block_domains: set[str] | None = None

    def __post_init__(self) -> None:
        if self.strategy not in {"shadow", "canary", "full"}:
            raise ValueError("strategy must be shadow, canary, or full")
        if not 0.0 <= self.percentage <= 100.0:
            raise ValueError("percentage must be in [0, 100]")
        if self.allow_domains is not None:
            object.__setattr__(
                self,
                "allow_domains",
                {d.lower() for d in self.allow_domains},
            )
        if self.block_domains is not None:
            object.__setattr__(
                self,
                "block_domains",
                {d.lower() for d in self.block_domains},
            )


def is_v2_enabled_for_row(row: Any, config: RolloutConfig) -> bool:
    if not config.enabled:
        return False
    domain = _domain(row)
    if domain and config.block_domains and domain in config.block_domains:
        return False
    if config.allow_domains is not None and domain not in config.allow_domains:
        return False
    if config.strategy == "shadow":
        return False
    if config.strategy == "full":
        return True
    return _bucket(row) < config.percentage


def is_v2_shadowed_for_row(row: Any, config: RolloutConfig) -> bool:
    if not config.enabled or config.strategy != "shadow":
        return False
    domain = _domain(row)
    if domain and config.block_domains and domain in config.block_domains:
        return False
    if config.allow_domains is not None and domain not in config.allow_domains:
        return False
    return True


def _bucket(row: Any) -> float:
    key = str(_get(row, "email") or _get(row, "id") or _domain(row) or row)
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    value = int(digest[:8], 16)
    return (value % 10_000) / 100.0


def _domain(row: Any) -> str | None:
    value = _get(row, "corrected_domain") or _get(row, "domain")
    if value is None:
        return None
    return str(value).strip().lower() or None


def _get(row: Any, key: str) -> Any:
    if isinstance(row, dict):
        return row.get(key)
    getter = getattr(row, "get", None)
    if callable(getter):
        return getter(key)
    return getattr(row, key, None)


__all__ = [
    "RolloutConfig",
    "is_v2_enabled_for_row",
    "is_v2_shadowed_for_row",
]
