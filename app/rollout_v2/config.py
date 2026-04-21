"""Default safe rollout configuration."""

from __future__ import annotations

from .feature_flags import RolloutConfig


DEFAULT_ROLLOUT = RolloutConfig(
    enabled=False,
    strategy="shadow",
    percentage=0.0,
)


def build_rollout_config(**overrides) -> RolloutConfig:
    values = {
        "enabled": DEFAULT_ROLLOUT.enabled,
        "strategy": DEFAULT_ROLLOUT.strategy,
        "percentage": DEFAULT_ROLLOUT.percentage,
        "allow_domains": DEFAULT_ROLLOUT.allow_domains,
        "block_domains": DEFAULT_ROLLOUT.block_domains,
    }
    values.update(overrides)
    return RolloutConfig(**values)


__all__ = ["DEFAULT_ROLLOUT", "build_rollout_config"]
