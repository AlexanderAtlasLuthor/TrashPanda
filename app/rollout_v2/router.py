"""Routing decisions for V1/V2 rollout."""

from __future__ import annotations

from typing import Any

from .feature_flags import (
    RolloutConfig,
    is_v2_enabled_for_row,
    is_v2_shadowed_for_row,
)


def route_row(row: Any, config: RolloutConfig) -> dict[str, bool]:
    use_v2 = is_v2_enabled_for_row(row, config)
    shadow_v2 = is_v2_shadowed_for_row(row, config)
    return {
        "use_v1": not use_v2,
        "use_v2": use_v2,
        "shadow_v2": shadow_v2,
    }


__all__ = ["route_row"]
