"""V2 Phase 6 — decision policy dataclass and action vocabulary.

Defines WHAT the Decision Engine can decide, plus the tunable
thresholds and feature flags. The runtime logic itself lives in
:mod:`.decision_engine`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final


# --------------------------------------------------------------------------- #
# Canonical action / reason vocabulary                                        #
# --------------------------------------------------------------------------- #


class FinalAction:
    """String values returned by the engine as ``DecisionResult.final_action``."""

    AUTO_APPROVE: Final[str] = "auto_approve"
    MANUAL_REVIEW: Final[str] = "manual_review"
    AUTO_REJECT: Final[str] = "auto_reject"

    ALL: Final[frozenset[str]] = frozenset({AUTO_APPROVE, MANUAL_REVIEW, AUTO_REJECT})


class DecisionReason:
    """Short, machine-readable reason codes. Map to human text in explanations."""

    HIGH_PROBABILITY: Final[str] = "high_probability"
    MEDIUM_PROBABILITY: Final[str] = "medium_probability"
    LOW_PROBABILITY: Final[str] = "low_probability"
    HARD_FAIL: Final[str] = "hard_fail"
    DUPLICATE: Final[str] = "duplicate"

    ALL: Final[frozenset[str]] = frozenset({
        HIGH_PROBABILITY, MEDIUM_PROBABILITY, LOW_PROBABILITY,
        HARD_FAIL, DUPLICATE,
    })


# Bucket names used when ``enable_bucket_override`` is True. We keep the
# bucket vocabulary aligned with Phase 2/3's ``v2_final_bucket`` values so
# downstream consumers don't need another mapping.
class OverrideBucket:
    READY: Final[str] = "ready"
    REVIEW: Final[str] = "review"
    INVALID: Final[str] = "invalid"

    ALL: Final[frozenset[str]] = frozenset({READY, REVIEW, INVALID})


# --------------------------------------------------------------------------- #
# Policy dataclass                                                            #
# --------------------------------------------------------------------------- #


@dataclass(slots=True, frozen=True)
class DecisionPolicy:
    """Runtime configuration consumed by :func:`apply_decision_policy`.

    ``approve_threshold`` and ``review_threshold`` are compared against
    ``deliverability_probability`` (Phase 5 output). Defaults match the
    spec: ≥0.80 approves, ≥0.50 routes to manual review, otherwise
    rejects.

    ``enable_bucket_override`` is off by default so the engine, when
    first enabled, does NOT move any row between buckets — it only
    annotates columns. Turning it on opts into moving ``auto_approve``
    rows toward ``ready`` and ``auto_reject`` rows toward ``invalid``.
    Hard-fails and duplicates are never moved, regardless of this flag.
    """

    approve_threshold: float = 0.80
    review_threshold: float = 0.50
    enable_bucket_override: bool = False

    def __post_init__(self) -> None:
        # Frozen dataclass — validate on construction so callers get
        # fail-fast feedback instead of silently-wrong decisions.
        if not 0.0 <= self.review_threshold < self.approve_threshold <= 1.0:
            raise ValueError(
                "decision policy thresholds must satisfy "
                "0.0 <= review_threshold < approve_threshold <= 1.0; "
                f"got review={self.review_threshold} approve={self.approve_threshold}"
            )


DEFAULT_DECISION_POLICY: DecisionPolicy = DecisionPolicy()


__all__ = [
    "DEFAULT_DECISION_POLICY",
    "DecisionPolicy",
    "DecisionReason",
    "FinalAction",
    "OverrideBucket",
]
