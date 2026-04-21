"""ScoringProfile: configuration object for ScoringEngineV2.

A profile is a declarative bundle of: per-reason-code weights, bucket
thresholds, hard-stop policy (which reason codes force hard_stop), and
an open-ended bucket_policy dict reserved for later rule extensions.

The profile is data-only. It does not contain scoring logic — the
engine consumes it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ScoringProfile:
    """Configuration bundle for ScoringEngineV2.

    Attributes:
        weights: Optional per-reason-code weight multipliers. Evaluators
            emit signals with an intrinsic weight; the profile may
            override or scale by ``reason_code``. Missing keys mean
            "use the evaluator's intrinsic weight as-is".
        high_confidence_threshold: Minimum score for the
            ``"high_confidence"`` bucket.
        review_threshold: Minimum score for the ``"review"`` bucket.
            Must satisfy ``review_threshold <= high_confidence_threshold``.
        hard_stop_policy: Reason codes that force ``hard_stop=True``
            regardless of the numeric score. Mirrors V1's hard-fail
            vocabulary (``"syntax_invalid"``, ``"no_domain"``,
            ``"nxdomain"``) and is open-ended for future additions.
        bucket_policy: Open-ended dict reserved for future bucket-rule
            extensions (per-industry profiles, override rules,
            calibration curves, etc.). Left intentionally untyped so
            early experimentation does not force schema churn.
    """

    weights: dict[str, float] = field(default_factory=dict)
    high_confidence_threshold: float = 70.0
    review_threshold: float = 40.0
    hard_stop_policy: list[str] = field(default_factory=list)
    bucket_policy: dict[str, Any] = field(default_factory=dict)
