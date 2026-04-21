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


# Default thresholds for V2.
#
# V2 expresses ``final_score`` in the normalized range [0.0, 1.0] (see
# ScoringEngineV2 normalization), so bucket thresholds live in that same
# space. These are *provisional* starting points chosen to be
# conservative without being empty:
#
#   * high_confidence_threshold = 0.80
#       A row must recover at least 80% of the weight of its positive
#       signals (after negative contributions) to land in
#       ``"high_confidence"``. This demands the majority of available
#       positive evidence is present and largely uncontested.
#   * review_threshold = 0.45
#       A row that clears ~45% of its positive evidence lands in
#       ``"review"`` — below that, V2 calls the row ``"invalid"``.
#
# Real product calibration happens later; these defaults just make the
# engine usable out-of-the-box and are intentionally overridable.
_DEFAULT_HIGH_CONFIDENCE_THRESHOLD = 0.80
_DEFAULT_REVIEW_THRESHOLD = 0.45


@dataclass
class ScoringProfile:
    """Configuration bundle for ScoringEngineV2.

    Attributes:
        weights: Optional per-reason-code weight multipliers. Evaluators
            emit signals with an intrinsic weight; the profile may
            override or scale by ``reason_code``. Missing keys mean
            "use the evaluator's intrinsic weight as-is". Unused by the
            aggregation subphase — reserved for later extension.
        high_confidence_threshold: Minimum ``final_score`` (in [0.0,
            1.0]) for the ``"high_confidence"`` bucket. Defaults to
            0.80.
        review_threshold: Minimum ``final_score`` (in [0.0, 1.0]) for
            the ``"review"`` bucket. Must satisfy ``review_threshold
            <= high_confidence_threshold``. Defaults to 0.45.
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
    high_confidence_threshold: float = _DEFAULT_HIGH_CONFIDENCE_THRESHOLD
    review_threshold: float = _DEFAULT_REVIEW_THRESHOLD
    hard_stop_policy: list[str] = field(default_factory=list)
    bucket_policy: dict[str, Any] = field(default_factory=dict)
