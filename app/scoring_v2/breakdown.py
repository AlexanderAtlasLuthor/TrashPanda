"""ScoreBreakdown: the full scoring result for a single row.

Pure Python, no pandas, JSON-serializable. The breakdown carries the
collected signals plus aggregate numeric fields and an audit-friendly
reason trail. Later scoring phases will populate the numeric fields;
the skeleton engine returns a minimal instance with placeholder values.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .signal import ScoringSignal


@dataclass
class ScoreBreakdown:
    """Full scoring result for a single row.

    Attributes:
        signals: Every ScoringSignal collected for this row, in the
            order the evaluators produced them.
        positive_total: Sum of positive signal contributions (populated
            by the full engine, 0.0 in the skeleton).
        negative_total: Sum of negative signal contributions (populated
            by the full engine, 0.0 in the skeleton).
        raw_score: The pre-clamp / pre-bucket score.
        final_score: The clamped, bucketed-ready score.
        confidence: Row-level confidence aggregated from per-signal
            confidences (0.0 in the skeleton).
        hard_stop: True if any hard-stop signal fired.
        hard_stop_reason: The reason_code of the hard-stop signal, or
            None.
        bucket: Final bucket label (``"high_confidence"`` / ``"review"``
            / ``"invalid"`` in the current vocabulary, or
            ``"unknown"`` when the skeleton hasn't computed it yet).
        reason_codes: Convenience list of every signal's ``reason_code``
            in arrival order, for fast analytics.
        explanation: Human-readable aggregate explanation.
        breakdown_dict: Structured per-row dict suitable for JSON
            serialization. Populated by the full engine; left empty in
            the skeleton. ``to_dict()`` produces an equivalent dict on
            demand regardless of whether this field is filled.
    """

    signals: list[ScoringSignal] = field(default_factory=list)
    positive_total: float = 0.0
    negative_total: float = 0.0
    raw_score: float = 0.0
    final_score: float = 0.0
    confidence: float = 0.0
    hard_stop: bool = False
    hard_stop_reason: str | None = None
    bucket: str = "unknown"
    reason_codes: list[str] = field(default_factory=list)
    explanation: str = ""
    breakdown_dict: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable dict of this breakdown.

        Works regardless of whether ``breakdown_dict`` has been filled;
        iterates ``signals`` and the top-level fields directly.
        """
        return {
            "positive_total": self.positive_total,
            "negative_total": self.negative_total,
            "raw_score": self.raw_score,
            "final_score": self.final_score,
            "confidence": self.confidence,
            "hard_stop": self.hard_stop,
            "hard_stop_reason": self.hard_stop_reason,
            "bucket": self.bucket,
            "reason_codes": list(self.reason_codes),
            "explanation": self.explanation,
            "signals": [
                {
                    "name": s.name,
                    "direction": s.direction,
                    "value": s.value,
                    "weight": s.weight,
                    "confidence": s.confidence,
                    "reason_code": s.reason_code,
                    "explanation": s.explanation,
                }
                for s in self.signals
            ],
        }
