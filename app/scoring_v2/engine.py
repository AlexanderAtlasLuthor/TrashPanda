"""ScoringEngineV2: the signal-based scoring engine with aggregation.

This is the aggregation-phase version of the engine. It walks every
configured evaluator, collects the signals they emit, and then turns
those signals into a fully populated :class:`ScoreBreakdown`:

  * ``positive_total`` / ``negative_total`` from ``value * weight``
  * ``raw_score = positive_total - negative_total`` (unclamped)
  * ``final_score`` normalized to [0.0, 1.0] via the sum of emitted
    positive weights
  * ``confidence`` as a signal-strength-weighted average of
    per-signal confidences
  * ``hard_stop`` / ``hard_stop_reason`` from the profile's hard-stop
    policy, applied over the emission order
  * ``bucket`` from the final_score / confidence / hard_stop decision
    rule (``"high_confidence"`` / ``"review"`` / ``"invalid"``)
  * ``reason_codes``, ``explanation``, and ``breakdown_dict`` for
    audit and downstream serialization

The engine is pure Python. It does not import pandas, does not
mutate its input row, and does not read from or modify any V1
scoring module. Pipeline integration is a later subphase.
"""

from __future__ import annotations

from typing import Any, Iterable

from .breakdown import ScoreBreakdown
from .evaluator import SignalEvaluator
from .profile import ScoringProfile
from .signal import ScoringSignal


# Minimum aggregated confidence required to land in the
# ``"high_confidence"`` bucket. Intentionally a hard-coded gate at this
# stage — calibration against real data is a later subphase.
_HIGH_CONFIDENCE_MIN_CONFIDENCE = 0.80


class ScoringEngineV2:
    """Signal-based scoring engine with aggregation and bucketing.

    The engine owns:
      * an ordered list of ``SignalEvaluator`` instances
      * a ``ScoringProfile``

    ``evaluate_row(row)`` calls each evaluator with the given row,
    concatenates their signal outputs in evaluator order, and
    transforms the combined signal list into a populated
    ``ScoreBreakdown``.
    """

    def __init__(
        self,
        evaluators: Iterable[SignalEvaluator],
        profile: ScoringProfile,
    ) -> None:
        self._evaluators: list[SignalEvaluator] = list(evaluators)
        self._profile = profile

    @property
    def evaluators(self) -> list[SignalEvaluator]:
        """Return a copy of the configured evaluator list."""
        return list(self._evaluators)

    @property
    def profile(self) -> ScoringProfile:
        """Return the configured scoring profile."""
        return self._profile

    def evaluate_row(self, row: dict) -> ScoreBreakdown:
        """Collect signals and produce a full :class:`ScoreBreakdown`.

        Steps, in order:
          1. Collect signals from every evaluator, preserving order.
          2. Compute positive / negative totals.
          3. Compute unclamped raw_score.
          4. Normalize to final_score in [0.0, 1.0].
          5. Compute the strength-weighted aggregate confidence.
          6. Apply hard-stop policy (does not erase signals or totals).
          7. Assign bucket from (final_score, confidence, hard_stop).
          8. Build reason_codes, explanation, and breakdown_dict.

        The input ``row`` is not mutated. Evaluators that emit nothing
        contribute nothing.
        """
        signals = self._collect(row)

        positive_total, negative_total = _totals(signals)
        raw_score = positive_total - negative_total
        final_score = _normalize(raw_score, signals)
        confidence = _confidence(signals)
        hard_stop, hard_stop_reason = _hard_stop(
            signals, self._profile.hard_stop_policy
        )
        bucket = _bucket(
            final_score=final_score,
            confidence=confidence,
            hard_stop=hard_stop,
            profile=self._profile,
        )
        reason_codes = [s.reason_code for s in signals]
        explanation = _explanation(signals, hard_stop, hard_stop_reason)
        breakdown_dict = _breakdown_dict(
            signals=signals,
            positive_total=positive_total,
            negative_total=negative_total,
            raw_score=raw_score,
            final_score=final_score,
            confidence=confidence,
            hard_stop=hard_stop,
            hard_stop_reason=hard_stop_reason,
            bucket=bucket,
            reason_codes=reason_codes,
        )

        return ScoreBreakdown(
            signals=signals,
            positive_total=positive_total,
            negative_total=negative_total,
            raw_score=raw_score,
            final_score=final_score,
            confidence=confidence,
            hard_stop=hard_stop,
            hard_stop_reason=hard_stop_reason,
            bucket=bucket,
            reason_codes=reason_codes,
            explanation=explanation,
            breakdown_dict=breakdown_dict,
        )

    def _collect(self, row: dict) -> list[ScoringSignal]:
        collected: list[ScoringSignal] = []
        for evaluator in self._evaluators:
            produced = evaluator.evaluate(row) or []
            collected.extend(produced)
        return collected


# ---------------------------------------------------------------------------
# Aggregation helpers (module-private, deterministic, pure)
# ---------------------------------------------------------------------------


def _totals(signals: list[ScoringSignal]) -> tuple[float, float]:
    """Return (positive_total, negative_total) = sum of ``value*weight``.

    Neutral signals are recorded but do not contribute to either total.
    """
    positive_total = 0.0
    negative_total = 0.0
    for s in signals:
        contribution = s.value * s.weight
        if s.direction == "positive":
            positive_total += contribution
        elif s.direction == "negative":
            negative_total += contribution
    return positive_total, negative_total


def _normalize(raw_score: float, signals: list[ScoringSignal]) -> float:
    """Normalize ``raw_score`` to [0.0, 1.0] using emitted positive weight.

    ``max_positive`` is the sum of weights of *actually emitted*
    positive-direction signals. If that sum is zero (no positive
    signals, or all zero-weight), the final score is 0.0. Otherwise
    ``raw_score / max_positive`` is clamped to [0.0, 1.0].
    """
    max_positive = sum(s.weight for s in signals if s.direction == "positive")
    if max_positive <= 0:
        return 0.0
    score = raw_score / max_positive
    if score < 0.0:
        return 0.0
    if score > 1.0:
        return 1.0
    return score


def _confidence(signals: list[ScoringSignal]) -> float:
    """Strength-weighted mean of per-signal confidences.

    ``signal_strength = value * weight``. If no signals were emitted
    (or every emitted signal has zero strength), confidence is 0.0.
    The result is clamped to [0.0, 1.0] for safety against any
    downstream rounding.
    """
    if not signals:
        return 0.0
    numerator = 0.0
    denominator = 0.0
    for s in signals:
        strength = s.value * s.weight
        if strength <= 0:
            continue
        numerator += s.confidence * strength
        denominator += strength
    if denominator <= 0:
        return 0.0
    confidence = numerator / denominator
    if confidence < 0.0:
        return 0.0
    if confidence > 1.0:
        return 1.0
    return confidence


def _hard_stop(
    signals: list[ScoringSignal],
    hard_stop_policy: list[str],
) -> tuple[bool, str | None]:
    """Return (hard_stop, hard_stop_reason) using evaluator emission order.

    The first signal whose ``reason_code`` appears in the policy wins.
    Hard-stop is independent of totals and confidence — it does not
    erase them.
    """
    if not hard_stop_policy:
        return False, None
    policy = set(hard_stop_policy)
    for s in signals:
        if s.reason_code in policy:
            return True, s.reason_code
    return False, None


def _bucket(
    *,
    final_score: float,
    confidence: float,
    hard_stop: bool,
    profile: ScoringProfile,
) -> str:
    """Assign the bucket label from final_score / confidence / hard_stop.

    * hard_stop → ``"invalid"`` unconditionally
    * high threshold AND confidence >= 0.80 → ``"high_confidence"``
    * review threshold → ``"review"``
    * otherwise → ``"invalid"``
    """
    if hard_stop:
        return "invalid"
    if (
        final_score >= profile.high_confidence_threshold
        and confidence >= _HIGH_CONFIDENCE_MIN_CONFIDENCE
    ):
        return "high_confidence"
    if final_score >= profile.review_threshold:
        return "review"
    return "invalid"


def _explanation(
    signals: list[ScoringSignal],
    hard_stop: bool,
    hard_stop_reason: str | None,
) -> str:
    """Build the human-readable aggregate explanation string.

    Empty signal list → a stable "no signals" message. Otherwise the
    per-signal explanations are joined with ``"; "`` in emission order.
    A hard-stop tail is appended when applicable.
    """
    if not signals:
        text = "No scoring signals were emitted."
    else:
        text = "; ".join(s.explanation for s in signals)
    if hard_stop:
        text = f"{text} Hard stop triggered: {hard_stop_reason}."
    return text


def _breakdown_dict(
    *,
    signals: list[ScoringSignal],
    positive_total: float,
    negative_total: float,
    raw_score: float,
    final_score: float,
    confidence: float,
    hard_stop: bool,
    hard_stop_reason: str | None,
    bucket: str,
    reason_codes: list[str],
) -> dict[str, Any]:
    """Build the JSON-serializable structured breakdown dict."""
    return {
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
            for s in signals
        ],
        "positive_total": positive_total,
        "negative_total": negative_total,
        "raw_score": raw_score,
        "final_score": final_score,
        "confidence": confidence,
        "hard_stop": hard_stop,
        "hard_stop_reason": hard_stop_reason,
        "bucket": bucket,
        "reason_codes": list(reason_codes),
    }
