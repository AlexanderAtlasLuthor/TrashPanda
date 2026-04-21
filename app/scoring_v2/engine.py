"""ScoringEngineV2: the skeleton scoring engine.

This is the foundational version used to prove the collection
contract. It walks every configured evaluator, collects the signals
they emit, and returns a minimally populated :class:`ScoreBreakdown`
with placeholder numeric values. The real scoring math
(positive/negative totals, raw_score, final_score, confidence,
bucket selection, hard_stop evaluation) will be layered on in a
subsequent step — the skeleton is deliberately logic-free so the
collection contract can be validated in isolation.
"""

from __future__ import annotations

from typing import Iterable

from .breakdown import ScoreBreakdown
from .evaluator import SignalEvaluator
from .profile import ScoringProfile
from .signal import ScoringSignal


class ScoringEngineV2:
    """Signal-based scoring engine — foundational skeleton.

    The engine owns:
      * an ordered list of ``SignalEvaluator`` instances
      * a ``ScoringProfile``

    ``evaluate_row(row)`` calls each evaluator with the given row,
    concatenates their signal outputs, and returns a ``ScoreBreakdown``.
    All numeric aggregates are placeholder ``0.0`` and the bucket is
    ``"unknown"`` — later phases replace those with real values.
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
        """Collect signals from every evaluator for one row.

        Returns a ``ScoreBreakdown`` with ``signals`` and
        ``reason_codes`` populated; every numeric field is a
        placeholder (``0.0``), ``hard_stop`` is ``False``, and
        ``bucket`` is ``"unknown"``. The skeleton does not look up
        profile weights, does not apply hard-stop policy, and does
        not compute any aggregate — those are all future work.
        """
        collected: list[ScoringSignal] = []
        for evaluator in self._evaluators:
            produced = evaluator.evaluate(row) or []
            collected.extend(produced)

        reason_codes = [s.reason_code for s in collected]

        return ScoreBreakdown(
            signals=collected,
            positive_total=0.0,
            negative_total=0.0,
            raw_score=0.0,
            final_score=0.0,
            confidence=0.0,
            hard_stop=False,
            hard_stop_reason=None,
            bucket="unknown",
            reason_codes=reason_codes,
            explanation="",
            breakdown_dict={},
        )
