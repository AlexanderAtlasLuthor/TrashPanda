"""ScoringEngineV2: the calibrated signal-based scoring engine.

The engine walks every configured evaluator, collects the signals they
emit, applies profile-driven weights, and produces a fully-populated
:class:`ScoreBreakdown` — raw totals, a normalized final score, an
aggregated confidence, a hard-stop flag, a bucket label, and a
structured audit trail.

This module deliberately keeps all scoring math in one place. Evaluators
are pure signal producers; the engine owns aggregation, normalization,
and bucketing policy. All policy is data-driven through
:class:`~app.scoring_v2.profile.ScoringProfile` — changing a weight,
swapping the strong-evidence set, or tightening a threshold is a
profile edit, not an engine edit.

Calibration overview (2026-04 pass):

    * ``final_score = raw_score / max_positive_possible`` where
      ``max_positive_possible`` is a *fixed* theoretical maximum read
      from the profile (not the emitted-signal sum). This prevents
      weak positive-only rows from scoring 1.0.

    * ``confidence_v2`` uses a *separate* set of weights
      (``confidence_weights``) so structural signals with low
      information content (syntax_valid, domain_present) cannot
      inflate confidence. DNS signals remain fully influential.

    * The ``high_confidence`` bucket is gated on strong evidence —
      at least one reason code from ``strong_evidence_reason_codes``
      (default: ``{"mx_present"}``) must be present. The ``review``
      bucket has no such gate, so timeout / A-only / typo rows
      remain eligible for review without a promotion path to
      high_confidence.

    * ``domain_mismatch`` is treated as a real inconsistency by
      default, but when the same row also carries ``typo_corrected``
      (the mismatch is a legitimate correction artifact) the engine
      reduces the mismatch contribution to the negative total.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Iterable

from .breakdown import ScoreBreakdown
from .evaluator import SignalEvaluator
from .profile import ScoringProfile
from .signal import ScoringSignal


class ScoringEngineV2:
    """Signal-based scoring engine.

    The engine owns:
      * an ordered list of ``SignalEvaluator`` instances
      * a ``ScoringProfile``

    ``evaluate_row(row)`` calls each evaluator with the given row,
    concatenates their signal outputs, applies calibration policy,
    and returns a fully-populated ``ScoreBreakdown``.
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
        """Score a single row.

        Steps:

          1. Collect signals from every evaluator, preserving order.
          2. Rewrite each signal's ``weight`` using the profile's
             ``signal_weights`` when present — the profile is the
             single source of truth for calibration.
          3. Detect a hard stop (any reason code in
             ``profile.hard_stop_policy``). First match wins.
          4. Sum positive and negative weights. Apply the
             typo+mismatch reduction described in the module
             docstring.
          5. Normalize: ``final_score = raw_score /
             max_positive_possible``, clamped to [0.0, 1.0]. A
             hard-stopped row short-circuits to ``0.0``.
          6. Aggregate ``confidence_v2`` as a weighted average of
             per-signal ``confidence`` using
             ``profile.confidence_weights``.
          7. Pick a bucket. ``high_confidence`` requires score
             threshold + confidence threshold + strong-evidence
             reason code. ``review`` requires only the review
             threshold. Everything else is ``invalid``. Hard-stopped
             rows are always ``invalid``.
          8. Build the audit trail: ``breakdown_dict`` and a
             deterministic human-readable ``explanation`` string.
        """
        collected: list[ScoringSignal] = []
        for evaluator in self._evaluators:
            produced = evaluator.evaluate(row) or []
            collected.extend(produced)

        # Apply profile weight overrides. Evaluators carry intrinsic
        # weights; the profile is authoritative, so we rewrite the
        # signal in place (signals are frozen — use dataclass.replace).
        calibrated: list[ScoringSignal] = []
        for sig in collected:
            eff = self._profile.effective_weight(sig.reason_code, sig.weight)
            if eff != sig.weight:
                calibrated.append(replace(sig, weight=eff))
            else:
                calibrated.append(sig)

        reason_codes = [s.reason_code for s in calibrated]
        reason_code_set = set(reason_codes)

        # Hard-stop detection — first matching policy code wins.
        hard_stop = False
        hard_stop_reason: str | None = None
        for code in self._profile.hard_stop_policy:
            if code in reason_code_set:
                hard_stop = True
                hard_stop_reason = code
                break

        # Aggregate positive / negative totals from the calibrated weights.
        positive_total = sum(
            s.weight for s in calibrated if s.direction == "positive"
        )
        negative_total = sum(
            s.weight for s in calibrated if s.direction == "negative"
        )

        # Typo + mismatch reduction.
        # A legitimate typo correction mechanically produces a
        # domain_mismatch signal (the corrected domain, by
        # definition, differs from the input column). Penalizing
        # both at full weight punishes the very case V2 was trying
        # to promote. We reduce — not remove — the mismatch
        # contribution when both signals co-occur.
        mismatch_adjustment = 0.0
        if (
            "typo_corrected" in reason_code_set
            and "domain_mismatch" in reason_code_set
        ):
            mismatch_weight = self._profile.effective_weight(
                "domain_mismatch", 0.0
            )
            mismatch_adjustment = (
                mismatch_weight
                * self._profile.mismatch_reduction_when_typo_corrected
            )
            negative_total -= mismatch_adjustment

        raw_score = positive_total - negative_total

        # Normalize against the fixed theoretical maximum, clamped.
        # Hard-stopped rows short-circuit to 0.0 regardless of raw.
        if hard_stop:
            final_score = 0.0
        else:
            denom = self._profile.derived_max_positive_possible()
            if denom > 0:
                final_score = raw_score / denom
            else:
                final_score = 0.0
            if final_score < 0.0:
                final_score = 0.0
            elif final_score > 1.0:
                final_score = 1.0

        confidence = _aggregate_confidence(calibrated, self._profile)

        # Structural-only cap. A row that fires zero DNS signals
        # (neither positive DNS nor negative DNS) has nothing but
        # high-confidence structural evidence, so the weighted
        # average hits 1.0 by construction even though we have no
        # discriminating evidence. Clamp to a ceiling below the
        # high-confidence-gate floor so these rows cannot cluster
        # above 0.80. Rows with any DNS signal — including timeouts
        # and A-fallback — are exempt from the cap.
        dns_signal_present = any(s.name == "dns" for s in calibrated)
        if not dns_signal_present and not hard_stop:
            ceiling = self._profile.structural_only_confidence_ceiling
            if confidence > ceiling:
                confidence = ceiling

        # Bucket selection.
        if hard_stop:
            bucket = "invalid"
        else:
            bucket = _select_bucket(
                final_score=final_score,
                confidence=confidence,
                reason_code_set=reason_code_set,
                profile=self._profile,
            )

        explanation = _build_explanation(
            hard_stop=hard_stop,
            hard_stop_reason=hard_stop_reason,
            bucket=bucket,
            final_score=final_score,
            confidence=confidence,
            reason_codes=reason_codes,
            mismatch_adjustment=mismatch_adjustment,
        )

        breakdown_dict = {
            "positive_total": positive_total,
            "negative_total": negative_total,
            "raw_score": raw_score,
            "final_score": final_score,
            "confidence": confidence,
            "hard_stop": hard_stop,
            "hard_stop_reason": hard_stop_reason,
            "bucket": bucket,
            "reason_codes": list(reason_codes),
            "explanation": explanation,
            "mismatch_adjustment": mismatch_adjustment,
            "max_positive_possible": (
                self._profile.derived_max_positive_possible()
            ),
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
                for s in calibrated
            ],
        }

        return ScoreBreakdown(
            signals=calibrated,
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


# ---------------------------------------------------------------------------
# Aggregation helpers
# ---------------------------------------------------------------------------


def _aggregate_confidence(
    signals: list[ScoringSignal], profile: ScoringProfile
) -> float:
    """Weighted average of per-signal confidences.

    Weight is read from ``profile.confidence_weights`` (reason-code
    keyed), NOT from the signal's score weight. The separation is the
    point of this helper: structural signals like ``syntax_valid``
    and ``domain_present`` are configured with low confidence weights
    so they cannot dominate the average the way they do under a
    plain score-weighted aggregation.
    """
    total_weight = 0.0
    weighted_confidence = 0.0
    for s in signals:
        w = profile.effective_confidence_weight(s.reason_code)
        if w <= 0.0:
            continue
        weighted_confidence += s.confidence * w
        total_weight += w
    if total_weight <= 0.0:
        return 0.0
    return weighted_confidence / total_weight


def _select_bucket(
    *,
    final_score: float,
    confidence: float,
    reason_code_set: set[str],
    profile: ScoringProfile,
) -> str:
    """Return the bucket label for a non-hard-stopped row.

    Rules (in order):
      * ``"high_confidence"`` requires ALL of:
        - ``final_score >= high_confidence_threshold``
        - ``confidence >= high_confidence_min_confidence``
        - at least one reason code in
          ``strong_evidence_reason_codes`` is present.
      * ``"review"`` requires ``final_score >= review_threshold``.
      * otherwise ``"invalid"``.

    The strong-evidence gate is the calibration that stops A-only
    rows from being promoted to ``high_confidence``. ``review`` is
    deliberately not gated on strong evidence so that timeout /
    A-only rows remain reviewable.
    """
    has_strong_evidence = bool(
        reason_code_set & profile.strong_evidence_reason_codes
    ) if profile.strong_evidence_reason_codes else True

    if (
        final_score >= profile.high_confidence_threshold
        and confidence >= profile.high_confidence_min_confidence
        and has_strong_evidence
    ):
        return "high_confidence"

    if final_score >= profile.review_threshold:
        return "review"

    return "invalid"


def _build_explanation(
    *,
    hard_stop: bool,
    hard_stop_reason: str | None,
    bucket: str,
    final_score: float,
    confidence: float,
    reason_codes: list[str],
    mismatch_adjustment: float,
) -> str:
    """Deterministic human-readable explanation string.

    Stable across runs given the same inputs. Does not include
    wall-clock timestamps, random ordering, or floating-point noise
    beyond a fixed precision. Suitable for audit exports.
    """
    if hard_stop:
        return (
            f"Hard stop: {hard_stop_reason}. Bucket=invalid, "
            f"score=0.00, confidence={confidence:.2f}."
        )

    codes_display = ", ".join(reason_codes) if reason_codes else "none"
    extras: list[str] = []
    if mismatch_adjustment > 0:
        extras.append(
            f"domain_mismatch reduced by {mismatch_adjustment:.2f} due "
            f"to typo_corrected co-signal"
        )
    extras_str = f" ({'; '.join(extras)})" if extras else ""
    return (
        f"Bucket={bucket}, score={final_score:.2f}, "
        f"confidence={confidence:.2f}. Reasons: {codes_display}{extras_str}."
    )
