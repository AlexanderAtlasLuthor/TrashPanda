"""V2 Phase 5 — row-level deliverability probability.

Takes the signals already attached to a technical-CSV row by Phases
1-4 (``score_post_history``, ``historical_label``,
``confidence_adjustment_applied``, ``catch_all_confidence``,
``smtp_result``, DNS/MX state) and collapses them into a single number
in [0, 1] with a ``high`` / ``medium`` / ``low`` bucket label.

Design principles
-----------------
* **Deterministic**: every input combination produces exactly one
  output. No ML, no randomness.
* **Interpretable**: the returned :class:`DeliverabilityComputation`
  carries each applied multiplier as a :class:`Factor`, so an auditor
  can reconstruct every decision.
* **Conservative**: hard guards (hard-fail, duplicate, no-MX) short-
  circuit to probability 0; nothing can "rescue" those rows.
* **Tunable**: all thresholds and multipliers live in
  :class:`ProbabilityThresholds` — callers can override in tests or
  future config without touching the formula.

Notes on ``confidence_adjustment_applied``
------------------------------------------
The Phase-2 adjustment is already reflected in ``score_post_history``
(which is ``score_pre_history + adjustment``). Surfacing it as its own
multiplier here would double-count; callers that want to audit the
signal can still read the raw column.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


# --------------------------------------------------------------------------- #
# Thresholds / multipliers                                                    #
# --------------------------------------------------------------------------- #


@dataclass(slots=True, frozen=True)
class ProbabilityThresholds:
    """All knobs for the Phase-5 model. Defaults are deliberately mild.

    The multipliers are intentionally NOT symmetric: negative signals
    push harder than positive ones, because false positives on a
    deliverability probability cause real bounces while false negatives
    only cost a manual review.
    """

    # Label boundaries.
    high_threshold: float = 0.70
    medium_threshold: float = 0.40

    # SMTP multipliers.
    smtp_deliverable_multiplier: float = 1.20
    smtp_undeliverable_multiplier: float = 0.20
    smtp_catch_all_multiplier: float = 0.60
    smtp_inconclusive_multiplier: float = 0.95  # mild penalty for ambiguity

    # Historical-label multipliers.
    historical_reliable_multiplier: float = 1.10
    historical_unstable_multiplier: float = 0.70
    historical_risky_multiplier: float = 0.60

    # Catch-all confidence bands.
    catch_all_strong_threshold: float = 0.60
    catch_all_strong_multiplier: float = 0.50
    catch_all_moderate_threshold: float = 0.35
    catch_all_moderate_multiplier: float = 0.75


DEFAULT_PROBABILITY_THRESHOLDS: ProbabilityThresholds = ProbabilityThresholds()


# --------------------------------------------------------------------------- #
# Inputs / outputs                                                            #
# --------------------------------------------------------------------------- #


@dataclass(slots=True, frozen=True)
class DeliverabilityInputs:
    """All signals the probability model consumes for a single row."""

    score_post_history: int               # 0-100
    historical_label: str                 # historically_reliable/unstable/risky/neutral/insufficient_data
    confidence_adjustment_applied: int    # recorded but not applied (see module docstring)
    catch_all_confidence: float           # 0-1
    smtp_result: str                      # deliverable / undeliverable / catch_all / inconclusive / not_tested
    smtp_confidence: float                # 0-1 (reserved for future nuance)
    has_mx_record: bool
    hard_fail: bool
    v2_final_bucket: str                  # ready / review / invalid / hard_fail / duplicate / unknown


@dataclass(slots=True, frozen=True)
class Factor:
    """One signal applied to the probability. Captured verbatim for audit."""

    name: str            # short machine-readable code, e.g. "smtp:deliverable"
    multiplier: float    # > 1 pushes up, < 1 pushes down
    description: str     # single sentence, fit for a CSV cell


@dataclass(slots=True, frozen=True)
class DeliverabilityComputation:
    """Returned by :func:`compute_deliverability_probability`."""

    probability: float                   # final, clamped to [0, 1]
    label: str                           # "high" | "medium" | "low"
    base_probability: float              # score_post_history / 100 (pre-factors)
    factors: tuple[Factor, ...]          # applied, in order
    override_reason: str                 # non-empty when a hard guard forced probability=0


# --------------------------------------------------------------------------- #
# Core model                                                                  #
# --------------------------------------------------------------------------- #


_HARD_OVERRIDE_REASONS: dict[str, str] = {
    "hard_fail": "Row was hard-failed by validation.",
    "duplicate": "Row was removed as a duplicate of a canonical record.",
    "no_mx_record": "Domain has no mail server (MX record missing).",
}


def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, value))


def _label_for(probability: float, thresholds: ProbabilityThresholds) -> str:
    if probability >= thresholds.high_threshold:
        return "high"
    if probability >= thresholds.medium_threshold:
        return "medium"
    return "low"


def _override(reason: str) -> DeliverabilityComputation:
    return DeliverabilityComputation(
        probability=0.0,
        label="low",
        base_probability=0.0,
        factors=(Factor(f"override:{reason}", 0.0, _HARD_OVERRIDE_REASONS[reason]),),
        override_reason=reason,
    )


def compute_deliverability_probability(
    inputs: DeliverabilityInputs,
    thresholds: ProbabilityThresholds = DEFAULT_PROBABILITY_THRESHOLDS,
) -> DeliverabilityComputation:
    """Collapse all V2 signals for one row into a probability in [0, 1].

    Evaluation order:
      1. Hard overrides (hard_fail, duplicate, no MX) → probability = 0.
      2. ``base = score_post_history / 100`` (clamped to [0, 1]).
      3. SMTP multiplier (if known).
      4. Historical-label multiplier (if set).
      5. Catch-all multiplier (tiered by confidence).
      6. Final clamp to [0, 1] and label assignment.

    The raw product is intentionally allowed to exceed 1 before the
    final clamp so strong positive stacking is visible in the factors.
    """
    # ── Hard overrides ──────────────────────────────────────────── #
    if inputs.hard_fail or inputs.v2_final_bucket == "hard_fail":
        return _override("hard_fail")
    if inputs.v2_final_bucket == "duplicate":
        return _override("duplicate")
    if not inputs.has_mx_record:
        return _override("no_mx_record")

    # ── Base ────────────────────────────────────────────────────── #
    base = _clamp(float(inputs.score_post_history) / 100.0)
    probability = base
    factors: list[Factor] = []

    # ── SMTP multiplier ─────────────────────────────────────────── #
    smtp_map = {
        "deliverable": (thresholds.smtp_deliverable_multiplier,
                        "SMTP probe confirmed deliverability."),
        "undeliverable": (thresholds.smtp_undeliverable_multiplier,
                          "SMTP probe reported the address as undeliverable."),
        "catch_all": (thresholds.smtp_catch_all_multiplier,
                      "SMTP probe suggests catch-all behaviour."),
        "inconclusive": (thresholds.smtp_inconclusive_multiplier,
                         "SMTP probe was inconclusive."),
    }
    if inputs.smtp_result in smtp_map:
        mult, description = smtp_map[inputs.smtp_result]
        factors.append(Factor(f"smtp:{inputs.smtp_result}", mult, description))
        probability *= mult

    # ── Historical label multiplier ─────────────────────────────── #
    hist_map = {
        "historically_reliable": (thresholds.historical_reliable_multiplier,
                                  "Domain has been historically reliable."),
        "historically_unstable": (thresholds.historical_unstable_multiplier,
                                  "Domain has elevated historical instability."),
        "historically_risky": (thresholds.historical_risky_multiplier,
                               "Domain has elevated historical invalid/hard-fail rate."),
    }
    if inputs.historical_label in hist_map:
        mult, description = hist_map[inputs.historical_label]
        factors.append(Factor(
            f"history:{inputs.historical_label}", mult, description,
        ))
        probability *= mult

    # ── Catch-all multiplier (tiered) ───────────────────────────── #
    if inputs.catch_all_confidence >= thresholds.catch_all_strong_threshold:
        mult = thresholds.catch_all_strong_multiplier
        factors.append(Factor(
            "catch_all:strong",
            mult,
            f"Strong catch-all signal (confidence {inputs.catch_all_confidence:.2f}).",
        ))
        probability *= mult
    elif inputs.catch_all_confidence >= thresholds.catch_all_moderate_threshold:
        mult = thresholds.catch_all_moderate_multiplier
        factors.append(Factor(
            "catch_all:moderate",
            mult,
            f"Moderate catch-all signal (confidence {inputs.catch_all_confidence:.2f}).",
        ))
        probability *= mult

    # ── Clamp + label ───────────────────────────────────────────── #
    final_probability = _clamp(probability)
    return DeliverabilityComputation(
        probability=final_probability,
        label=_label_for(final_probability, thresholds),
        base_probability=base,
        factors=tuple(factors),
        override_reason="",
    )


# --------------------------------------------------------------------------- #
# Convenience: build DeliverabilityInputs from a CSV row dict                 #
# --------------------------------------------------------------------------- #


def _truthy(value: Any) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() in ("1", "true", "t", "yes", "y")


def _int_or(value: Any, default: int) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _float_or(value: Any, default: float) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def inputs_from_row(row: dict[str, str]) -> DeliverabilityInputs:
    """Best-effort extraction from a CSV row dict.

    Gracefully handles missing columns (falls back to V1 values or
    neutral defaults) so the model still produces something sensible
    even if earlier V2 phases were disabled.
    """
    # Prefer Phase-2 post-history score; fall back to V1 score; then 0.
    score = _int_or(row.get("score_post_history"), _int_or(row.get("score"), 0))
    return DeliverabilityInputs(
        score_post_history=score,
        historical_label=(row.get("historical_label") or "").strip() or "neutral",
        confidence_adjustment_applied=_int_or(row.get("confidence_adjustment_applied"), 0),
        catch_all_confidence=_float_or(row.get("catch_all_confidence"), 0.0),
        smtp_result=(row.get("smtp_result") or "").strip() or "not_tested",
        smtp_confidence=_float_or(row.get("smtp_confidence"), 0.0),
        has_mx_record=_truthy(row.get("has_mx_record")),
        hard_fail=_truthy(row.get("hard_fail")),
        v2_final_bucket=(row.get("v2_final_bucket") or "").strip() or "unknown",
    )


__all__ = [
    "DEFAULT_PROBABILITY_THRESHOLDS",
    "DeliverabilityComputation",
    "DeliverabilityInputs",
    "Factor",
    "ProbabilityThresholds",
    "compute_deliverability_probability",
    "inputs_from_row",
]
