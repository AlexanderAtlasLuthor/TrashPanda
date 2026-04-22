"""V2 Phase 5 — row-level deliverability probability (additive model v2).

**This is the redesigned additive model** that replaces the previous
multiplicative one. See ``docs`` below for rationale.

Why a new model
---------------
The previous multiplicative model (``base = score/100; *= mult; ...``)
produced only a few discrete probability values in practice because:

* the base already lived in a narrow band (0.70–0.85 for real domains),
* multipliers were few and stacked clusters of rows on the same product,
* hard guards collapsed every "no-MX" case to exactly 0.000.

The new model is:

* **Additive**: each signal adds a signed delta to a constant base.
* **Soft**: hard guards are reserved for ``hard_fail`` / ``duplicate``;
  missing MX is a large negative delta (``-0.25``) that can still be
  partially rescued by an A-fallback or strong history.
* **Continuous**: a tiny deterministic noise (±0.02) seeded from the
  row's identity is added so near-identical signal sets spread slightly
  and the distribution is continuous, not clustered.
* **Interpretable**: each applied delta is captured as a :class:`Factor`
  so auditors can reconstruct every decision.

Breaking change notes
---------------------
``Factor.multiplier`` is retained as the field name for backward
compatibility with downstream consumers (reports, serializers), **but
its semantics are now a signed delta**, not a multiplicative factor:

* ``multiplier > 0``  → positive signal (pushed probability up)
* ``multiplier < 0``  → negative signal (pushed probability down)
* ``multiplier == 0`` → override / recorded but inert

Public API (``compute_deliverability_probability``, ``inputs_from_row``,
``DeliverabilityInputs``, ``DeliverabilityComputation``, ``Factor``,
``ProbabilityThresholds``, ``DEFAULT_PROBABILITY_THRESHOLDS``) is
signature-stable; only the math changed.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any


# --------------------------------------------------------------------------- #
# Thresholds / weights                                                        #
# --------------------------------------------------------------------------- #


@dataclass(slots=True, frozen=True)
class ProbabilityThresholds:
    """All knobs for the additive Phase-5 model.

    Each ``*_weight`` field is the signed delta added to ``base_score``
    when that signal fires. Negative weights are deliberately bigger in
    magnitude than positive ones: false positives on a deliverability
    probability cause real bounces, false negatives only cost a review.
    """

    # Label boundaries.
    high_threshold: float = 0.80
    medium_threshold: float = 0.50

    # Base score (constant). Rows start here and deltas move them.
    base_score: float = 0.50

    # --- DNS / routing -------------------------------------------------- #
    mx_present_weight: float = 0.20
    a_fallback_weight: float = 0.05           # no MX but A record exists
    no_dns_weight: float = -0.25              # neither MX nor A

    # --- Domain quality ------------------------------------------------- #
    domain_match_weight: float = 0.05         # email domain == input domain column
    typo_detected_weight: float = -0.05       # typo suggested (not corrected)

    # --- Historical label ---------------------------------------------- #
    historical_reliable_weight: float = 0.05
    historical_unstable_weight: float = -0.15
    historical_risky_weight: float = -0.10
    # neutral / insufficient_data contribute 0.

    # --- SMTP probe ---------------------------------------------------- #
    smtp_deliverable_weight: float = 0.10
    smtp_undeliverable_weight: float = -0.25
    smtp_catch_all_weight: float = -0.15
    smtp_inconclusive_weight: float = -0.03
    # not_tested contributes 0.

    # --- Catch-all (heuristic) ---------------------------------------- #
    catch_all_flag_weight: float = -0.10       # possible_catch_all bool
    catch_all_strong_threshold: float = 0.60
    catch_all_strong_weight: float = -0.05     # stacks on catch_all_flag_weight
    catch_all_moderate_threshold: float = 0.35
    catch_all_moderate_weight: float = -0.02

    # --- Smoothing ----------------------------------------------------- #
    noise_amplitude: float = 0.02              # ± this much, deterministic


DEFAULT_PROBABILITY_THRESHOLDS: ProbabilityThresholds = ProbabilityThresholds()


# --------------------------------------------------------------------------- #
# Inputs / outputs                                                            #
# --------------------------------------------------------------------------- #


@dataclass(slots=True, frozen=True)
class DeliverabilityInputs:
    """All signals the probability model consumes for a single row."""

    # V1 aggregates (kept for backward compatibility and reports).
    score_post_history: int               # 0-100, informational only in new model
    # History.
    historical_label: str                 # historically_reliable/unstable/risky/neutral/insufficient_data
    confidence_adjustment_applied: int    # recorded but not applied (see module docstring)
    # Catch-all (heuristic pass).
    catch_all_confidence: float           # 0-1
    possible_catch_all: bool = False      # ``possible_catch_all`` flag from catch-all pass
    # SMTP probe.
    smtp_result: str = "not_tested"       # deliverable / undeliverable / catch_all / inconclusive / not_tested
    smtp_confidence: float = 0.0
    # DNS / routing.
    has_mx_record: bool = False
    has_a_record: bool = False
    # V1 quality signals.
    domain_match: bool = False            # ``domain_matches_input_column``
    typo_detected: bool = False           # ``typo_detected`` (suggestion only)
    # Structural.
    hard_fail: bool = False
    v2_final_bucket: str = "unknown"      # ready / review / invalid / hard_fail / duplicate / unknown
    # Row identity (seeds the noise).
    email: str = ""
    domain: str = ""


@dataclass(slots=True, frozen=True)
class Factor:
    """One signal applied to the probability. Captured verbatim for audit.

    ``multiplier`` is the **signed delta** that was added to the running
    score. The historical field name is kept so downstream serializers
    and reports do not need updates; consumers that want strict typing
    should read the sign: ``> 0`` for positive, ``< 0`` for negative,
    ``== 0`` for override / informational.
    """

    name: str            # short machine-readable code, e.g. "smtp:deliverable"
    multiplier: float    # signed additive delta (see class docstring)
    description: str     # single sentence, fit for a CSV cell


@dataclass(slots=True, frozen=True)
class DeliverabilityComputation:
    """Returned by :func:`compute_deliverability_probability`."""

    probability: float                   # final, clamped to [0, 1]
    label: str                           # "high" | "medium" | "low"
    base_probability: float              # pre-factors anchor (== thresholds.base_score)
    factors: tuple[Factor, ...]          # applied, in order
    override_reason: str                 # non-empty when a hard guard forced probability=0


# --------------------------------------------------------------------------- #
# Core model                                                                  #
# --------------------------------------------------------------------------- #


_HARD_OVERRIDE_REASONS: dict[str, str] = {
    "hard_fail": "Row was hard-failed by validation.",
    "duplicate": "Row was removed as a duplicate of a canonical record.",
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


def _deterministic_noise(seed_text: str, amplitude: float) -> float:
    """Return a value in ``[-amplitude, +amplitude]`` derived from ``seed_text``.

    Deterministic: same input always yields the same output. Runs are
    stable; distributions are continuous rather than clustered.
    """
    if amplitude <= 0.0 or not seed_text:
        return 0.0
    digest = hashlib.sha1(seed_text.encode("utf-8", errors="ignore")).digest()
    # Take 6 bytes → 48-bit int → unit float in [0, 1).
    as_int = int.from_bytes(digest[:6], "big")
    unit = as_int / float(1 << 48)
    # Center on 0 and scale to [-amplitude, +amplitude].
    return (unit - 0.5) * 2.0 * amplitude


def compute_deliverability_probability(
    inputs: DeliverabilityInputs,
    thresholds: ProbabilityThresholds = DEFAULT_PROBABILITY_THRESHOLDS,
) -> DeliverabilityComputation:
    """Collapse all V2 signals for one row into a probability in [0, 1].

    Additive model:

      1. Start from ``thresholds.base_score`` (default 0.50).
      2. Add signed deltas from each fired signal (DNS, domain quality,
         history, SMTP, catch-all).
      3. Add a tiny deterministic ±noise based on (email|domain) so
         rows with identical signal sets still spread slightly,
         preventing discrete clustering.
      4. Clamp to [0, 1].
      5. Assign label by ``high_threshold`` / ``medium_threshold``.

    Hard overrides (``hard_fail``, ``duplicate``) short-circuit to 0.
    *Missing MX* is intentionally a soft negative delta, not an
    override, so A-fallback domains can still receive a meaningful
    (but low) probability.
    """
    # ── Hard overrides (narrowed: only truly unrecoverable cases). ── #
    if inputs.hard_fail or inputs.v2_final_bucket == "hard_fail":
        return _override("hard_fail")
    if inputs.v2_final_bucket == "duplicate":
        return _override("duplicate")

    base = thresholds.base_score
    probability = base
    factors: list[Factor] = []

    def apply(code: str, weight: float, description: str) -> None:
        nonlocal probability
        if weight == 0.0:
            return
        factors.append(Factor(code, weight, description))
        probability += weight

    # ── DNS / routing ─────────────────────────────────────────────── #
    if inputs.has_mx_record:
        apply("mx_present", thresholds.mx_present_weight,
              "Domain publishes an MX record.")
    elif inputs.has_a_record:
        apply("a_fallback", thresholds.a_fallback_weight,
              "No MX record, but an A record is available as fallback.")
    else:
        apply("no_dns", thresholds.no_dns_weight,
              "Domain publishes neither MX nor A records.")

    # ── Domain quality ────────────────────────────────────────────── #
    if inputs.domain_match:
        apply("domain_match", thresholds.domain_match_weight,
              "Email's domain matches the input domain column.")
    if inputs.typo_detected:
        apply("typo_detected", thresholds.typo_detected_weight,
              "A typo suggestion was produced for this domain.")

    # ── Historical label ──────────────────────────────────────────── #
    hist_map = {
        "historically_reliable": (
            thresholds.historical_reliable_weight,
            "Domain has been historically reliable.",
        ),
        "historically_unstable": (
            thresholds.historical_unstable_weight,
            "Domain has elevated historical instability.",
        ),
        "historically_risky": (
            thresholds.historical_risky_weight,
            "Domain has elevated historical invalid/hard-fail rate.",
        ),
    }
    if inputs.historical_label in hist_map:
        w, desc = hist_map[inputs.historical_label]
        apply(f"history:{inputs.historical_label}", w, desc)

    # ── SMTP probe ────────────────────────────────────────────────── #
    smtp_map = {
        "deliverable": (
            thresholds.smtp_deliverable_weight,
            "SMTP probe confirmed deliverability.",
        ),
        "undeliverable": (
            thresholds.smtp_undeliverable_weight,
            "SMTP probe reported the address as undeliverable.",
        ),
        "catch_all": (
            thresholds.smtp_catch_all_weight,
            "SMTP probe suggests catch-all behaviour.",
        ),
        "inconclusive": (
            thresholds.smtp_inconclusive_weight,
            "SMTP probe was inconclusive.",
        ),
    }
    if inputs.smtp_result in smtp_map:
        w, desc = smtp_map[inputs.smtp_result]
        apply(f"smtp:{inputs.smtp_result}", w, desc)

    # ── Catch-all (heuristic + confidence bands) ──────────────────── #
    if inputs.possible_catch_all:
        apply("catch_all:flag", thresholds.catch_all_flag_weight,
              "Row flagged as possible catch-all by heuristics.")
    if inputs.catch_all_confidence >= thresholds.catch_all_strong_threshold:
        apply(
            "catch_all:strong",
            thresholds.catch_all_strong_weight,
            f"Strong catch-all confidence ({inputs.catch_all_confidence:.2f}).",
        )
    elif inputs.catch_all_confidence >= thresholds.catch_all_moderate_threshold:
        apply(
            "catch_all:moderate",
            thresholds.catch_all_moderate_weight,
            f"Moderate catch-all confidence ({inputs.catch_all_confidence:.2f}).",
        )

    # ── Smoothing: tiny deterministic noise keyed to row identity. ── #
    noise = _deterministic_noise(
        f"{inputs.email}|{inputs.domain}|{inputs.score_post_history}",
        thresholds.noise_amplitude,
    )
    if noise != 0.0:
        factors.append(Factor(
            "smoothing:noise", noise,
            "Deterministic per-row smoothing to avoid discrete clustering.",
        ))
        probability += noise

    # ── Clamp + label ─────────────────────────────────────────────── #
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
    score = _int_or(row.get("score_post_history"), _int_or(row.get("score"), 0))
    return DeliverabilityInputs(
        score_post_history=score,
        historical_label=(row.get("historical_label") or "").strip() or "neutral",
        confidence_adjustment_applied=_int_or(row.get("confidence_adjustment_applied"), 0),
        catch_all_confidence=_float_or(row.get("catch_all_confidence"), 0.0),
        possible_catch_all=_truthy(row.get("possible_catch_all")),
        smtp_result=(row.get("smtp_result") or "").strip() or "not_tested",
        smtp_confidence=_float_or(row.get("smtp_confidence"), 0.0),
        has_mx_record=_truthy(row.get("has_mx_record")),
        has_a_record=_truthy(row.get("has_a_record")),
        domain_match=_truthy(row.get("domain_matches_input_column")),
        typo_detected=_truthy(row.get("typo_detected")),
        hard_fail=_truthy(row.get("hard_fail")),
        v2_final_bucket=(row.get("v2_final_bucket") or "").strip() or "unknown",
        email=(row.get("email") or "").strip(),
        domain=(row.get("domain") or row.get("corrected_domain") or "").strip(),
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
