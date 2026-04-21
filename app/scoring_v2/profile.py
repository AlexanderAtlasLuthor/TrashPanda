"""ScoringProfile: configuration object for ScoringEngineV2.

A profile is a declarative bundle of: per-reason-code weights (for both
raw-score aggregation and confidence aggregation), bucket thresholds,
hard-stop policy (which reason codes force ``hard_stop``), the fixed
theoretical positive maximum used to normalize ``final_score``, and an
open-ended ``bucket_policy`` dict reserved for later rule extensions.

The profile is data-only. It does not contain scoring logic — the engine
consumes it.

Calibration notes (2026-04 pass):

The profile now carries three concepts that previously lived as
hard-coded literals inside the engine. Calibrating V2 means changing
values here, not code in ``engine.py``.

    * ``signal_weights`` — per reason-code weight used in the raw-score
      aggregation. Overrides the evaluator's intrinsic weight when the
      reason code appears in the dict. Missing keys fall back to the
      evaluator's intrinsic weight.

    * ``confidence_weights`` — per reason-code influence on
      ``confidence_v2``. Structural signals (syntax_valid,
      domain_present) are deliberately down-weighted here so a row
      with trivial positive evidence cannot inflate confidence the
      way the raw weighted-average used to.

    * ``max_positive_possible`` + ``max_positive_contributors`` —
      the fixed theoretical maximum raw positive score a single row
      can achieve. The engine normalizes ``raw_score`` against this
      fixed denominator so ``final_score`` lives on a stable [0.0,
      1.0] scale and weak-positive-only rows cannot score 1.0 just
      because they happen to emit only positive signals.

    * ``strong_evidence_reason_codes`` — the set of reason codes
      that qualify a row as having *strong* evidence. The
      ``high_confidence`` bucket is gated on at least one of these
      being present. The default set is ``{"mx_present"}`` —
      conservative but clean.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ScoringProfile:
    """Configuration bundle for ScoringEngineV2.

    Attributes:
        weights: Deprecated alias for ``signal_weights`` — kept so older
            call sites that passed ``weights={...}`` keep working. If
            both are provided, ``signal_weights`` wins.
        signal_weights: Per-reason-code weight multipliers applied
            during raw-score aggregation. Missing keys → use the
            evaluator's intrinsic weight.
        confidence_weights: Per-reason-code influence on the row-level
            confidence aggregation. Distinct from ``signal_weights``
            so calibration can down-weight structural signals (which
            otherwise dominate a raw weighted average) without
            changing how the score itself is computed. Missing keys
            default to ``1.0``.
        max_positive_possible: Fixed theoretical maximum raw positive
            score used as the denominator when normalizing
            ``final_score``. Must be > 0 if ``max_positive_contributors``
            is empty. When ``max_positive_contributors`` is non-empty
            the engine recomputes this value from ``signal_weights``,
            overriding any explicit value set here.
        max_positive_contributors: The set of positive reason codes
            a single row can achieve simultaneously. Used by the
            engine to derive ``max_positive_possible`` deterministically
            from ``signal_weights``. Mutually-exclusive positives
            (e.g. ``mx_present`` vs ``a_fallback``) should include
            only the strongest — the denominator represents the best
            possible row, not a sum of every positive reason code.
        high_confidence_threshold: Minimum ``final_score`` (in [0.0,
            1.0]) required for the ``"high_confidence"`` bucket.
        review_threshold: Minimum ``final_score`` (in [0.0, 1.0])
            required for the ``"review"`` bucket. Must satisfy
            ``review_threshold <= high_confidence_threshold``.
        high_confidence_min_confidence: Minimum ``confidence_v2``
            required for the ``"high_confidence"`` bucket. Defaults
            to 0.80.
        strong_evidence_reason_codes: Reason codes that qualify a
            row for strong-evidence gating. A row cannot land in
            ``"high_confidence"`` unless at least one of these is
            present. Default: ``{"mx_present"}``.
        hard_stop_policy: Reason codes that force ``hard_stop=True``
            regardless of the numeric score. Mirrors V1's hard-fail
            vocabulary (``"syntax_invalid"``, ``"no_domain"``,
            ``"nxdomain"``) and is open-ended for future additions.
        mismatch_reduction_when_typo_corrected: Fraction of the
            ``domain_mismatch`` weight removed from the negative
            total when the same row also carries
            ``typo_corrected``. A legitimate typo correction
            mechanically produces a mismatch — V2 should not double-
            penalize that scenario. Defaults to 0.5 (cut in half);
            set to 1.0 to fully remove the penalty for corrected
            rows, 0.0 to keep it as-is.
        structural_only_confidence_ceiling: Upper bound on the
            aggregated row confidence for rows that emitted zero
            DNS signals (positive or negative). Trivial rows that
            only fire structural positives (syntax_valid,
            domain_present) have nothing but 1.0-confidence
            structural evidence, so their weighted-average
            confidence is 1.0 by construction — which is misleading.
            Clamping the reported confidence below the
            high-confidence-gate floor prevents that cluster from
            polluting downstream confidence-based analytics.
        bucket_policy: Open-ended dict reserved for future bucket-rule
            extensions (per-industry profiles, override rules,
            calibration curves, etc.). Left intentionally untyped so
            early experimentation does not force schema churn.
    """

    weights: dict[str, float] = field(default_factory=dict)
    signal_weights: dict[str, float] = field(default_factory=dict)
    confidence_weights: dict[str, float] = field(default_factory=dict)
    max_positive_possible: float = 0.0
    max_positive_contributors: set[str] = field(default_factory=set)
    high_confidence_threshold: float = 0.75
    review_threshold: float = 0.40
    high_confidence_min_confidence: float = 0.80
    strong_evidence_reason_codes: set[str] = field(default_factory=set)
    hard_stop_policy: list[str] = field(default_factory=list)
    mismatch_reduction_when_typo_corrected: float = 0.5
    structural_only_confidence_ceiling: float = 0.75
    bucket_policy: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        # Merge legacy ``weights`` into ``signal_weights`` — signal_weights wins.
        if self.weights and not self.signal_weights:
            self.signal_weights = dict(self.weights)

    def effective_weight(self, reason_code: str, intrinsic: float) -> float:
        """Return the weight the engine should use for ``reason_code``.

        If the profile configured a weight for ``reason_code`` use
        that; otherwise fall back to the evaluator's intrinsic weight.
        """
        if reason_code in self.signal_weights:
            return float(self.signal_weights[reason_code])
        return intrinsic

    def effective_confidence_weight(self, reason_code: str) -> float:
        """Return the confidence-aggregation weight for ``reason_code``.

        Missing keys default to ``1.0`` — a reason code without an
        explicit confidence weight is treated as fully influential.
        """
        return float(self.confidence_weights.get(reason_code, 1.0))

    def derived_max_positive_possible(self) -> float:
        """Deterministically compute ``max_positive_possible``.

        If ``max_positive_contributors`` is non-empty, sum the
        ``signal_weights`` of those reason codes. Otherwise return
        the explicitly-set ``max_positive_possible``.

        Why this shape: the engine must normalize against the full
        positive evidence space, not just the subset a particular
        row emitted. Listing the reason codes that form the
        theoretical maximum — and reading their weights from the
        profile, not from hard-coded literals — keeps the denominator
        in sync with any future weight change.
        """
        if self.max_positive_contributors:
            total = 0.0
            for rc in self.max_positive_contributors:
                if rc in self.signal_weights:
                    total += float(self.signal_weights[rc])
            if total > 0.0:
                return total
        return float(self.max_positive_possible)


# ---------------------------------------------------------------------------
# Default calibrated profile
# ---------------------------------------------------------------------------


def build_default_profile() -> ScoringProfile:
    """Return the current calibrated default profile.

    Represents the 2026-04 calibration pass:

      * Positive weights mirror V1's point budget so V2 and V1 can be
        compared side by side without rescale.
      * The theoretical positive max is the sum of the strongest set
        of simultaneously-achievable positives:
        ``syntax_valid`` (25) + ``domain_present`` (10) +
        ``mx_present`` (50) + ``domain_match`` (5) = 90.
        ``a_fallback`` is *not* in the max set — it is mutually
        exclusive with ``mx_present`` and weaker. That is what makes
        A-only rows unable to reach ``final_score = 1.0`` under the
        new normalization.
      * Confidence weights down-weight structural signals
        (``syntax_valid``, ``domain_present``) so a row with only
        trivial positive evidence no longer drags confidence above
        0.80. DNS signals keep their full influence because they are
        the discriminating evidence.
      * ``domain_mismatch`` is bumped from 5 → 8. A conservative
        increase: still recoverable, but no longer trivially
        outweighed. When the same row carries ``typo_corrected``
        (i.e. the mismatch is a legitimate correction artifact) the
        engine cuts the penalty in half — preserving V2's
        typo+MX promotion behavior.
      * ``strong_evidence_reason_codes = {"mx_present"}`` — the
        ``high_confidence`` bucket is gated on ``mx_present`` being
        present. A-only rows cannot be high-confidence.
    """
    signal_weights: dict[str, float] = {
        # Positive
        "syntax_valid": 25.0,
        "domain_present": 10.0,
        "mx_present": 50.0,
        "a_fallback": 20.0,
        "domain_match": 5.0,
        # Negative — hard-stop-class signals carry V1's weights.
        "syntax_invalid": 50.0,
        "no_domain": 50.0,
        "nxdomain": 50.0,
        # Negative — DNS uncertainty. These are transient, low-evidence
        # signals: we reduced their score-weight from V1's 15 → 10 so
        # a timeout on an otherwise-valid row lands in ``review``, not
        # ``invalid``. Their confidence-weight stays at 1.0 so they
        # still pull ``confidence_v2`` down.
        "dns_timeout": 10.0,
        "dns_no_nameservers": 10.0,
        "dns_error": 10.0,
        "dns_no_mx": 10.0,
        "dns_no_mx_no_a": 10.0,
        "domain_not_resolving": 10.0,
        # Negative — structural mild
        "typo_corrected": 3.0,
        "domain_mismatch": 8.0,
    }

    confidence_weights: dict[str, float] = {
        # Structural positives — low influence on confidence. Their
        # presence is necessary but not discriminating; letting them
        # dominate the weighted average was the root cause of
        # confidence inflation in the pre-calibration V2.
        "syntax_valid": 0.2,
        "domain_present": 0.1,
        # Structural negatives still carry full weight — a structural
        # failure is definitive.
        "syntax_invalid": 1.0,
        "no_domain": 1.0,
        # Domain comparison — moderate influence.
        "domain_match": 0.3,
        "domain_mismatch": 0.5,
        "typo_corrected": 0.3,
        # DNS signals — full influence. These are the discriminating
        # evidence and should drive confidence.
        "mx_present": 1.0,
        "a_fallback": 1.0,
        "nxdomain": 1.0,
        "dns_timeout": 1.0,
        "dns_no_nameservers": 1.0,
        "dns_error": 1.0,
        "dns_no_mx": 1.0,
        "dns_no_mx_no_a": 1.0,
        "domain_not_resolving": 1.0,
    }

    return ScoringProfile(
        signal_weights=signal_weights,
        confidence_weights=confidence_weights,
        # syntax_valid + domain_present + mx_present + domain_match
        # = 25 + 10 + 50 + 5 = 90. Sum is derived from signal_weights
        # above, not hard-coded as a literal here.
        max_positive_contributors={
            "syntax_valid",
            "domain_present",
            "mx_present",
            "domain_match",
        },
        high_confidence_threshold=0.75,
        review_threshold=0.30,
        high_confidence_min_confidence=0.80,
        strong_evidence_reason_codes={"mx_present"},
        hard_stop_policy=["syntax_invalid", "no_domain", "nxdomain"],
        mismatch_reduction_when_typo_corrected=0.5,
        structural_only_confidence_ceiling=0.75,
    )
