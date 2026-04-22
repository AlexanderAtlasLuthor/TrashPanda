"""V2 Phase 5 — natural-language explanations for the deliverability model.

Given a :class:`DeliverabilityComputation`, produce a short sentence
describing why the row earned its probability. Every combination of
factors maps to exactly one string; there is no templating magic.
"""

from __future__ import annotations

from .row_model import DeliverabilityComputation, Factor


# Short human phrases for each factor key. Preserving these in a
# central dict lets the summary report reuse the same names.
_FACTOR_PHRASES: dict[str, str] = {
    "smtp:deliverable": "strong SMTP signal",
    "smtp:undeliverable": "SMTP rejection",
    "smtp:catch_all": "SMTP catch-all signal",
    "smtp:inconclusive": "inconclusive SMTP probe",
    "history:historically_reliable": "reliable domain history",
    "history:historically_unstable": "unstable domain history",
    "history:historically_risky": "risky domain history",
    "catch_all:strong": "strong catch-all heuristic",
    "catch_all:moderate": "moderate catch-all heuristic",
}


_OVERRIDE_TEXT: dict[str, str] = {
    "hard_fail": "Zero probability: row was hard-failed by validation.",
    "duplicate": "Zero probability: row was removed as a duplicate.",
    "no_mx_record": "Zero probability: domain has no mail server.",
}


def _phrase(factor: Factor) -> str:
    return _FACTOR_PHRASES.get(factor.name, factor.name)


def explain_deliverability(computation: DeliverabilityComputation) -> str:
    """Return one sentence describing the probability and its main drivers."""
    if computation.override_reason:
        return _OVERRIDE_TEXT.get(
            computation.override_reason,
            "Zero probability (overridden).",
        )

    label = computation.label.capitalize()
    positives = tuple(f for f in computation.factors if f.multiplier > 1.0)
    negatives = tuple(f for f in computation.factors if f.multiplier < 1.0)

    # Sort so the strongest signal is first in either direction.
    positives = tuple(sorted(positives, key=lambda f: f.multiplier, reverse=True))
    negatives = tuple(sorted(negatives, key=lambda f: f.multiplier))

    if positives and negatives:
        pos = _phrase(positives[0])
        neg = _phrase(negatives[0])
        return f"{label} probability: boosted by {pos}, tempered by {neg}."

    if positives:
        names = " and ".join(_phrase(f) for f in positives[:2])
        return f"{label} probability driven by {names}."

    if negatives:
        names = " and ".join(_phrase(f) for f in negatives[:2])
        return f"{label} probability: reduced by {names}."

    return f"{label} probability based on V1 score alone (no V2 signals fired)."


__all__ = ["explain_deliverability"]
