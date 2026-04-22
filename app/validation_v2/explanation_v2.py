"""Human-readable explanations built from domain history.

Deterministic strings — no templating magic, no randomness. Every
returned phrase is grounded in the record's actual counts so the text
stays defensible when a user asks "why does TrashPanda say this?".
"""

from __future__ import annotations

from .domain_memory import DEFAULT_THRESHOLDS, LabelThresholds, classify_domain
from .history_models import DomainHistoryRecord, HistoricalLabel


def _pct(rate: float) -> str:
    return f"{rate * 100:.0f}%"


def explain_domain_history(
    record: DomainHistoryRecord | None,
    thresholds: LabelThresholds = DEFAULT_THRESHOLDS,
) -> str:
    """Return a single-sentence explanation for this domain's history.

    Callers may embed this in review explanations, CSV reports, or
    tooltips. The text deliberately avoids pipeline jargon (``a_fallback``,
    ``hard_stop``) in favour of plain English.
    """
    if record is None:
        return "No prior history for this domain."

    label = classify_domain(record, thresholds)
    seen = record.total_seen_count

    if label == HistoricalLabel.INSUFFICIENT_DATA:
        if seen == 0:
            return "No prior history for this domain."
        if seen == 1:
            return "Domain has been observed once before; history is too thin to draw conclusions."
        return (
            f"Domain has only been observed {seen} times; not enough history to draw conclusions."
        )

    if label == HistoricalLabel.RELIABLE:
        return (
            f"Domain has been historically reliable: MX present in {_pct(record.mx_rate)} of "
            f"{seen} prior observations and ready-to-send in {_pct(record.ready_rate)} of them."
        )

    if label == HistoricalLabel.UNSTABLE:
        worst = max(record.timeout_rate, record.dns_failure_rate)
        return (
            f"Domain has elevated network instability across prior runs "
            f"(timeout or DNS failure in {_pct(worst)} of {seen} observations)."
        )

    if label == HistoricalLabel.RISKY:
        worst = max(record.invalid_rate, record.hard_fail_rate)
        return (
            f"Domain has been historically risky: rejected in {_pct(worst)} of "
            f"{seen} prior observations."
        )

    # Neutral — summarise the dominant signal if there is one.
    return (
        f"Domain has mixed history across {seen} prior observations "
        f"(ready {_pct(record.ready_rate)}, review {_pct(record.review_rate)}, "
        f"invalid {_pct(record.invalid_rate)})."
    )


def readiness_label_for_report(
    record: DomainHistoryRecord | None,
    thresholds: LabelThresholds = DEFAULT_THRESHOLDS,
) -> str:
    """Shorter label suitable for a CSV ``readiness_label`` column."""
    if record is None:
        return HistoricalLabel.INSUFFICIENT_DATA
    return classify_domain(record, thresholds)


# --------------------------------------------------------------------------- #
# Phase 2 — per-row human-readable explanations                               #
# --------------------------------------------------------------------------- #


# Risk grades used in human outputs. Ordered most-severe → least.
_RISK_CRITICAL = "Critical"
_RISK_HIGH = "High"
_RISK_MEDIUM_HIGH = "Medium-High"
_RISK_MEDIUM = "Medium"
_RISK_LOW_MEDIUM = "Low-Medium"
_RISK_LOW = "Low"
_RISK_NONE = "N/A"


def explain_row_with_history(
    decision: object,  # AdjustmentDecision — duck-typed to avoid import cycle
    record: DomainHistoryRecord | None,
    catch_all: object | None = None,  # CatchAllSignal — duck-typed
) -> dict[str, str]:
    """Return ``human_reason`` / ``human_risk`` / ``human_recommendation``.

    Deterministic dispatch on ``(final_bucket, historical_label)``. Every
    combination has an explicit branch so the output is auditable.

    When ``catch_all`` is provided and fires, the base explanation is
    overridden for ``ready`` / ``review`` buckets to surface catch-all
    context. For ``invalid`` / ``hard_fail`` / ``duplicate`` buckets the
    catch-all signal is ignored — those classifications are terminal.

    ``decision`` and ``catch_all`` are duck-typed to avoid a circular
    import with :mod:`app.validation_v2.scoring_adjustment`.
    """
    bucket = getattr(decision, "final_bucket", "unknown")
    label = getattr(decision, "historical_label", HistoricalLabel.INSUFFICIENT_DATA)

    base_explanation = _base_explanation_by_bucket_and_label(bucket, label)
    if (
        catch_all is not None
        and getattr(catch_all, "is_possible_catch_all", False)
        and bucket in ("ready", "review")
    ):
        return _augment_with_catch_all(base_explanation, bucket, catch_all)
    return base_explanation


def _augment_with_catch_all(
    base: dict[str, str], bucket: str, catch_all: object,
) -> dict[str, str]:
    """Override reason/risk/recommendation when catch-all fires."""
    confidence = getattr(catch_all, "confidence", 0.0)
    pct = int(round(confidence * 100))

    if bucket == "review":
        # Catch-all in review → explicit, calls out what "accept-all" means.
        return {
            "human_reason": (
                f"Domain may accept all inbound emails (catch-all behaviour "
                f"suspected, confidence {pct}%)."
            ),
            "human_risk": _RISK_MEDIUM_HIGH,
            "human_recommendation": (
                "Approve only if you have a direct relationship with this contact."
            ),
        }

    # bucket == "ready"
    return {
        "human_reason": (
            f"Validation passed, but domain shows historical ambiguity "
            f"(possible catch-all, confidence {pct}%)."
        ),
        "human_risk": _RISK_LOW_MEDIUM,
        "human_recommendation": (
            "Safe for known contacts; verify before unsolicited outreach."
        ),
    }


def _base_explanation_by_bucket_and_label(bucket: str, label: str) -> dict[str, str]:
    """Phase-2 explanation matrix, no catch-all awareness."""

    # ── Terminal buckets that override any history signal. ───────── #
    if bucket == "hard_fail":
        return {
            "human_reason": "Row was hard-failed by validation (syntax, disposable, or similar).",
            "human_risk": _RISK_CRITICAL,
            "human_recommendation": "Do not use.",
        }
    if bucket == "duplicate":
        return {
            "human_reason": "Row was removed as a duplicate of a canonical record.",
            "human_risk": _RISK_NONE,
            "human_recommendation": "Use the canonical record instead.",
        }

    # ── Ready ────────────────────────────────────────────────────── #
    if bucket == "ready":
        if label == HistoricalLabel.RELIABLE:
            return {
                "human_reason": "Domain has strong historical reliability and valid mail routing.",
                "human_risk": _RISK_LOW,
                "human_recommendation": "Safe to use.",
            }
        if label == HistoricalLabel.RISKY:
            return {
                "human_reason": "Current run passed validation, but domain has elevated invalid rate in prior runs.",
                "human_risk": _RISK_MEDIUM,
                "human_recommendation": "Monitor delivery; consider a manual spot-check before large sends.",
            }
        if label == HistoricalLabel.UNSTABLE:
            return {
                "human_reason": "Current run passed validation, but domain has elevated network instability in prior runs.",
                "human_risk": _RISK_MEDIUM,
                "human_recommendation": "Safe to use; watch for delivery timeouts.",
            }
        if label == HistoricalLabel.INSUFFICIENT_DATA:
            return {
                "human_reason": "Validation passed; no meaningful historical signal yet for this domain.",
                "human_risk": _RISK_LOW,
                "human_recommendation": "Safe to use.",
            }
        # neutral
        return {
            "human_reason": "Validation passed; historical signal is neutral.",
            "human_risk": _RISK_LOW,
            "human_recommendation": "Safe to use.",
        }

    # ── Review ──────────────────────────────────────────────────── #
    if bucket == "review":
        if label == HistoricalLabel.UNSTABLE:
            return {
                "human_reason": "Domain has elevated timeout or DNS-failure rate across prior runs.",
                "human_risk": _RISK_MEDIUM,
                "human_recommendation": "Review manually before sending.",
            }
        if label == HistoricalLabel.RISKY:
            return {
                "human_reason": "Domain has elevated invalid or hard-fail rate across prior runs.",
                "human_risk": _RISK_MEDIUM_HIGH,
                "human_recommendation": "Review manually; prefer rejecting unless the contact is verified.",
            }
        if label == HistoricalLabel.RELIABLE:
            return {
                "human_reason": "Current run flagged this for review, but domain has strong historical reliability.",
                "human_risk": _RISK_LOW_MEDIUM,
                "human_recommendation": "Likely safe to approve after a quick review.",
            }
        if label == HistoricalLabel.INSUFFICIENT_DATA:
            return {
                "human_reason": "Current run flagged this for review; no decisive historical signal yet.",
                "human_risk": _RISK_MEDIUM,
                "human_recommendation": "Review manually before sending.",
            }
        # neutral
        return {
            "human_reason": "Current run flagged this for review; historical signal is neutral.",
            "human_risk": _RISK_MEDIUM,
            "human_recommendation": "Review manually before sending.",
        }

    # ── Invalid ─────────────────────────────────────────────────── #
    if bucket == "invalid":
        if label == HistoricalLabel.RISKY:
            return {
                "human_reason": "Domain has high historical invalid rate and current run failed validation.",
                "human_risk": _RISK_HIGH,
                "human_recommendation": "Do not use.",
            }
        if label == HistoricalLabel.UNSTABLE:
            return {
                "human_reason": "Current run failed validation; domain also shows unstable historical network signals.",
                "human_risk": _RISK_HIGH,
                "human_recommendation": "Do not use.",
            }
        if label == HistoricalLabel.RELIABLE:
            return {
                "human_reason": "Current run failed validation despite the domain's historical reliability — treat as a one-off failure for now.",
                "human_risk": _RISK_HIGH,
                "human_recommendation": "Do not use for this row; the domain may be fine for other contacts.",
            }
        # insufficient / neutral
        return {
            "human_reason": "Current run failed validation; no strong historical evidence either way.",
            "human_risk": _RISK_HIGH,
            "human_recommendation": "Do not use.",
        }

    # Unknown / unhandled bucket — fallback.
    return {
        "human_reason": "Classification is not determined.",
        "human_risk": "Unknown",
        "human_recommendation": "Review manually.",
    }


__all__ = [
    "explain_domain_history",
    "explain_row_with_history",
    "readiness_label_for_report",
]


# Re-export for external consumers who already import from this module.
_ = _base_explanation_by_bucket_and_label  # referenced by explain_row_with_history
