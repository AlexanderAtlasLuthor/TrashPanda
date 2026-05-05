"""V2.9.8 — Feedback bridge readiness preview.

Reads V2.7 ``BounceOutcomeStore`` aggregates, runs each one through the
existing :func:`bounce_aggregate_to_domain_intel` bridge helper, and
produces an operator-only preview of how V2.7 feedback would shape
V2.6 domain intelligence in a future run.

This module is preview-only. It does **not**:

* change V2 classification logic,
* mutate ``DomainIntelligenceStage`` or ``domain_intel_cache``,
* touch ``DecisionStage`` or final_action,
* change export routing or report generation,
* change SMTP / catch-all behaviour,
* mutate the feedback store,
* perform any network activity.

The output report ``feedback_domain_intel_preview.json`` is registered
as ``operator_only`` in the V2.9.5 artifact contract so the V2.9.6
client package builder excludes it automatically.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .validation_v2.feedback import (
    DEFAULT_REPUTATION_THRESHOLDS,
    BounceOutcomeStore,
    ReputationThresholds,
    bounce_aggregate_to_domain_intel,
)


_REPORT_VERSION = "v2.9.8"
_REPORT_FILENAME = "feedback_domain_intel_preview.json"

# Behavior-class sort priority: known_risky first, then known_good,
# cold_start, unknown last.
_BEHAVIOR_PRIORITY: dict[str, int] = {
    "known_risky": 0,
    "known_good": 1,
    "cold_start": 2,
    "unknown": 3,
}

# Warning codes
WARNING_FEEDBACK_STORE_MISSING = "feedback_store_missing"
WARNING_FEEDBACK_STORE_EMPTY = "feedback_store_empty"
WARNING_FEEDBACK_STORE_UNREADABLE = "feedback_store_unreadable"


# --------------------------------------------------------------------------- #
# Result models
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class FeedbackDomainIntelPreviewRecord:
    """Per-domain preview projecting V2.7 feedback through V2.6 helper."""

    domain: str
    total_observations: int
    delivered_count: int
    hard_bounce_count: int
    soft_bounce_count: int
    blocked_count: int
    complaint_count: int
    reputation_score: float
    risk_level: str
    behavior_class: str
    cold_start: bool
    reason: str


@dataclass(frozen=True)
class FeedbackDomainIntelPreviewResult:
    """Operator-facing summary of feedback → domain intel preview."""

    report_version: str
    generated_at: str
    feedback_store_path: Path
    output_path: Path | None
    feedback_available: bool
    total_domains: int
    total_observations: int
    known_good_count: int
    known_risky_count: int
    cold_start_count: int
    unknown_count: int
    records: tuple[FeedbackDomainIntelPreviewRecord, ...]
    warnings: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        """Return JSON-friendly dict (paths as strings, no dataclasses)."""
        return {
            "report_version": self.report_version,
            "generated_at": self.generated_at,
            "feedback_store_path": str(self.feedback_store_path),
            "output_path": (
                str(self.output_path) if self.output_path is not None else None
            ),
            "feedback_available": self.feedback_available,
            "total_domains": self.total_domains,
            "total_observations": self.total_observations,
            "known_good_count": self.known_good_count,
            "known_risky_count": self.known_risky_count,
            "cold_start_count": self.cold_start_count,
            "unknown_count": self.unknown_count,
            "records": [
                {
                    "domain": r.domain,
                    "total_observations": int(r.total_observations),
                    "delivered_count": int(r.delivered_count),
                    "hard_bounce_count": int(r.hard_bounce_count),
                    "soft_bounce_count": int(r.soft_bounce_count),
                    "blocked_count": int(r.blocked_count),
                    "complaint_count": int(r.complaint_count),
                    "reputation_score": float(r.reputation_score),
                    "risk_level": r.risk_level,
                    "behavior_class": r.behavior_class,
                    "cold_start": bool(r.cold_start),
                    "reason": r.reason,
                }
                for r in self.records
            ],
            "warnings": list(self.warnings),
        }


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _record_sort_key(r: FeedbackDomainIntelPreviewRecord) -> tuple[int, int, str]:
    """Sort: behavior priority asc, observations desc, domain asc."""
    return (
        _BEHAVIOR_PRIORITY.get(r.behavior_class, 99),
        -int(r.total_observations),
        r.domain,
    )


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _record_from_aggregate(
    aggregate: Any,
    intel: dict[str, Any],
) -> FeedbackDomainIntelPreviewRecord:
    return FeedbackDomainIntelPreviewRecord(
        domain=str(aggregate.domain),
        total_observations=int(aggregate.total_observations),
        delivered_count=int(aggregate.delivered_count),
        hard_bounce_count=int(aggregate.hard_bounce_count),
        soft_bounce_count=int(aggregate.soft_bounce_count),
        blocked_count=int(aggregate.blocked_count),
        complaint_count=int(aggregate.complaint_count),
        reputation_score=float(intel.get("reputation_score", 0.0)),
        risk_level=str(intel.get("risk_level", "unknown")),
        behavior_class=str(intel.get("behavior_class", "unknown")),
        cold_start=bool(intel.get("cold_start", False)),
        reason=str(intel.get("reason", "")),
    )


# --------------------------------------------------------------------------- #
# Builder
# --------------------------------------------------------------------------- #


def build_feedback_domain_intel_preview(
    feedback_store_path: str | Path,
    *,
    output_dir: str | Path | None = None,
    thresholds: ReputationThresholds = DEFAULT_REPUTATION_THRESHOLDS,
) -> FeedbackDomainIntelPreviewResult:
    """Build the V2.9.8 feedback → domain intel preview.

    Parameters
    ----------
    feedback_store_path:
        Path to the V2.7 ``bounce_outcomes.sqlite`` store. Missing /
        unreadable / empty stores are non-fatal (recorded as warnings).
    output_dir:
        Optional directory to write ``feedback_domain_intel_preview.json``
        into. When omitted, the JSON file is not written and
        ``output_path`` stays ``None``.
    thresholds:
        Reputation thresholds passed to
        :func:`bounce_aggregate_to_domain_intel`. Defaults to the V2.7
        defaults — overriding is for testing / what-if analysis.

    Returns
    -------
    FeedbackDomainIntelPreviewResult
        Always returns; never raises on store failures.
    """
    store_path = Path(feedback_store_path)
    output_dir_path: Path | None = (
        Path(output_dir) if output_dir is not None else None
    )
    output_path: Path | None = None
    if output_dir_path is not None:
        output_dir_path.mkdir(parents=True, exist_ok=True)
        output_path = output_dir_path / _REPORT_FILENAME

    warnings: list[str] = []
    records: list[FeedbackDomainIntelPreviewRecord] = []
    feedback_available = False
    total_observations = 0

    if not store_path.is_file():
        warnings.append(WARNING_FEEDBACK_STORE_MISSING)
    else:
        store: BounceOutcomeStore | None = None
        aggregates: list[Any] = []
        try:
            store = BounceOutcomeStore(store_path)
            aggregates = list(store.list_all())
        except Exception:  # pragma: no cover - defensive
            warnings.append(WARNING_FEEDBACK_STORE_UNREADABLE)
            aggregates = []
        finally:
            if store is not None:
                try:
                    store.close()
                except Exception:  # pragma: no cover - defensive
                    pass

        if (
            WARNING_FEEDBACK_STORE_UNREADABLE not in warnings
            and not aggregates
        ):
            warnings.append(WARNING_FEEDBACK_STORE_EMPTY)
        elif aggregates:
            feedback_available = True
            for agg in aggregates:
                intel = bounce_aggregate_to_domain_intel(agg, thresholds)
                rec = _record_from_aggregate(agg, intel)
                records.append(rec)
                total_observations += int(agg.total_observations)

    records.sort(key=_record_sort_key)

    counts = {"known_good": 0, "known_risky": 0, "cold_start": 0, "unknown": 0}
    for r in records:
        if r.behavior_class in counts:
            counts[r.behavior_class] += 1
        else:
            counts["unknown"] += 1

    result = FeedbackDomainIntelPreviewResult(
        report_version=_REPORT_VERSION,
        generated_at=_utc_now_iso(),
        feedback_store_path=store_path,
        output_path=output_path,
        feedback_available=feedback_available,
        total_domains=len(records),
        total_observations=total_observations,
        known_good_count=counts["known_good"],
        known_risky_count=counts["known_risky"],
        cold_start_count=counts["cold_start"],
        unknown_count=counts["unknown"],
        records=tuple(records),
        warnings=tuple(warnings),
    )

    if output_path is not None:
        try:
            output_path.write_text(
                json.dumps(result.to_dict(), indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        except Exception:  # pragma: no cover - defensive
            pass

    return result


__all__ = [
    "FeedbackDomainIntelPreviewRecord",
    "FeedbackDomainIntelPreviewResult",
    "WARNING_FEEDBACK_STORE_EMPTY",
    "WARNING_FEEDBACK_STORE_MISSING",
    "WARNING_FEEDBACK_STORE_UNREADABLE",
    "build_feedback_domain_intel_preview",
]
