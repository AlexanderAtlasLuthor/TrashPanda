"""V2.8 — Reporting & Visibility.

Builds V2-aware deliverability reports that surface the new V2.1–V2.7
signals: classification action distribution, deliverability probability
buckets, SMTP coverage, catch-all share, domain risk / cold-start, and
optional bounce-feedback aggregates from the V2.7 store.

Design principles
-----------------

  * **Pure / additive.** This module never mutates input frames, never
    changes routing, and never overwrites legacy reports. It writes a
    new JSON summary plus three CSV breakdowns in the run directory.
  * **Read-only.** It opens the V2.7 feedback store in read mode if a
    path is provided; never writes. Falls back to an empty
    ``FeedbackSummary`` on any error so a missing/locked store cannot
    break the cleaning pipeline.
  * **Missing columns are not errors.** Pre-V2 frames render as zero
    counts, never as exceptions.

Outputs (written under ``run_dir``):

  * ``v2_deliverability_summary.json`` — machine-readable top-level
    summary with every section.
  * ``v2_reason_breakdown.csv`` — counts grouped by
    ``decision_reason`` × ``final_action``.
  * ``v2_domain_risk_summary.csv`` — per-domain counts with risk
    level, behavior class, and per-action breakdown.
  * ``v2_probability_distribution.csv`` — histogram of
    ``deliverability_probability``.
"""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


# Pinned report version. Bump when the JSON schema changes in a
# breaking way; consumers can use this to gate parsing.
V2_REPORT_VERSION: str = "v2.8.0"


# Probability buckets — closed-open intervals on the lower bound,
# closed on the upper bound for the last bucket. Order matters for
# deterministic JSON output.
_PROBABILITY_BUCKETS: tuple[tuple[str, float, float], ...] = (
    ("0.00-0.20", 0.0, 0.20),
    ("0.20-0.40", 0.20, 0.40),
    ("0.40-0.60", 0.40, 0.60),
    ("0.60-0.80", 0.60, 0.80),
    ("0.80-1.00", 0.80, 1.0),
)


# SMTP statuses bucketed for coverage reporting.
_INCONCLUSIVE_SMTP: frozenset[str] = frozenset({
    "blocked", "timeout", "temp_fail", "error",
})


# =========================================================================== #
# Section dataclasses                                                         #
# =========================================================================== #


@dataclass
class ClassificationSummary:
    total_rows: int = 0
    by_final_action: dict[str, int] = field(default_factory=dict)
    by_final_output_reason: dict[str, int] = field(default_factory=dict)
    by_decision_reason: dict[str, int] = field(default_factory=dict)


@dataclass
class ProbabilityDistribution:
    buckets: dict[str, int] = field(default_factory=dict)
    percentages: dict[str, float] = field(default_factory=dict)
    total_with_probability: int = 0
    missing: int = 0


@dataclass
class SMTPCoverage:
    total_rows: int = 0
    candidate_count: int = 0
    tested_count: int = 0
    valid_count: int = 0
    invalid_count: int = 0
    inconclusive_count: int = 0
    catch_all_possible_count: int = 0
    not_tested_count: int = 0
    coverage_rate: float = 0.0
    by_smtp_status: dict[str, int] = field(default_factory=dict)


@dataclass
class CatchAllSummary:
    total_rows: int = 0
    by_catch_all_status: dict[str, int] = field(default_factory=dict)
    catch_all_risk_count: int = 0
    possible_catch_all_count: int = 0
    confirmed_catch_all_count: int = 0
    not_catch_all_count: int = 0
    unknown_count: int = 0


@dataclass
class DomainIntelligenceSummary:
    total_rows: int = 0
    by_risk_level: dict[str, int] = field(default_factory=dict)
    by_behavior_class: dict[str, int] = field(default_factory=dict)
    cold_start_count: int = 0
    high_risk_domain_count: int = 0
    low_risk_domain_count: int = 0
    medium_risk_domain_count: int = 0
    unknown_domain_count: int = 0
    top_high_risk_domains: list[dict[str, Any]] = field(default_factory=list)
    top_review_domains: list[dict[str, Any]] = field(default_factory=list)
    top_reject_domains: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class FeedbackSummary:
    feedback_available: bool = False
    domains_with_feedback: int = 0
    total_observations: int = 0
    delivered_count: int = 0
    hard_bounce_count: int = 0
    soft_bounce_count: int = 0
    blocked_count: int = 0
    deferred_count: int = 0
    complaint_count: int = 0
    unsubscribed_count: int = 0
    unknown_count: int = 0
    top_high_risk_feedback_domains: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class V2DeliverabilityReport:
    """Top-level V2.8 report, serializable to JSON via :meth:`to_dict`.

    Field names match the explicit V2.8 validation contract:
    ``classification_summary``, ``probability_distribution``,
    ``smtp_coverage``, ``catch_all_summary``,
    ``domain_intelligence_summary``, ``feedback_summary``. Renaming any
    of these breaks downstream JSON consumers, so they're load-bearing.
    """

    report_version: str
    generated_at: str
    classification_summary: ClassificationSummary
    probability_distribution: ProbabilityDistribution
    smtp_coverage: SMTPCoverage
    catch_all_summary: CatchAllSummary
    domain_intelligence_summary: DomainIntelligenceSummary
    feedback_summary: FeedbackSummary

    def to_dict(self) -> dict[str, Any]:
        from dataclasses import asdict

        return asdict(self)


# =========================================================================== #
# Coercion helpers                                                            #
# =========================================================================== #


def _series(df: pd.DataFrame, column: str) -> pd.Series:
    """Return ``df[column]`` if present; an empty Series otherwise.

    Used by every section helper so a frame missing V2 columns degrades
    to zero counts instead of raising.
    """
    if df is None or df.empty or column not in df.columns:
        return pd.Series([], dtype=str)
    return df[column].astype(str)


def _series_truthy(df: pd.DataFrame, column: str) -> pd.Series:
    """Return a boolean series for ``column``, accepting CSV-string truthy."""
    if df is None or df.empty or column not in df.columns:
        return pd.Series([], dtype=bool)
    raw = df[column].astype(str).str.strip().str.lower()
    truthy = {"1", "true", "t", "yes", "y"}
    return raw.isin(truthy)


def _series_numeric(df: pd.DataFrame, column: str) -> pd.Series:
    """Return a numeric Series for ``column``, with non-numeric → NaN."""
    if df is None or df.empty or column not in df.columns:
        return pd.Series([], dtype=float)
    return pd.to_numeric(df[column], errors="coerce")


def _value_counts_dict(s: pd.Series) -> dict[str, int]:
    """``Series.value_counts(dropna=False)`` as an ordinary dict."""
    if s is None or len(s) == 0:
        return {}
    counts = s.value_counts(dropna=False)
    out: dict[str, int] = {}
    for k, v in counts.items():
        # Treat NaN / empty as the literal string ``""`` so JSON
        # serialization stays clean.
        if k is None or (isinstance(k, float) and pd.isna(k)):
            key = ""
        else:
            key = str(k)
        out[key] = int(v)
    return out


def _concat(frames: Iterable[pd.DataFrame | None]) -> pd.DataFrame:
    real = [f for f in frames if f is not None and not f.empty]
    if not real:
        return pd.DataFrame()
    return pd.concat(real, ignore_index=True)


# =========================================================================== #
# Section builders                                                            #
# =========================================================================== #


def build_classification_summary(combined: pd.DataFrame) -> ClassificationSummary:
    """Counts by ``final_action`` / ``final_output_reason`` / ``decision_reason``."""
    return ClassificationSummary(
        total_rows=int(len(combined)),
        by_final_action=_value_counts_dict(_series(combined, "final_action")),
        by_final_output_reason=_value_counts_dict(
            _series(combined, "final_output_reason")
        ),
        by_decision_reason=_value_counts_dict(
            _series(combined, "decision_reason")
        ),
    )


def build_probability_distribution(combined: pd.DataFrame) -> ProbabilityDistribution:
    """Histogram of ``deliverability_probability`` into 5 buckets + missing."""
    buckets: dict[str, int] = {label: 0 for label, _, _ in _PROBABILITY_BUCKETS}
    buckets["missing"] = 0

    series = _series_numeric(combined, "deliverability_probability")
    if len(series) == 0:
        buckets["missing"] = int(len(combined))
        return ProbabilityDistribution(
            buckets=buckets,
            percentages={k: 0.0 for k in buckets},
            total_with_probability=0,
            missing=int(len(combined)),
        )

    missing = int(series.isna().sum())
    buckets["missing"] = missing

    valid = series.dropna()
    for label, lo, hi in _PROBABILITY_BUCKETS:
        # Last bucket is closed on the upper bound so 1.0 lands in 0.80-1.00.
        if hi == 1.0:
            mask = (valid >= lo) & (valid <= hi)
        else:
            mask = (valid >= lo) & (valid < hi)
        buckets[label] = int(mask.sum())

    total_with_probability = int(len(valid))
    total_rows = int(len(series))
    percentages: dict[str, float] = {}
    for k, v in buckets.items():
        percentages[k] = (
            round(100.0 * v / total_rows, 2) if total_rows > 0 else 0.0
        )

    return ProbabilityDistribution(
        buckets=buckets,
        percentages=percentages,
        total_with_probability=total_with_probability,
        missing=missing,
    )


def build_smtp_coverage(combined: pd.DataFrame) -> SMTPCoverage:
    """SMTP status distribution + candidate/tested coverage rate."""
    by_status = _value_counts_dict(_series(combined, "smtp_status"))
    candidate_count = int(_series_truthy(combined, "smtp_was_candidate").sum())
    valid = int(by_status.get("valid", 0))
    invalid = int(by_status.get("invalid", 0))
    catch_all_possible = int(by_status.get("catch_all_possible", 0))
    inconclusive = sum(int(by_status.get(s, 0)) for s in _INCONCLUSIVE_SMTP)
    not_tested = int(by_status.get("not_tested", 0))

    # ``tested`` = anything other than not_tested. The status-bucket
    # split is the source of truth so the math always reconciles.
    tested = int(len(combined)) - not_tested

    coverage_rate = (
        round(tested / candidate_count, 4) if candidate_count > 0 else 0.0
    )

    return SMTPCoverage(
        total_rows=int(len(combined)),
        candidate_count=candidate_count,
        tested_count=tested,
        valid_count=valid,
        invalid_count=invalid,
        inconclusive_count=inconclusive,
        catch_all_possible_count=catch_all_possible,
        not_tested_count=not_tested,
        coverage_rate=coverage_rate,
        by_smtp_status=by_status,
    )


def build_catch_all_summary(combined: pd.DataFrame) -> CatchAllSummary:
    by_status = _value_counts_dict(_series(combined, "catch_all_status"))
    flag_count = int(_series_truthy(combined, "catch_all_flag").sum())
    return CatchAllSummary(
        total_rows=int(len(combined)),
        by_catch_all_status=by_status,
        catch_all_risk_count=flag_count,
        possible_catch_all_count=int(by_status.get("possible_catch_all", 0)),
        confirmed_catch_all_count=int(by_status.get("confirmed_catch_all", 0)),
        not_catch_all_count=int(by_status.get("not_catch_all", 0)),
        unknown_count=int(by_status.get("unknown", 0)),
    )


def build_domain_intelligence_summary(
    combined: pd.DataFrame,
    *,
    top_n: int = 10,
) -> DomainIntelligenceSummary:
    """Per-domain risk distribution + top problem domains."""
    by_risk = _value_counts_dict(_series(combined, "domain_risk_level"))
    by_behavior = _value_counts_dict(_series(combined, "domain_behavior_class"))
    cold_start_count = int(_series_truthy(combined, "domain_cold_start").sum())

    summary = DomainIntelligenceSummary(
        total_rows=int(len(combined)),
        by_risk_level=by_risk,
        by_behavior_class=by_behavior,
        cold_start_count=cold_start_count,
        high_risk_domain_count=int(by_risk.get("high", 0)),
        low_risk_domain_count=int(by_risk.get("low", 0)),
        medium_risk_domain_count=int(by_risk.get("medium", 0)),
        unknown_domain_count=int(by_risk.get("unknown", 0)),
    )

    if combined is None or combined.empty:
        return summary

    # Domain key — prefer corrected_domain, fall back to domain.
    if "corrected_domain" in combined.columns:
        domain_series = combined["corrected_domain"].astype(str)
    elif "domain" in combined.columns:
        domain_series = combined["domain"].astype(str)
    else:
        return summary

    domain_series = domain_series.str.strip().str.lower()
    domain_series = domain_series.replace({"": pd.NA, "nan": pd.NA, "none": pd.NA})

    final_action_series = _series(combined, "final_action")
    risk_series = _series(combined, "domain_risk_level")
    behavior_series = _series(combined, "domain_behavior_class")

    grouped = pd.DataFrame(
        {
            "domain": domain_series,
            "final_action": final_action_series,
            "risk": risk_series,
            "behavior": behavior_series,
        }
    ).dropna(subset=["domain"])

    if grouped.empty:
        return summary

    # Top high-risk domains: count rows where risk_level == "high".
    high_risk = grouped[grouped["risk"] == "high"]
    summary.top_high_risk_domains = _top_domain_table(
        high_risk, top_n=top_n, group_by="domain", extra_columns=("behavior",)
    )

    review = grouped[grouped["final_action"] == "manual_review"]
    summary.top_review_domains = _top_domain_table(
        review, top_n=top_n, group_by="domain", extra_columns=("risk", "behavior")
    )

    rejected = grouped[grouped["final_action"] == "auto_reject"]
    summary.top_reject_domains = _top_domain_table(
        rejected, top_n=top_n, group_by="domain", extra_columns=("risk", "behavior")
    )

    return summary


def _top_domain_table(
    df: pd.DataFrame,
    *,
    top_n: int,
    group_by: str,
    extra_columns: tuple[str, ...] = (),
) -> list[dict[str, Any]]:
    """Group by domain, sort by count desc, then domain asc — deterministic."""
    if df is None or df.empty or group_by not in df.columns:
        return []
    grouped = df.groupby(group_by, dropna=True).size().reset_index(name="count")
    if grouped.empty:
        return []
    # Most-frequent extras (e.g. risk / behavior) per domain.
    out_rows: list[dict[str, Any]] = []
    grouped = grouped.sort_values(
        ["count", group_by], ascending=[False, True]
    ).head(top_n)
    for _, row in grouped.iterrows():
        domain_val = str(row[group_by])
        record = {"domain": domain_val, "count": int(row["count"])}
        for col in extra_columns:
            if col not in df.columns:
                continue
            sub = df[df[group_by] == domain_val][col]
            if sub.empty:
                record[col] = ""
                continue
            mode_series = sub.mode()
            record[col] = str(mode_series.iloc[0]) if not mode_series.empty else ""
        out_rows.append(record)
    return out_rows


def build_feedback_summary(
    feedback_store: Any | None = None,
    *,
    top_n: int = 10,
) -> FeedbackSummary:
    """Optional V2.7 bounce-feedback aggregates.

    ``feedback_store`` is an open :class:`BounceOutcomeStore` or any
    object with a ``list_all()`` returning :class:`DomainBounceAggregate`
    instances. When ``None`` or any failure occurs, returns an empty
    summary with ``feedback_available=False`` — this section never
    fails the report.
    """
    summary = FeedbackSummary(feedback_available=False)
    if feedback_store is None:
        return summary

    try:
        aggregates = feedback_store.list_all()
    except Exception:  # pragma: no cover - defensive
        return summary

    if not aggregates:
        # Store exists but is empty — still mark unavailable so
        # consumers can short-circuit.
        return summary

    # Lazy import keeps V2.8 free of V2.7 imports at module load.
    from .validation_v2.feedback import (
        DEFAULT_REPUTATION_THRESHOLDS,
        compute_risk_level,
    )

    summary.feedback_available = True
    summary.domains_with_feedback = len(aggregates)
    for agg in aggregates:
        summary.total_observations += int(agg.total_observations or 0)
        summary.delivered_count += int(agg.delivered_count or 0)
        summary.hard_bounce_count += int(agg.hard_bounce_count or 0)
        summary.soft_bounce_count += int(agg.soft_bounce_count or 0)
        summary.blocked_count += int(agg.blocked_count or 0)
        summary.deferred_count += int(agg.deferred_count or 0)
        summary.complaint_count += int(agg.complaint_count or 0)
        summary.unsubscribed_count += int(agg.unsubscribed_count or 0)
        summary.unknown_count += int(agg.unknown_count or 0)

    # Top high-risk feedback domains: any aggregate that classifies
    # as ``high`` under the default thresholds, sorted by total
    # observations descending then domain ascending.
    high_risk_records: list[dict[str, Any]] = []
    for agg in aggregates:
        risk = compute_risk_level(agg, DEFAULT_REPUTATION_THRESHOLDS)
        if risk == "high":
            high_risk_records.append(
                {
                    "domain": agg.domain,
                    "total_observations": int(agg.total_observations),
                    "delivered_count": int(agg.delivered_count),
                    "hard_bounce_count": int(agg.hard_bounce_count),
                    "blocked_count": int(agg.blocked_count),
                    "complaint_count": int(agg.complaint_count),
                    "risk_level": risk,
                }
            )

    high_risk_records.sort(
        key=lambda r: (-r["total_observations"], r["domain"])
    )
    summary.top_high_risk_feedback_domains = high_risk_records[:top_n]
    return summary


# =========================================================================== #
# Top-level builder                                                           #
# =========================================================================== #


def build_v2_deliverability_report(
    *,
    clean_df: pd.DataFrame | None = None,
    review_df: pd.DataFrame | None = None,
    invalid_df: pd.DataFrame | None = None,
    duplicates_df: pd.DataFrame | None = None,
    hard_fail_df: pd.DataFrame | None = None,
    feedback_store: Any | None = None,
    generated_at: str | None = None,
    top_n: int = 10,
) -> V2DeliverabilityReport:
    """Build the full V2.8 report from the materialized CSV frames.

    Inputs are optional; missing frames degrade to zero counts. The
    function never mutates input frames.

    ``invalid_df`` is the legacy union (V2 auto_reject + duplicates +
    hard fails). ``duplicates_df`` and ``hard_fail_df`` are the V2.5
    separated subsets and are kept distinct for breakdowns; they are
    NOT folded into the combined frame to avoid double-counting (they
    are already in ``invalid_df``).
    """
    combined = _concat([clean_df, review_df, invalid_df])

    return V2DeliverabilityReport(
        report_version=V2_REPORT_VERSION,
        generated_at=generated_at or datetime.now(timezone.utc).isoformat(),
        classification_summary=build_classification_summary(combined),
        probability_distribution=build_probability_distribution(combined),
        smtp_coverage=build_smtp_coverage(combined),
        catch_all_summary=build_catch_all_summary(combined),
        domain_intelligence_summary=build_domain_intelligence_summary(
            combined, top_n=top_n
        ),
        feedback_summary=build_feedback_summary(feedback_store, top_n=top_n),
    )


# =========================================================================== #
# Writers                                                                     #
# =========================================================================== #


def write_v2_report_files(
    run_dir: str | Path,
    report: V2DeliverabilityReport,
    *,
    clean_df: pd.DataFrame | None = None,
    review_df: pd.DataFrame | None = None,
    invalid_df: pd.DataFrame | None = None,
) -> dict[str, Path]:
    """Materialize the four V2.8 report files in ``run_dir``.

    Always writes:

      * ``v2_deliverability_summary.json``
      * ``v2_reason_breakdown.csv``
      * ``v2_domain_risk_summary.csv``
      * ``v2_probability_distribution.csv``

    Returns a dict ``{logical_name → Path}`` for the artifact
    manifest.
    """
    run_dir = Path(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    out: dict[str, Path] = {}

    # 1. JSON summary.
    json_path = run_dir / "v2_deliverability_summary.json"
    json_path.write_text(
        json.dumps(report.to_dict(), indent=2, default=str),
        encoding="utf-8",
    )
    out["v2_deliverability_summary"] = json_path

    # 2. Reason breakdown CSV.
    breakdown_path = run_dir / "v2_reason_breakdown.csv"
    _write_reason_breakdown_csv(breakdown_path, _concat([clean_df, review_df, invalid_df]))
    out["v2_reason_breakdown"] = breakdown_path

    # 3. Domain risk summary CSV.
    domain_path = run_dir / "v2_domain_risk_summary.csv"
    _write_domain_risk_csv(domain_path, _concat([clean_df, review_df, invalid_df]))
    out["v2_domain_risk_summary"] = domain_path

    # 4. Probability distribution CSV.
    prob_path = run_dir / "v2_probability_distribution.csv"
    _write_probability_distribution_csv(prob_path, report.probability_distribution)
    out["v2_probability_distribution"] = prob_path

    return out


def _write_reason_breakdown_csv(path: Path, combined: pd.DataFrame) -> None:
    headers = ["final_action", "decision_reason", "count"]
    if combined.empty:
        with path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(headers)
        return

    final_action = _series(combined, "final_action").fillna("").replace(
        {"": "<missing>"}
    )
    decision_reason = _series(combined, "decision_reason").fillna("").replace(
        {"": "<missing>"}
    )
    grouped = (
        pd.DataFrame({"final_action": final_action, "decision_reason": decision_reason})
        .groupby(["final_action", "decision_reason"], dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values(
            ["final_action", "count", "decision_reason"],
            ascending=[True, False, True],
        )
    )
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(headers)
        for _, row in grouped.iterrows():
            writer.writerow([row["final_action"], row["decision_reason"], int(row["count"])])


def _write_domain_risk_csv(path: Path, combined: pd.DataFrame) -> None:
    headers = [
        "domain",
        "total_rows",
        "auto_approve_count",
        "manual_review_count",
        "auto_reject_count",
        "domain_risk_level",
        "domain_behavior_class",
        "cold_start_share",
    ]
    if combined.empty or "corrected_domain" not in combined.columns and "domain" not in combined.columns:
        with path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(headers)
        return

    domain_col = "corrected_domain" if "corrected_domain" in combined.columns else "domain"
    domain_series = combined[domain_col].astype(str).str.strip().str.lower()
    domain_series = domain_series.replace({"": pd.NA, "nan": pd.NA, "none": pd.NA})

    final_action = _series(combined, "final_action")
    risk = _series(combined, "domain_risk_level")
    behavior = _series(combined, "domain_behavior_class")
    cold_start = _series_truthy(combined, "domain_cold_start")

    df = pd.DataFrame({
        "domain": domain_series,
        "final_action": final_action,
        "risk": risk,
        "behavior": behavior,
        "cold_start": cold_start,
    }).dropna(subset=["domain"])

    if df.empty:
        with path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(headers)
        return

    rows: list[list[Any]] = []
    for domain_val, group in df.groupby("domain", dropna=True):
        total = len(group)
        approve = int((group["final_action"] == "auto_approve").sum())
        review = int((group["final_action"] == "manual_review").sum())
        reject = int((group["final_action"] == "auto_reject").sum())
        risk_mode = group["risk"].mode()
        behavior_mode = group["behavior"].mode()
        cold_share = round(float(group["cold_start"].sum()) / total, 4) if total > 0 else 0.0
        rows.append([
            str(domain_val),
            total,
            approve,
            review,
            reject,
            str(risk_mode.iloc[0]) if not risk_mode.empty else "",
            str(behavior_mode.iloc[0]) if not behavior_mode.empty else "",
            cold_share,
        ])

    rows.sort(key=lambda r: (-r[1], r[0]))  # total desc, domain asc

    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(headers)
        writer.writerows(rows)


def _write_probability_distribution_csv(
    path: Path, distribution: ProbabilityDistribution,
) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["bucket", "count", "percentage"])
        for label, count in distribution.buckets.items():
            pct = distribution.percentages.get(label, 0.0)
            writer.writerow([label, count, pct])


# =========================================================================== #
# Convenience: read CSVs from a run dir + open feedback store                 #
# =========================================================================== #


def _read_csv_safe(path: Path) -> pd.DataFrame:
    if not path.is_file() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False)
    except (pd.errors.EmptyDataError, OSError):
        return pd.DataFrame()


def generate_v2_reports(
    run_dir: str | Path,
    *,
    feedback_store_path: str | Path | None = None,
) -> dict[str, Path]:
    """High-level entry point used by the pipeline.

    Reads the materialized CSVs from ``run_dir``, optionally opens the
    V2.7 feedback store at ``feedback_store_path``, builds the
    :class:`V2DeliverabilityReport`, and writes the four output files.

    Failures opening the feedback store are silently swallowed — the
    main report is generated regardless. The function returns the
    dict of artifact paths or an empty dict if writing fails.
    """
    run_dir = Path(run_dir)
    clean_df = _read_csv_safe(run_dir / "clean_high_confidence.csv")
    review_df = _read_csv_safe(run_dir / "review_medium_confidence.csv")
    invalid_df = _read_csv_safe(run_dir / "removed_invalid.csv")

    feedback_store = None
    if feedback_store_path is not None:
        feedback_store = _open_feedback_store_safe(feedback_store_path)

    try:
        report = build_v2_deliverability_report(
            clean_df=clean_df,
            review_df=review_df,
            invalid_df=invalid_df,
            feedback_store=feedback_store,
        )
        return write_v2_report_files(
            run_dir,
            report,
            clean_df=clean_df,
            review_df=review_df,
            invalid_df=invalid_df,
        )
    except Exception:  # pragma: no cover - defensive
        return {}
    finally:
        if feedback_store is not None:
            try:
                feedback_store.close()
            except Exception:
                pass


def _open_feedback_store_safe(path: str | Path) -> Any | None:
    """Open the V2.7 feedback store read-only-ish; return None on failure."""
    try:
        from .validation_v2.feedback import BounceOutcomeStore

        p = Path(path)
        if not p.is_file():
            return None
        return BounceOutcomeStore(p)
    except Exception:
        return None


__all__ = [
    "V2_REPORT_VERSION",
    "ClassificationSummary",
    "ProbabilityDistribution",
    "SMTPCoverage",
    "CatchAllSummary",
    "DomainIntelligenceSummary",
    "FeedbackSummary",
    "V2DeliverabilityReport",
    "build_v2_deliverability_report",
    "build_classification_summary",
    "build_probability_distribution",
    "build_smtp_coverage",
    "build_catch_all_summary",
    "build_domain_intelligence_summary",
    "build_feedback_summary",
    "write_v2_report_files",
    "generate_v2_reports",
]
