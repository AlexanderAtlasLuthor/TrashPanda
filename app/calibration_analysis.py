"""Phase 3 — Calibration analysis helpers (read-only).

This module does NOT change any pipeline logic. It only reads the
artifacts produced by a completed pipeline run (the three technical
CSVs plus ``processing_report.json``) and derives distribution,
breakdown, and insight metrics to support threshold calibration.

It also provides a small orchestrator that runs the pipeline with
temporary YAML configs (different thresholds) and compares the
resulting distributions.
"""

from __future__ import annotations

import json
import logging
import re
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import yaml


# ---------------------------------------------------------------------------
# Bucket → CSV map (mirrors app/client_output.py without importing it)
# ---------------------------------------------------------------------------

BUCKET_FILES: dict[str, str] = {
    "valid": "clean_high_confidence.csv",
    "review": "review_medium_confidence.csv",
    "invalid": "removed_invalid.csv",
}


# Target distribution ranges (reference only, not enforced).
TARGET_RANGES: dict[str, tuple[float, float]] = {
    "valid_pct": (60.0, 80.0),
    "review_pct": (5.0, 20.0),
    "invalid_pct": (15.0, 30.0),
}


# Client reason substrings used to derive category counts.
REASON_SUBSTRINGS: dict[str, tuple[str, ...]] = {
    "disposable": ("disposable", "temporary/disposable"),
    "placeholder": ("placeholder", "fake"),
    "role_account": ("role-based", "role based", "role account"),
}


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

def _read_csv_safe(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _load_bucket_frames(run_dir: Path) -> dict[str, pd.DataFrame]:
    return {
        bucket: _read_csv_safe(run_dir / fname)
        for bucket, fname in BUCKET_FILES.items()
    }


def _load_processing_report(run_dir: Path) -> dict[str, Any]:
    path = run_dir / "processing_report.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

def _reason_count(df: pd.DataFrame, substrings: tuple[str, ...]) -> int:
    if df.empty or "client_reason" not in df.columns:
        return 0
    col = df["client_reason"].astype(str).str.lower()
    mask = pd.Series(False, index=df.index)
    for sub in substrings:
        mask = mask | col.str.contains(sub.lower(), regex=False, na=False)
    return int(mask.sum())


def _top_reasons(df: pd.DataFrame, n: int = 10) -> list[dict[str, Any]]:
    if df.empty or "client_reason" not in df.columns:
        return []
    counts = df["client_reason"].astype(str).value_counts().head(n)
    return [{"client_reason": reason, "count": int(cnt)} for reason, cnt in counts.items()]


def _top_domains(df: pd.DataFrame, n: int = 20) -> list[dict[str, Any]]:
    if df.empty:
        return []
    col = None
    for candidate in ("corrected_domain", "domain_from_email", "domain"):
        if candidate in df.columns:
            col = candidate
            break
    if col is None:
        return []
    series = df[col].astype(str).str.lower().str.strip()
    series = series[(series != "") & (series != "nan")]
    counts = series.value_counts().head(n)
    return [{"domain": d, "count": int(c)} for d, c in counts.items()]


def _pct(part: int, whole: int) -> float:
    return round(100.0 * part / whole, 2) if whole else 0.0


def _derive_insights(distribution: dict[str, float], categories: dict[str, int], total: int) -> list[str]:
    insights: list[str] = []
    if distribution["review_pct"] > 25.0:
        insights.append("High review rate (>25%)")
    if distribution["invalid_pct"] < 10.0:
        insights.append("Low invalid rate (<10%)")
    if distribution["invalid_pct"] > 40.0:
        insights.append("High invalid rate (>40%)")
    if distribution["valid_pct"] < 50.0:
        insights.append("Low valid rate (<50%)")
    if distribution["valid_pct"] > TARGET_RANGES["valid_pct"][1]:
        insights.append("Valid rate above target range (>80%)")
    if total:
        if categories["placeholder_count"] / total > 0.05:
            insights.append("High placeholder usage (>5%)")
        if categories["disposable_count"] / total > 0.02:
            insights.append("High disposable usage (>2%)")
        if categories["role_account_count"] / total > 0.10:
            insights.append("High role-based usage (>10%)")
    for key, (lo, hi) in TARGET_RANGES.items():
        val = distribution[key]
        if lo <= val <= hi:
            insights.append(f"{key} within target range ({lo}-{hi}%)")
    return insights


# ---------------------------------------------------------------------------
# Public: analyze_run
# ---------------------------------------------------------------------------

def analyze_run(run_dir: Path) -> dict[str, Any]:
    """Analyze a completed pipeline run directory.

    Returns a dictionary with distribution, breakdown, key metrics,
    domain stats, and heuristic insights. The function is read-only.
    """
    run_dir = Path(run_dir)
    frames = _load_bucket_frames(run_dir)
    report = _load_processing_report(run_dir)

    valid_df = frames["valid"]
    review_df = frames["review"]
    invalid_df = frames["invalid"]

    valid_count = len(valid_df)
    review_count = len(review_df)
    invalid_count = len(invalid_df)
    total_output_rows = valid_count + review_count + invalid_count

    total_rows = int(
        report.get("total_rows_processed")
        or report.get("total_rows")
        or total_output_rows
    )
    denom = total_output_rows if total_output_rows else total_rows

    distribution = {
        "valid_pct": _pct(valid_count, denom),
        "review_pct": _pct(review_count, denom),
        "invalid_pct": _pct(invalid_count, denom),
    }

    breakdown = {
        "top_reasons_overall": _top_reasons(
            pd.concat([d for d in (valid_df, review_df, invalid_df) if not d.empty], ignore_index=True)
            if any(not d.empty for d in (valid_df, review_df, invalid_df))
            else pd.DataFrame(),
            n=10,
        ),
        "top_reasons_review": _top_reasons(review_df, n=10),
        "top_reasons_invalid": _top_reasons(invalid_df, n=10),
    }

    categories = {
        "disposable_count": _reason_count(invalid_df, REASON_SUBSTRINGS["disposable"]),
        "placeholder_count": _reason_count(invalid_df, REASON_SUBSTRINGS["placeholder"]),
        "role_account_count": _reason_count(review_df, REASON_SUBSTRINGS["role_account"])
            + _reason_count(invalid_df, REASON_SUBSTRINGS["role_account"]),
    }

    key_metrics = {
        "duplicates_removed": int(
            report.get("total_duplicates_removed")
            or report.get("total_duplicate_rows")
            or 0
        ),
        "typo_corrections": int(report.get("total_typo_corrections") or 0),
        **categories,
    }

    all_df = (
        pd.concat([d for d in (valid_df, review_df, invalid_df) if not d.empty], ignore_index=True)
        if any(not d.empty for d in (valid_df, review_df, invalid_df))
        else pd.DataFrame()
    )

    domains = {
        "top_domains_overall": _top_domains(all_df, n=20),
        "top_domains_invalid": _top_domains(invalid_df, n=20),
    }

    insights = _derive_insights(distribution, categories, total_output_rows or total_rows)

    return {
        "run_dir": str(run_dir),
        "total_rows": total_rows,
        "total_output_rows": total_output_rows,
        "valid_count": valid_count,
        "review_count": review_count,
        "invalid_count": invalid_count,
        "distribution": distribution,
        "breakdown": breakdown,
        "key_metrics": key_metrics,
        "domains": domains,
        "insights": insights,
    }


# ---------------------------------------------------------------------------
# Calibration orchestration
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class ScenarioSpec:
    name: str
    high_confidence_threshold: int
    review_threshold: int
    description: str


def _write_temp_config(
    base_config_path: Path,
    out_path: Path,
    *,
    high_confidence_threshold: int,
    review_threshold: int,
) -> Path:
    """Copy the base YAML config and override only the two thresholds."""
    base = yaml.safe_load(base_config_path.read_text(encoding="utf-8")) or {}
    base["high_confidence_threshold"] = high_confidence_threshold
    base["review_threshold"] = review_threshold
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(yaml.safe_dump(base, sort_keys=False), encoding="utf-8")
    return out_path


def run_calibration_iteration(
    *,
    scenario: ScenarioSpec,
    input_file: Path,
    output_dir: Path,
    base_config_path: Path,
    project_root: Path,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """Run the pipeline for one scenario and analyze its outputs."""
    log = logger or logging.getLogger(__name__)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    cfg_path = output_dir / "scenario_config.yaml"
    _write_temp_config(
        base_config_path,
        cfg_path,
        high_confidence_threshold=scenario.high_confidence_threshold,
        review_threshold=scenario.review_threshold,
    )

    cmd = [
        sys.executable, "-m", "app",
        "--input-file", str(input_file),
        "--output-dir", str(output_dir),
        "--config", str(cfg_path),
    ]
    log.info("Running scenario %s | thresholds=(hc=%s, rev=%s)",
             scenario.name, scenario.high_confidence_threshold, scenario.review_threshold)
    proc = subprocess.run(
        cmd, cwd=str(project_root),
        capture_output=True, text=True, check=False,
    )
    (output_dir / "stdout.log").write_text(proc.stdout or "", encoding="utf-8")
    (output_dir / "stderr.log").write_text(proc.stderr or "", encoding="utf-8")
    if proc.returncode != 0:
        raise RuntimeError(
            f"Pipeline run failed for scenario {scenario.name} (exit={proc.returncode}). "
            f"See {output_dir / 'stderr.log'}"
        )

    analysis = analyze_run(output_dir)
    analysis["scenario"] = {
        "name": scenario.name,
        "description": scenario.description,
        "high_confidence_threshold": scenario.high_confidence_threshold,
        "review_threshold": scenario.review_threshold,
    }
    (output_dir / "calibration_report.json").write_text(
        json.dumps(analysis, indent=2), encoding="utf-8"
    )
    return analysis


def compare_runs(*analyses: dict[str, Any]) -> dict[str, Any]:
    """Compare multiple analyze_run() outputs side by side."""
    rows = []
    for a in analyses:
        scen = a.get("scenario") or {"name": Path(a["run_dir"]).name}
        rows.append({
            "scenario": scen.get("name"),
            "high_confidence_threshold": scen.get("high_confidence_threshold"),
            "review_threshold": scen.get("review_threshold"),
            "valid_count": a["valid_count"],
            "review_count": a["review_count"],
            "invalid_count": a["invalid_count"],
            "valid_pct": a["distribution"]["valid_pct"],
            "review_pct": a["distribution"]["review_pct"],
            "invalid_pct": a["distribution"]["invalid_pct"],
            "insights": a.get("insights", []),
        })

    baseline = rows[0]
    deltas: list[dict[str, Any]] = []
    for row in rows[1:]:
        deltas.append({
            "scenario": row["scenario"],
            "valid_count_delta": row["valid_count"] - baseline["valid_count"],
            "review_count_delta": row["review_count"] - baseline["review_count"],
            "invalid_count_delta": row["invalid_count"] - baseline["invalid_count"],
            "valid_pct_delta": round(row["valid_pct"] - baseline["valid_pct"], 2),
            "review_pct_delta": round(row["review_pct"] - baseline["review_pct"], 2),
            "invalid_pct_delta": round(row["invalid_pct"] - baseline["invalid_pct"], 2),
        })

    return {"rows": rows, "deltas_vs_baseline": deltas}


def _distance_to_target(distribution: dict[str, float]) -> float:
    """L1 distance of the distribution to the nearest edge of each target range."""
    penalty = 0.0
    for key, (lo, hi) in TARGET_RANGES.items():
        val = distribution[key]
        if val < lo:
            penalty += lo - val
        elif val > hi:
            penalty += val - hi
    return round(penalty, 2)


def pick_best_scenario(analyses: list[dict[str, Any]]) -> dict[str, Any]:
    """Pick scenario with smallest distance to the target distribution window."""
    scored = []
    for a in analyses:
        scored.append({
            "scenario": a["scenario"]["name"],
            "high_confidence_threshold": a["scenario"]["high_confidence_threshold"],
            "review_threshold": a["scenario"]["review_threshold"],
            "distribution": a["distribution"],
            "distance_to_target": _distance_to_target(a["distribution"]),
        })
    scored.sort(key=lambda r: r["distance_to_target"])
    return {"ranking": scored, "winner": scored[0]}


# ---------------------------------------------------------------------------
# Email column detection (read-only helper; file is never modified)
# ---------------------------------------------------------------------------

EMAIL_COLUMN_CANDIDATES: tuple[str, ...] = (
    "email", "correo", "correo_electronico", "e-mail", "e_mail", "mail",
)

_EMAIL_RE = re.compile(r"@")


def detect_email_column(csv_path: Path, sample_rows: int = 2000) -> str:
    df = pd.read_csv(csv_path, nrows=sample_rows, dtype=str, keep_default_na=False)
    cols_lower = {c.lower(): c for c in df.columns}
    for cand in EMAIL_COLUMN_CANDIDATES:
        if cand in cols_lower:
            return cols_lower[cand]
    # Fallback: pick the column with the highest proportion of '@' characters.
    best_col, best_ratio = None, -1.0
    for col in df.columns:
        series = df[col].astype(str)
        ratio = series.str.contains("@", regex=False, na=False).mean()
        if ratio > best_ratio:
            best_ratio = ratio
            best_col = col
    if best_col is None or best_ratio <= 0:
        raise ValueError(f"Could not detect an email column in {csv_path}")
    return best_col
