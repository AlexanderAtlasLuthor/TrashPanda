"""V1 vs V2 scoring comparison utilities.

Purely observational. These helpers take a DataFrame that already
contains both the V1 scoring columns (``score``, ``preliminary_bucket``,
``hard_fail``) and the V2 scoring columns produced by
:class:`app.engine.stages.scoring_v2.ScoringV2Stage` (``score_v2``,
``bucket_v2``, ``hard_stop_v2``, ``confidence_v2``, ``reason_codes_v2``)
and return either:

  * a new frame with per-row comparison columns appended
    (``compare_scoring``), or
  * a JSON-serializable aggregate summary dict
    (``summarize_comparison``).

Nothing here changes any existing column, any V1 logic, or any
pipeline decision path. Every output is additive.

Bucket ordering (used for "higher" / "lower" classification):

    invalid  (rank 0)  <  review (rank 1)  <  high_confidence (rank 2)

Any bucket label outside that vocabulary is treated as rank ``-1``
("below invalid"). That keeps the comparison deterministic on
unexpected labels without silently classifying them as equal to a
known bucket.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


# ---------------------------------------------------------------------------
# Bucket ordering
# ---------------------------------------------------------------------------

_BUCKET_RANK: dict[str, int] = {
    "invalid": 0,
    "review": 1,
    "high_confidence": 2,
}

# Columns compare_scoring appends. Kept as a module-level constant so
# tests and downstream code can reference the exact set.
COMPARISON_COLUMNS: tuple[str, ...] = (
    "score_delta",
    "abs_score_delta",
    "bucket_changed",
    "v2_higher_bucket",
    "v2_lower_bucket",
    "hard_decision_changed",
    "v2_more_strict",
    "v2_more_permissive",
    "low_confidence_v2",
)


def _rank_series(buckets: pd.Series) -> pd.Series:
    """Map bucket labels to their integer rank; unknowns → -1."""
    return (
        buckets.astype(object)
        .map(_BUCKET_RANK)
        .fillna(-1)
        .astype(int)
    )


def _bool_series(col: pd.Series) -> pd.Series:
    """Coerce a (possibly nullable-boolean, possibly object) column to
    a plain numpy bool Series. NA is treated as False — which is the
    right semantics for "did hard_fail fire?" / "did hard_stop fire?"
    when the upstream value was never populated."""
    return col.fillna(False).astype(bool)


# ---------------------------------------------------------------------------
# Row-level comparison
# ---------------------------------------------------------------------------


def compare_scoring(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of ``df`` with per-row V1-vs-V2 comparison columns.

    The caller is responsible for ensuring both the V1 columns
    (``score``, ``preliminary_bucket``, ``hard_fail``) and the V2
    columns (``score_v2``, ``bucket_v2``, ``hard_stop_v2``,
    ``confidence_v2``) are present. Appended columns are:

      * ``score_delta`` = ``score_v2 - score``  (note: the two scales
        differ — V2 is ``[0.0, 1.0]``, V1 is integer 0..~100 — the
        delta is intentionally raw so downstream analysis can bucket
        it however it likes; a calibration subphase will reconcile
        the scales).
      * ``abs_score_delta`` = ``|score_delta|``
      * ``bucket_changed`` = ``bucket_v2 != preliminary_bucket``
      * ``v2_higher_bucket`` = V2 bucket rank > V1 bucket rank
      * ``v2_lower_bucket`` = V2 bucket rank < V1 bucket rank
      * ``hard_decision_changed`` = ``hard_stop_v2 != hard_fail``
      * ``v2_more_strict`` = ``hard_stop_v2 and not hard_fail``
      * ``v2_more_permissive`` = ``hard_fail and not hard_stop_v2``
      * ``low_confidence_v2`` = ``confidence_v2 < 0.5``

    The input frame is not mutated.
    """
    out = df.copy()

    score_v2 = out["score_v2"].astype(float)
    score_v1 = out["score"].astype(float)
    out["score_delta"] = score_v2 - score_v1
    out["abs_score_delta"] = out["score_delta"].abs()

    rank_v1 = _rank_series(out["preliminary_bucket"])
    rank_v2 = _rank_series(out["bucket_v2"])
    out["bucket_changed"] = (
        out["bucket_v2"].astype(object) != out["preliminary_bucket"].astype(object)
    )
    out["v2_higher_bucket"] = rank_v2 > rank_v1
    out["v2_lower_bucket"] = rank_v2 < rank_v1

    v1_hf = _bool_series(out["hard_fail"])
    v2_hs = _bool_series(out["hard_stop_v2"])
    out["hard_decision_changed"] = v1_hf != v2_hs
    out["v2_more_strict"] = v2_hs & ~v1_hf
    out["v2_more_permissive"] = v1_hf & ~v2_hs

    out["low_confidence_v2"] = out["confidence_v2"].astype(float) < 0.5

    return out


# ---------------------------------------------------------------------------
# Aggregate summary
# ---------------------------------------------------------------------------


def _top_reason_codes(
    reason_codes_series: pd.Series, top_n: int = 10
) -> list[dict[str, Any]]:
    """Count individual pipe-joined reason codes across the series."""
    counts: dict[str, int] = {}
    for raw in reason_codes_series:
        if not isinstance(raw, str) or not raw:
            continue
        for token in raw.split("|"):
            if not token:
                continue
            counts[token] = counts.get(token, 0) + 1
    ordered = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[:top_n]
    return [{"reason_code": code, "count": count} for code, count in ordered]


def _confidence_distribution(conf: pd.Series) -> dict[str, int]:
    """Bucket confidence into four readable bins."""
    conf = conf.astype(float)
    return {
        "lt_0_25": int((conf < 0.25).sum()),
        "0_25_to_0_5": int(((conf >= 0.25) & (conf < 0.5)).sum()),
        "0_5_to_0_75": int(((conf >= 0.5) & (conf < 0.75)).sum()),
        "gte_0_75": int((conf >= 0.75).sum()),
    }


def _safe_float(value: Any) -> float:
    """Coerce a numeric value to float; NaN/None → 0.0.

    ``json.dumps`` would emit ``NaN`` as the bare token ``NaN`` which
    is not valid JSON — strict parsers reject it. We clamp to ``0.0``
    so the summary is portable to strict JSON consumers.
    """
    try:
        f = float(value)
    except (TypeError, ValueError):
        return 0.0
    if f != f:  # NaN check
        return 0.0
    return f


def summarize_comparison(df: pd.DataFrame) -> dict[str, Any]:
    """Return a JSON-serializable aggregate summary of V1-vs-V2 drift.

    ``df`` must already carry the per-row columns produced by
    :func:`compare_scoring`. An empty frame yields a well-formed
    summary with all numeric fields set to ``0``.
    """
    n = len(df)

    def _pct(x: int) -> float:
        return (x / n * 100.0) if n else 0.0

    bucket_change_count = int(df["bucket_changed"].sum()) if n else 0
    v2_higher_count = int(df["v2_higher_bucket"].sum()) if n else 0
    v2_lower_count = int(df["v2_lower_bucket"].sum()) if n else 0
    hard_changed_count = int(df["hard_decision_changed"].sum()) if n else 0
    more_strict_count = int(df["v2_more_strict"].sum()) if n else 0
    more_permissive_count = int(df["v2_more_permissive"].sum()) if n else 0

    if n:
        deltas = df["score_delta"].astype(float)
        avg_delta = _safe_float(deltas.mean())
        median_delta = _safe_float(deltas.median())
        max_inc = _safe_float(deltas.max())
        max_dec = _safe_float(deltas.min())
        conf = df["confidence_v2"].astype(float)
        avg_conf = _safe_float(conf.mean())
        low_conf_rate = float(df["low_confidence_v2"].sum()) / n
        downgraded = df[df["v2_lower_bucket"]]
        upgraded = df[df["v2_higher_bucket"]]
        top_downgrade = _top_reason_codes(downgraded["reason_codes_v2"])
        top_upgrade = _top_reason_codes(upgraded["reason_codes_v2"])
        bucket_v2_counts = df["bucket_v2"].value_counts().to_dict()
        bucket_v2_dist = {str(k): int(v) for k, v in bucket_v2_counts.items()}
        conf_dist = _confidence_distribution(conf)
    else:
        avg_delta = 0.0
        median_delta = 0.0
        max_inc = 0.0
        max_dec = 0.0
        avg_conf = 0.0
        low_conf_rate = 0.0
        top_downgrade = []
        top_upgrade = []
        bucket_v2_dist = {}
        conf_dist = {
            "lt_0_25": 0,
            "0_25_to_0_5": 0,
            "0_5_to_0_75": 0,
            "gte_0_75": 0,
        }

    return {
        "total_rows": n,
        "bucket_change_count": bucket_change_count,
        "bucket_change_pct": _pct(bucket_change_count),
        "v2_higher_bucket_count": v2_higher_count,
        "v2_lower_bucket_count": v2_lower_count,
        "hard_decision_changed_count": hard_changed_count,
        "v2_more_strict_count": more_strict_count,
        "v2_more_permissive_count": more_permissive_count,
        "avg_score_delta": avg_delta,
        "median_score_delta": median_delta,
        "max_score_increase": max_inc,
        "max_score_decrease": max_dec,
        "avg_confidence_v2": avg_conf,
        "low_confidence_rate": low_conf_rate,
        "top_reason_codes_when_v2_downgraded": top_downgrade,
        "top_reason_codes_when_v2_upgraded": top_upgrade,
        "bucket_v2_distribution": bucket_v2_dist,
        "confidence_v2_distribution": conf_dist,
    }


# ---------------------------------------------------------------------------
# Optional JSON report writer
# ---------------------------------------------------------------------------


def write_comparison_report(
    df: pd.DataFrame,
    path: str | Path,
) -> dict[str, Any]:
    """Write the comparison summary for ``df`` to ``path`` as JSON.

    ``df`` may be either the raw scored frame (in which case
    ``compare_scoring`` is called here) or a frame that already
    carries the comparison columns. The resulting dict is also
    returned so callers don't need to re-read the file.

    This helper is intentionally **not** wired into the pipeline's
    automatic report generation — callers can invoke it themselves
    when they want to emit ``scoring_v2_comparison.json`` alongside
    an existing run. The spec for this subphase is explicit that
    existing reports and output files must not change.
    """
    if "score_delta" not in df.columns:
        df = compare_scoring(df)
    summary = summarize_comparison(df)
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(summary, indent=2, sort_keys=True))
    return summary
