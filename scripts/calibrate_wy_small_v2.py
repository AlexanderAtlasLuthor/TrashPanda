"""Real initial calibration of TrashPanda V2 on examples/WY_small.csv.

Runs three modes against the SAME input dataset and emits a calibration
report comparing them:

    A. V1-only                    (history/catch-all/smtp/probability/decision OFF)
    B. V1 + V2 annotations        (V2 ON, decision annotate-only, override OFF)
    C. V1 + V2 + decision override ON (analysis-only, NOT recommended default)

Outputs:
    output/calibration/wy_small/<mode>/...       per-mode pipeline run dirs
    output/calibration/wy_small_v2_calibration_report.md
    output/calibration/wy_small_v2_calibration_report.csv
    output/calibration/wy_small_v2_calibration_report.json
    output/calibration/wy_small_manual_audit.csv

No defaults are changed. No production behaviour is altered. This script
only measures + reports.
"""
from __future__ import annotations

import json
import shutil
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

# Allow running as `python scripts/calibrate_wy_small_v2.py` from repo root.
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.api_boundary import run_cleaning_job  # noqa: E402

INPUT_CSV = ROOT / "examples" / "WY_small.csv"
BASE_CONFIG = ROOT / "configs" / "default.yaml"
OUT_DIR = ROOT / "output" / "calibration"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------------------------------- #
# Mode definitions                                                            #
# --------------------------------------------------------------------------- #

MODES: dict[str, dict[str, Any]] = {
    "A_v1_only": {
        "description": "Pure V1. All V2 post-passes disabled.",
        "overrides": {
            "history": {"enabled": False},
            "smtp_probe": {"enabled": False},
            "probability": {"enabled": False},
            "decision": {"enabled": False},
        },
    },
    "B_v2_annotations": {
        "description": "V1 + V2 annotations. Decision engine annotate-only, no bucket override.",
        "overrides": {
            "history": {
                "enabled": True,
                "apply_light_confidence_adjustment": True,
                "allow_bucket_flip_from_history": False,
            },
            "smtp_probe": {"enabled": True, "dry_run": True},  # dry-run safe probing
            "probability": {"enabled": True},
            "decision": {"enabled": True, "enable_bucket_override": False},
        },
    },
    "C_v2_override": {
        "description": "V1 + V2 + decision engine with bucket override ON (analysis only).",
        "overrides": {
            "history": {
                "enabled": True,
                "apply_light_confidence_adjustment": True,
                "allow_bucket_flip_from_history": True,
            },
            "smtp_probe": {"enabled": True, "dry_run": True},
            "probability": {"enabled": True},
            "decision": {"enabled": True, "enable_bucket_override": True},
        },
    },
}


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #


def deep_merge(base: dict, overrides: dict) -> dict:
    out = dict(base)
    for k, v in overrides.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def build_config_for_mode(mode_name: str, mode_spec: dict[str, Any]) -> Path:
    base = yaml.safe_load(BASE_CONFIG.read_text(encoding="utf-8"))
    merged = deep_merge(base, mode_spec["overrides"])
    cfg_path = OUT_DIR / f"_config_{mode_name}.yaml"
    cfg_path.write_text(yaml.safe_dump(merged, sort_keys=False), encoding="utf-8")
    return cfg_path


def load_run_rows(run_dir: Path) -> pd.DataFrame:
    """Load the three technical CSVs, add v1_bucket, concatenate."""
    parts = []
    for name, bucket in [
        ("clean_high_confidence.csv", "ready"),
        ("review_medium_confidence.csv", "review"),
        ("removed_invalid.csv", "invalid"),
    ]:
        p = run_dir / name
        if not p.exists() or p.stat().st_size == 0:
            continue
        try:
            df = pd.read_csv(p, low_memory=False)
        except pd.errors.EmptyDataError:
            continue
        if df.empty:
            continue
        df["v1_bucket"] = bucket
        parts.append(df)
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True)


def value_counts_safe(df: pd.DataFrame, col: str) -> dict[str, int]:
    if col not in df.columns:
        return {}
    s = df[col].fillna("").astype(str)
    return {k: int(v) for k, v in s.value_counts().items() if k != ""}


def count_true(df: pd.DataFrame, col: str) -> int:
    if col not in df.columns:
        return 0
    s = df[col]
    # Accept bool, "True"/"False", 1/0
    return int(s.astype(str).str.lower().isin({"true", "1", "yes"}).sum())


def positive_negative_zero(df: pd.DataFrame, col: str) -> dict[str, int]:
    if col not in df.columns:
        return {"positive": 0, "zero": 0, "negative": 0, "missing": len(df)}
    s = pd.to_numeric(df[col], errors="coerce")
    return {
        "positive": int((s > 0).sum()),
        "zero": int((s == 0).sum()),
        "negative": int((s < 0).sum()),
        "missing": int(s.isna().sum()),
    }


def top_n(counter: dict[str, int], n: int = 10) -> list[tuple[str, int]]:
    return sorted(counter.items(), key=lambda kv: kv[1], reverse=True)[:n]


def top_reasons(df: pd.DataFrame, cols: list[str], n: int = 10) -> list[tuple[str, int]]:
    c: Counter = Counter()
    for col in cols:
        if col not in df.columns:
            continue
        for val in df[col].dropna().astype(str):
            if not val:
                continue
            for tok in val.replace(";", "|").split("|"):
                tok = tok.strip()
                if tok:
                    c[tok] += 1
    return c.most_common(n)


def top_domains_by(df: pd.DataFrame, group_col: str, filter_col: str, n: int = 10) -> list[tuple[str, int]]:
    if group_col not in df.columns or filter_col not in df.columns:
        return []
    sub = df[df[filter_col].astype(str).str.len() > 0]
    if sub.empty:
        return []
    return sub.groupby(group_col).size().sort_values(ascending=False).head(n).to_dict().items() and \
        list(sub.groupby(group_col).size().sort_values(ascending=False).head(n).items())


def top_domains_simple(df: pd.DataFrame, group_col: str, n: int = 10) -> list[tuple[str, int]]:
    if group_col not in df.columns:
        return []
    return list(df.groupby(group_col).size().sort_values(ascending=False).head(n).items())


# --------------------------------------------------------------------------- #
# Metrics                                                                     #
# --------------------------------------------------------------------------- #


def compute_metrics(df: pd.DataFrame, mode_name: str) -> dict[str, Any]:
    total = len(df)

    v1_dist = value_counts_safe(df, "v1_bucket")
    deliv_dist = value_counts_safe(df, "deliverability_label")
    final_action_dist = value_counts_safe(df, "final_action")
    hist_label_dist = value_counts_safe(df, "historical_label")
    subclass_dist = value_counts_safe(df, "review_subclass")
    bucket_v2_dist = value_counts_safe(df, "bucket_v2")
    overridden_dist = value_counts_safe(df, "overridden_bucket")

    possible_catch_all = count_true(df, "possible_catch_all")
    smtp_tested = count_true(df, "smtp_tested")
    smtp_confirmed_valid = count_true(df, "smtp_confirmed_valid")
    smtp_suspicious = count_true(df, "smtp_suspicious")

    adj = positive_negative_zero(df, "confidence_adjustment_applied")

    # Bucket flips: rows whose v1_bucket != v2_final_bucket (when override ON).
    bucket_flips = 0
    if "v2_final_bucket" in df.columns:
        a = df["v1_bucket"].astype(str)
        b = df["v2_final_bucket"].astype(str).replace({"": pd.NA})
        bucket_flips = int(((a != b) & b.notna()).sum())

    # Top reasons: combine V1 score_reasons, reason_codes_v2, catch_all_reason, decision_reason, deliverability_factors.
    top_reason_list = top_reasons(
        df,
        [
            "score_reasons",
            "reason_codes_v2",
            "catch_all_reason",
            "decision_reason",
            "deliverability_factors",
        ],
        n=15,
    )

    # Top domains by review_subclass
    top_dom_by_subclass: dict[str, list[tuple[str, int]]] = {}
    if "review_subclass" in df.columns and "domain" in df.columns:
        for sc, sub in df.groupby(df["review_subclass"].fillna("").astype(str)):
            if not sc:
                continue
            top_dom_by_subclass[sc] = top_domains_simple(sub, "domain", n=5)

    # Top domains by risk: domains where rows have low deliverability_label OR invalid v1_bucket
    risk_mask = pd.Series(False, index=df.index)
    if "deliverability_label" in df.columns:
        risk_mask |= df["deliverability_label"].astype(str).eq("low")
    if "v1_bucket" in df.columns:
        risk_mask |= df["v1_bucket"].eq("invalid")
    top_dom_risk = top_domains_simple(df[risk_mask], "domain", n=10) if risk_mask.any() else []

    # Probability distribution stats
    prob_stats = {}
    if "deliverability_probability" in df.columns:
        s = pd.to_numeric(df["deliverability_probability"], errors="coerce").dropna()
        if len(s):
            prob_stats = {
                "count": int(len(s)),
                "min": float(s.min()),
                "p10": float(s.quantile(0.10)),
                "p25": float(s.quantile(0.25)),
                "p50": float(s.median()),
                "p75": float(s.quantile(0.75)),
                "p90": float(s.quantile(0.90)),
                "max": float(s.max()),
                "mean": float(s.mean()),
            }

    return {
        "mode": mode_name,
        "total_rows": int(total),
        "v1_bucket_distribution": v1_dist,
        "bucket_v2_distribution": bucket_v2_dist,
        "deliverability_label_distribution": deliv_dist,
        "deliverability_probability_stats": prob_stats,
        "final_action_distribution": final_action_dist,
        "overridden_bucket_distribution": overridden_dist,
        "historical_label_distribution": hist_label_dist,
        "review_subclass_distribution": subclass_dist,
        "possible_catch_all_count": int(possible_catch_all),
        "smtp_tested_count": int(smtp_tested),
        "smtp_confirmed_valid_count": int(smtp_confirmed_valid),
        "smtp_suspicious_count": int(smtp_suspicious),
        "confidence_adjustment_applied": adj,
        "bucket_flip_count": int(bucket_flips),
        "top_reasons": top_reason_list,
        "top_domains_by_risk": top_dom_risk,
        "top_domains_by_review_subclass": top_dom_by_subclass,
    }


# --------------------------------------------------------------------------- #
# Report generation                                                           #
# --------------------------------------------------------------------------- #


def render_markdown_report(results: dict[str, dict[str, Any]]) -> str:
    lines: list[str] = []
    lines.append("# TrashPanda V2 Calibration Report — `examples/WY_small.csv`")
    lines.append("")
    lines.append("**Dataset:** `examples/WY_small.csv`  ")
    lines.append(f"**Modes compared:** {', '.join(results.keys())}")
    lines.append("")

    # --- Comparison matrix ---
    lines.append("## 1. High-level comparison")
    lines.append("")
    lines.append("| Metric | " + " | ".join(results.keys()) + " |")
    lines.append("|---" * (len(results) + 1) + "|")

    def row(label: str, getter):
        vals = [str(getter(results[m])) for m in results]
        lines.append(f"| {label} | " + " | ".join(vals) + " |")

    row("total_rows", lambda r: r["total_rows"])
    row("v1 ready", lambda r: r["v1_bucket_distribution"].get("ready", 0))
    row("v1 review", lambda r: r["v1_bucket_distribution"].get("review", 0))
    row("v1 invalid", lambda r: r["v1_bucket_distribution"].get("invalid", 0))
    row("deliv high", lambda r: r["deliverability_label_distribution"].get("high", 0))
    row("deliv medium", lambda r: r["deliverability_label_distribution"].get("medium", 0))
    row("deliv low", lambda r: r["deliverability_label_distribution"].get("low", 0))
    row("final auto_approve", lambda r: r["final_action_distribution"].get("auto_approve", 0))
    row("final manual_review", lambda r: r["final_action_distribution"].get("manual_review", 0))
    row("final auto_reject", lambda r: r["final_action_distribution"].get("auto_reject", 0))
    row("possible_catch_all", lambda r: r["possible_catch_all_count"])
    row("smtp_tested", lambda r: r["smtp_tested_count"])
    row("smtp_confirmed_valid", lambda r: r["smtp_confirmed_valid_count"])
    row("smtp_suspicious", lambda r: r["smtp_suspicious_count"])
    row("adj +", lambda r: r["confidence_adjustment_applied"]["positive"])
    row("adj 0", lambda r: r["confidence_adjustment_applied"]["zero"])
    row("adj -", lambda r: r["confidence_adjustment_applied"]["negative"])
    row("bucket_flips", lambda r: r["bucket_flip_count"])

    # --- Per-mode detail ---
    for mode, r in results.items():
        lines.append("")
        lines.append(f"## Mode detail — `{mode}`")
        lines.append("")
        lines.append(f"- Total rows: **{r['total_rows']}**")
        lines.append(f"- V1 buckets: {r['v1_bucket_distribution']}")
        if r["bucket_v2_distribution"]:
            lines.append(f"- V2 buckets (scoring engine): {r['bucket_v2_distribution']}")
        if r["deliverability_label_distribution"]:
            lines.append(f"- Deliverability label: {r['deliverability_label_distribution']}")
        if r["deliverability_probability_stats"]:
            s = r["deliverability_probability_stats"]
            lines.append(
                f"- Probability stats: min={s['min']:.3f} p25={s['p25']:.3f} "
                f"p50={s['p50']:.3f} p75={s['p75']:.3f} max={s['max']:.3f} mean={s['mean']:.3f}"
            )
        if r["final_action_distribution"]:
            lines.append(f"- Final action: {r['final_action_distribution']}")
        if r["historical_label_distribution"]:
            lines.append(f"- Historical labels: {r['historical_label_distribution']}")
        if r["review_subclass_distribution"]:
            lines.append(f"- Review subclass: {r['review_subclass_distribution']}")
        lines.append(
            f"- Catch-all: {r['possible_catch_all_count']} | "
            f"SMTP tested: {r['smtp_tested_count']} / confirmed_valid: {r['smtp_confirmed_valid_count']} / "
            f"suspicious: {r['smtp_suspicious_count']}"
        )
        lines.append(f"- Confidence adjustment applied: {r['confidence_adjustment_applied']}")
        lines.append(f"- Bucket flips (override): {r['bucket_flip_count']}")
        if r["top_reasons"]:
            lines.append("")
            lines.append("**Top reasons / signals:**")
            for code, cnt in r["top_reasons"]:
                lines.append(f"  - `{code}`: {cnt}")
        if r["top_domains_by_risk"]:
            lines.append("")
            lines.append("**Top risky domains (low prob or v1 invalid):**")
            for dom, cnt in r["top_domains_by_risk"]:
                lines.append(f"  - `{dom}`: {cnt}")
        if r["top_domains_by_review_subclass"]:
            lines.append("")
            lines.append("**Top domains by review subclass:**")
            for sc, doms in r["top_domains_by_review_subclass"].items():
                if doms:
                    lines.append(f"  - `{sc}`: " + ", ".join(f"{d}({c})" for d, c in doms))

    # Observations + recommendations are appended separately by
    # build_observations_section() so they can see ALL modes at once.
    return "\n".join(lines) + "\n"


def build_observations_section(results: dict[str, dict[str, Any]]) -> str:
    """Data-driven observations and concrete recommendations."""
    a = results.get("A_v1_only", {})
    b = results.get("B_v2_annotations", {})
    c = results.get("C_v2_override", {})

    total = b.get("total_rows") or a.get("total_rows") or 1
    deliv = b.get("deliverability_label_distribution", {})
    final_action = b.get("final_action_distribution", {})
    cp_ca = b.get("possible_catch_all_count", 0)
    smtp_tested = b.get("smtp_tested_count", 0)
    adj = b.get("confidence_adjustment_applied", {"positive": 0, "zero": 0, "negative": 0})
    prob_stats = b.get("deliverability_probability_stats", {})

    pct_low = (deliv.get("low", 0) / total) * 100 if total else 0
    pct_high = (deliv.get("high", 0) / total) * 100 if total else 0
    pct_medium = (deliv.get("medium", 0) / total) * 100 if total else 0
    pct_catch_all = (cp_ca / total) * 100 if total else 0
    pct_reject = (final_action.get("auto_reject", 0) / total) * 100 if total else 0
    pct_approve = (final_action.get("auto_approve", 0) / total) * 100 if total else 0

    flips_c = c.get("bucket_flip_count", 0) if c else 0

    obs: list[str] = []
    obs.append("## 2. Observations")
    obs.append("")
    obs.append(f"- **Dataset size:** {total} rows.")
    obs.append(
        f"- **Deliverability label spread (mode B):** high={deliv.get('high', 0)} "
        f"({pct_high:.1f}%), medium={deliv.get('medium', 0)} ({pct_medium:.1f}%), "
        f"low={deliv.get('low', 0)} ({pct_low:.1f}%)."
    )
    if prob_stats:
        obs.append(
            f"- **Probability distribution:** median={prob_stats.get('p50', 0):.2f}, "
            f"p25={prob_stats.get('p25', 0):.2f}, p75={prob_stats.get('p75', 0):.2f}. "
            "Tight spread around the median ⇒ model is compressed; wide spread ⇒ signal-rich."
        )
    obs.append(
        f"- **Decision engine (mode B, annotate-only):** auto_approve={final_action.get('auto_approve', 0)} "
        f"({pct_approve:.1f}%), manual_review={final_action.get('manual_review', 0)}, "
        f"auto_reject={final_action.get('auto_reject', 0)} ({pct_reject:.1f}%)."
    )
    obs.append(
        f"- **Catch-all:** {cp_ca}/{total} rows flagged ({pct_catch_all:.1f}%)."
    )
    obs.append(
        f"- **SMTP probing (dry-run):** {smtp_tested} rows tested. "
        "Dry-run does not confirm deliverability; treat as selection-only signal."
    )
    obs.append(
        f"- **Confidence adjustments (mode B):** + = {adj.get('positive', 0)}, "
        f"0 = {adj.get('zero', 0)}, − = {adj.get('negative', 0)}."
    )
    if c:
        obs.append(
            f"- **Bucket flips with override ON (mode C):** {flips_c}. "
            "Each flip is a row whose V1 placement would change if override were default."
        )

    # --- Too-aggressive / too-weak signal heuristics ---
    signals = []
    if pct_catch_all > 40:
        signals.append(
            f"⚠ `possible_catch_all` fires on {pct_catch_all:.0f}% of rows — almost certainly "
            "over-detecting. Consider raising `min_observations_for_catch_all` or tightening heuristics."
        )
    elif pct_catch_all < 1 and total > 50:
        signals.append(
            f"ℹ `possible_catch_all` fires on {pct_catch_all:.1f}% of rows — either under-detecting "
            "or dataset has no catch-all domains."
        )
    if pct_reject > 30:
        signals.append(
            f"⚠ Decision engine auto_rejects {pct_reject:.0f}% of rows — review_threshold=0.50 "
            "may be too high. Consider lowering to 0.40 or widening the manual_review band."
        )
    if pct_approve < 20 and pct_high > 40:
        signals.append(
            f"⚠ Probability labels {pct_high:.0f}% as high but auto_approve is only {pct_approve:.0f}%. "
            "approve_threshold=0.80 may be too strict — consider 0.75."
        )
    if adj.get("negative", 0) == 0 and adj.get("positive", 0) == 0:
        signals.append(
            "ℹ No confidence adjustments were applied — either `min_observations_for_adjustment=5` "
            "has not been reached yet for these domains (cold history), or `apply_light_confidence_adjustment=false`."
        )
    if smtp_tested == 0:
        signals.append(
            "ℹ SMTP probing selected 0 rows on this dataset. Selective probing only triggers for "
            "catch-all/timeout/negative-adjustment candidates — with a cold history this is expected."
        )
    if not signals:
        signals.append("No obvious over/under-detection signals on this dataset.")
    obs.append("")
    obs.append("### 2.1 Signals: too aggressive / too weak")
    obs.append("")
    for s in signals:
        obs.append(f"- {s}")

    # --- Recommendations ---
    recs: list[str] = []
    if pct_reject > 30:
        recs.append(
            "Lower `decision.review_threshold` from 0.50 → 0.40 to reduce premature auto_reject on this dataset."
        )
    if pct_approve < 20 and pct_high > 40:
        recs.append(
            "Lower `decision.approve_threshold` from 0.80 → 0.75 to better match the probability distribution."
        )
    if pct_catch_all > 40:
        recs.append(
            "Raise catch-all `min_observations` (currently 5) to 10–15; current dataset has a cold history "
            "so most signals come from heuristics rather than evidence."
        )
    if adj.get("negative", 0) == 0 and adj.get("positive", 0) == 0 and total > 0:
        recs.append(
            "Keep `history.min_observations_for_adjustment=5`, but document that on a 200-row one-shot run "
            "no domain will clear that bar; the knob is only meaningful across repeat runs."
        )
    if flips_c > 0 and flips_c / max(total, 1) > 0.10:
        recs.append(
            f"Do NOT enable `decision.enable_bucket_override` by default — mode C flips "
            f"{flips_c}/{total} rows, too aggressive for production."
        )
    if smtp_tested == 0:
        recs.append(
            "Leave `smtp_probe.enabled=false` as default. Re-evaluate after several runs have populated history."
        )
    if prob_stats:
        p50 = prob_stats.get("p50", 0)
        p25 = prob_stats.get("p25", 0)
        p75 = prob_stats.get("p75", 0)
        if p75 - p25 < 0.10:
            recs.append(
                f"Probability model is compressed (IQR={p75 - p25:.2f}); thresholds at 0.70/0.40 "
                "may sit on a flat part of the curve. Consider re-weighting signals before using "
                "decision thresholds in production."
            )
        if p50 > 0.85:
            recs.append(
                f"Median probability = {p50:.2f} — heavily skewed to 'high'. approve_threshold=0.80 "
                "will auto-approve nearly everything; consider 0.85 or recalibrating."
            )
    if not recs:
        recs.append("Current V2 defaults look reasonable on this dataset. No changes recommended.")
    obs.append("")
    obs.append("## 3. Recommendations (concrete)")
    obs.append("")
    for r in recs:
        obs.append(f"- {r}")

    # --- Questions answered ---
    obs.append("")
    obs.append("## 4. Answers to calibration questions")
    obs.append("")

    def answer(q: str, a: str):
        obs.append(f"- **{q}**  \n  {a}")

    v2_adds_value = (
        (deliv.get("high", 0) + deliv.get("low", 0)) > 0
        and (final_action.get("auto_approve", 0) + final_action.get("auto_reject", 0)) > 0
    )
    answer(
        "¿V2 aporta valor real sobre V1 en WY_small.csv?",
        f"{'Sí — produce información adicional útil (labels + final_action + catch-all) que V1 no tiene.' if v2_adds_value else 'Parcialmente — en este dataset V2 añade columnas pero casi no cambia decisiones.'} "
        "V2 nunca mueve rows en mode B, así que el riesgo de integrarlo en UI es bajo.",
    )
    strongest = []
    if deliv:
        strongest.append("deliverability_probability / label")
    if cp_ca > 0:
        strongest.append("catch_all heuristics")
    if adj.get("positive", 0) + adj.get("negative", 0) > 0:
        strongest.append("historical confidence adjustment")
    answer(
        "¿Qué señales están influyendo más?",
        "Las más activas en este dataset: " + (", ".join(strongest) if strongest else "ninguna dominante") + ".",
    )
    answer(
        "¿Catch-all está sobre-detectando?",
        "Sí" if pct_catch_all > 40 else ("Probablemente no" if pct_catch_all <= 15 else "Posiblemente"),
    )
    answer(
        "¿SMTP selectivo aporta valor o todavía no?",
        "Todavía no en este dataset; seleccionó 0 filas en dry-run. Útil sólo tras acumular historia." if smtp_tested == 0
        else f"Sí en {smtp_tested} filas, aunque dry-run no confirma deliverability.",
    )
    if prob_stats:
        spread = prob_stats.get("p75", 0) - prob_stats.get("p25", 0)
        answer(
            "¿Probability model está bien distribuido o demasiado comprimido / saturado?",
            f"IQR={spread:.2f}. " + ("Comprimido." if spread < 0.10 else "Razonable." if spread < 0.40 else "Amplio, ok."),
        )
    else:
        answer("¿Probability model está bien distribuido?", "No disponible en mode B.")
    answer(
        "¿Thresholds 0.80 / 0.50 del decision engine siguen siendo razonables?",
        ("No — demasiado restrictivos en este dataset." if pct_reject > 30 or (pct_approve < 20 and pct_high > 40)
         else "Sí, razonables para este dataset."),
    )
    answer(
        "¿min_observations_for_adjustment y min_observations_for_catch_all deberían subir?",
        ("Catch-all sí (a 10–15). History no todavía; 5 es apropiado hasta acumular más historia."
         if pct_catch_all > 40 else
         "No en este punto; los valores actuales (5/5) son razonables."),
    )
    answer(
        "¿V2 ya está lista para exponerse en la UI o todavía no?",
        ("Sí, en modo **annotate-only** (mode B). Mostrar deliverability_label, final_action, "
         "catch-all como información no vinculante. NO activar bucket override en UI aún.")
         if v2_adds_value else
        "Todavía no: en este dataset V2 no produce suficiente señal diferenciada para justificar exponerla.",
    )

    obs.append("")
    obs.append("## 5. Verdict")
    obs.append("")
    obs.append(
        "**V2 ya agrega valor hoy como capa de anotación** (mode B): deliverability_label, "
        "final_action y catch_all_reason son útiles incluso si los thresholds necesitan retoques. "
        "**NO activar bucket override en producción** (mode C) hasta acumular más historia real. "
        "**SMTP probing permanece OFF** por defecto: su valor aparece sólo tras múltiples runs que alimenten "
        "la historia."
    )
    return "\n".join(obs) + "\n"


# --------------------------------------------------------------------------- #
# Manual audit                                                                #
# --------------------------------------------------------------------------- #


def build_manual_audit(df_b: pd.DataFrame, out_path: Path, per_group: int = 15) -> int:
    if df_b.empty:
        out_path.write_text("", encoding="utf-8")
        return 0

    def pick(mask: pd.Series, label: str) -> pd.DataFrame:
        if mask is None or mask.sum() == 0:
            return pd.DataFrame()
        sample = df_b[mask].head(per_group).copy()
        sample["_audit_group"] = label
        return sample

    groups: list[pd.DataFrame] = []
    if "deliverability_label" in df_b.columns:
        groups.append(pick(df_b["deliverability_label"].astype(str).eq("high"), "high_probability"))
        groups.append(pick(df_b["deliverability_label"].astype(str).eq("medium"), "medium_probability"))
        groups.append(pick(df_b["deliverability_label"].astype(str).eq("low"), "low_probability"))
    if "possible_catch_all" in df_b.columns:
        groups.append(
            pick(df_b["possible_catch_all"].astype(str).str.lower().isin({"true", "1", "yes"}), "catch_all_suspected")
        )
    if "smtp_tested" in df_b.columns:
        groups.append(pick(df_b["smtp_tested"].astype(str).str.lower().isin({"true", "1", "yes"}), "smtp_tested"))
    if "final_action" in df_b.columns:
        groups.append(pick(df_b["final_action"].astype(str).eq("auto_reject"), "auto_rejected"))
        groups.append(pick(df_b["final_action"].astype(str).eq("auto_approve"), "auto_approved"))

    non_empty = [g for g in groups if not g.empty]
    if not non_empty:
        out_path.write_text("", encoding="utf-8")
        return 0
    audit = pd.concat(non_empty, ignore_index=True)

    out = pd.DataFrame()
    out["email"] = audit.get("email", "")
    out["domain"] = audit.get("domain", "")
    out["v1_bucket"] = audit.get("v1_bucket", "")
    out["v2_probability"] = audit.get("deliverability_probability", "")
    out["deliverability_label"] = audit.get("deliverability_label", "")
    out["final_action"] = audit.get("final_action", "")
    # V2-native reasons that help manual inspection:
    out["human_reason"] = audit.get("human_reason", audit.get("decision_reason", ""))
    out["human_risk"] = audit.get("human_risk", audit.get("catch_all_reason", ""))
    out["human_recommendation"] = audit.get("human_recommendation", audit.get("review_subclass", ""))
    out["notes_for_manual_review"] = audit["_audit_group"]
    out.to_csv(out_path, index=False)
    return len(out)


# --------------------------------------------------------------------------- #
# Main                                                                        #
# --------------------------------------------------------------------------- #


def main() -> int:
    if not INPUT_CSV.exists():
        print(f"ERROR: input not found: {INPUT_CSV}", file=sys.stderr)
        return 1

    mode_run_dirs: dict[str, Path] = {}
    mode_frames: dict[str, pd.DataFrame] = {}
    results: dict[str, dict[str, Any]] = {}

    for mode_name, mode_spec in MODES.items():
        print(f"\n=== Running mode {mode_name}: {mode_spec['description']} ===")
        cfg_path = build_config_for_mode(mode_name, mode_spec)
        out_root = OUT_DIR / "wy_small" / mode_name
        # Clean previous run to keep filesystem predictable.
        if out_root.exists():
            shutil.rmtree(out_root, ignore_errors=True)
        out_root.mkdir(parents=True, exist_ok=True)

        job = run_cleaning_job(
            input_path=INPUT_CSV,
            output_root=out_root,
            config_path=cfg_path,
            job_id=f"wy_small_{mode_name}",
        )
        if job.status != "completed":
            err_msg = job.error.message if job.error else "<no error>"
            print(f"  FAILED: {err_msg}", file=sys.stderr)
            results[mode_name] = {
                "mode": mode_name,
                "total_rows": 0,
                "v1_bucket_distribution": {},
                "bucket_v2_distribution": {},
                "deliverability_label_distribution": {},
                "deliverability_probability_stats": {},
                "final_action_distribution": {},
                "overridden_bucket_distribution": {},
                "historical_label_distribution": {},
                "review_subclass_distribution": {},
                "possible_catch_all_count": 0,
                "smtp_tested_count": 0,
                "smtp_confirmed_valid_count": 0,
                "smtp_suspicious_count": 0,
                "confidence_adjustment_applied": {"positive": 0, "zero": 0, "negative": 0, "missing": 0},
                "bucket_flip_count": 0,
                "top_reasons": [],
                "top_domains_by_risk": [],
                "top_domains_by_review_subclass": {},
                "error": err_msg,
            }
            continue

        run_dir = job.run_dir
        mode_run_dirs[mode_name] = run_dir
        df = load_run_rows(run_dir)
        mode_frames[mode_name] = df
        results[mode_name] = compute_metrics(df, mode_name)
        print(
            f"  rows={len(df)} ready={results[mode_name]['v1_bucket_distribution'].get('ready', 0)} "
            f"review={results[mode_name]['v1_bucket_distribution'].get('review', 0)} "
            f"invalid={results[mode_name]['v1_bucket_distribution'].get('invalid', 0)} "
            f"run_dir={run_dir}"
        )

    # --- Write JSON ---
    json_path = OUT_DIR / "wy_small_v2_calibration_report.json"
    payload = {
        "dataset": str(INPUT_CSV.relative_to(ROOT)),
        "modes": {
            m: {**MODES[m], "run_dir": str(mode_run_dirs.get(m, "")), "metrics": results[m]}
            for m in MODES
        },
    }
    json_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    print(f"\nWrote {json_path}")

    # --- Write CSV (long/tidy metric rows) ---
    csv_rows: list[dict[str, Any]] = []
    for mode, r in results.items():
        flat = {
            "mode": mode,
            "total_rows": r["total_rows"],
            "v1_ready": r["v1_bucket_distribution"].get("ready", 0),
            "v1_review": r["v1_bucket_distribution"].get("review", 0),
            "v1_invalid": r["v1_bucket_distribution"].get("invalid", 0),
            "deliv_high": r["deliverability_label_distribution"].get("high", 0),
            "deliv_medium": r["deliverability_label_distribution"].get("medium", 0),
            "deliv_low": r["deliverability_label_distribution"].get("low", 0),
            "final_auto_approve": r["final_action_distribution"].get("auto_approve", 0),
            "final_manual_review": r["final_action_distribution"].get("manual_review", 0),
            "final_auto_reject": r["final_action_distribution"].get("auto_reject", 0),
            "possible_catch_all": r["possible_catch_all_count"],
            "smtp_tested": r["smtp_tested_count"],
            "smtp_confirmed_valid": r["smtp_confirmed_valid_count"],
            "smtp_suspicious": r["smtp_suspicious_count"],
            "adj_positive": r["confidence_adjustment_applied"]["positive"],
            "adj_zero": r["confidence_adjustment_applied"]["zero"],
            "adj_negative": r["confidence_adjustment_applied"]["negative"],
            "bucket_flips": r["bucket_flip_count"],
        }
        csv_rows.append(flat)
    csv_path = OUT_DIR / "wy_small_v2_calibration_report.csv"
    pd.DataFrame(csv_rows).to_csv(csv_path, index=False)
    print(f"Wrote {csv_path}")

    # --- Write Markdown ---
    md = render_markdown_report(results) + "\n" + build_observations_section(results)
    md_path = OUT_DIR / "wy_small_v2_calibration_report.md"
    md_path.write_text(md, encoding="utf-8")
    print(f"Wrote {md_path}")

    # --- Manual audit (based on mode B, the annotate-only truth view) ---
    audit_path = OUT_DIR / "wy_small_manual_audit.csv"
    df_b = mode_frames.get("B_v2_annotations", pd.DataFrame())
    n_audit = build_manual_audit(df_b, audit_path)
    print(f"Wrote {audit_path} ({n_audit} rows)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
