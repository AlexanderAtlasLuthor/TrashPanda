"""V1 vs V2.2 evaluation harness.

Runs both scoring engines over a synthetic dataset that exercises every
targeted scenario (A-only, MX, timeout, typo+MX, domain mismatch, hard
stops, etc.) and reports the comparison metrics the evaluation spec
requires. Does NOT modify any production code.
"""

from __future__ import annotations

import random
import statistics
from collections import Counter
from dataclasses import asdict

import pandas as pd

from app.scoring import score_row
from app.scoring_v2 import (
    DnsSignalEvaluator,
    DomainMatchSignalEvaluator,
    DomainPresenceSignalEvaluator,
    ScoringEngineV2,
    SyntaxSignalEvaluator,
    TypoCorrectionSignalEvaluator,
    build_default_profile,
)


# ---------------------------------------------------------------------------
# Bucket ranking used to decide upgrade / downgrade directionality.
# ---------------------------------------------------------------------------

BUCKET_RANK = {"invalid": 0, "review": 1, "high_confidence": 2}


# ---------------------------------------------------------------------------
# Synthetic dataset
# ---------------------------------------------------------------------------


def _generate_rows(rng: random.Random) -> list[dict]:
    """Return a mix of rows covering every scenario the eval spec calls out.

    Each row is a plain dict mirroring the pipeline's per-row columns
    after the preprocess / enrichment stages have run.
    """
    rows: list[dict] = []

    # Scenario 1: Clean MX rows (major fraction, like a typical real dataset).
    for _ in range(250):
        rows.append(
            {
                "syntax_valid": True,
                "corrected_domain": "gmail.com",
                "typo_corrected": False,
                "domain_matches_input_column": True,
                "has_mx_record": True,
                "has_a_record": False,
                "domain_exists": True,
                "dns_error": None,
            }
        )

    # Scenario 2: MX rows with minor issues — domain mismatch but no typo.
    for _ in range(30):
        rows.append(
            {
                "syntax_valid": True,
                "corrected_domain": "gmail.com",
                "typo_corrected": False,
                "domain_matches_input_column": False,
                "has_mx_record": True,
                "has_a_record": False,
                "domain_exists": True,
                "dns_error": None,
            }
        )

    # Scenario 3: A-only fallback rows (the headline pre-calibration bug).
    for _ in range(80):
        rows.append(
            {
                "syntax_valid": True,
                "corrected_domain": "website-only.example",
                "typo_corrected": False,
                "domain_matches_input_column": True,
                "has_mx_record": False,
                "has_a_record": True,
                "domain_exists": True,
                "dns_error": None,
            }
        )

    # Scenario 4: Typo-corrected + MX-confirmed (should remain strong).
    for _ in range(40):
        rows.append(
            {
                "syntax_valid": True,
                "corrected_domain": "gmail.com",
                "typo_corrected": True,
                "domain_matches_input_column": False,
                "has_mx_record": True,
                "has_a_record": False,
                "domain_exists": True,
                "dns_error": None,
            }
        )

    # Scenario 5: DNS timeout rows (should stay review-eligible).
    for _ in range(60):
        rows.append(
            {
                "syntax_valid": True,
                "corrected_domain": "slow.example",
                "typo_corrected": False,
                "domain_matches_input_column": True,
                "has_mx_record": None,
                "has_a_record": None,
                "domain_exists": None,
                "dns_error": "timeout",
            }
        )

    # Scenario 6: DNS no-records (no_mx / no_mx_no_a).
    for _ in range(25):
        rows.append(
            {
                "syntax_valid": True,
                "corrected_domain": "parked.example",
                "typo_corrected": False,
                "domain_matches_input_column": True,
                "has_mx_record": False,
                "has_a_record": False,
                "domain_exists": True,
                "dns_error": "no_mx",
            }
        )
    for _ in range(15):
        rows.append(
            {
                "syntax_valid": True,
                "corrected_domain": "parked2.example",
                "typo_corrected": False,
                "domain_matches_input_column": True,
                "has_mx_record": False,
                "has_a_record": False,
                "domain_exists": True,
                "dns_error": "no_mx_no_a",
            }
        )

    # Scenario 7: Hard-stop NXDOMAIN rows.
    for _ in range(40):
        rows.append(
            {
                "syntax_valid": True,
                "corrected_domain": "does-not-exist.example",
                "typo_corrected": False,
                "domain_matches_input_column": True,
                "has_mx_record": False,
                "has_a_record": False,
                "domain_exists": False,
                "dns_error": "nxdomain",
            }
        )

    # Scenario 8: Hard-stop syntax_invalid and no_domain.
    for _ in range(20):
        rows.append(
            {
                "syntax_valid": False,
                "corrected_domain": "whatever.example",
                "typo_corrected": False,
                "domain_matches_input_column": True,
                "has_mx_record": False,
                "has_a_record": False,
                "domain_exists": True,
                "dns_error": None,
            }
        )
    for _ in range(20):
        rows.append(
            {
                "syntax_valid": True,
                "corrected_domain": "",
                "typo_corrected": False,
                "domain_matches_input_column": None,
                "has_mx_record": None,
                "has_a_record": None,
                "domain_exists": None,
                "dns_error": None,
            }
        )

    # Scenario 9: A-only + domain mismatch (no typo) — pure mismatch stress test.
    for _ in range(20):
        rows.append(
            {
                "syntax_valid": True,
                "corrected_domain": "mismatch.example",
                "typo_corrected": False,
                "domain_matches_input_column": False,
                "has_mx_record": False,
                "has_a_record": True,
                "domain_exists": True,
                "dns_error": None,
            }
        )

    # Scenario 10: DNS generic errors and no_nameservers (transient family).
    for _ in range(15):
        rows.append(
            {
                "syntax_valid": True,
                "corrected_domain": "ns-fail.example",
                "typo_corrected": False,
                "domain_matches_input_column": True,
                "has_mx_record": None,
                "has_a_record": None,
                "domain_exists": None,
                "dns_error": "no_nameservers",
            }
        )
    for _ in range(10):
        rows.append(
            {
                "syntax_valid": True,
                "corrected_domain": "ns-err.example",
                "typo_corrected": False,
                "domain_matches_input_column": True,
                "has_mx_record": None,
                "has_a_record": None,
                "domain_exists": None,
                "dns_error": "error",
            }
        )

    # Scenario 11: structural-only rows (syntax + domain, no DNS probed).
    for _ in range(15):
        rows.append(
            {
                "syntax_valid": True,
                "corrected_domain": "unprobed.example",
                "typo_corrected": False,
                "domain_matches_input_column": None,
                "has_mx_record": None,
                "has_a_record": None,
                "domain_exists": None,
                "dns_error": None,
            }
        )

    rng.shuffle(rows)
    return rows


# ---------------------------------------------------------------------------
# Evaluation pipeline
# ---------------------------------------------------------------------------


def _v1_row(row: dict) -> dict:
    r = score_row(
        syntax_valid=row.get("syntax_valid"),
        corrected_domain=row.get("corrected_domain"),
        has_mx_record=row.get("has_mx_record"),
        has_a_record=row.get("has_a_record"),
        domain_exists=row.get("domain_exists"),
        dns_error=row.get("dns_error"),
        typo_corrected=row.get("typo_corrected"),
        domain_matches_input_column=row.get("domain_matches_input_column"),
    )
    return asdict(r)


def _v2_engine() -> ScoringEngineV2:
    return ScoringEngineV2(
        evaluators=[
            SyntaxSignalEvaluator(),
            DomainPresenceSignalEvaluator(),
            TypoCorrectionSignalEvaluator(),
            DomainMatchSignalEvaluator(),
            DnsSignalEvaluator(),
        ],
        profile=build_default_profile(),
    )


def build_comparison_frame() -> pd.DataFrame:
    rng = random.Random(42)
    rows = _generate_rows(rng)
    engine = _v2_engine()

    records: list[dict] = []
    for row in rows:
        v1 = _v1_row(row)
        v2 = engine.evaluate_row(row)
        # V1 score is 0..100; express V2's final_score on the same scale
        # so score_delta is meaningful.
        v1_score_100 = v1["score"]
        v2_score_100 = v2.final_score * 100.0
        delta = v2_score_100 - v1_score_100
        v1_bucket = v1["preliminary_bucket"]
        v2_bucket = v2.bucket
        v1_rank = BUCKET_RANK[v1_bucket]
        v2_rank = BUCKET_RANK[v2_bucket]
        rec = {
            # inputs
            **{k: row.get(k) for k in row},
            # v1
            "score": v1_score_100,
            "preliminary_bucket": v1_bucket,
            "hard_fail": v1["hard_fail"],
            # v2
            "score_v2": v2.final_score,
            "confidence_v2": v2.confidence,
            "bucket_v2": v2_bucket,
            "hard_stop_v2": v2.hard_stop,
            "reason_codes_v2": tuple(v2.reason_codes),
            # comparison
            "score_delta": delta,
            "abs_score_delta": abs(delta),
            "bucket_changed": v1_bucket != v2_bucket,
            "v2_higher_bucket": v2_rank > v1_rank,
            "v2_lower_bucket": v2_rank < v1_rank,
            "hard_decision_changed": bool(v1["hard_fail"]) != bool(v2.hard_stop),
            "v2_more_strict": v2_rank < v1_rank,
            "v2_more_permissive": v2_rank > v1_rank,
            "low_confidence_v2": v2.confidence < 0.5,
        }
        records.append(rec)

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def _pct(num: int, denom: int) -> float:
    return round(100.0 * num / denom, 2) if denom else 0.0


def _dist(series: pd.Series) -> dict:
    return {str(k): int(v) for k, v in series.value_counts().items()}


def _confidence_bucket(conf: float) -> str:
    if conf < 0.25:
        return "<0.25"
    if conf < 0.5:
        return "0.25-0.5"
    if conf < 0.75:
        return "0.5-0.75"
    return ">=0.75"


def report(df: pd.DataFrame) -> str:
    lines: list[str] = []
    total = len(df)
    lines.append("=" * 72)
    lines.append(" V1 vs V2.2 EVALUATION REPORT ")
    lines.append("=" * 72)

    # --- Step 2: summary metrics ---
    lines.append("\n[STEP 2] SUMMARY METRICS\n")
    lines.append(f"total_rows                      : {total}")
    lines.append(
        f"% bucket_changed                : "
        f"{_pct(int(df['bucket_changed'].sum()), total)}%"
    )
    lines.append(
        f"% v2_higher_bucket              : "
        f"{_pct(int(df['v2_higher_bucket'].sum()), total)}%"
    )
    lines.append(
        f"% v2_lower_bucket               : "
        f"{_pct(int(df['v2_lower_bucket'].sum()), total)}%"
    )
    lines.append(
        f"% hard_decision_changed         : "
        f"{_pct(int(df['hard_decision_changed'].sum()), total)}%"
    )
    lines.append(
        f"% v2_more_strict                : "
        f"{_pct(int(df['v2_more_strict'].sum()), total)}%"
    )
    lines.append(
        f"% v2_more_permissive            : "
        f"{_pct(int(df['v2_more_permissive'].sum()), total)}%"
    )
    lines.append(
        f"avg confidence_v2               : "
        f"{round(df['confidence_v2'].mean(), 4)}"
    )
    lines.append(
        f"% low_confidence_v2 (<0.5)      : "
        f"{_pct(int(df['low_confidence_v2'].sum()), total)}%"
    )
    lines.append("")
    lines.append(
        f"mean score_delta                : "
        f"{round(df['score_delta'].mean(), 2)}"
    )
    lines.append(
        f"median score_delta              : "
        f"{round(df['score_delta'].median(), 2)}"
    )
    lines.append(
        f"max score increase              : "
        f"{round(df['score_delta'].max(), 2)}"
    )
    lines.append(
        f"max score decrease              : "
        f"{round(df['score_delta'].min(), 2)}"
    )

    # --- Cross-tab ---
    lines.append("\nV1 vs V2 BUCKET CROSS-TAB (rows)\n")
    ctab = pd.crosstab(
        df["preliminary_bucket"], df["bucket_v2"], margins=True
    )
    lines.append(ctab.to_string())

    # --- Step 3: targeted scenarios ---
    lines.append("\n" + "=" * 72)
    lines.append(" [STEP 3] TARGETED SCENARIO ANALYSES ")
    lines.append("=" * 72)

    def _has(codes, code):
        return code in codes

    a_only = df[df["reason_codes_v2"].apply(lambda c: _has(c, "a_fallback"))]
    lines.append(f"\n(A) A-only fallback rows — count={len(a_only)}")
    lines.append("    V1 bucket dist: " + str(_dist(a_only["preliminary_bucket"])))
    lines.append("    V2 bucket dist: " + str(_dist(a_only["bucket_v2"])))
    hc_count = int((a_only["bucket_v2"] == "high_confidence").sum())
    review_count = int((a_only["bucket_v2"] == "review").sum())
    lines.append(
        f"    ASSERTIONS: NOT high_confidence? "
        f"{'PASS' if hc_count == 0 else f'FAIL ({hc_count})'}; "
        f"mostly review? "
        f"{'PASS' if review_count >= 0.8 * len(a_only) else 'FAIL'}"
    )

    mx = df[df["reason_codes_v2"].apply(lambda c: _has(c, "mx_present"))]
    lines.append(f"\n(B) MX-present rows — count={len(mx)}")
    lines.append("    V2 bucket dist: " + str(_dist(mx["bucket_v2"])))
    hc_mx = int((mx["bucket_v2"] == "high_confidence").sum())
    hc_total = int((df["bucket_v2"] == "high_confidence").sum())
    lines.append(
        f"    MX contribution to high_confidence: {hc_mx}/{hc_total} "
        f"({_pct(hc_mx, hc_total)}%)"
    )
    if len(mx) and len(a_only):
        lines.append(
            f"    mean score_v2 MX={round(mx['score_v2'].mean(), 4)} vs "
            f"A-only={round(a_only['score_v2'].mean(), 4)}"
        )

    timeout = df[df["reason_codes_v2"].apply(lambda c: _has(c, "dns_timeout"))]
    lines.append(f"\n(C) Timeout rows — count={len(timeout)}")
    lines.append("    V2 bucket dist: " + str(_dist(timeout["bucket_v2"])))
    if len(timeout):
        cbuckets = Counter(
            _confidence_bucket(c) for c in timeout["confidence_v2"]
        )
        lines.append(f"    confidence_v2 histogram: {dict(cbuckets)}")
        lines.append(
            f"    mean confidence_v2={round(timeout['confidence_v2'].mean(), 4)}"
        )
    invalid_count = int((timeout["bucket_v2"] == "invalid").sum())
    lines.append(
        f"    ASSERTIONS: none forced invalid? "
        f"{'PASS' if invalid_count == 0 else f'FAIL ({invalid_count})'}; "
        f"tend toward review? "
        f"{'PASS' if (timeout['bucket_v2'] == 'review').sum() >= 0.8 * len(timeout) else 'FAIL'}"
    )

    typo_mx = df[
        df["reason_codes_v2"].apply(
            lambda c: _has(c, "typo_corrected") and _has(c, "mx_present")
        )
    ]
    lines.append(f"\n(D) Typo-corrected + MX rows — count={len(typo_mx)}")
    if len(typo_mx):
        lines.append("    V1 bucket dist: " + str(_dist(typo_mx["preliminary_bucket"])))
        lines.append("    V2 bucket dist: " + str(_dist(typo_mx["bucket_v2"])))
        lines.append(
            f"    mean score_delta={round(typo_mx['score_delta'].mean(), 2)}"
        )
    hc_typo = int((typo_mx["bucket_v2"] == "high_confidence").sum())
    lines.append(
        f"    ASSERTIONS: can reach high_confidence? "
        f"{'PASS' if hc_typo > 0 else 'FAIL'}; "
        f"stronger than V1 review? "
        f"{'PASS' if (typo_mx['bucket_v2'].map(BUCKET_RANK) >= typo_mx['preliminary_bucket'].map(BUCKET_RANK)).all() else 'FAIL'}"
    )

    mismatch_non_typo = df[
        df["reason_codes_v2"].apply(
            lambda c: _has(c, "domain_mismatch") and not _has(c, "typo_corrected")
        )
    ]
    lines.append(
        f"\n(E) Domain mismatch (non-typo) rows — count={len(mismatch_non_typo)}"
    )
    if len(mismatch_non_typo):
        lines.append(
            "    V1 bucket dist: "
            + str(_dist(mismatch_non_typo["preliminary_bucket"]))
        )
        lines.append(
            "    V2 bucket dist: "
            + str(_dist(mismatch_non_typo["bucket_v2"]))
        )
        lines.append(
            f"    mean score_delta={round(mismatch_non_typo['score_delta'].mean(), 2)}"
        )
        hc_mismatch = int(
            (mismatch_non_typo["bucket_v2"] == "high_confidence").sum()
        )
        lines.append(
            f"    in high_confidence? {hc_mismatch}/{len(mismatch_non_typo)}"
        )

    # --- Step 4: directionality ---
    lines.append("\n" + "=" * 72)
    lines.append(" [STEP 4] DIRECTIONALITY CHECK ")
    lines.append("=" * 72)
    up = int(df["v2_higher_bucket"].sum())
    down = int(df["v2_lower_bucket"].sum())
    lines.append(f"upgrades (V2 higher bucket)   : {up}")
    lines.append(f"downgrades (V2 lower bucket)  : {down}")
    lines.append(
        "V2 is still one-directional? "
        + ("NO — both directions observed" if (up > 0 and down > 0) else "YES")
    )

    # --- Step 5: confidence histogram ---
    lines.append("\n" + "=" * 72)
    lines.append(" [STEP 5] CONFIDENCE DISTRIBUTION ")
    lines.append("=" * 72)
    buckets = Counter(_confidence_bucket(c) for c in df["confidence_v2"])
    order = ["<0.25", "0.25-0.5", "0.5-0.75", ">=0.75"]
    for k in order:
        lines.append(f"  {k:10s}: {buckets.get(k, 0)}")
    lines.append(
        f"  mean confidence_v2 — MX: {round(mx['confidence_v2'].mean(), 4)}, "
        f"A-only: {round(a_only['confidence_v2'].mean(), 4)}, "
        f"Timeout: {round(timeout['confidence_v2'].mean(), 4)}"
    )

    # --- Step 6: failure-mode regression checks ---
    lines.append("\n" + "=" * 72)
    lines.append(" [STEP 6] FAILURE-MODE REGRESSION CHECK ")
    lines.append("=" * 72)
    # (1) A-only rows no longer promoted to high_confidence.
    fm_a_only = int((a_only["bucket_v2"] == "high_confidence").sum()) == 0

    # (2) Weak-positive rows no longer score near 1.0. A row is
    # "weak-positive" if it lacks mx_present. score_v2 >= 0.95
    # without mx_present would be the pre-calibration failure mode.
    weak_pos_near_one = int(
        (
            (df["reason_codes_v2"].apply(lambda c: not _has(c, "mx_present")))
            & (df["score_v2"] >= 0.95)
        ).sum()
    )
    fm_weak_pos = weak_pos_near_one == 0

    # (3) Confidence is no longer uniformly high. The pre-calibration
    # failure was that EVERY non-hard-stop row had confidence >= 0.80
    # because structural signals dominated. Check that there is at
    # least one confidence tier below 0.80 populated with a
    # non-trivial number of rows, AND that MX > A-only > timeout
    # hierarchy is intact.
    non_hard_stop = df[~df["hard_stop_v2"]]
    below_80 = int((non_hard_stop["confidence_v2"] < 0.80).sum())
    fm_conf_discriminates = (
        below_80 > 0
        and mx["confidence_v2"].mean() > a_only["confidence_v2"].mean()
        and a_only["confidence_v2"].mean() > timeout["confidence_v2"].mean()
    )

    # (4) Domain mismatch has visible impact — compared against an
    # otherwise-equivalent MX row, WITHIN V2 (the correct reference
    # frame, since V1 and V2 normalizations differ). Clean-MX rows
    # score 1.0; mismatch-MX (non-typo) rows should score noticeably
    # less within V2.
    clean_mx_score = df[
        (df["reason_codes_v2"].apply(lambda c: _has(c, "mx_present")))
        & (df["reason_codes_v2"].apply(lambda c: _has(c, "domain_match")))
        & (df["reason_codes_v2"].apply(lambda c: not _has(c, "domain_mismatch")))
    ]["score_v2"].mean()
    mismatch_mx_score = df[
        (df["reason_codes_v2"].apply(lambda c: _has(c, "mx_present")))
        & (df["reason_codes_v2"].apply(lambda c: _has(c, "domain_mismatch")))
        & (df["reason_codes_v2"].apply(lambda c: not _has(c, "typo_corrected")))
    ]["score_v2"].mean()
    fm_mismatch_visible = (
        pd.notna(clean_mx_score)
        and pd.notna(mismatch_mx_score)
        and (clean_mx_score - mismatch_mx_score) >= 0.05
    )

    lines.append(
        f"  A-only no longer promoted to high_confidence : "
        f"{'PASS' if fm_a_only else 'FAIL'}"
    )
    lines.append(
        f"  weak-positive rows no longer score near 1.0  : "
        f"{'PASS' if fm_weak_pos else f'FAIL ({weak_pos_near_one} rows)'}"
    )
    lines.append(
        f"  confidence discriminates across tiers        : "
        f"{'PASS' if fm_conf_discriminates else 'FAIL'}  "
        f"(MX={round(mx['confidence_v2'].mean(), 3)}, "
        f"A-only={round(a_only['confidence_v2'].mean(), 3)}, "
        f"timeout={round(timeout['confidence_v2'].mean(), 3)}, "
        f"rows<0.80={below_80})"
    )
    lines.append(
        f"  domain mismatch has visible impact within V2 : "
        f"{'PASS' if fm_mismatch_visible else 'FAIL'}  "
        f"(clean-MX score={round(clean_mx_score, 3) if pd.notna(clean_mx_score) else 'n/a'}, "
        f"mismatch-MX score={round(mismatch_mx_score, 3) if pd.notna(mismatch_mx_score) else 'n/a'})"
    )

    return "\n".join(lines)


if __name__ == "__main__":
    df = build_comparison_frame()
    print(report(df))
