"""End-to-end tests for V2 Phase 5 aggregator + explanation.

Builds fabricated CSVs carrying the columns that Phases 1-4 would have
populated, runs :func:`run_probability_pass`, and asserts that:

  * four new columns land on every row;
  * label distribution / override counts in the summary match what the
    model computed;
  * ``explain_deliverability`` emits deterministic, non-empty text for
    every combination of factors we care about;
  * the pass is a no-op when ``enabled=False``.
"""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from app.validation_v2.probability import (
    PROBABILITY_COLUMNS,
    DeliverabilityComputation,
    Factor,
    ProbabilityConfig,
    explain_deliverability,
    run_probability_pass,
)


_COLUMNS: tuple[str, ...] = (
    "id", "email", "domain", "corrected_domain",
    "has_mx_record", "hard_fail",
    # V1 + V2 signals consumed by the model.
    "score", "score_post_history",
    "historical_label", "confidence_adjustment_applied",
    "catch_all_confidence",
    "smtp_result", "smtp_confidence",
    "v2_final_bucket", "final_output_reason",
)


def _row(**kw: str) -> dict[str, str]:
    d: dict[str, str] = {
        "id": "1", "email": "user@example.com",
        "domain": "example.com", "corrected_domain": "example.com",
        "has_mx_record": "True", "hard_fail": "False",
        "score": "70", "score_post_history": "70",
        "historical_label": "neutral", "confidence_adjustment_applied": "0",
        "catch_all_confidence": "0.0",
        "smtp_result": "not_tested", "smtp_confidence": "0.0",
        "v2_final_bucket": "review", "final_output_reason": "kept_review",
    }
    d.update(kw)
    return d


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=_COLUMNS)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in _COLUMNS})


def _build_run(
    tmp: Path,
    *,
    ready: list[dict[str, str]] | None = None,
    review: list[dict[str, str]] | None = None,
    invalid: list[dict[str, str]] | None = None,
) -> Path:
    run = tmp / "run"
    _write_csv(run / "clean_high_confidence.csv", ready or [])
    _write_csv(run / "review_medium_confidence.csv", review or [])
    _write_csv(run / "removed_invalid.csv", invalid or [])
    return run


_CFG = ProbabilityConfig(enabled=True, write_summary_report=True)
_CFG_OFF = ProbabilityConfig(enabled=False)


# ─────────────────────────────────────────────────────────────────────── #
# disabled == no-op                                                       #
# ─────────────────────────────────────────────────────────────────────── #


def test_disabled_returns_none_and_touches_nothing(tmp_path: Path) -> None:
    run = _build_run(tmp_path, review=[_row()])
    original = (run / "review_medium_confidence.csv").read_bytes()

    assert run_probability_pass(run, _CFG_OFF) is None
    assert (run / "review_medium_confidence.csv").read_bytes() == original
    assert not (run / "deliverability_summary.csv").exists()


# ─────────────────────────────────────────────────────────────────────── #
# Column presence on every CSV                                            #
# ─────────────────────────────────────────────────────────────────────── #


def test_columns_appended_on_all_three_csvs(tmp_path: Path) -> None:
    run = _build_run(
        tmp_path,
        ready=[_row(v2_final_bucket="ready", score_post_history="80")],
        review=[_row(v2_final_bucket="review", score_post_history="55")],
        invalid=[_row(v2_final_bucket="invalid", score_post_history="20")],
    )
    result = run_probability_pass(run, _CFG)
    assert result is not None
    assert result.rows_processed == 3

    for name in ("clean_high_confidence.csv",
                 "review_medium_confidence.csv",
                 "removed_invalid.csv"):
        with (run / name).open(encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for col in PROBABILITY_COLUMNS:
                assert col in (reader.fieldnames or []), f"{name} missing {col}"
            for row in reader:
                # Every cell must be populated.
                assert row["deliverability_label"] in ("high", "medium", "low")
                p = float(row["deliverability_probability"])
                assert 0.0 <= p <= 1.0
                assert row["deliverability_note"]


# ─────────────────────────────────────────────────────────────────────── #
# Hard guards propagate through the aggregator                            #
# ─────────────────────────────────────────────────────────────────────── #


def test_hard_fail_rows_get_zero_probability(tmp_path: Path) -> None:
    run = _build_run(
        tmp_path,
        invalid=[_row(
            hard_fail="True", v2_final_bucket="hard_fail", score_post_history="10",
        )],
    )
    run_probability_pass(run, _CFG)
    with (run / "removed_invalid.csv").open(encoding="utf-8") as fh:
        row = next(csv.DictReader(fh))
    assert float(row["deliverability_probability"]) == 0.0
    assert row["deliverability_label"] == "low"
    assert "override:hard_fail" in row["deliverability_factors"]


def test_no_mx_rows_are_soft_negative_not_zero(tmp_path: Path) -> None:
    """Additive model: missing MX is a large soft negative (-0.25),
    not a hard override. Probability stays in the low/medium band but
    is never forced exactly to 0 unless another hard guard fires."""
    run = _build_run(
        tmp_path,
        invalid=[_row(
            has_mx_record="False", v2_final_bucket="invalid", score_post_history="40",
        )],
    )
    run_probability_pass(run, _CFG)
    with (run / "removed_invalid.csv").open(encoding="utf-8") as fh:
        row = next(csv.DictReader(fh))
    prob = float(row["deliverability_probability"])
    assert 0.0 <= prob < 0.50
    assert row["deliverability_label"] == "low"
    assert "no_dns" in row["deliverability_factors"]


# ─────────────────────────────────────────────────────────────────────── #
# Summary report                                                          #
# ─────────────────────────────────────────────────────────────────────── #


def test_summary_label_counts_cover_all_three_labels(tmp_path: Path) -> None:
    """The additive model's exact label assignments depend on weights and
    smoothing noise; this test only asserts that the summary tallies up
    to the total, that each label is representable, and that hard-fail
    rows are counted as overrides."""
    run = _build_run(
        tmp_path,
        ready=[
            # High: MX + reliable history + deliverable SMTP + domain_match.
            _row(v2_final_bucket="ready", historical_label="historically_reliable",
                 smtp_result="deliverable"),
            _row(v2_final_bucket="ready", historical_label="historically_reliable",
                 smtp_result="deliverable"),
        ],
        review=[
            # Medium: MX only, neutral history.
            _row(v2_final_bucket="review"),
            _row(v2_final_bucket="review"),
        ],
        invalid=[
            # Low: no DNS + risky history.
            _row(v2_final_bucket="invalid", has_mx_record="False",
                 historical_label="historically_risky"),
            # Low: hard-fail override.
            _row(hard_fail="True", v2_final_bucket="hard_fail"),
        ],
    )
    result = run_probability_pass(run, _CFG)
    assert result is not None and result.report_path is not None

    metrics = dict(csv.reader(result.report_path.open(encoding="utf-8")))
    total = int(metrics["total_rows_scanned"])
    assert total == 6
    high = int(metrics["rows_high"])
    medium = int(metrics["rows_medium"])
    low = int(metrics["rows_low"])
    assert high + medium + low == total
    assert high >= 1 and medium >= 1 and low >= 1
    assert int(metrics["rows_overridden_hard_fail"]) == 1


def test_summary_mean_probability_is_in_unit_interval(tmp_path: Path) -> None:
    rows = [_row(score_post_history=str(v)) for v in (10, 50, 90)]
    run = _build_run(tmp_path, review=rows)
    result = run_probability_pass(run, _CFG)
    assert result is not None and result.report_path is not None
    metrics = dict(csv.reader(result.report_path.open(encoding="utf-8")))
    mean = float(metrics["mean_probability"])
    assert 0.0 <= mean <= 1.0


# ─────────────────────────────────────────────────────────────────────── #
# Label stacking                                                          #
# ─────────────────────────────────────────────────────────────────────── #


def test_strong_positive_signals_yield_high_label(tmp_path: Path) -> None:
    run = _build_run(
        tmp_path,
        ready=[_row(
            v2_final_bucket="ready", score_post_history="80",
            historical_label="historically_reliable", smtp_result="deliverable",
        )],
    )
    run_probability_pass(run, _CFG)
    with (run / "clean_high_confidence.csv").open(encoding="utf-8") as fh:
        row = next(csv.DictReader(fh))
    assert row["deliverability_label"] == "high"
    # base 0.5 + mx(0.20) + history(0.05) + smtp(0.10) ≈ 0.85 ± noise.
    assert float(row["deliverability_probability"]) >= 0.80


def test_strong_negative_signals_yield_low_label(tmp_path: Path) -> None:
    run = _build_run(
        tmp_path,
        review=[_row(
            v2_final_bucket="review", score_post_history="60",
            historical_label="historically_risky", smtp_result="undeliverable",
            catch_all_confidence="0.80",
        )],
    )
    run_probability_pass(run, _CFG)
    with (run / "review_medium_confidence.csv").open(encoding="utf-8") as fh:
        row = next(csv.DictReader(fh))
    assert row["deliverability_label"] == "low"


# ─────────────────────────────────────────────────────────────────────── #
# explain_deliverability                                                  #
# ─────────────────────────────────────────────────────────────────────── #


def _comp(
    *,
    probability: float = 0.5, label: str = "medium",
    factors: tuple[Factor, ...] = (),
    override_reason: str = "",
) -> DeliverabilityComputation:
    return DeliverabilityComputation(
        probability=probability, label=label,
        base_probability=probability, factors=factors,
        override_reason=override_reason,
    )


class TestExplanation:
    def test_hard_fail_override_text(self) -> None:
        text = explain_deliverability(_comp(
            probability=0.0, label="low", override_reason="hard_fail",
        ))
        assert "hard-failed" in text.lower()

    def test_duplicate_override_text(self) -> None:
        text = explain_deliverability(_comp(
            probability=0.0, label="low", override_reason="duplicate",
        ))
        assert "duplicate" in text.lower()

    def test_no_mx_override_text(self) -> None:
        text = explain_deliverability(_comp(
            probability=0.0, label="low", override_reason="no_mx_record",
        ))
        assert "mail server" in text.lower()

    def test_only_positives_uses_due_to_phrasing(self) -> None:
        # Additive model: Factor.multiplier is now a signed delta.
        text = explain_deliverability(_comp(
            probability=0.9, label="high",
            factors=(
                Factor("smtp:deliverable", 0.10, ""),
                Factor("history:historically_reliable", 0.05, ""),
            ),
        ))
        assert text.startswith("High probability")
        assert "strong SMTP signal" in text
        assert "reliable domain history" in text

    def test_mixed_factors_describe_tempered_direction(self) -> None:
        text = explain_deliverability(_comp(
            probability=0.5, label="medium",
            factors=(
                Factor("smtp:deliverable", 0.10, ""),
                Factor("catch_all:strong", -0.05, ""),
            ),
        ))
        assert "boosted by" in text
        assert "tempered by" in text

    def test_only_negatives_describe_reduction(self) -> None:
        text = explain_deliverability(_comp(
            probability=0.2, label="low",
            factors=(
                Factor("smtp:undeliverable", -0.25, ""),
                Factor("history:historically_risky", -0.10, ""),
            ),
        ))
        assert "reduced by" in text

    def test_no_factors_falls_back_to_base_sentence(self) -> None:
        text = explain_deliverability(_comp(
            probability=0.5, label="medium", factors=(),
        ))
        assert "V1 score alone" in text
