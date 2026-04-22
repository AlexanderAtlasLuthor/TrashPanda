"""End-to-end tests for V2 Phase 6 aggregator.

Builds fabricated CSVs with Phase-5 columns already populated, runs
:func:`run_decision_pass`, and verifies the 5 new columns, the
``decision_summary.csv`` metrics, and the disabled no-op path.
"""

from __future__ import annotations

import csv
from pathlib import Path

from app.validation_v2.decision import (
    DECISION_COLUMNS,
    DecisionConfig,
    run_decision_pass,
)


_COLUMNS: tuple[str, ...] = (
    "id", "email", "domain", "hard_fail",
    "v2_final_bucket", "deliverability_probability",
    "smtp_result",
)


def _row(**kw: str) -> dict[str, str]:
    d: dict[str, str] = {
        "id": "1", "email": "user@example.com", "domain": "example.com",
        "hard_fail": "False", "v2_final_bucket": "review",
        "deliverability_probability": "0.60",
        "smtp_result": "not_tested",
    }
    d.update(kw)
    return d


def _write(path: Path, rows: list[dict[str, str]]) -> None:
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
    _write(run / "clean_high_confidence.csv", ready or [])
    _write(run / "review_medium_confidence.csv", review or [])
    _write(run / "removed_invalid.csv", invalid or [])
    return run


_CFG_ON = DecisionConfig(enabled=True, write_summary_report=True)
_CFG_OFF = DecisionConfig(enabled=False)
_CFG_OVERRIDE = DecisionConfig(
    enabled=True, enable_bucket_override=True, write_summary_report=True,
)


# ─────────────────────────────────────────────────────────────────────── #
# disabled == no-op                                                       #
# ─────────────────────────────────────────────────────────────────────── #


def test_disabled_is_full_noop(tmp_path: Path) -> None:
    run = _build_run(tmp_path, review=[_row()])
    original = (run / "review_medium_confidence.csv").read_bytes()
    assert run_decision_pass(run, _CFG_OFF) is None
    assert (run / "review_medium_confidence.csv").read_bytes() == original
    assert not (run / "decision_summary.csv").exists()


# ─────────────────────────────────────────────────────────────────────── #
# Column presence + content                                               #
# ─────────────────────────────────────────────────────────────────────── #


def test_all_five_columns_land_on_every_row(tmp_path: Path) -> None:
    run = _build_run(
        tmp_path,
        ready=[_row(v2_final_bucket="ready", deliverability_probability="0.95")],
        review=[_row(v2_final_bucket="review", deliverability_probability="0.60")],
        invalid=[_row(v2_final_bucket="invalid", deliverability_probability="0.10")],
    )
    run_decision_pass(run, _CFG_ON)
    for name in (
        "clean_high_confidence.csv",
        "review_medium_confidence.csv",
        "removed_invalid.csv",
    ):
        with (run / name).open(encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for col in DECISION_COLUMNS:
                assert col in (reader.fieldnames or [])
            for row in reader:
                assert row["final_action"] in (
                    "auto_approve", "manual_review", "auto_reject",
                )
                # decision_confidence echoes probability, bounded in [0,1].
                v = float(row["decision_confidence"])
                assert 0.0 <= v <= 1.0
                assert row["decision_note"]


def test_high_probability_rows_are_auto_approved(tmp_path: Path) -> None:
    run = _build_run(
        tmp_path,
        ready=[_row(
            v2_final_bucket="ready", deliverability_probability="0.90",
            smtp_result="deliverable",
        )],
    )
    run_decision_pass(run, _CFG_ON)
    row = next(csv.DictReader(
        (run / "clean_high_confidence.csv").open(encoding="utf-8")
    ))
    assert row["final_action"] == "auto_approve"
    assert row["decision_reason"] == "high_probability"
    # Explanation should mention SMTP when smtp_result=deliverable.
    assert "smtp" in row["decision_note"].lower()


def test_medium_probability_rows_are_manual_review(tmp_path: Path) -> None:
    run = _build_run(
        tmp_path,
        review=[_row(deliverability_probability="0.55")],
    )
    run_decision_pass(run, _CFG_ON)
    row = next(csv.DictReader(
        (run / "review_medium_confidence.csv").open(encoding="utf-8")
    ))
    assert row["final_action"] == "manual_review"
    assert row["decision_reason"] == "medium_probability"


def test_low_probability_rows_are_auto_rejected(tmp_path: Path) -> None:
    run = _build_run(
        tmp_path,
        invalid=[_row(
            v2_final_bucket="invalid", deliverability_probability="0.15",
            smtp_result="undeliverable",
        )],
    )
    run_decision_pass(run, _CFG_ON)
    row = next(csv.DictReader(
        (run / "removed_invalid.csv").open(encoding="utf-8")
    ))
    assert row["final_action"] == "auto_reject"
    assert row["decision_reason"] == "low_probability"


# ─────────────────────────────────────────────────────────────────────── #
# Hard guards propagate                                                   #
# ─────────────────────────────────────────────────────────────────────── #


def test_hard_fail_row_is_auto_rejected_regardless_of_probability(tmp_path: Path) -> None:
    run = _build_run(
        tmp_path,
        invalid=[_row(
            hard_fail="True", v2_final_bucket="hard_fail",
            deliverability_probability="0.99",
        )],
    )
    run_decision_pass(run, _CFG_OVERRIDE)
    row = next(csv.DictReader(
        (run / "removed_invalid.csv").open(encoding="utf-8")
    ))
    assert row["final_action"] == "auto_reject"
    assert row["decision_reason"] == "hard_fail"
    # Even with override on, hard-fails never get overridden.
    assert row["overridden_bucket"] == ""


# ─────────────────────────────────────────────────────────────────────── #
# Bucket override                                                         #
# ─────────────────────────────────────────────────────────────────────── #


def test_bucket_override_off_never_sets_column(tmp_path: Path) -> None:
    run = _build_run(
        tmp_path,
        review=[
            _row(deliverability_probability="0.95"),   # would auto_approve
            _row(deliverability_probability="0.10"),   # would auto_reject
        ],
    )
    run_decision_pass(run, _CFG_ON)  # override OFF
    with (run / "review_medium_confidence.csv").open(encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    for row in rows:
        assert row["overridden_bucket"] == ""


def test_bucket_override_on_annotates_target_bucket(tmp_path: Path) -> None:
    run = _build_run(
        tmp_path,
        review=[
            _row(deliverability_probability="0.95"),   # review → auto_approve → ready
            _row(deliverability_probability="0.10"),   # review → auto_reject → invalid
            _row(deliverability_probability="0.60"),   # review → manual_review → (no override)
        ],
    )
    result = run_decision_pass(run, _CFG_OVERRIDE)
    assert result is not None
    with (run / "review_medium_confidence.csv").open(encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    targets = [r["overridden_bucket"] for r in rows]
    assert targets == ["ready", "invalid", ""]

    stats = result.stats
    assert stats.bucket_overrides_to_ready == 1
    assert stats.bucket_overrides_to_invalid == 1


# ─────────────────────────────────────────────────────────────────────── #
# Summary report                                                          #
# ─────────────────────────────────────────────────────────────────────── #


def test_summary_counts_match_computed_actions(tmp_path: Path) -> None:
    run = _build_run(
        tmp_path,
        ready=[
            _row(v2_final_bucket="ready", deliverability_probability="0.95"),  # approve
            _row(v2_final_bucket="ready", deliverability_probability="0.85"),  # approve
        ],
        review=[
            _row(deliverability_probability="0.65"),  # review
            _row(deliverability_probability="0.55"),  # review
        ],
        invalid=[
            _row(v2_final_bucket="invalid", deliverability_probability="0.10"),  # reject low
            _row(hard_fail="True", v2_final_bucket="hard_fail",
                 deliverability_probability="0.99"),  # reject hard_fail
        ],
    )
    result = run_decision_pass(run, _CFG_ON)
    assert result is not None and result.report_path is not None
    metrics = dict(csv.reader(result.report_path.open(encoding="utf-8")))
    assert int(metrics["total_rows_scanned"]) == 6
    assert int(metrics["total_auto_approved"]) == 2
    assert int(metrics["total_manual_review"]) == 2
    assert int(metrics["total_auto_rejected"]) == 2
    assert int(metrics["rejected_hard_fail"]) == 1
    assert int(metrics["rejected_low_probability"]) == 1


def test_summary_reflects_bucket_override_counts(tmp_path: Path) -> None:
    run = _build_run(
        tmp_path,
        review=[
            _row(deliverability_probability="0.90"),   # → override ready
            _row(deliverability_probability="0.05"),   # → override invalid
        ],
    )
    result = run_decision_pass(run, _CFG_OVERRIDE)
    assert result is not None
    metrics = dict(csv.reader(result.report_path.open(encoding="utf-8")))
    assert int(metrics["bucket_overrides_to_ready"]) == 1
    assert int(metrics["bucket_overrides_to_invalid"]) == 1


# ─────────────────────────────────────────────────────────────────────── #
# Non-destructive to other V2 columns                                     #
# ─────────────────────────────────────────────────────────────────────── #


def test_existing_columns_are_preserved(tmp_path: Path) -> None:
    original_row = _row(
        v2_final_bucket="review", deliverability_probability="0.77",
        smtp_result="deliverable",
    )
    run = _build_run(tmp_path, review=[original_row])
    run_decision_pass(run, _CFG_ON)
    with (run / "review_medium_confidence.csv").open(encoding="utf-8") as fh:
        row = next(csv.DictReader(fh))
    for col in _COLUMNS:
        assert row[col] == original_row.get(col, ""), f"column {col} was modified"
