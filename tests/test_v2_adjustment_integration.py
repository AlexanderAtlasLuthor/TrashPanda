"""End-to-end tests for Phase-2 adjustment integration.

Focus: when update_history_from_run receives an AdjustmentConfig with
apply=True, it:
  * rewrites all three technical CSVs in-place with new columns,
  * writes historical_adjustment_summary.csv,
  * uses a PRE-update snapshot so domains never self-adjust on first
    observation,
  * preserves existing columns and row counts exactly.
"""

from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path

import pytest

from app.validation_v2.history_integration import (
    HistoryUpdateResult,
    update_history_from_run,
)
from app.validation_v2.history_models import (
    DomainHistoryRecord,
    HistoricalLabel,
)
from app.validation_v2.history_store import DomainHistoryStore
from app.validation_v2.scoring_adjustment import (
    NEW_COLUMNS,
    AdjustmentConfig,
)


_FIXTURE_COLUMNS: tuple[str, ...] = (
    "id", "email", "domain", "corrected_domain", "domain_from_email",
    "typo_corrected", "dns_check_performed", "has_mx_record", "has_a_record",
    "dns_error", "hard_fail", "score", "final_output_reason",
)


def _row(**kw: str) -> dict[str, str]:
    d: dict[str, str] = {
        "id": "1", "email": "a@x.com",
        "domain": "x.com", "corrected_domain": "x.com", "domain_from_email": "x.com",
        "typo_corrected": "False", "dns_check_performed": "True",
        "has_mx_record": "True", "has_a_record": "True",
        "dns_error": "", "hard_fail": "False",
        "score": "80", "final_output_reason": "kept_high_confidence",
    }
    d.update(kw)
    return d


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=_FIXTURE_COLUMNS)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in _FIXTURE_COLUMNS})


def _build_run(
    run_dir: Path,
    *,
    ready: list[dict[str, str]] | None = None,
    review: list[dict[str, str]] | None = None,
    invalid: list[dict[str, str]] | None = None,
) -> Path:
    _write_csv(run_dir / "clean_high_confidence.csv", ready or [])
    _write_csv(run_dir / "review_medium_confidence.csv", review or [])
    _write_csv(run_dir / "removed_invalid.csv", invalid or [])
    return run_dir


_ADJ_ON_NO_FLIPS = AdjustmentConfig(
    apply=True,
    max_positive_adjustment=3,
    max_negative_adjustment=5,
    min_observations_for_adjustment=5,
    allow_bucket_flip_from_history=False,
    high_confidence_threshold=70,
    review_threshold=40,
)
_ADJ_ON_WITH_FLIPS = AdjustmentConfig(
    apply=True,
    max_positive_adjustment=3,
    max_negative_adjustment=5,
    min_observations_for_adjustment=5,
    allow_bucket_flip_from_history=True,
    high_confidence_threshold=70,
    review_threshold=40,
)


# ──────────────────────────────────────────────────────────────────────── #
# A — apply=False leaves CSVs untouched                                   #
# ──────────────────────────────────────────────────────────────────────── #


def test_adjustment_disabled_does_not_modify_csvs(tmp_path: Path) -> None:
    run_dir = _build_run(
        tmp_path / "run",
        ready=[_row()],
        review=[_row(final_output_reason="kept_review", score="55")],
        invalid=[_row(final_output_reason="removed_low_score", score="20")],
    )
    # Snapshot the original CSV byte contents.
    originals = {
        p.name: p.read_bytes()
        for p in run_dir.iterdir() if p.suffix == ".csv"
    }

    with DomainHistoryStore(tmp_path / "h.sqlite") as store:
        update_history_from_run(
            run_dir, store,
            adjustment_config=AdjustmentConfig(apply=False),
            write_adjustment_report=False,
        )

    for name, original in originals.items():
        assert (run_dir / name).read_bytes() == original, (
            f"{name} was modified even though adjustment was disabled"
        )
    assert not (run_dir / "historical_adjustment_summary.csv").exists()


# ──────────────────────────────────────────────────────────────────────── #
# B — apply=True adds all new columns with correct values                 #
# ──────────────────────────────────────────────────────────────────────── #


def test_adjustment_on_adds_all_expected_columns(tmp_path: Path) -> None:
    run_dir = _build_run(
        tmp_path / "run",
        ready=[_row()],
        review=[_row(final_output_reason="kept_review", score="55")],
        invalid=[_row(final_output_reason="removed_low_score", score="20")],
    )
    with DomainHistoryStore(tmp_path / "h.sqlite") as store:
        result = update_history_from_run(
            run_dir, store,
            adjustment_config=_ADJ_ON_NO_FLIPS,
            write_adjustment_report=True,
        )

    assert isinstance(result, HistoryUpdateResult)
    assert result.adjustment_report_path is not None
    assert result.adjustment_report_path.is_file()

    for name in (
        "clean_high_confidence.csv",
        "review_medium_confidence.csv",
        "removed_invalid.csv",
    ):
        with (run_dir / name).open(encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for col in NEW_COLUMNS:
                assert col in reader.fieldnames, f"{name} missing column {col}"


# ──────────────────────────────────────────────────────────────────────── #
# C — PRE-update snapshot: first run cannot self-adjust                   #
# ──────────────────────────────────────────────────────────────────────── #


def test_first_run_uses_empty_history_snapshot(tmp_path: Path) -> None:
    """On the first run, no domain has prior history → no adjustment."""
    run_dir = _build_run(
        tmp_path / "run",
        ready=[_row(domain="fresh.com", corrected_domain="fresh.com") for _ in range(20)],
    )

    with DomainHistoryStore(tmp_path / "h.sqlite") as store:
        update_history_from_run(
            run_dir, store,
            adjustment_config=_ADJ_ON_NO_FLIPS,
            write_adjustment_report=True,
        )

    with (run_dir / "clean_high_confidence.csv").open(encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))

    for r in rows:
        assert r["historical_label"] == HistoricalLabel.INSUFFICIENT_DATA
        assert int(r["confidence_adjustment_applied"]) == 0
        assert int(r["score_post_history"]) == int(r["score_pre_history"])
        assert r["flip_blocked_reason"] == "insufficient_data"


# ──────────────────────────────────────────────────────────────────────── #
# D — After priming history, second run adjusts correctly                 #
# ──────────────────────────────────────────────────────────────────────── #


def test_second_run_uses_primed_history_snapshot(tmp_path: Path) -> None:
    db_path = tmp_path / "h.sqlite"

    # Manually prime the store with a risky domain history so the adjustment
    # pass has something to act on (bypasses the insufficient_data guardrail).
    with DomainHistoryStore(db_path) as store:
        store.upsert(
            DomainHistoryRecord(
                domain="dicey.com",
                first_seen_at=datetime(2026, 1, 1),
                last_seen_at=datetime(2026, 4, 1),
                total_seen_count=100,
                mx_present_count=100,
                invalid_count=80,   # risky
                ready_count=10,
                review_count=10,
            )
        )

    run_dir = _build_run(
        tmp_path / "run",
        ready=[_row(
            domain="dicey.com", corrected_domain="dicey.com",
            score="75", final_output_reason="kept_high_confidence",
        )],
    )

    with DomainHistoryStore(db_path) as store:
        update_history_from_run(
            run_dir, store,
            adjustment_config=_ADJ_ON_NO_FLIPS,
            write_adjustment_report=True,
        )

    with (run_dir / "clean_high_confidence.csv").open(encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
    assert len(rows) == 1
    r = rows[0]
    assert r["historical_label"] == HistoricalLabel.RISKY
    assert int(r["confidence_adjustment_applied"]) < 0
    assert int(r["score_post_history"]) < int(r["score_pre_history"])
    # Flips are disabled, bucket stays.
    assert r["v2_final_bucket"] == "ready"
    assert r["flip_blocked_reason"] == "flips_disabled"


# ──────────────────────────────────────────────────────────────────────── #
# E — Hard-fail rows preserved regardless of history                      #
# ──────────────────────────────────────────────────────────────────────── #


def test_hard_fail_rows_never_receive_adjustment(tmp_path: Path) -> None:
    db_path = tmp_path / "h.sqlite"

    # Prime "spamdomain.com" as reliable.
    with DomainHistoryStore(db_path) as store:
        store.upsert(
            DomainHistoryRecord(
                domain="spamdomain.com",
                first_seen_at=datetime(2026, 1, 1),
                last_seen_at=datetime(2026, 4, 1),
                total_seen_count=100,
                mx_present_count=100,
                ready_count=90,
            )
        )

    # But the row is a hard-fail — history must not rescue it.
    run_dir = _build_run(
        tmp_path / "run",
        invalid=[_row(
            domain="spamdomain.com", corrected_domain="spamdomain.com",
            score="5", hard_fail="True",
            final_output_reason="removed_hard_fail",
        )],
    )

    with DomainHistoryStore(db_path) as store:
        update_history_from_run(
            run_dir, store,
            adjustment_config=_ADJ_ON_WITH_FLIPS,  # even with flips enabled
            write_adjustment_report=True,
        )

    with (run_dir / "removed_invalid.csv").open(encoding="utf-8", newline="") as fh:
        row = next(csv.DictReader(fh))

    assert int(row["confidence_adjustment_applied"]) == 0
    assert int(row["score_post_history"]) == int(row["score_pre_history"])
    assert row["v2_final_bucket"] == "hard_fail"
    assert row["flip_blocked_reason"] == "hard_fail"


# ──────────────────────────────────────────────────────────────────────── #
# F — Summary CSV structure                                               #
# ──────────────────────────────────────────────────────────────────────── #


def test_adjustment_summary_csv_has_expected_metrics(tmp_path: Path) -> None:
    db_path = tmp_path / "h.sqlite"

    # Prime a reliable domain to guarantee at least one positive adjustment
    # on the subsequent run.
    with DomainHistoryStore(db_path) as store:
        store.upsert(
            DomainHistoryRecord(
                domain="gold.com",
                first_seen_at=datetime(2026, 1, 1),
                last_seen_at=datetime(2026, 4, 1),
                total_seen_count=200,
                mx_present_count=200,
                ready_count=180,
            )
        )

    run_dir = _build_run(
        tmp_path / "run",
        review=[_row(
            domain="gold.com", corrected_domain="gold.com",
            score="68", final_output_reason="kept_review",
        )],
        invalid=[_row(
            domain="other.com", corrected_domain="other.com",
            score="20", hard_fail="True",
            final_output_reason="removed_hard_fail",
        )],
    )

    with DomainHistoryStore(db_path) as store:
        result = update_history_from_run(
            run_dir, store,
            adjustment_config=_ADJ_ON_WITH_FLIPS,
            write_adjustment_report=True,
        )

    stats = result.adjustment_stats
    assert stats is not None
    assert stats.total_rows_scanned == 2
    assert stats.rows_with_positive_adjustment >= 1
    assert stats.flips_blocked_hard_fail == 1

    # Report file is parseable and contains the expected metrics.
    with result.adjustment_report_path.open(encoding="utf-8", newline="") as fh:
        metrics = dict(csv.reader(fh))
    assert metrics.get("total_rows_scanned") == "2"
    assert int(metrics["rows_with_positive_adjustment"]) >= 1
    assert int(metrics["flips_blocked_hard_fail"]) == 1


# ──────────────────────────────────────────────────────────────────────── #
# G — Original V1 columns are preserved exactly                           #
# ──────────────────────────────────────────────────────────────────────── #


def test_original_columns_and_values_preserved(tmp_path: Path) -> None:
    original_row = _row(score="75")
    run_dir = _build_run(
        tmp_path / "run", ready=[original_row],
    )

    with DomainHistoryStore(tmp_path / "h.sqlite") as store:
        update_history_from_run(
            run_dir, store,
            adjustment_config=_ADJ_ON_NO_FLIPS,
            write_adjustment_report=False,
        )

    with (run_dir / "clean_high_confidence.csv").open(encoding="utf-8", newline="") as fh:
        row = next(csv.DictReader(fh))

    for col, expected in original_row.items():
        assert row[col] == expected, f"V1 column {col} changed: {row[col]!r} vs {expected!r}"


# ──────────────────────────────────────────────────────────────────────── #
# H — Bucket flip counts incremented only when flips are enabled          #
# ──────────────────────────────────────────────────────────────────────── #


def test_flip_stats_zero_when_flips_disabled(tmp_path: Path) -> None:
    db_path = tmp_path / "h.sqlite"
    with DomainHistoryStore(db_path) as store:
        # Prime reliable domain
        store.upsert(
            DomainHistoryRecord(
                domain="gold.com",
                first_seen_at=datetime(2026, 1, 1),
                last_seen_at=datetime(2026, 4, 1),
                total_seen_count=200,
                mx_present_count=200,
                ready_count=180,
            )
        )

    run_dir = _build_run(
        tmp_path / "run",
        review=[_row(
            domain="gold.com", corrected_domain="gold.com",
            score="68", final_output_reason="kept_review",
        )],
    )

    with DomainHistoryStore(db_path) as store:
        result = update_history_from_run(
            run_dir, store,
            adjustment_config=_ADJ_ON_NO_FLIPS,   # flips off
            write_adjustment_report=True,
        )

    stats = result.adjustment_stats
    assert stats is not None
    assert stats.bucket_flips_review_to_ready == 0
    assert stats.bucket_flips_ready_to_review == 0
    assert stats.flips_blocked_config >= 1


def test_flip_stats_nonzero_when_flips_enabled(tmp_path: Path) -> None:
    db_path = tmp_path / "h.sqlite"
    with DomainHistoryStore(db_path) as store:
        store.upsert(
            DomainHistoryRecord(
                domain="gold.com",
                first_seen_at=datetime(2026, 1, 1),
                last_seen_at=datetime(2026, 4, 1),
                total_seen_count=200,
                mx_present_count=200,
                ready_count=180,
            )
        )

    # Score 68 + reliable(+3) = 71, crosses high threshold 70.
    run_dir = _build_run(
        tmp_path / "run",
        review=[_row(
            domain="gold.com", corrected_domain="gold.com",
            score="68", final_output_reason="kept_review",
        )],
    )

    with DomainHistoryStore(db_path) as store:
        result = update_history_from_run(
            run_dir, store,
            adjustment_config=_ADJ_ON_WITH_FLIPS,
            write_adjustment_report=True,
        )

    stats = result.adjustment_stats
    assert stats is not None
    assert stats.bucket_flips_review_to_ready == 1
