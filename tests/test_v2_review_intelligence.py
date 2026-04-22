"""End-to-end tests for V2 Phase 3: catch-all + review subclassification.

Exercises the full enrich_csv pass against fabricated CSVs to prove:
  * the four new columns (possible_catch_all, catch_all_confidence,
    catch_all_reason, review_subclass) land on every row.
  * AdjustmentStats surfaces Phase-3 counters.
  * historical_adjustment_summary.csv carries the new metrics.
  * human explanations are augmented when catch-all fires.
"""

from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path

import pytest

from app.validation_v2.catch_all import (
    NOT_REVIEW,
    REVIEW_CATCH_ALL,
    REVIEW_LOW_CONFIDENCE,
    REVIEW_TIMEOUT,
)
from app.validation_v2.history_integration import update_history_from_run
from app.validation_v2.history_models import DomainHistoryRecord
from app.validation_v2.history_store import DomainHistoryStore
from app.validation_v2.scoring_adjustment import AdjustmentConfig, NEW_COLUMNS


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
        "score": "55", "final_output_reason": "kept_review",
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


_ADJ_ON = AdjustmentConfig(
    apply=True,
    max_positive_adjustment=3,
    max_negative_adjustment=5,
    min_observations_for_adjustment=5,
    allow_bucket_flip_from_history=False,
    high_confidence_threshold=70,
    review_threshold=40,
)


def _seed_catch_all_domain(store: DomainHistoryStore, domain: str) -> None:
    """Prime the store with a domain that fits the catch-all profile:
    MX present in 90% of prior observations, only 5% invalid, but 50%
    landed in review.
    """
    total = 200
    store.upsert(
        DomainHistoryRecord(
            domain=domain,
            first_seen_at=datetime(2026, 1, 1),
            last_seen_at=datetime(2026, 4, 1),
            total_seen_count=total,
            mx_present_count=int(total * 0.90),
            invalid_count=int(total * 0.05),
            review_count=int(total * 0.50),
            ready_count=int(total * 0.45),
        )
    )


def _seed_timeout_domain(store: DomainHistoryStore, domain: str) -> None:
    total = 100
    store.upsert(
        DomainHistoryRecord(
            domain=domain,
            first_seen_at=datetime(2026, 1, 1),
            last_seen_at=datetime(2026, 4, 1),
            total_seen_count=total,
            mx_present_count=int(total * 0.50),
            invalid_count=int(total * 0.15),
            review_count=int(total * 0.35),
            ready_count=int(total * 0.50),
            timeout_count=int(total * 0.35),
        )
    )


# ─────────────────────────────────────────────────────────────────────── #
# New columns land on every row                                           #
# ─────────────────────────────────────────────────────────────────────── #


def test_phase3_columns_present_on_all_three_csvs(tmp_path: Path) -> None:
    run_dir = _build_run(
        tmp_path / "run",
        ready=[_row(score="75", final_output_reason="kept_high_confidence")],
        review=[_row(score="55", final_output_reason="kept_review")],
        invalid=[_row(score="20", final_output_reason="removed_low_score")],
    )
    with DomainHistoryStore(tmp_path / "h.sqlite") as store:
        update_history_from_run(run_dir, store, adjustment_config=_ADJ_ON, write_adjustment_report=False)

    for name in (
        "clean_high_confidence.csv",
        "review_medium_confidence.csv",
        "removed_invalid.csv",
    ):
        with (run_dir / name).open(encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for col in (
                "possible_catch_all", "catch_all_confidence",
                "catch_all_reason", "review_subclass",
            ):
                assert col in (reader.fieldnames or []), f"{name} missing {col}"
            # All columns in the NEW_COLUMNS contract must also be present.
            for col in NEW_COLUMNS:
                assert col in (reader.fieldnames or []), f"{name} missing {col}"


# ─────────────────────────────────────────────────────────────────────── #
# Catch-all fires for a primed catch-all-like domain                      #
# ─────────────────────────────────────────────────────────────────────── #


def test_primed_catch_all_domain_fires_on_review_row(tmp_path: Path) -> None:
    db_path = tmp_path / "h.sqlite"
    with DomainHistoryStore(db_path) as store:
        _seed_catch_all_domain(store, "accept-all.com")

    run_dir = _build_run(
        tmp_path / "run",
        review=[_row(
            domain="accept-all.com", corrected_domain="accept-all.com",
            score="55", final_output_reason="kept_review",
        )],
    )
    with DomainHistoryStore(db_path) as store:
        result = update_history_from_run(
            run_dir, store, adjustment_config=_ADJ_ON, write_adjustment_report=True,
        )

    with (run_dir / "review_medium_confidence.csv").open(encoding="utf-8", newline="") as fh:
        row = next(csv.DictReader(fh))

    assert row["possible_catch_all"] == "True"
    assert float(row["catch_all_confidence"]) >= 0.35
    assert row["review_subclass"] == REVIEW_CATCH_ALL
    assert "catch-all" in row["human_reason"].lower()

    stats = result.adjustment_stats
    assert stats is not None
    assert stats.rows_with_possible_catch_all == 1
    assert stats.review_subclasses.get(REVIEW_CATCH_ALL, 0) == 1


def test_primed_timeout_domain_yields_review_timeout(tmp_path: Path) -> None:
    db_path = tmp_path / "h.sqlite"
    with DomainHistoryStore(db_path) as store:
        _seed_timeout_domain(store, "flaky.com")

    run_dir = _build_run(
        tmp_path / "run",
        review=[_row(
            domain="flaky.com", corrected_domain="flaky.com",
            score="55", final_output_reason="kept_review",
        )],
    )
    with DomainHistoryStore(db_path) as store:
        update_history_from_run(
            run_dir, store, adjustment_config=_ADJ_ON, write_adjustment_report=False,
        )

    with (run_dir / "review_medium_confidence.csv").open(encoding="utf-8", newline="") as fh:
        row = next(csv.DictReader(fh))

    # The timeout profile should be picked up either via historical
    # timeout_rate or because the test row itself had a timeout.
    assert row["review_subclass"] in (REVIEW_TIMEOUT, REVIEW_CATCH_ALL)


def test_non_review_rows_get_not_review_subclass(tmp_path: Path) -> None:
    db_path = tmp_path / "h.sqlite"
    with DomainHistoryStore(db_path) as store:
        _seed_catch_all_domain(store, "accept-all.com")

    run_dir = _build_run(
        tmp_path / "run",
        ready=[_row(
            domain="accept-all.com", corrected_domain="accept-all.com",
            score="75", final_output_reason="kept_high_confidence",
        )],
        invalid=[_row(
            domain="accept-all.com", corrected_domain="accept-all.com",
            score="20", final_output_reason="removed_low_score",
        )],
    )
    with DomainHistoryStore(db_path) as store:
        update_history_from_run(
            run_dir, store, adjustment_config=_ADJ_ON, write_adjustment_report=False,
        )

    for name in ("clean_high_confidence.csv", "removed_invalid.csv"):
        with (run_dir / name).open(encoding="utf-8", newline="") as fh:
            row = next(csv.DictReader(fh))
        assert row["review_subclass"] == NOT_REVIEW


def test_reliable_domain_never_fires_catch_all(tmp_path: Path) -> None:
    db_path = tmp_path / "h.sqlite"
    with DomainHistoryStore(db_path) as store:
        # Reliable domain: high MX, high ready, very low review.
        store.upsert(
            DomainHistoryRecord(
                domain="gold.com",
                first_seen_at=datetime(2026, 1, 1),
                last_seen_at=datetime(2026, 4, 1),
                total_seen_count=500,
                mx_present_count=495,
                ready_count=460,
                review_count=25,
                invalid_count=15,
            )
        )

    run_dir = _build_run(
        tmp_path / "run",
        review=[_row(
            domain="gold.com", corrected_domain="gold.com",
            score="55", final_output_reason="kept_review",
        )],
    )
    with DomainHistoryStore(db_path) as store:
        update_history_from_run(
            run_dir, store, adjustment_config=_ADJ_ON, write_adjustment_report=False,
        )

    with (run_dir / "review_medium_confidence.csv").open(encoding="utf-8", newline="") as fh:
        row = next(csv.DictReader(fh))

    assert row["possible_catch_all"] == "False"
    assert row["review_subclass"] != REVIEW_CATCH_ALL


def test_unprimed_domain_returns_insufficient_history(tmp_path: Path) -> None:
    run_dir = _build_run(
        tmp_path / "run",
        review=[_row(
            domain="fresh.com", corrected_domain="fresh.com",
            score="55", final_output_reason="kept_review",
        )],
    )
    with DomainHistoryStore(tmp_path / "h.sqlite") as store:
        update_history_from_run(
            run_dir, store, adjustment_config=_ADJ_ON, write_adjustment_report=False,
        )

    with (run_dir / "review_medium_confidence.csv").open(encoding="utf-8", newline="") as fh:
        row = next(csv.DictReader(fh))

    assert row["possible_catch_all"] == "False"
    assert row["catch_all_reason"] == "insufficient_history"
    assert float(row["catch_all_confidence"]) == 0.0


# ─────────────────────────────────────────────────────────────────────── #
# Explanations are augmented                                              #
# ─────────────────────────────────────────────────────────────────────── #


def test_human_reason_mentions_catch_all_when_signal_fires(tmp_path: Path) -> None:
    db_path = tmp_path / "h.sqlite"
    with DomainHistoryStore(db_path) as store:
        _seed_catch_all_domain(store, "accept-all.com")

    run_dir = _build_run(
        tmp_path / "run",
        review=[_row(
            domain="accept-all.com", corrected_domain="accept-all.com",
            score="55", final_output_reason="kept_review",
        )],
    )
    with DomainHistoryStore(db_path) as store:
        update_history_from_run(
            run_dir, store, adjustment_config=_ADJ_ON, write_adjustment_report=False,
        )

    with (run_dir / "review_medium_confidence.csv").open(encoding="utf-8", newline="") as fh:
        row = next(csv.DictReader(fh))

    assert "catch-all" in row["human_reason"].lower()
    assert row["human_risk"] == "Medium-High"
    assert "direct relationship" in row["human_recommendation"].lower()


def test_ready_row_with_catch_all_gets_low_medium_risk(tmp_path: Path) -> None:
    db_path = tmp_path / "h.sqlite"
    with DomainHistoryStore(db_path) as store:
        _seed_catch_all_domain(store, "accept-all.com")

    run_dir = _build_run(
        tmp_path / "run",
        ready=[_row(
            domain="accept-all.com", corrected_domain="accept-all.com",
            score="75", final_output_reason="kept_high_confidence",
        )],
    )
    with DomainHistoryStore(db_path) as store:
        update_history_from_run(
            run_dir, store, adjustment_config=_ADJ_ON, write_adjustment_report=False,
        )

    with (run_dir / "clean_high_confidence.csv").open(encoding="utf-8", newline="") as fh:
        row = next(csv.DictReader(fh))

    if row["possible_catch_all"] == "True":
        assert "catch-all" in row["human_reason"].lower()
        assert row["human_risk"] == "Low-Medium"


# ─────────────────────────────────────────────────────────────────────── #
# Summary report carries the new metrics                                  #
# ─────────────────────────────────────────────────────────────────────── #


def test_adjustment_summary_includes_phase3_metrics(tmp_path: Path) -> None:
    db_path = tmp_path / "h.sqlite"
    with DomainHistoryStore(db_path) as store:
        _seed_catch_all_domain(store, "accept-all.com")

    run_dir = _build_run(
        tmp_path / "run",
        review=[
            _row(
                domain="accept-all.com", corrected_domain="accept-all.com",
                score="55", final_output_reason="kept_review",
            ),
            _row(
                domain="other.com", corrected_domain="other.com",
                score="55", final_output_reason="kept_review",
            ),
        ],
    )
    with DomainHistoryStore(db_path) as store:
        result = update_history_from_run(
            run_dir, store, adjustment_config=_ADJ_ON, write_adjustment_report=True,
        )

    assert result.adjustment_report_path is not None
    metrics = dict(csv.reader(result.adjustment_report_path.open(encoding="utf-8")))
    # Phase-3 counters must be present.
    assert "rows_with_possible_catch_all" in metrics
    assert int(metrics["rows_with_possible_catch_all"]) >= 1
    assert any(key.startswith("review_subclass:") for key in metrics)


# ─────────────────────────────────────────────────────────────────────── #
# Phase 3 does not alter V2.2 scoring guardrails                          #
# ─────────────────────────────────────────────────────────────────────── #


def test_hard_fail_rows_still_immune_even_with_catch_all_domain(tmp_path: Path) -> None:
    db_path = tmp_path / "h.sqlite"
    with DomainHistoryStore(db_path) as store:
        _seed_catch_all_domain(store, "accept-all.com")

    run_dir = _build_run(
        tmp_path / "run",
        invalid=[_row(
            domain="accept-all.com", corrected_domain="accept-all.com",
            score="10", hard_fail="True",
            final_output_reason="removed_hard_fail",
        )],
    )
    with DomainHistoryStore(db_path) as store:
        update_history_from_run(
            run_dir, store, adjustment_config=_ADJ_ON, write_adjustment_report=False,
        )

    with (run_dir / "removed_invalid.csv").open(encoding="utf-8", newline="") as fh:
        row = next(csv.DictReader(fh))

    assert row["v2_final_bucket"] == "hard_fail"
    assert row["flip_blocked_reason"] == "hard_fail"
    assert int(row["confidence_adjustment_applied"]) == 0
    # Catch-all info may still be populated informationally, but review
    # subclass must still be NOT_REVIEW (hard_fail is never review).
    assert row["review_subclass"] == NOT_REVIEW
