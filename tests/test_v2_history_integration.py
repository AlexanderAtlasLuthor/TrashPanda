"""End-to-end integration tests for the V2 history layer.

These tests fabricate run directories with the exact technical-CSV
header that the V1 pipeline emits, then exercise
:func:`build_observations_from_run` and :func:`update_history_from_run`
against those fixtures.
"""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from app.validation_v2.history_integration import (
    build_observations_from_run,
    update_history_from_run,
    write_domain_history_summary,
)
from app.validation_v2.history_models import FinalDecision, HistoricalLabel
from app.validation_v2.history_store import DomainHistoryStore


# ─────────────────────────────────────────────────────────────────────── #
# Fixture helpers                                                         #
# ─────────────────────────────────────────────────────────────────────── #


_MIN_COLUMNS: tuple[str, ...] = (
    "id",
    "email",
    "domain",
    "corrected_domain",
    "domain_from_email",
    "typo_corrected",
    "dns_check_performed",
    "has_mx_record",
    "has_a_record",
    "dns_error",
    "hard_fail",
    "final_output_reason",
)


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=_MIN_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in _MIN_COLUMNS})


def _row(**kwargs: str) -> dict[str, str]:
    defaults = {
        "id": "1",
        "email": "a@example.com",
        "domain": "example.com",
        "corrected_domain": "example.com",
        "domain_from_email": "example.com",
        "typo_corrected": "False",
        "dns_check_performed": "True",
        "has_mx_record": "True",
        "has_a_record": "True",
        "dns_error": "",
        "hard_fail": "False",
        "final_output_reason": "kept_high_confidence",
    }
    defaults.update(kwargs)
    return defaults


@pytest.fixture
def fake_run_dir(tmp_path: Path) -> Path:
    """A tmp run dir populated with all three technical CSVs."""
    run_dir = tmp_path / "run_fake"

    _write_csv(
        run_dir / "clean_high_confidence.csv",
        [
            _row(domain="reliable.com", corrected_domain="reliable.com"),
            _row(domain="reliable.com", corrected_domain="reliable.com"),
            _row(domain="reliable.com", corrected_domain="reliable.com"),
            _row(domain="mixed.com", corrected_domain="mixed.com"),
        ],
    )
    _write_csv(
        run_dir / "review_medium_confidence.csv",
        [
            _row(
                domain="mixed.com",
                corrected_domain="mixed.com",
                has_mx_record="False",
                has_a_record="True",
                final_output_reason="kept_review",
            ),
        ],
    )
    _write_csv(
        run_dir / "removed_invalid.csv",
        [
            _row(
                domain="broken.com",
                corrected_domain="broken.com",
                has_mx_record="False",
                has_a_record="False",
                dns_error="The DNS operation timed out",
                final_output_reason="removed_low_score",
            ),
            _row(
                domain="broken.com",
                corrected_domain="broken.com",
                has_mx_record="False",
                has_a_record="False",
                dns_error="timeout",
                final_output_reason="removed_low_score",
            ),
            _row(
                domain="spam.com",
                corrected_domain="spam.com",
                hard_fail="True",
                final_output_reason="removed_hard_fail",
            ),
        ],
    )

    return run_dir


# ─────────────────────────────────────────────────────────────────────── #
# build_observations_from_run                                             #
# ─────────────────────────────────────────────────────────────────────── #


def test_build_observations_covers_all_three_csvs(fake_run_dir: Path) -> None:
    observations = build_observations_from_run(fake_run_dir)
    # 4 ready + 1 review + 2 invalid + 1 hard_fail = 8
    assert len(observations) == 8


def test_build_observations_maps_final_decision_from_reason(fake_run_dir: Path) -> None:
    observations = build_observations_from_run(fake_run_dir)
    decisions = [o.final_decision for o in observations]
    assert decisions.count(FinalDecision.READY) == 4
    assert decisions.count(FinalDecision.REVIEW) == 1
    assert decisions.count(FinalDecision.INVALID) == 2
    assert decisions.count(FinalDecision.HARD_FAIL) == 1


def test_build_observations_detects_a_fallback(fake_run_dir: Path) -> None:
    # mixed.com appears in review with MX=False, A=True -> a_fallback
    observations = build_observations_from_run(fake_run_dir)
    mixed_review = [
        o for o in observations
        if o.domain == "mixed.com" and o.final_decision == FinalDecision.REVIEW
    ]
    assert len(mixed_review) == 1
    assert mixed_review[0].had_a_fallback is True
    assert mixed_review[0].had_mx is False


def test_build_observations_detects_dns_failure_and_timeout(fake_run_dir: Path) -> None:
    observations = build_observations_from_run(fake_run_dir)
    broken = [o for o in observations if o.domain == "broken.com"]
    assert len(broken) == 2
    assert all(o.had_dns_failure for o in broken)
    assert all(o.had_timeout for o in broken)


def test_build_observations_returns_empty_for_missing_run_dir(tmp_path: Path) -> None:
    # Directory exists but no CSVs.
    empty_dir = tmp_path / "empty_run"
    empty_dir.mkdir()
    assert build_observations_from_run(empty_dir) == []


def test_build_observations_skips_rows_with_no_domain(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_blank_domain"
    _write_csv(
        run_dir / "clean_high_confidence.csv",
        [
            _row(domain="", corrected_domain="", domain_from_email=""),
            _row(domain="ok.com", corrected_domain="ok.com"),
        ],
    )
    observations = build_observations_from_run(run_dir)
    assert len(observations) == 1
    assert observations[0].domain == "ok.com"


# ─────────────────────────────────────────────────────────────────────── #
# update_history_from_run                                                 #
# ─────────────────────────────────────────────────────────────────────── #


def test_update_history_from_run_populates_store(fake_run_dir: Path, tmp_path: Path) -> None:
    db_path = tmp_path / "hist.sqlite"
    with DomainHistoryStore(db_path) as store:
        result = update_history_from_run(fake_run_dir, store)
    assert result.observations_processed == 8
    assert result.domains_updated == 4

    with DomainHistoryStore(db_path) as store:
        reliable = store.get("reliable.com")
        broken = store.get("broken.com")
    assert reliable is not None
    assert reliable.total_seen_count == 3
    assert reliable.ready_count == 3
    assert reliable.mx_present_count == 3
    assert broken is not None
    assert broken.total_seen_count == 2
    assert broken.invalid_count == 2
    assert broken.timeout_count == 2


def test_update_history_from_run_writes_summary_report(
    fake_run_dir: Path, tmp_path: Path,
) -> None:
    db_path = tmp_path / "hist.sqlite"
    with DomainHistoryStore(db_path) as store:
        result = update_history_from_run(fake_run_dir, store)
    assert result.report_path is not None
    assert result.report_path.name == "domain_history_summary.csv"
    assert result.report_path.is_file()
    with result.report_path.open(encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
    domains_in_report = {r["domain"] for r in rows}
    assert domains_in_report == {"reliable.com", "mixed.com", "broken.com", "spam.com"}


def test_update_history_cross_run_increments_counts(
    fake_run_dir: Path, tmp_path: Path,
) -> None:
    db_path = tmp_path / "hist.sqlite"
    with DomainHistoryStore(db_path) as store:
        update_history_from_run(fake_run_dir, store)
    # Run again against the same fake run -> counters must double.
    with DomainHistoryStore(db_path) as store:
        update_history_from_run(fake_run_dir, store)
        reliable = store.get("reliable.com")
    assert reliable is not None
    assert reliable.total_seen_count == 6
    assert reliable.ready_count == 6


def test_update_history_respects_report_flag(
    fake_run_dir: Path, tmp_path: Path,
) -> None:
    db_path = tmp_path / "hist.sqlite"
    with DomainHistoryStore(db_path) as store:
        result = update_history_from_run(fake_run_dir, store, write_summary_report=False)
    assert result.report_path is None
    assert not (fake_run_dir / "domain_history_summary.csv").exists()


# ─────────────────────────────────────────────────────────────────────── #
# Summary report                                                          #
# ─────────────────────────────────────────────────────────────────────── #


def test_summary_report_contains_readiness_labels(
    fake_run_dir: Path, tmp_path: Path,
) -> None:
    """A single run's small counts should label domains as insufficient_data."""
    db_path = tmp_path / "hist.sqlite"
    with DomainHistoryStore(db_path) as store:
        update_history_from_run(fake_run_dir, store)
    with (fake_run_dir / "domain_history_summary.csv").open(encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    labels = {r["domain"]: r["readiness_label"] for r in rows}
    # min_observations default is 5; none of the fake domains reach that.
    assert all(label == HistoricalLabel.INSUFFICIENT_DATA for label in labels.values())


def test_summary_report_sorted_by_total_seen_desc(tmp_path: Path) -> None:
    from datetime import datetime

    from app.validation_v2.history_models import DomainHistoryRecord

    records = [
        DomainHistoryRecord(
            domain="small.com", first_seen_at=datetime.now(),
            last_seen_at=datetime.now(), total_seen_count=1,
        ),
        DomainHistoryRecord(
            domain="big.com", first_seen_at=datetime.now(),
            last_seen_at=datetime.now(), total_seen_count=100,
        ),
        DomainHistoryRecord(
            domain="mid.com", first_seen_at=datetime.now(),
            last_seen_at=datetime.now(), total_seen_count=10,
        ),
    ]
    out = write_domain_history_summary(tmp_path, records)
    with out.open(encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert [r["domain"] for r in rows] == ["big.com", "mid.com", "small.com"]
