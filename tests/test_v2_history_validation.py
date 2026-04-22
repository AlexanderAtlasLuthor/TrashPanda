"""Deep validation suite for V2 Phase 1 — Domain Historical Memory.

Structured around the 7 checkpoints from the product spec:
  1. Consistency across runs
  2. New vs existing domain lifecycle
  3. Edge-case classification
  4. Isolation (history disabled = V1 identical)
  5. Controlled failure (corrupt SQLite, bad path)
  6. Performance (100K+ row fixture)
  7. Report shape + accuracy
"""

from __future__ import annotations

import csv
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

import pytest

from app.api_boundary import JobStatus, run_cleaning_job
from app.validation_v2.domain_memory import DEFAULT_THRESHOLDS, classify_domain
from app.validation_v2.history_integration import (
    _REPORT_HEADER,
    build_observations_from_run,
    update_history_from_run,
)
from app.validation_v2.history_models import (
    DomainHistoryRecord,
    DomainObservation,
    FinalDecision,
    HistoricalLabel,
)
from app.validation_v2.history_store import DomainHistoryStore


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SAMPLE_CSV = PROJECT_ROOT / "examples" / "sample_contacts.csv"


# ─────────────────────────────────────────────────────────────────────── #
# Shared fixture helpers                                                  #
# ─────────────────────────────────────────────────────────────────────── #


_FIXTURE_COLUMNS: tuple[str, ...] = (
    "id", "email", "domain", "corrected_domain", "domain_from_email",
    "typo_corrected", "dns_check_performed", "has_mx_record", "has_a_record",
    "dns_error", "hard_fail", "final_output_reason",
)


def _write_fixture_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=_FIXTURE_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in _FIXTURE_COLUMNS})


def _row(**kwargs: str) -> dict[str, str]:
    defaults: dict[str, str] = {
        "id": "1", "email": "user@example.com",
        "domain": "example.com", "corrected_domain": "example.com",
        "domain_from_email": "example.com",
        "typo_corrected": "False", "dns_check_performed": "True",
        "has_mx_record": "True", "has_a_record": "True",
        "dns_error": "", "hard_fail": "False",
        "final_output_reason": "kept_high_confidence",
    }
    defaults.update(kwargs)
    return defaults


def _make_tmp_config(
    tmp_path: Path,
    *,
    history_enabled: bool = True,
    db_path: Path | None = None,
    write_summary_report: bool = True,
    min_obs: int = 2,
) -> Path:
    """Build a tmp YAML config pointing at a tmp SQLite file."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    if db_path is None:
        db_path = tmp_path / "history.sqlite"
    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        f"""chunk_size: 5000
max_workers: 4
high_confidence_threshold: 70
review_threshold: 40
fallback_to_a_record: true
invalid_if_disposable: true
dns_timeout_seconds: 4.0
retry_dns_times: 1
export_review_bucket: true
keep_original_columns: true
log_level: WARNING
staging_db_name: staging.sqlite3
temp_dir_name: temp

history:
  enabled: {str(history_enabled).lower()}
  backend: sqlite
  sqlite_path: {db_path.as_posix()}
  apply_light_confidence_adjustment: false
  max_positive_adjustment: 3
  max_negative_adjustment: 5
  min_observations_for_labeling: {min_obs}
  write_summary_report: {str(write_summary_report).lower()}
""",
        encoding="utf-8",
    )
    return cfg


def _build_run_fixture(
    run_dir: Path,
    *,
    ready_rows: Iterable[dict[str, str]] = (),
    review_rows: Iterable[dict[str, str]] = (),
    invalid_rows: Iterable[dict[str, str]] = (),
) -> Path:
    _write_fixture_csv(run_dir / "clean_high_confidence.csv", list(ready_rows))
    _write_fixture_csv(run_dir / "review_medium_confidence.csv", list(review_rows))
    _write_fixture_csv(run_dir / "removed_invalid.csv", list(invalid_rows))
    return run_dir


# ──────────────────────────────────────────────────────────────────────── #
# 1. CONSISTENCY ACROSS RUNS                                               #
# ──────────────────────────────────────────────────────────────────────── #


class TestConsistencyAcrossRuns:
    """Run the layer three times on the same synthetic outputs.

    Counts must accumulate monotonically, rates must stay internally
    consistent, ``last_seen_at`` must advance, ``first_seen_at`` must not.
    """

    def _fixture(self, tmp_path: Path) -> Path:
        return _build_run_fixture(
            tmp_path / "run_same",
            ready_rows=[
                _row(domain="alpha.com", corrected_domain="alpha.com"),
                _row(domain="alpha.com", corrected_domain="alpha.com"),
                _row(domain="beta.com", corrected_domain="beta.com"),
            ],
            review_rows=[
                _row(
                    domain="beta.com", corrected_domain="beta.com",
                    has_mx_record="False", has_a_record="True",
                    final_output_reason="kept_review",
                ),
            ],
            invalid_rows=[
                _row(
                    domain="gamma.com", corrected_domain="gamma.com",
                    has_mx_record="False", has_a_record="False",
                    dns_error="The DNS operation timed out",
                    final_output_reason="removed_low_score",
                ),
            ],
        )

    def test_three_runs_accumulate_linearly(self, tmp_path: Path) -> None:
        run_dir = self._fixture(tmp_path)
        db_path = tmp_path / "cohist.sqlite"

        expected_per_run = {"alpha.com": 2, "beta.com": 2, "gamma.com": 1}

        for run_idx in range(1, 4):
            with DomainHistoryStore(db_path) as store:
                update_history_from_run(run_dir, store)
                for domain, per_run in expected_per_run.items():
                    rec = store.get(domain)
                    assert rec is not None, f"record missing for {domain}"
                    assert rec.total_seen_count == per_run * run_idx, (
                        f"{domain} seen={rec.total_seen_count} expected={per_run * run_idx} "
                        f"after run {run_idx}"
                    )

    def test_rates_remain_consistent_across_runs(self, tmp_path: Path) -> None:
        run_dir = self._fixture(tmp_path)
        db_path = tmp_path / "cohist.sqlite"

        previous_rates: dict[str, dict[str, float]] = {}
        for _ in range(3):
            with DomainHistoryStore(db_path) as store:
                update_history_from_run(run_dir, store)
                snapshots = {
                    d: {
                        "mx_rate": r.mx_rate,
                        "invalid_rate": r.invalid_rate,
                        "ready_rate": r.ready_rate,
                        "timeout_rate": r.timeout_rate,
                    }
                    for d in ("alpha.com", "beta.com", "gamma.com")
                    for r in [store.get(d)] if r is not None
                }
            if previous_rates:
                assert snapshots == previous_rates, (
                    "rates should be stable across runs on identical input"
                )
            previous_rates = snapshots

        # Sanity: the captured rates must themselves be correct.
        #   alpha.com: 2/2 ready, 2/2 mx, 0 invalid.
        assert previous_rates["alpha.com"] == pytest.approx(
            {"mx_rate": 1.0, "invalid_rate": 0.0, "ready_rate": 1.0, "timeout_rate": 0.0}
        )
        #   beta.com: 1 ready (mx) + 1 review (no-mx) out of 2.
        assert previous_rates["beta.com"]["mx_rate"] == pytest.approx(0.5)
        #   gamma.com: always timeout, always invalid.
        assert previous_rates["gamma.com"]["timeout_rate"] == pytest.approx(1.0)
        assert previous_rates["gamma.com"]["invalid_rate"] == pytest.approx(1.0)

    def test_first_seen_at_is_immutable_last_seen_at_advances(
        self, tmp_path: Path,
    ) -> None:
        run_dir = self._fixture(tmp_path)
        db_path = tmp_path / "ts.sqlite"

        t0 = datetime(2026, 4, 22, 10, 0, 0)
        t1 = t0 + timedelta(hours=1)
        t2 = t0 + timedelta(days=1)

        for t in (t0, t1, t2):
            with DomainHistoryStore(db_path) as store:
                store.bulk_update(build_observations_from_run(run_dir), now=t)

        with DomainHistoryStore(db_path) as store:
            alpha = store.get("alpha.com")
        assert alpha is not None
        assert alpha.first_seen_at == t0
        assert alpha.last_seen_at == t2


# ──────────────────────────────────────────────────────────────────────── #
# 2. DOMAIN LIFECYCLE                                                      #
# ──────────────────────────────────────────────────────────────────────── #


class TestDomainLifecycle:
    """New domains create rows; existing domains update them; PK is unique."""

    def test_new_domain_creates_single_row(self, tmp_path: Path) -> None:
        run_dir = _build_run_fixture(
            tmp_path / "run",
            ready_rows=[_row(domain="novel.com", corrected_domain="novel.com")],
        )
        with DomainHistoryStore(tmp_path / "h.sqlite") as store:
            before = store.count()
            update_history_from_run(run_dir, store)
            after = store.count()
        assert before == 0
        assert after == 1

    def test_existing_domain_updates_not_duplicates(self, tmp_path: Path) -> None:
        run_dir = _build_run_fixture(
            tmp_path / "run",
            ready_rows=[
                _row(domain="repeat.com", corrected_domain="repeat.com"),
                _row(domain="repeat.com", corrected_domain="repeat.com"),
            ],
            review_rows=[
                _row(
                    domain="repeat.com", corrected_domain="repeat.com",
                    final_output_reason="kept_review",
                ),
            ],
        )
        db_path = tmp_path / "h.sqlite"

        for _ in range(4):
            with DomainHistoryStore(db_path) as store:
                update_history_from_run(run_dir, store)

        with DomainHistoryStore(db_path) as store:
            assert store.count() == 1
            rec = store.get("repeat.com")
        assert rec is not None
        assert rec.total_seen_count == 12  # 3 obs × 4 runs
        assert rec.ready_count == 8
        assert rec.review_count == 4

    def test_one_row_per_domain_invariant_from_sqlite(self, tmp_path: Path) -> None:
        run_dir = _build_run_fixture(
            tmp_path / "run",
            ready_rows=[_row(domain=f"d{i}.com", corrected_domain=f"d{i}.com") for i in range(5)]
            + [_row(domain="d0.com", corrected_domain="d0.com")],  # collision
        )
        db_path = tmp_path / "h.sqlite"
        with DomainHistoryStore(db_path) as store:
            update_history_from_run(run_dir, store)
            update_history_from_run(run_dir, store)

        # Introspect the underlying table directly to catch any duplicate PKs.
        conn = sqlite3.connect(db_path)
        try:
            rows = conn.execute(
                "SELECT domain, COUNT(*) FROM domain_history GROUP BY domain"
            ).fetchall()
        finally:
            conn.close()

        assert all(count == 1 for _, count in rows), f"duplicate PKs found: {rows}"
        assert {domain for domain, _ in rows} == {f"d{i}.com" for i in range(5)}

    def test_domain_case_variants_collapse_to_single_row(self, tmp_path: Path) -> None:
        run_dir = _build_run_fixture(
            tmp_path / "run",
            ready_rows=[
                _row(domain="MiXeD.CoM", corrected_domain="MiXeD.CoM"),
                _row(domain="mixed.com", corrected_domain="mixed.com"),
                _row(domain="MIXED.COM", corrected_domain="MIXED.COM"),
            ],
        )
        with DomainHistoryStore(tmp_path / "h.sqlite") as store:
            update_history_from_run(run_dir, store)
            assert store.count() == 1
            rec = store.get("mixed.com")
        assert rec is not None
        assert rec.total_seen_count == 3


# ──────────────────────────────────────────────────────────────────────── #
# 3. EDGE-CASE CLASSIFICATION                                              #
# ──────────────────────────────────────────────────────────────────────── #


class TestEdgeCaseClassification:
    """Synthetic domains that stress each rule bucket."""

    def _make(
        self, domain: str, total: int, **counters: int,
    ) -> DomainHistoryRecord:
        now = datetime(2026, 4, 22)
        defaults = dict(
            mx_present_count=0, a_fallback_count=0, dns_failure_count=0,
            timeout_count=0, typo_corrected_count=0, review_count=0,
            invalid_count=0, ready_count=0, hard_fail_count=0,
        )
        defaults.update(counters)
        return DomainHistoryRecord(
            domain=domain, first_seen_at=now, last_seen_at=now,
            total_seen_count=total, **defaults,
        )

    def test_always_timeout_labelled_unstable(self) -> None:
        rec = self._make(
            "flaky.com", 50,
            timeout_count=50, dns_failure_count=50, invalid_count=50,
        )
        # Even though invalid_rate would suggest "risky", unstable wins.
        assert classify_domain(rec) == HistoricalLabel.UNSTABLE

    def test_always_invalid_labelled_risky(self) -> None:
        rec = self._make(
            "rejected.com", 50,
            mx_present_count=50, invalid_count=50,
        )
        assert classify_domain(rec) == HistoricalLabel.RISKY

    def test_always_mx_and_ready_labelled_reliable(self) -> None:
        rec = self._make(
            "trusted.com", 100,
            mx_present_count=100, ready_count=90, review_count=8, invalid_count=2,
        )
        assert classify_domain(rec) == HistoricalLabel.RELIABLE

    def test_mixed_inconsistent_signals_labelled_neutral(self) -> None:
        rec = self._make(
            "mixed.com", 100,
            mx_present_count=70,       # below reliable threshold
            ready_count=50,            # below reliable ready threshold
            review_count=30,
            invalid_count=20,          # below risky threshold
            timeout_count=5,           # below unstable threshold
        )
        assert classify_domain(rec) == HistoricalLabel.NEUTRAL

    def test_too_few_observations_insufficient_data(self) -> None:
        few = self._make(
            "new.com", DEFAULT_THRESHOLDS.min_observations - 1,
            mx_present_count=4, ready_count=4,
        )
        assert classify_domain(few) == HistoricalLabel.INSUFFICIENT_DATA

    def test_exactly_threshold_observations_graduates_from_insufficient(self) -> None:
        just_enough = self._make(
            "graduated.com", DEFAULT_THRESHOLDS.min_observations,
            mx_present_count=DEFAULT_THRESHOLDS.min_observations,
            ready_count=DEFAULT_THRESHOLDS.min_observations,
        )
        assert classify_domain(just_enough) != HistoricalLabel.INSUFFICIENT_DATA


# ──────────────────────────────────────────────────────────────────────── #
# 4. ISOLATION (CRITICAL): history disabled == V1 unchanged                #
# ──────────────────────────────────────────────────────────────────────── #


pytest_skipif_no_sample = pytest.mark.skipif(
    not SAMPLE_CSV.is_file(), reason=f"sample input missing at {SAMPLE_CSV}",
)


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


class TestIsolation:
    """When disabled, history layer must be completely inert."""

    @pytest_skipif_no_sample
    def test_v1_outputs_byte_equivalent_rows_with_history_disabled(
        self, tmp_path: Path,
    ) -> None:
        # Run A: history off.
        cfg_off = _make_tmp_config(tmp_path / "off_cfg_dir", history_enabled=False,
                                   db_path=tmp_path / "off_cfg_dir" / "ignored.sqlite")
        (tmp_path / "off_cfg_dir").mkdir(exist_ok=True)
        out_off = tmp_path / "out_off"
        res_off = run_cleaning_job(
            input_path=SAMPLE_CSV, output_root=out_off,
            config_path=cfg_off, job_id="iso_off",
        )
        # Run B: history on.
        (tmp_path / "on_cfg_dir").mkdir(exist_ok=True)
        cfg_on = _make_tmp_config(tmp_path / "on_cfg_dir", history_enabled=True,
                                  db_path=tmp_path / "on_cfg_dir" / "history.sqlite")
        out_on = tmp_path / "out_on"
        res_on = run_cleaning_job(
            input_path=SAMPLE_CSV, output_root=out_on,
            config_path=cfg_on, job_id="iso_on",
        )

        assert res_off.status == JobStatus.COMPLETED
        assert res_on.status == JobStatus.COMPLETED

        # Compare per-CSV row counts + the email column exactly.
        for csv_name in (
            "clean_high_confidence.csv",
            "review_medium_confidence.csv",
            "removed_invalid.csv",
        ):
            rows_off = _read_csv_rows(res_off.run_dir / csv_name)
            rows_on = _read_csv_rows(res_on.run_dir / csv_name)
            assert len(rows_off) == len(rows_on), (
                f"{csv_name}: row count differs with history on/off"
            )
            assert [r["email"] for r in rows_off] == [r["email"] for r in rows_on], (
                f"{csv_name}: email list differs with history on/off"
            )

        # Summary counts must be identical.
        assert res_off.summary is not None and res_on.summary is not None
        for field in ("total_input_rows", "total_valid", "total_review",
                      "total_invalid_or_bounce_risk"):
            assert getattr(res_off.summary, field) == getattr(res_on.summary, field), (
                f"summary.{field} differs with history on/off"
            )

    @pytest_skipif_no_sample
    def test_disabled_history_creates_no_sqlite(self, tmp_path: Path) -> None:
        (tmp_path / "cfg_dir").mkdir()
        db_path = tmp_path / "cfg_dir" / "history.sqlite"
        cfg = _make_tmp_config(tmp_path / "cfg_dir", history_enabled=False,
                               db_path=db_path)
        result = run_cleaning_job(
            input_path=SAMPLE_CSV, output_root=tmp_path / "out",
            config_path=cfg, job_id="iso_no_db",
        )
        assert result.status == JobStatus.COMPLETED
        assert not db_path.exists()
        summary_report = result.run_dir / "domain_history_summary.csv"
        assert not summary_report.exists()


# ──────────────────────────────────────────────────────────────────────── #
# 5. CONTROLLED FAILURE                                                    #
# ──────────────────────────────────────────────────────────────────────── #


class TestControlledFailure:
    """Any failure in the V2 layer must be swallowed; V1 stays intact."""

    def test_corrupt_sqlite_raises_from_store_but_is_caught_at_integration(
        self, tmp_path: Path,
    ) -> None:
        # Create a file with garbage bytes where SQLite expects a DB header.
        corrupt = tmp_path / "history.sqlite"
        corrupt.write_bytes(b"this is not a sqlite database, obviously" * 100)

        # The low-level store call must fail loudly.
        with pytest.raises(sqlite3.DatabaseError):
            with DomainHistoryStore(corrupt) as store:
                store.count()

        # But running the full pipeline with history pointed at this file
        # must still succeed and leave the V1 artifacts intact.
        if not SAMPLE_CSV.is_file():
            pytest.skip("sample input missing")
        (tmp_path / "cfg_dir").mkdir()
        cfg = _make_tmp_config(tmp_path / "cfg_dir", history_enabled=True,
                               db_path=corrupt)
        result = run_cleaning_job(
            input_path=SAMPLE_CSV, output_root=tmp_path / "out",
            config_path=cfg, job_id="corrupt_db",
        )
        assert result.status == JobStatus.COMPLETED
        assert (result.run_dir / "clean_high_confidence.csv").is_file()

    def test_invalid_sqlite_path_does_not_break_pipeline(
        self, tmp_path: Path,
    ) -> None:
        if not SAMPLE_CSV.is_file():
            pytest.skip("sample input missing")

        # Create a file that blocks mkdir() on the parent directory below it.
        blocker = tmp_path / "blocker.txt"
        blocker.write_text("I am a file, not a directory.", encoding="utf-8")
        bad_db = blocker / "nested" / "history.sqlite"  # parent is a regular file

        (tmp_path / "cfg_dir").mkdir()
        cfg = _make_tmp_config(tmp_path / "cfg_dir", history_enabled=True,
                               db_path=bad_db)
        result = run_cleaning_job(
            input_path=SAMPLE_CSV, output_root=tmp_path / "out",
            config_path=cfg, job_id="bad_path",
        )
        assert result.status == JobStatus.COMPLETED
        assert (result.run_dir / "clean_high_confidence.csv").is_file()

    def test_history_failure_preserves_v1_artifact_set(self, tmp_path: Path) -> None:
        if not SAMPLE_CSV.is_file():
            pytest.skip("sample input missing")

        corrupt = tmp_path / "history.sqlite"
        corrupt.write_bytes(b"\x00" * 2048)  # definitely not sqlite
        (tmp_path / "cfg_dir").mkdir()
        cfg = _make_tmp_config(tmp_path / "cfg_dir", history_enabled=True,
                               db_path=corrupt)
        result = run_cleaning_job(
            input_path=SAMPLE_CSV, output_root=tmp_path / "out",
            config_path=cfg, job_id="fail_preserve",
        )
        assert result.status == JobStatus.COMPLETED

        expected_v1 = {
            "clean_high_confidence.csv", "review_medium_confidence.csv",
            "removed_invalid.csv", "valid_emails.xlsx", "review_emails.xlsx",
            "invalid_or_bounce_risk.xlsx", "summary_report.xlsx",
            "processing_report.json", "processing_report.csv",
        }
        present = {p.name for p in result.run_dir.iterdir() if p.is_file()}
        missing = expected_v1 - present
        assert not missing, f"V1 artifacts missing after history failure: {missing}"


# ──────────────────────────────────────────────────────────────────────── #
# 6. PERFORMANCE                                                           #
# ──────────────────────────────────────────────────────────────────────── #


class TestPerformance:
    """Benchmark the V2 layer in isolation against a 100K-row fixture."""

    def _fabricate_large_run(self, run_dir: Path, total: int = 100_000) -> None:
        # 200 unique domains, rotated through the 100K rows so we exercise
        # both the grouping and the single-row-per-domain upsert path.
        ready: list[dict[str, str]] = []
        review: list[dict[str, str]] = []
        invalid: list[dict[str, str]] = []
        for i in range(total):
            domain = f"d{i % 200}.example"
            bucket = i % 10
            if bucket < 7:
                ready.append(_row(
                    domain=domain, corrected_domain=domain,
                    email=f"u{i}@{domain}",
                ))
            elif bucket < 9:
                review.append(_row(
                    domain=domain, corrected_domain=domain,
                    email=f"u{i}@{domain}",
                    has_mx_record="False", has_a_record="True",
                    final_output_reason="kept_review",
                ))
            else:
                invalid.append(_row(
                    domain=domain, corrected_domain=domain,
                    email=f"u{i}@{domain}",
                    has_mx_record="False", has_a_record="False",
                    dns_error="timeout",
                    final_output_reason="removed_low_score",
                ))
        _build_run_fixture(
            run_dir, ready_rows=ready, review_rows=review, invalid_rows=invalid,
        )

    def test_observation_build_is_under_threshold_for_100k_rows(
        self, tmp_path: Path,
    ) -> None:
        run_dir = tmp_path / "big_run"
        self._fabricate_large_run(run_dir, total=100_000)

        t0 = time.perf_counter()
        observations = build_observations_from_run(run_dir)
        elapsed = time.perf_counter() - t0

        assert len(observations) == 100_000
        assert elapsed < 5.0, f"observation build took {elapsed:.2f}s for 100K rows"

    def test_full_update_under_threshold_for_100k_rows(
        self, tmp_path: Path,
    ) -> None:
        run_dir = tmp_path / "big_run"
        self._fabricate_large_run(run_dir, total=100_000)
        db_path = tmp_path / "big.sqlite"

        t0 = time.perf_counter()
        with DomainHistoryStore(db_path) as store:
            result = update_history_from_run(run_dir, store)
        elapsed = time.perf_counter() - t0

        assert result.observations_processed == 100_000
        assert result.domains_updated == 200  # 200 unique domains
        assert elapsed < 10.0, (
            f"history update took {elapsed:.2f}s for 100K rows / 200 domains"
        )

    def test_single_transaction_per_bulk_update_no_lock_contention(
        self, tmp_path: Path,
    ) -> None:
        """Two back-to-back bulk updates should both succeed without locking."""
        run_dir = tmp_path / "mini_run"
        self._fabricate_large_run(run_dir, total=2_000)

        db_path = tmp_path / "lock.sqlite"
        with DomainHistoryStore(db_path) as store:
            update_history_from_run(run_dir, store)
            update_history_from_run(run_dir, store)
            update_history_from_run(run_dir, store)
            # Still queryable after three tightly-packed transactions.
            assert store.count() == 200
            sample = store.get("d0.example")
        assert sample is not None
        assert sample.total_seen_count == 30  # (10 rows per domain) × 3 runs


# ──────────────────────────────────────────────────────────────────────── #
# 7. REPORT SHAPE + ACCURACY                                               #
# ──────────────────────────────────────────────────────────────────────── #


class TestReportShape:
    """Validate domain_history_summary.csv shape, rates, labels, adjustment."""

    def _fixture(self, run_dir: Path) -> Path:
        return _build_run_fixture(
            run_dir,
            ready_rows=[_row(domain="rel.com", corrected_domain="rel.com") for _ in range(10)],
            review_rows=[],
            invalid_rows=[
                _row(
                    domain="risk.com", corrected_domain="risk.com",
                    has_mx_record="True", has_a_record="True",
                    final_output_reason="removed_low_score",
                ) for _ in range(10)
            ],
        )

    def test_header_matches_contract(self, tmp_path: Path) -> None:
        run_dir = self._fixture(tmp_path / "rpt")
        with DomainHistoryStore(tmp_path / "h.sqlite") as store:
            result = update_history_from_run(run_dir, store)
        assert result.report_path is not None

        with result.report_path.open(encoding="utf-8", newline="") as fh:
            header = next(csv.reader(fh))
        assert tuple(header) == _REPORT_HEADER

    def test_rates_in_report_match_store(self, tmp_path: Path) -> None:
        run_dir = self._fixture(tmp_path / "rpt")
        db_path = tmp_path / "h.sqlite"
        with DomainHistoryStore(db_path) as store:
            update_history_from_run(run_dir, store)
            rel = store.get("rel.com")
            risk = store.get("risk.com")

        with (run_dir / "domain_history_summary.csv").open(encoding="utf-8") as fh:
            rows = {r["domain"]: r for r in csv.DictReader(fh)}

        assert rel is not None and risk is not None
        assert float(rows["rel.com"]["mx_rate"]) == pytest.approx(round(rel.mx_rate, 3))
        assert float(rows["rel.com"]["ready_rate"]) == pytest.approx(
            round(rel.ready_rate, 3)
        )
        assert float(rows["risk.com"]["invalid_rate"]) == pytest.approx(
            round(risk.invalid_rate, 3)
        )

    def test_labels_match_classify_domain(self, tmp_path: Path) -> None:
        run_dir = self._fixture(tmp_path / "rpt")
        db_path = tmp_path / "h.sqlite"
        with DomainHistoryStore(db_path) as store:
            update_history_from_run(run_dir, store)
            records = {r.domain: r for r in store.iter_all()}

        with (run_dir / "domain_history_summary.csv").open(encoding="utf-8") as fh:
            report_rows = {r["domain"]: r for r in csv.DictReader(fh)}

        for domain, record in records.items():
            assert report_rows[domain]["readiness_label"] == classify_domain(record)

    def test_adjustment_column_is_present_and_bounded(self, tmp_path: Path) -> None:
        run_dir = self._fixture(tmp_path / "rpt")
        with DomainHistoryStore(tmp_path / "h.sqlite") as store:
            update_history_from_run(
                run_dir, store,
                max_positive_adjustment=3,
                max_negative_adjustment=5,
            )
        with (run_dir / "domain_history_summary.csv").open(encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))

        assert rows, "report must have at least one row"
        for row in rows:
            assert "confidence_adjustment" in row
            value = int(row["confidence_adjustment"])
            assert -5 <= value <= 3, (
                f"adjustment out of bounds for {row['domain']}: {value}"
            )

    def test_report_ordering_descending_by_seen_count(self, tmp_path: Path) -> None:
        run_dir = _build_run_fixture(
            tmp_path / "rpt",
            ready_rows=(
                [_row(domain="big.com", corrected_domain="big.com") for _ in range(10)]
                + [_row(domain="small.com", corrected_domain="small.com")]
                + [_row(domain="mid.com", corrected_domain="mid.com") for _ in range(5)]
            ),
        )
        with DomainHistoryStore(tmp_path / "h.sqlite") as store:
            update_history_from_run(run_dir, store)
        with (run_dir / "domain_history_summary.csv").open(encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        seen = [int(r["total_seen_count"]) for r in rows]
        assert seen == sorted(seen, reverse=True)


# ──────────────────────────────────────────────────────────────────────── #
# Silence helper                                                           #
# ──────────────────────────────────────────────────────────────────────── #


# Observations reference (keeps mypy quiet about unused imports when the
# file is edited without touching every class).
_ = DomainObservation, FinalDecision
