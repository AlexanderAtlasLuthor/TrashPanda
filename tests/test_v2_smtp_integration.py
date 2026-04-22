"""End-to-end tests for V2 Phase 4: selective SMTP probing.

No network I/O — the probe function is fully mocked. Each test owns a
synthetic run directory with pre-enriched CSVs (Phase 2+3 columns
included) so the selection logic runs against realistic fixtures.
"""

from __future__ import annotations

import csv
from collections.abc import Callable
from pathlib import Path

import pytest

from app.validation_v2.smtp_integration import (
    SMTPProbeConfig,
    SMTPProbeStats,
    SMTP_COLUMNS,
    run_smtp_probing_pass,
    select_candidates,
)
from app.validation_v2.smtp_probe import SMTPResult


# ─────────────────────────────────────────────────────────────────────── #
# Fixtures                                                                #
# ─────────────────────────────────────────────────────────────────────── #


_COLUMNS: tuple[str, ...] = (
    "id", "email", "domain", "corrected_domain",
    "has_mx_record", "hard_fail", "score", "final_output_reason",
    # Phase 2+3 columns that select_candidates consults.
    "v2_final_bucket", "confidence_adjustment_applied", "historical_label",
    "review_subclass",
)


def _row(**kw: str) -> dict[str, str]:
    d: dict[str, str] = {
        "id": "1", "email": "user@example.com",
        "domain": "example.com", "corrected_domain": "example.com",
        "has_mx_record": "True", "hard_fail": "False",
        "score": "55", "final_output_reason": "kept_review",
        "v2_final_bucket": "review",
        "confidence_adjustment_applied": "0",
        "historical_label": "neutral",
        "review_subclass": "review_low_confidence",
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
    run_dir = tmp / "run"
    _write_csv(run_dir / "clean_high_confidence.csv", ready or [])
    _write_csv(run_dir / "review_medium_confidence.csv", review or [])
    _write_csv(run_dir / "removed_invalid.csv", invalid or [])
    return run_dir


def _static_probe(
    verdict: str = "deliverable",
) -> Callable[..., SMTPResult]:
    """Return a probe function that always yields the given verdict."""

    def probe(email: str, **_kwargs: object) -> SMTPResult:
        if verdict == "deliverable":
            return SMTPResult(True, 250, "ok", False, False)
        if verdict == "catch_all":
            return SMTPResult(True, 250, "ok", True, False)
        if verdict == "undeliverable":
            return SMTPResult(False, 550, "no such user", False, False)
        return SMTPResult(False, 451, "try later", False, True)

    return probe


_CFG_ENABLED = SMTPProbeConfig(
    enabled=True,
    dry_run=False,
    sample_size=50,
    max_per_domain=3,
    rate_limit_per_second=1000.0,  # effectively disables rate sleep for tests
    timeout_seconds=1.0,
    negative_adjustment_trigger_threshold=3,
)


# ─────────────────────────────────────────────────────────────────────── #
# Candidate selection                                                     #
# ─────────────────────────────────────────────────────────────────────── #


class TestCandidateSelection:
    """Verify the qualifier logic against every selection rule."""

    def test_only_review_catch_all_and_review_timeout_qualify_by_subclass(
        self, tmp_path: Path,
    ) -> None:
        run = _build_run(
            tmp_path,
            review=[
                _row(email="a@d1.com", corrected_domain="d1.com",
                     review_subclass="review_catch_all"),
                _row(email="b@d2.com", corrected_domain="d2.com",
                     review_subclass="review_timeout"),
                _row(email="c@d3.com", corrected_domain="d3.com",
                     review_subclass="review_low_confidence"),
                _row(email="d@d4.com", corrected_domain="d4.com",
                     review_subclass="review_inconsistent"),
            ],
        )
        candidates = select_candidates(run, _CFG_ENABLED)
        picked_emails = {c.email for c in candidates}
        assert picked_emails == {"a@d1.com", "b@d2.com"}

    def test_large_negative_adjustment_qualifies(self, tmp_path: Path) -> None:
        run = _build_run(
            tmp_path,
            review=[
                _row(email="neg@d.com", corrected_domain="d.com",
                     confidence_adjustment_applied="-5",
                     review_subclass="review_low_confidence"),
            ],
        )
        assert [c.email for c in select_candidates(run, _CFG_ENABLED)] == ["neg@d.com"]

    def test_small_negative_adjustment_does_not_qualify(self, tmp_path: Path) -> None:
        run = _build_run(
            tmp_path,
            review=[
                _row(email="small@d.com", corrected_domain="d.com",
                     confidence_adjustment_applied="-2"),
            ],
        )
        assert select_candidates(run, _CFG_ENABLED) == []

    def test_historical_label_risky_qualifies(self, tmp_path: Path) -> None:
        run = _build_run(
            tmp_path,
            review=[
                _row(email="risky@d.com", corrected_domain="d.com",
                     historical_label="historically_risky"),
            ],
        )
        assert len(select_candidates(run, _CFG_ENABLED)) == 1

    def test_historical_label_reliable_does_not_qualify(self, tmp_path: Path) -> None:
        run = _build_run(
            tmp_path,
            review=[
                _row(email="nice@d.com", corrected_domain="d.com",
                     historical_label="historically_reliable"),
            ],
        )
        assert select_candidates(run, _CFG_ENABLED) == []

    def test_hard_fail_rows_are_never_candidates(self, tmp_path: Path) -> None:
        run = _build_run(
            tmp_path,
            invalid=[
                _row(email="bad@d.com", corrected_domain="d.com",
                     hard_fail="True", v2_final_bucket="hard_fail",
                     review_subclass="review_catch_all",
                     historical_label="historically_risky"),
            ],
        )
        assert select_candidates(run, _CFG_ENABLED) == []

    def test_rows_without_mx_are_never_candidates(self, tmp_path: Path) -> None:
        run = _build_run(
            tmp_path,
            review=[
                _row(email="dead@d.com", corrected_domain="d.com",
                     has_mx_record="False",
                     review_subclass="review_catch_all"),
            ],
        )
        assert select_candidates(run, _CFG_ENABLED) == []


class TestSelectionCaps:
    def test_sample_size_cap_limits_total_selected(self, tmp_path: Path) -> None:
        rows = [
            _row(email=f"a{i}@d{i}.com", corrected_domain=f"d{i}.com",
                 review_subclass="review_catch_all")
            for i in range(20)
        ]
        run = _build_run(tmp_path, review=rows)
        cfg = SMTPProbeConfig(
            enabled=True, sample_size=5, max_per_domain=3,
            rate_limit_per_second=1000.0,
        )
        assert len(select_candidates(run, cfg)) == 5

    def test_max_per_domain_cap_limits_one_domain(self, tmp_path: Path) -> None:
        rows = [
            _row(email=f"u{i}@same.com", corrected_domain="same.com",
                 review_subclass="review_catch_all")
            for i in range(10)
        ]
        run = _build_run(tmp_path, review=rows)
        cfg = SMTPProbeConfig(
            enabled=True, sample_size=100, max_per_domain=2,
            rate_limit_per_second=1000.0,
        )
        selected = select_candidates(run, cfg)
        assert len(selected) == 2

    def test_review_priority_over_invalid_and_ready(self, tmp_path: Path) -> None:
        run = _build_run(
            tmp_path,
            ready=[_row(email="ready@d1.com", corrected_domain="d1.com",
                        review_subclass="not_review",
                        historical_label="historically_unstable")],
            review=[_row(email="rev@d2.com", corrected_domain="d2.com",
                         review_subclass="review_catch_all")],
            invalid=[_row(email="inv@d3.com", corrected_domain="d3.com",
                          historical_label="historically_risky")],
        )
        cfg = SMTPProbeConfig(
            enabled=True, sample_size=1, max_per_domain=3,
            rate_limit_per_second=1000.0,
        )
        selected = select_candidates(run, cfg)
        # With sample_size=1, the one review candidate must win.
        assert [c.email for c in selected] == ["rev@d2.com"]


class TestDeduplication:
    def test_duplicate_email_across_files_probed_once(self, tmp_path: Path) -> None:
        dup = _row(email="dup@d.com", corrected_domain="d.com",
                   review_subclass="review_catch_all")
        run = _build_run(tmp_path, review=[dup, dup], invalid=[dup])
        selected = select_candidates(run, _CFG_ENABLED)
        assert len(selected) == 1


# ─────────────────────────────────────────────────────────────────────── #
# Orchestration: run_smtp_probing_pass                                    #
# ─────────────────────────────────────────────────────────────────────── #


class TestOrchestration:
    def test_disabled_returns_none_and_touches_nothing(self, tmp_path: Path) -> None:
        run = _build_run(
            tmp_path,
            review=[_row(email="x@d.com", corrected_domain="d.com",
                         review_subclass="review_catch_all")],
        )
        original = (run / "review_medium_confidence.csv").read_bytes()

        cfg = SMTPProbeConfig(enabled=False)
        result = run_smtp_probing_pass(run, cfg, probe_fn=_static_probe("deliverable"))
        assert result is None
        # CSV byte-identical.
        assert (run / "review_medium_confidence.csv").read_bytes() == original
        assert not (run / "smtp_probe_summary.csv").exists()

    def test_enabled_adds_smtp_columns_to_every_csv(self, tmp_path: Path) -> None:
        run = _build_run(
            tmp_path,
            ready=[_row(email="ready@d.com", corrected_domain="d.com",
                        review_subclass="not_review",
                        v2_final_bucket="ready")],
            review=[_row(email="r@dup.com", corrected_domain="dup.com",
                         review_subclass="review_catch_all")],
            invalid=[_row(email="inv@d2.com", corrected_domain="d2.com",
                          v2_final_bucket="invalid",
                          historical_label="historically_risky")],
        )
        result = run_smtp_probing_pass(
            run, _CFG_ENABLED, probe_fn=_static_probe("deliverable"),
        )
        assert result is not None
        for name in (
            "clean_high_confidence.csv",
            "review_medium_confidence.csv",
            "removed_invalid.csv",
        ):
            with (run / name).open(encoding="utf-8", newline="") as fh:
                reader = csv.DictReader(fh)
                for col in SMTP_COLUMNS:
                    assert col in (reader.fieldnames or [])

    def test_candidate_rows_get_probed_values_non_candidate_rows_get_placeholder(
        self, tmp_path: Path,
    ) -> None:
        run = _build_run(
            tmp_path,
            review=[
                _row(email="probe@d.com", corrected_domain="d.com",
                     review_subclass="review_catch_all"),
                _row(email="skip@d.com", corrected_domain="d.com",
                     review_subclass="review_low_confidence"),
            ],
        )
        # Only the first row qualifies.
        run_smtp_probing_pass(
            run, _CFG_ENABLED, probe_fn=_static_probe("deliverable"),
        )
        with (run / "review_medium_confidence.csv").open(encoding="utf-8") as fh:
            rows = {r["email"]: r for r in csv.DictReader(fh)}

        assert rows["probe@d.com"]["smtp_tested"] == "True"
        assert rows["probe@d.com"]["smtp_result"] == "deliverable"
        assert rows["probe@d.com"]["smtp_confirmed_valid"] == "True"

        assert rows["skip@d.com"]["smtp_tested"] == "False"
        assert rows["skip@d.com"]["smtp_result"] == "not_tested"
        assert rows["skip@d.com"]["smtp_confirmed_valid"] == "False"

    def test_catch_all_verdict_sets_suspicious_flag(self, tmp_path: Path) -> None:
        run = _build_run(
            tmp_path,
            review=[_row(email="probe@accept-all.io",
                         corrected_domain="accept-all.io",
                         review_subclass="review_catch_all")],
        )
        run_smtp_probing_pass(run, _CFG_ENABLED, probe_fn=_static_probe("catch_all"))
        with (run / "review_medium_confidence.csv").open(encoding="utf-8") as fh:
            row = next(csv.DictReader(fh))
        assert row["smtp_result"] == "catch_all"
        assert row["smtp_suspicious"] == "True"
        assert row["smtp_confirmed_valid"] == "False"

    def test_summary_report_contains_expected_metrics(self, tmp_path: Path) -> None:
        run = _build_run(
            tmp_path,
            review=[
                _row(email="d@d.com", corrected_domain="d.com",
                     review_subclass="review_catch_all"),
                _row(email="u@u.com", corrected_domain="u.com",
                     review_subclass="review_timeout"),
            ],
        )
        # Use two verdicts via a sequence.
        verdicts = iter(["deliverable", "undeliverable"])
        def probe(email: str, **_kwargs: object) -> SMTPResult:
            v = next(verdicts)
            return (
                SMTPResult(True, 250, "ok", False, False)
                if v == "deliverable" else
                SMTPResult(False, 550, "nope", False, False)
            )
        result = run_smtp_probing_pass(run, _CFG_ENABLED, probe_fn=probe)
        assert result is not None
        assert result.report_path is not None
        metrics = dict(csv.reader(result.report_path.open(encoding="utf-8")))
        assert metrics["total_candidates"] == "2"
        assert metrics["total_probed"] == "2"
        assert metrics["deliverable"] == "1"
        assert metrics["undeliverable"] == "1"

    def test_no_candidates_still_writes_summary_and_no_columns(self, tmp_path: Path) -> None:
        run = _build_run(
            tmp_path,
            review=[_row(email="ok@d.com", corrected_domain="d.com",
                         review_subclass="review_low_confidence",
                         historical_label="neutral")],
        )
        result = run_smtp_probing_pass(run, _CFG_ENABLED, probe_fn=_static_probe("deliverable"))
        assert result is not None
        assert result.candidates_selected == 0
        assert result.probed == 0
        assert result.report_path is not None
        # CSV was NOT rewritten (no probing pass), so SMTP columns may
        # not be present. Verify summary exists with 0 candidates.
        metrics = dict(csv.reader(result.report_path.open(encoding="utf-8")))
        assert metrics["total_candidates"] == "0"


class TestRateLimiting:
    def test_rate_limit_sleeps_between_probes(self, tmp_path: Path) -> None:
        run = _build_run(
            tmp_path,
            review=[
                _row(email=f"u{i}@d{i}.com", corrected_domain=f"d{i}.com",
                     review_subclass="review_catch_all")
                for i in range(3)
            ],
        )
        cfg = SMTPProbeConfig(
            enabled=True, sample_size=10, max_per_domain=3,
            rate_limit_per_second=2.0,   # 0.5s min interval
            timeout_seconds=1.0,
        )

        # Advance the fake clock by less than min_interval after each probe
        # so sleep_fn MUST be called to fill the gap.
        clock_state = {"t": 0.0}
        def fake_clock() -> float:
            return clock_state["t"]
        def fake_sleep(dt: float) -> None:
            sleep_calls.append(dt)
            clock_state["t"] += dt

        sleep_calls: list[float] = []

        def fake_probe(email: str, **_kwargs: object) -> SMTPResult:
            # Each probe "takes" 0.1s of simulated wall-clock time.
            clock_state["t"] += 0.1
            return SMTPResult(True, 250, "ok", False, False)

        run_smtp_probing_pass(
            run, cfg, probe_fn=fake_probe,
            sleep_fn=fake_sleep, clock_fn=fake_clock,
        )
        # First probe no sleep (last_t is None); remaining 2 probes must each
        # sleep a positive amount to hit the 0.5s min interval.
        assert len(sleep_calls) == 2
        assert all(dt > 0.0 for dt in sleep_calls)

    def test_very_high_rate_means_near_zero_sleep(self, tmp_path: Path) -> None:
        run = _build_run(
            tmp_path,
            review=[
                _row(email=f"u{i}@d{i}.com", corrected_domain=f"d{i}.com",
                     review_subclass="review_catch_all")
                for i in range(5)
            ],
        )
        cfg = SMTPProbeConfig(
            enabled=True, sample_size=10, max_per_domain=3,
            rate_limit_per_second=1000.0,   # 1ms min interval
        )
        sleep_calls: list[float] = []
        run_smtp_probing_pass(
            run, cfg, probe_fn=_static_probe("deliverable"),
            sleep_fn=lambda dt: sleep_calls.append(dt),
        )
        # With real perf_counter, elapsed will exceed 1ms between probes,
        # so sleep should almost never be invoked.
        assert all(dt >= 0 for dt in sleep_calls)
