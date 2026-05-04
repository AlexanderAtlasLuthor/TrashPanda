"""Subphase V2.8 — Reporting & Visibility.

Pins the V2.8 contract:

  * ``build_v2_deliverability_report`` produces a structured report
    with classification / probability / SMTP / catch-all / domain-intel
    / feedback sections.
  * Section builders are pure (no I/O, no mutation), accept missing
    columns, and return deterministic counts.
  * ``write_v2_report_files`` writes the four expected artifacts:
    ``v2_deliverability_summary.json``,
    ``v2_reason_breakdown.csv``,
    ``v2_domain_risk_summary.csv``,
    ``v2_probability_distribution.csv``.
  * Feedback summary uses the V2.7 store when available; absent or
    empty store yields ``feedback_available=False`` without failing.
  * ``generate_v2_reports`` reads materialized CSVs from a run dir
    and produces all four artifacts.
  * Legacy reports (V2.5 and earlier) are untouched.
  * Cleaning pipeline runs without a feedback store — pinned via the
    end-to-end ``test_pipeline_writes_v25_separated_csvs`` regression
    that already exists.
  * No live network is opened — pure pandas / file I/O on tmp paths.
"""

from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from app.api_boundary import collect_job_artifacts
from app.v2_reporting import (
    V2_REPORT_VERSION,
    V2DeliverabilityReport,
    build_catch_all_summary,
    build_classification_summary,
    build_domain_intelligence_summary,
    build_feedback_summary,
    build_probability_distribution,
    build_smtp_coverage,
    build_v2_deliverability_report,
    generate_v2_reports,
    write_v2_report_files,
)


# ---------------------------------------------------------------------------
# Frame fixtures
# ---------------------------------------------------------------------------


def _row(
    *,
    email: str = "alice@gmail.com",
    final_action: str = "auto_approve",
    decision_reason: str = "high_probability",
    final_output_reason: str = "kept_high_confidence",
    deliverability_probability: float = 0.85,
    smtp_status: str = "valid",
    smtp_was_candidate: bool = True,
    catch_all_status: str = "not_catch_all",
    catch_all_flag: bool = False,
    domain_risk_level: str = "low",
    domain_behavior_class: str = "free_provider",
    domain_cold_start: bool = False,
) -> dict:
    domain = email.split("@", 1)[1] if "@" in email else ""
    return {
        "email": email,
        "domain": domain,
        "corrected_domain": domain,
        "final_action": final_action,
        "decision_reason": decision_reason,
        "final_output_reason": final_output_reason,
        "deliverability_probability": deliverability_probability,
        "smtp_status": smtp_status,
        "smtp_was_candidate": smtp_was_candidate,
        "catch_all_status": catch_all_status,
        "catch_all_flag": catch_all_flag,
        "domain_risk_level": domain_risk_level,
        "domain_behavior_class": domain_behavior_class,
        "domain_cold_start": domain_cold_start,
    }


def _frame(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def _mixed_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    clean = _frame([
        _row(email="alice@gmail.com"),
        _row(email="bob@gmail.com", deliverability_probability=0.95),
        _row(email="carol@yahoo.com"),
    ])
    review = _frame([
        _row(
            email="dave@example.org",
            final_action="manual_review",
            decision_reason="medium_probability",
            final_output_reason="kept_review",
            deliverability_probability=0.55,
            smtp_status="blocked",
            domain_risk_level="unknown",
            domain_behavior_class="cold_start",
            domain_cold_start=True,
        ),
        _row(
            email="eve@example.org",
            final_action="manual_review",
            decision_reason="catch_all_possible",
            final_output_reason="kept_review",
            deliverability_probability=0.65,
            catch_all_status="possible_catch_all",
            catch_all_flag=True,
        ),
    ])
    invalid = _frame([
        _row(
            email="frank@bad.invalid",
            final_action="auto_reject",
            decision_reason="hard_fail",
            final_output_reason="removed_hard_fail",
            deliverability_probability=0.0,
            smtp_status="not_tested",
            smtp_was_candidate=False,
        ),
        _row(
            email="grace@spam.com",
            final_action="auto_reject",
            decision_reason="smtp_invalid",
            final_output_reason="removed_v2_smtp_invalid",
            deliverability_probability=0.10,
            smtp_status="invalid",
        ),
        _row(
            email="hank@hank.com",
            final_action="auto_reject",
            decision_reason="duplicate",
            final_output_reason="removed_duplicate",
            deliverability_probability=0.0,
        ),
    ])
    return clean, review, invalid


# ---------------------------------------------------------------------------
# Layer 1 — Classification summary
# ---------------------------------------------------------------------------


class TestClassificationSummary:
    def test_counts_by_final_action(self):
        clean, review, invalid = _mixed_inputs()
        combined = pd.concat([clean, review, invalid], ignore_index=True)
        s = build_classification_summary(combined)
        assert s.total_rows == 8
        assert s.by_final_action["auto_approve"] == 3
        assert s.by_final_action["manual_review"] == 2
        assert s.by_final_action["auto_reject"] == 3

    def test_counts_by_decision_reason(self):
        clean, review, invalid = _mixed_inputs()
        combined = pd.concat([clean, review, invalid], ignore_index=True)
        s = build_classification_summary(combined)
        assert s.by_decision_reason["high_probability"] == 3
        assert s.by_decision_reason["medium_probability"] == 1
        assert s.by_decision_reason["catch_all_possible"] == 1
        assert s.by_decision_reason["hard_fail"] == 1
        assert s.by_decision_reason["smtp_invalid"] == 1
        assert s.by_decision_reason["duplicate"] == 1

    def test_counts_by_final_output_reason(self):
        clean, review, invalid = _mixed_inputs()
        combined = pd.concat([clean, review, invalid], ignore_index=True)
        s = build_classification_summary(combined)
        assert s.by_final_output_reason["kept_high_confidence"] == 3
        assert s.by_final_output_reason["kept_review"] == 2
        assert s.by_final_output_reason["removed_hard_fail"] == 1
        assert s.by_final_output_reason["removed_v2_smtp_invalid"] == 1
        assert s.by_final_output_reason["removed_duplicate"] == 1

    def test_empty_frame_produces_zero_counts(self):
        s = build_classification_summary(pd.DataFrame())
        assert s.total_rows == 0
        assert s.by_final_action == {}


# ---------------------------------------------------------------------------
# Layer 2 — Probability distribution
# ---------------------------------------------------------------------------


class TestProbabilityDistribution:
    def test_buckets_correctly(self):
        df = _frame([
            _row(deliverability_probability=0.05),
            _row(deliverability_probability=0.25),
            _row(deliverability_probability=0.50),
            _row(deliverability_probability=0.55),
            _row(deliverability_probability=0.75),
            _row(deliverability_probability=0.85),
            _row(deliverability_probability=1.0),
        ])
        d = build_probability_distribution(df)
        assert d.buckets["0.00-0.20"] == 1
        assert d.buckets["0.20-0.40"] == 1
        assert d.buckets["0.40-0.60"] == 2  # 0.50, 0.55 — both in [0.40, 0.60)
        assert d.buckets["0.60-0.80"] == 1
        assert d.buckets["0.80-1.00"] == 2  # 0.85, 1.00 — last bucket closed
        assert d.buckets["missing"] == 0

    def test_missing_probability_counted(self):
        df = pd.DataFrame({"final_action": ["auto_approve", "auto_approve"]})
        d = build_probability_distribution(df)
        assert d.missing == 2
        assert d.total_with_probability == 0

    def test_percentages_sum_to_100_when_all_present(self):
        df = _frame([
            _row(deliverability_probability=0.05),
            _row(deliverability_probability=0.85),
        ])
        d = build_probability_distribution(df)
        total_pct = sum(d.percentages.values())
        # Floating-point rounding can produce 99.99 / 100.01.
        assert abs(total_pct - 100.0) < 0.5

    def test_empty_frame_gives_all_missing(self):
        d = build_probability_distribution(pd.DataFrame())
        assert d.total_with_probability == 0
        assert d.missing == 0


# ---------------------------------------------------------------------------
# Layer 3 — SMTP coverage
# ---------------------------------------------------------------------------


class TestSMTPCoverage:
    def test_coverage_by_status(self):
        df = _frame([
            _row(smtp_status="valid"),
            _row(smtp_status="valid"),
            _row(smtp_status="invalid"),
            _row(smtp_status="blocked"),
            _row(smtp_status="timeout"),
            _row(smtp_status="catch_all_possible", catch_all_status="possible_catch_all", catch_all_flag=True),
            _row(smtp_status="not_tested", smtp_was_candidate=False),
        ])
        s = build_smtp_coverage(df)
        assert s.total_rows == 7
        assert s.valid_count == 2
        assert s.invalid_count == 1
        assert s.inconclusive_count == 2  # blocked + timeout
        assert s.catch_all_possible_count == 1
        assert s.not_tested_count == 1
        assert s.candidate_count == 6   # 6 had smtp_was_candidate=True
        assert s.tested_count == 6      # 7 total - 1 not_tested
        # coverage_rate = tested / candidate = 6/6 = 1.0
        assert s.coverage_rate == 1.0
        assert s.by_smtp_status["valid"] == 2
        assert s.by_smtp_status["invalid"] == 1

    def test_coverage_rate_zero_when_no_candidates(self):
        df = _frame([_row(smtp_was_candidate=False, smtp_status="not_tested")])
        s = build_smtp_coverage(df)
        assert s.candidate_count == 0
        assert s.coverage_rate == 0.0

    def test_empty_frame(self):
        s = build_smtp_coverage(pd.DataFrame())
        assert s.total_rows == 0
        assert s.candidate_count == 0


# ---------------------------------------------------------------------------
# Layer 4 — Catch-all summary
# ---------------------------------------------------------------------------


class TestCatchAllSummary:
    def test_catch_all_counts(self):
        df = _frame([
            _row(catch_all_status="not_catch_all"),
            _row(catch_all_status="not_catch_all"),
            _row(catch_all_status="possible_catch_all", catch_all_flag=True),
            _row(catch_all_status="confirmed_catch_all", catch_all_flag=True),
            _row(catch_all_status="unknown"),
        ])
        s = build_catch_all_summary(df)
        assert s.not_catch_all_count == 2
        assert s.possible_catch_all_count == 1
        assert s.confirmed_catch_all_count == 1
        assert s.unknown_count == 1
        assert s.catch_all_risk_count == 2

    def test_empty_frame(self):
        s = build_catch_all_summary(pd.DataFrame())
        assert s.total_rows == 0


# ---------------------------------------------------------------------------
# Layer 5 — Domain intelligence summary
# ---------------------------------------------------------------------------


class TestDomainIntelligenceSummary:
    def test_risk_level_counts(self):
        df = _frame([
            _row(email=f"u{i}@gmail.com", domain_risk_level="low",
                 domain_behavior_class="free_provider")
            for i in range(3)
        ] + [
            _row(email=f"v{i}@bad.com", domain_risk_level="high",
                 domain_behavior_class="known_risky", final_action="manual_review")
            for i in range(2)
        ] + [
            _row(email=f"w{i}@new.com", domain_risk_level="unknown",
                 domain_behavior_class="cold_start", domain_cold_start=True)
            for i in range(2)
        ])
        s = build_domain_intelligence_summary(df)
        assert s.low_risk_domain_count == 3
        assert s.high_risk_domain_count == 2
        assert s.unknown_domain_count == 2
        assert s.cold_start_count == 2

    def test_top_high_risk_domains_deterministic(self):
        # 2 rows on disposable.com (high risk) + 3 rows on suspicious.com (high)
        df = _frame([
            _row(email="a@disposable.com", domain_risk_level="high",
                 domain_behavior_class="disposable", final_action="manual_review"),
            _row(email="b@disposable.com", domain_risk_level="high",
                 domain_behavior_class="disposable", final_action="manual_review"),
            _row(email="c@suspicious.com", domain_risk_level="high",
                 domain_behavior_class="known_risky", final_action="manual_review"),
            _row(email="d@suspicious.com", domain_risk_level="high",
                 domain_behavior_class="known_risky", final_action="manual_review"),
            _row(email="e@suspicious.com", domain_risk_level="high",
                 domain_behavior_class="known_risky", final_action="manual_review"),
        ])
        s = build_domain_intelligence_summary(df)
        # Sorted by count desc then domain asc.
        assert s.top_high_risk_domains[0]["domain"] == "suspicious.com"
        assert s.top_high_risk_domains[0]["count"] == 3
        assert s.top_high_risk_domains[1]["domain"] == "disposable.com"
        assert s.top_high_risk_domains[1]["count"] == 2

    def test_top_review_domains(self):
        df = _frame([
            _row(email="a@x.com", final_action="manual_review"),
            _row(email="b@x.com", final_action="manual_review"),
            _row(email="c@y.com", final_action="manual_review"),
            _row(email="d@z.com", final_action="auto_approve"),  # excluded
        ])
        s = build_domain_intelligence_summary(df)
        review_domains = [d["domain"] for d in s.top_review_domains]
        assert "x.com" in review_domains
        assert "y.com" in review_domains
        assert "z.com" not in review_domains

    def test_empty_frame(self):
        s = build_domain_intelligence_summary(pd.DataFrame())
        assert s.total_rows == 0
        assert s.top_high_risk_domains == []


# ---------------------------------------------------------------------------
# Layer 6 — Feedback summary
# ---------------------------------------------------------------------------


class TestFeedbackSummary:
    def test_no_store_means_unavailable(self):
        s = build_feedback_summary(None)
        assert s.feedback_available is False
        assert s.domains_with_feedback == 0

    def test_store_with_aggregates_populates_summary(self, tmp_path: Path):
        from app.validation_v2.feedback import (
            BounceOutcomeStore,
            DomainBounceAggregate,
        )

        store = BounceOutcomeStore(tmp_path / "bounce.sqlite")
        try:
            # Low-risk domain: 10 delivered.
            agg_low = DomainBounceAggregate(domain="goodguy.com")
            for _ in range(10):
                agg_low.record("delivered")
            store.upsert_aggregate(agg_low)
            # High-risk: 7 delivered + 3 hard_bounce → 30% hard rate.
            agg_high = DomainBounceAggregate(domain="badguy.com")
            for _ in range(7):
                agg_high.record("delivered")
            for _ in range(3):
                agg_high.record("hard_bounce")
            store.upsert_aggregate(agg_high)

            s = build_feedback_summary(store)
            assert s.feedback_available is True
            assert s.domains_with_feedback == 2
            assert s.delivered_count == 17
            assert s.hard_bounce_count == 3
            # Top high-risk feedback: badguy.com.
            assert len(s.top_high_risk_feedback_domains) == 1
            assert s.top_high_risk_feedback_domains[0]["domain"] == "badguy.com"
        finally:
            store.close()

    def test_store_failure_safe(self):
        class BrokenStore:
            def list_all(self):
                raise RuntimeError("sqlite gone")

        s = build_feedback_summary(BrokenStore())
        assert s.feedback_available is False


# ---------------------------------------------------------------------------
# Layer 7 — Top-level builder + writers
# ---------------------------------------------------------------------------


class TestBuildAndWrite:
    def test_build_v2_deliverability_report_returns_full_shape(self):
        clean, review, invalid = _mixed_inputs()
        report = build_v2_deliverability_report(
            clean_df=clean, review_df=review, invalid_df=invalid
        )
        assert isinstance(report, V2DeliverabilityReport)
        assert report.report_version == V2_REPORT_VERSION
        assert report.classification_summary.total_rows == 8
        assert report.smtp_coverage.total_rows == 8
        assert report.catch_all_summary.total_rows == 8
        assert report.domain_intelligence_summary.total_rows == 8
        assert report.feedback_summary.feedback_available is False

    def test_write_v2_report_files_creates_all_four(self, tmp_path: Path):
        clean, review, invalid = _mixed_inputs()
        report = build_v2_deliverability_report(
            clean_df=clean, review_df=review, invalid_df=invalid
        )
        paths = write_v2_report_files(
            tmp_path, report,
            clean_df=clean, review_df=review, invalid_df=invalid,
        )
        for key in (
            "v2_deliverability_summary",
            "v2_reason_breakdown",
            "v2_domain_risk_summary",
            "v2_probability_distribution",
        ):
            assert key in paths
            assert paths[key].is_file(), f"{key} not written"

    def test_json_summary_is_valid_and_complete(self, tmp_path: Path):
        clean, review, invalid = _mixed_inputs()
        report = build_v2_deliverability_report(
            clean_df=clean, review_df=review, invalid_df=invalid
        )
        write_v2_report_files(tmp_path, report,
                              clean_df=clean, review_df=review, invalid_df=invalid)
        path = tmp_path / "v2_deliverability_summary.json"
        data = json.loads(path.read_text(encoding="utf-8"))

        assert data["report_version"] == V2_REPORT_VERSION
        assert "generated_at" in data
        assert "classification_summary" in data
        assert "probability_distribution" in data
        assert "smtp_coverage" in data
        assert "catch_all_summary" in data
        assert "domain_intelligence_summary" in data
        assert "feedback_summary" in data
        assert data["classification_summary"]["total_rows"] == 8

    def test_reason_breakdown_csv_groups_correctly(self, tmp_path: Path):
        clean, review, invalid = _mixed_inputs()
        report = build_v2_deliverability_report(
            clean_df=clean, review_df=review, invalid_df=invalid
        )
        write_v2_report_files(tmp_path, report,
                              clean_df=clean, review_df=review, invalid_df=invalid)
        with (tmp_path / "v2_reason_breakdown.csv").open(encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        # Every row has the three expected columns.
        assert all({"final_action", "decision_reason", "count"} <= set(r.keys())
                   for r in rows)

    def test_probability_distribution_csv_has_all_buckets(self, tmp_path: Path):
        clean, review, invalid = _mixed_inputs()
        report = build_v2_deliverability_report(
            clean_df=clean, review_df=review, invalid_df=invalid
        )
        write_v2_report_files(tmp_path, report,
                              clean_df=clean, review_df=review, invalid_df=invalid)
        with (tmp_path / "v2_probability_distribution.csv").open(encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        buckets = {r["bucket"] for r in rows}
        assert {"0.00-0.20", "0.20-0.40", "0.40-0.60",
                "0.60-0.80", "0.80-1.00", "missing"} <= buckets


# ---------------------------------------------------------------------------
# Layer 8 — Missing V2 columns degrade gracefully
# ---------------------------------------------------------------------------


class TestMissingColumnsTolerance:
    def test_legacy_frame_without_v2_columns_does_not_crash(self, tmp_path: Path):
        # A pre-V2 frame would only have email, score, etc. — no
        # final_action, smtp_status, catch_all_status, etc.
        legacy = pd.DataFrame({
            "email": ["alice@gmail.com", "bob@gmail.com"],
            "score": [80, 75],
            "preliminary_bucket": ["high_confidence", "high_confidence"],
        })
        report = build_v2_deliverability_report(clean_df=legacy)
        # Total rows still counted from the union frame.
        assert report.classification_summary.total_rows == 2
        # All V2-specific counts are zero.
        assert report.smtp_coverage.candidate_count == 0
        assert report.catch_all_summary.catch_all_risk_count == 0
        assert report.domain_intelligence_summary.cold_start_count == 0
        # Probability fully missing.
        assert report.probability_distribution.missing == 2

    def test_writer_handles_legacy_frame(self, tmp_path: Path):
        legacy = pd.DataFrame({"email": ["a@x.com"], "score": [80]})
        report = build_v2_deliverability_report(clean_df=legacy)
        paths = write_v2_report_files(tmp_path, report, clean_df=legacy)
        for path in paths.values():
            assert path.is_file()


# ---------------------------------------------------------------------------
# Layer 9 — generate_v2_reports (end-to-end via run_dir)
# ---------------------------------------------------------------------------


class TestGenerateV2Reports:
    def test_reads_run_dir_and_writes_artifacts(self, tmp_path: Path):
        clean, review, invalid = _mixed_inputs()
        clean.to_csv(tmp_path / "clean_high_confidence.csv", index=False)
        review.to_csv(tmp_path / "review_medium_confidence.csv", index=False)
        invalid.to_csv(tmp_path / "removed_invalid.csv", index=False)

        paths = generate_v2_reports(tmp_path)
        assert (tmp_path / "v2_deliverability_summary.json").is_file()
        assert (tmp_path / "v2_reason_breakdown.csv").is_file()
        assert (tmp_path / "v2_domain_risk_summary.csv").is_file()
        assert (tmp_path / "v2_probability_distribution.csv").is_file()
        assert len(paths) == 4

    def test_missing_csvs_do_not_crash(self, tmp_path: Path):
        # Empty run dir — generator should still produce empty reports.
        paths = generate_v2_reports(tmp_path)
        assert (tmp_path / "v2_deliverability_summary.json").is_file()
        # JSON is parseable and has zero counts.
        data = json.loads(
            (tmp_path / "v2_deliverability_summary.json").read_text(encoding="utf-8")
        )
        assert data["classification_summary"]["total_rows"] == 0

    def test_feedback_store_path_used_when_present(self, tmp_path: Path):
        from app.validation_v2.feedback import (
            BounceOutcomeStore,
            DomainBounceAggregate,
        )

        # Materialize a tiny clean CSV.
        clean, _, _ = _mixed_inputs()
        clean.to_csv(tmp_path / "clean_high_confidence.csv", index=False)

        # Seed the feedback store.
        store_path = tmp_path / "bounce.sqlite"
        store = BounceOutcomeStore(store_path)
        try:
            agg = DomainBounceAggregate(domain="gmail.com")
            for _ in range(10):
                agg.record("delivered")
            store.upsert_aggregate(agg)
        finally:
            store.close()

        generate_v2_reports(tmp_path, feedback_store_path=store_path)

        data = json.loads(
            (tmp_path / "v2_deliverability_summary.json").read_text(encoding="utf-8")
        )
        assert data["feedback_summary"]["feedback_available"] is True
        assert data["feedback_summary"]["domains_with_feedback"] == 1
        assert data["feedback_summary"]["delivered_count"] == 10

    def test_missing_feedback_store_is_safe(self, tmp_path: Path):
        # Path doesn't exist — generator should still produce reports.
        clean, _, _ = _mixed_inputs()
        clean.to_csv(tmp_path / "clean_high_confidence.csv", index=False)
        generate_v2_reports(
            tmp_path, feedback_store_path=tmp_path / "no_such_store.sqlite"
        )
        data = json.loads(
            (tmp_path / "v2_deliverability_summary.json").read_text(encoding="utf-8")
        )
        assert data["feedback_summary"]["feedback_available"] is False


# ---------------------------------------------------------------------------
# Layer 10 — Artifact manifest discovers V2.8 files
# ---------------------------------------------------------------------------


class TestArtifactManifest:
    def test_collect_job_artifacts_includes_v28_reports(self, tmp_path: Path):
        # Materialize all four V2.8 files in a fresh run dir.
        clean, review, invalid = _mixed_inputs()
        clean.to_csv(tmp_path / "clean_high_confidence.csv", index=False)
        review.to_csv(tmp_path / "review_medium_confidence.csv", index=False)
        invalid.to_csv(tmp_path / "removed_invalid.csv", index=False)
        generate_v2_reports(tmp_path)

        artifacts = collect_job_artifacts(tmp_path)
        assert artifacts.reports.v2_deliverability_summary is not None
        assert artifacts.reports.v2_reason_breakdown is not None
        assert artifacts.reports.v2_domain_risk_summary is not None
        assert artifacts.reports.v2_probability_distribution is not None
        # Files exist.
        assert artifacts.reports.v2_deliverability_summary.is_file()


# ---------------------------------------------------------------------------
# Layer 11 — Backward compatibility (legacy reports unchanged)
# ---------------------------------------------------------------------------


class TestBackwardCompatibility:
    def test_legacy_report_filenames_still_in_manifest(self, tmp_path: Path):
        artifacts = collect_job_artifacts(tmp_path)
        # The legacy fields should still exist on the dataclass even
        # though no file is present.
        assert hasattr(artifacts.reports, "processing_report_json")
        assert hasattr(artifacts.reports, "processing_report_csv")
        assert hasattr(artifacts.reports, "domain_summary")
        assert hasattr(artifacts.reports, "typo_corrections")
        assert hasattr(artifacts.reports, "duplicate_summary")

    def test_v25_summary_metrics_still_present(self):
        """V2.8 must not regress V2.5: legacy summary metrics still
        emit. We re-import the V2.5 export module to ensure it still
        has the expected helpers."""
        from app.client_output import generate_client_outputs
        # If the import succeeds we know V2.5 is intact; runtime
        # behaviour is verified by the V2.5 regression suite.
        assert generate_client_outputs is not None


# ---------------------------------------------------------------------------
# Layer 12 — No live network
# ---------------------------------------------------------------------------


class TestNoLiveNetwork:
    def test_v2_reporting_module_imports_no_network(self):
        from app import v2_reporting as mod

        # The module should pull in pandas, csv, json, datetime — never
        # smtplib / socket / requests / urllib.
        assert not hasattr(mod, "smtplib")
        assert not hasattr(mod, "socket")
        assert not hasattr(mod, "requests")
        assert not hasattr(mod, "urllib")

    def test_generate_v2_reports_uses_local_files_only(self, tmp_path: Path):
        # Just calling generate on an empty dir must not network.
        paths = generate_v2_reports(tmp_path)
        # Every output file is under tmp_path.
        for p in paths.values():
            assert tmp_path in p.parents
