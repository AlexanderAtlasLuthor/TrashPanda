"""V2.10.13 — customer_bundle tests."""

from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd

from app.customer_bundle import (
    CLEAN_DELIVERABLE_CSV,
    CUSTOMER_BUNDLE_DIRNAME,
    CUSTOMER_README,
    HIGH_RISK_REMOVED_CSV,
    REVIEW_PROVIDER_LIMITED_CSV,
    emit_customer_bundle,
)
from app.pilot_send.evidence import (
    CSV_COLUMNS,
    SMTP_EVIDENCE_REPORT_FILENAME,
)


def _write_xlsx(path: Path, rows: list[dict]) -> None:
    df = pd.DataFrame(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="data", index=False)


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False)


class TestCustomerBundleRouting:
    def test_clean_excludes_emails_in_negative_sources(
        self, tmp_path: Path,
    ):
        # Setup: two pilot-delivered rows, but one is also in the
        # do_not_send list (operator marked it bad in a previous run).
        _write_xlsx(
            tmp_path / "delivery_verified.xlsx",
            [
                {"email": "good@example.com"},
                {"email": "contradicted@example.com"},
            ],
        )
        _write_xlsx(
            tmp_path / "updated_do_not_send.xlsx",
            [{"email": "contradicted@example.com"}],
        )

        result = emit_customer_bundle(tmp_path)

        clean = _read_csv(result.files_written["clean_deliverable"])
        emails = set(clean["email"].astype(str).str.lower())
        assert "good@example.com" in emails
        assert "contradicted@example.com" not in emails

    def test_review_combines_infra_and_blocked_or_deferred(
        self, tmp_path: Path,
    ):
        _write_xlsx(
            tmp_path / "pilot_infrastructure_blocked.xlsx",
            [{"email": "msft@outlook.com"}, {"email": "yhoo@yahoo.com"}],
        )
        _write_xlsx(
            tmp_path / "pilot_blocked_or_deferred.xlsx",
            [{"email": "deferred@x.com"}],
        )

        result = emit_customer_bundle(tmp_path)

        review = _read_csv(result.files_written["review_provider_limited"])
        emails = set(review["email"].astype(str).str.lower())
        assert emails == {
            "msft@outlook.com",
            "yhoo@yahoo.com",
            "deferred@x.com",
        }

    def test_high_risk_removed_unions_negative_sources(
        self, tmp_path: Path,
    ):
        _write_xlsx(
            tmp_path / "updated_do_not_send.xlsx",
            [{"email": "complaint@x.com"}],
        )
        _write_xlsx(
            tmp_path / "pilot_hard_bounces.xlsx",
            [{"email": "unknown@x.com"}],
        )
        _write_csv(
            tmp_path / "removed_invalid.csv",
            [{"email": "syntax@invalid"}],
        )

        result = emit_customer_bundle(tmp_path)

        removed = _read_csv(result.files_written["high_risk_removed"])
        emails = set(removed["email"].astype(str).str.lower())
        assert emails == {
            "complaint@x.com",
            "unknown@x.com",
            "syntax@invalid",
        }


class TestCustomerBundleStructure:
    def test_emits_all_four_csvs_plus_readme(self, tmp_path: Path):
        # Spec: 4 files, all CSV, + README.
        result = emit_customer_bundle(tmp_path)

        bundle_dir = tmp_path / CUSTOMER_BUNDLE_DIRNAME
        assert (bundle_dir / CLEAN_DELIVERABLE_CSV).is_file()
        assert (bundle_dir / REVIEW_PROVIDER_LIMITED_CSV).is_file()
        assert (bundle_dir / HIGH_RISK_REMOVED_CSV).is_file()
        assert (bundle_dir / SMTP_EVIDENCE_REPORT_FILENAME).is_file()
        assert (bundle_dir / CUSTOMER_README).is_file()
        # All four data files end in .csv per spec.
        for filename in (
            CLEAN_DELIVERABLE_CSV,
            REVIEW_PROVIDER_LIMITED_CSV,
            HIGH_RISK_REMOVED_CSV,
            SMTP_EVIDENCE_REPORT_FILENAME,
        ):
            assert filename.endswith(".csv"), filename

        readme_text = (bundle_dir / CUSTOMER_README).read_text(encoding="utf-8")
        # Honest framing: both promise and explicit non-promise.
        assert "What we promise" in readme_text
        assert "What we do NOT promise" in readme_text
        assert "review_provider_limited" in readme_text

        assert result.counts["clean_deliverable"] == 0
        assert result.counts["smtp_evidence_report"] == 0

    def test_evidence_report_is_copied_when_present(self, tmp_path: Path):
        evidence_path = tmp_path / SMTP_EVIDENCE_REPORT_FILENAME
        with evidence_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(CSV_COLUMNS)
            writer.writerow([
                "a@x.com", "delivered", "250", "ok", "accepted",
                "x.com", "corp", "true", "deliver",
            ])

        result = emit_customer_bundle(tmp_path)

        copied = result.files_written["smtp_evidence_report"]
        assert copied.is_file()
        with copied.open("r", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        assert len(rows) == 1
        assert rows[0]["email"] == "a@x.com"
        assert rows[0]["evidence_class"] == "accepted"
        assert result.counts["smtp_evidence_report"] == 1

    def test_idempotent(self, tmp_path: Path):
        _write_xlsx(
            tmp_path / "delivery_verified.xlsx",
            [{"email": "good@x.com"}],
        )
        first = emit_customer_bundle(tmp_path)
        second = emit_customer_bundle(tmp_path)
        assert first.files_written.keys() == second.files_written.keys()
        assert first.counts == second.counts


class TestReviewActionSources:
    """V2.10.10.b review-action xlsx files (ready_probable / low_risk /
    catch_all_consumer) feed the clean_deliverable bucket."""

    def test_catch_all_consumer_lands_in_clean(self, tmp_path: Path):
        # Critical regression: 525 Yahoo/AOL emails that the SMTP
        # pilot can't probe must NOT end up in review or removed —
        # they pass every other defensive check.
        _write_xlsx(
            tmp_path / "review_catch_all_consumer.xlsx",
            [
                {"email": "alice@yahoo.com"},
                {"email": "bob@aol.com"},
                {"email": "carol@hotmail.com"},
            ],
        )
        result = emit_customer_bundle(tmp_path)
        clean = _read_csv(result.files_written["clean_deliverable"])
        emails = set(clean["email"].astype(str).str.lower())
        assert emails == {"alice@yahoo.com", "bob@aol.com", "carol@hotmail.com"}
        assert result.counts["clean_deliverable"] == 3

    def test_ready_probable_and_low_risk_land_in_clean(self, tmp_path: Path):
        _write_xlsx(
            tmp_path / "review_ready_probable.xlsx",
            [{"email": "ready@x.com"}],
        )
        _write_xlsx(
            tmp_path / "review_low_risk.xlsx",
            [{"email": "lowrisk@x.com"}],
        )
        result = emit_customer_bundle(tmp_path)
        clean = _read_csv(result.files_written["clean_deliverable"])
        emails = set(clean["email"].astype(str).str.lower())
        assert emails == {"ready@x.com", "lowrisk@x.com"}

    def test_review_action_emails_excluded_when_in_negative_sources(
        self, tmp_path: Path,
    ):
        # If an email appears in BOTH review_catch_all_consumer AND
        # do_not_send (e.g. a stale review-action xlsx from a prior
        # run), do_not_send wins.
        _write_xlsx(
            tmp_path / "review_catch_all_consumer.xlsx",
            [{"email": "good@yahoo.com"}, {"email": "bad@yahoo.com"}],
        )
        _write_xlsx(
            tmp_path / "updated_do_not_send.xlsx",
            [{"email": "bad@yahoo.com"}],
        )
        result = emit_customer_bundle(tmp_path)
        clean = _read_csv(result.files_written["clean_deliverable"])
        emails = set(clean["email"].astype(str).str.lower())
        assert "good@yahoo.com" in emails
        assert "bad@yahoo.com" not in emails

    def test_review_high_risk_lands_in_high_risk_removed(
        self, tmp_path: Path,
    ):
        _write_xlsx(
            tmp_path / "review_high_risk.xlsx",
            [{"email": "highrisk@x.com"}],
        )
        result = emit_customer_bundle(tmp_path)
        removed = _read_csv(result.files_written["high_risk_removed"])
        emails = set(removed["email"].astype(str).str.lower())
        assert "highrisk@x.com" in emails

    def test_review_timeout_retry_lands_in_review(self, tmp_path: Path):
        _write_xlsx(
            tmp_path / "review_timeout_retry.xlsx",
            [{"email": "timeout@x.com"}],
        )
        result = emit_customer_bundle(tmp_path)
        review = _read_csv(result.files_written["review_provider_limited"])
        emails = set(review["email"].astype(str).str.lower())
        assert "timeout@x.com" in emails

    def test_v2_vetted_rows_exempt_from_rubric_demotion(self, tmp_path: Path):
        # A consumer-provider row may have ``domain_risk_level=medium``
        # in the rubric report (the rubric flags every non-low-risk
        # provider as risky). Without an exemption, the rubric would
        # pull these out of clean — exactly the regression we're
        # avoiding.
        _write_xlsx(
            tmp_path / "review_catch_all_consumer.xlsx",
            [{"email": "alice@yahoo.com"}],
        )
        # Synthesize a defensive_rubric_report.csv that says risky.
        from app.defensive_rubric import (
            CSV_COLUMNS,
            DEFENSIVE_RUBRIC_REPORT_FILENAME,
        )
        import csv
        with (tmp_path / DEFENSIVE_RUBRIC_REPORT_FILENAME).open(
            "w", newline="", encoding="utf-8",
        ) as fh:
            writer = csv.writer(fh)
            writer.writerow(CSV_COLUMNS)
            writer.writerow([
                "alice@yahoo.com",
                "true", "true", "true", "true", "false",
                "risky", "risky: domain_risk=medium",
            ])

        result = emit_customer_bundle(tmp_path)
        clean = _read_csv(result.files_written["clean_deliverable"])
        emails = set(clean["email"].astype(str).str.lower())
        assert "alice@yahoo.com" in emails


class TestMissingSources:
    def test_no_pilot_files_still_emits_bundle(self, tmp_path: Path):
        # Defensive-only mode: only clean_high_confidence.csv exists,
        # no pilot ran. Bundle still builds.
        _write_csv(
            tmp_path / "clean_high_confidence.csv",
            [{"email": "ok@x.com"}, {"email": "ok2@x.com"}],
        )
        result = emit_customer_bundle(tmp_path)
        clean = _read_csv(result.files_written["clean_deliverable"])
        emails = set(clean["email"].astype(str).str.lower())
        assert emails == {"ok@x.com", "ok2@x.com"}
        assert result.counts["clean_deliverable"] == 2
        assert result.counts["review_provider_limited"] == 0
        assert result.counts["high_risk_removed"] == 0

    def test_falls_back_from_updated_to_legacy_do_not_send(
        self, tmp_path: Path,
    ):
        _write_xlsx(
            tmp_path / "do_not_send.xlsx",
            [{"email": "legacy@x.com"}],
        )
        result = emit_customer_bundle(tmp_path)
        removed = _read_csv(result.files_written["high_risk_removed"])
        emails = set(removed["email"].astype(str).str.lower())
        assert "legacy@x.com" in emails
