"""V2.10.13 — smtp_evidence_report tests.

Asserts the report matches the customer-facing spec:
  columns: email | bucket | smtp_code | smtp_reason | evidence_class
           (+ operational extras: domain, provider_family,
            actionable_for_customer, recommended_action)
  evidence_class values: rcpt_refused | infra_blocked | provider_deferred
           | accepted | no_evidence
"""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path

from app.db.pilot_send_tracker import (
    PilotRow,
    VERDICT_BLOCKED,
    VERDICT_COMPLAINT,
    VERDICT_DEFERRED,
    VERDICT_DELIVERED,
    VERDICT_HARD_BOUNCE,
    VERDICT_INFRA_BLOCKED,
    VERDICT_PROVIDER_DEFERRED,
    VERDICT_SOFT_BOUNCE,
    VERDICT_UNKNOWN,
)
from app.pilot_send.evidence import (
    ALL_EVIDENCE_CLASSES,
    CSV_COLUMNS,
    EVIDENCE_ACCEPTED,
    EVIDENCE_INFRA_BLOCKED,
    EVIDENCE_NO_EVIDENCE,
    EVIDENCE_PROVIDER_DEFERRED,
    EVIDENCE_RCPT_REFUSED,
    SMTP_EVIDENCE_REPORT_FILENAME,
    write_smtp_evidence_report,
)


def _row(
    *,
    email: str,
    verdict: str | None,
    diagnostic: str = "",
    smtp_code: str = "",
    state: str = "verdict_ready",
) -> PilotRow:
    now = datetime.now(tz=timezone.utc)
    return PilotRow(
        id=1,
        job_id="j1",
        batch_id="b1",
        source_row=1,
        email=email,
        domain=email.rsplit("@", 1)[-1],
        provider_family="corporate_unknown",
        verp_token="tok",
        message_id=None,
        sent_at=now,
        state=state,
        dsn_status=verdict,
        dsn_received_at=now if verdict else None,
        dsn_diagnostic=diagnostic or None,
        dsn_smtp_code=smtp_code or None,
        last_polled_at=None,
        created_at=now,
        updated_at=now,
    )


def _read_report(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


class TestSpecConformance:
    """Hard-pin the schema to the customer-facing spec exactly."""

    def test_evidence_class_vocabulary_is_exactly_five(self):
        assert set(ALL_EVIDENCE_CLASSES) == {
            "rcpt_refused",
            "accepted",
            "infra_blocked",
            "provider_deferred",
            "no_evidence",
        }

    def test_csv_header_starts_with_spec_columns(self, tmp_path: Path):
        path = tmp_path / SMTP_EVIDENCE_REPORT_FILENAME
        write_smtp_evidence_report([], path=path)
        with path.open("r", encoding="utf-8") as fh:
            header = next(csv.reader(fh))
        # Spec-required columns must come first, in this order.
        assert header[:5] == [
            "email", "bucket", "smtp_code", "smtp_reason", "evidence_class",
        ]
        # Full schema = spec + operational extras.
        assert tuple(header) == CSV_COLUMNS


class TestVerdictToEvidenceMapping:
    def test_delivered_is_accepted_actionable(self, tmp_path: Path):
        path = tmp_path / SMTP_EVIDENCE_REPORT_FILENAME
        write_smtp_evidence_report(
            [_row(email="a@x.com", verdict=VERDICT_DELIVERED)], path=path,
        )
        rows = _read_report(path)
        assert rows[0]["evidence_class"] == EVIDENCE_ACCEPTED
        assert rows[0]["actionable_for_customer"] == "true"
        assert rows[0]["bucket"] == VERDICT_DELIVERED

    def test_hard_bounce_is_rcpt_refused_actionable(self, tmp_path: Path):
        path = tmp_path / SMTP_EVIDENCE_REPORT_FILENAME
        write_smtp_evidence_report(
            [_row(email="a@x.com", verdict=VERDICT_HARD_BOUNCE,
                  diagnostic="550 user unknown", smtp_code="550")],
            path=path,
        )
        rows = _read_report(path)
        assert rows[0]["evidence_class"] == EVIDENCE_RCPT_REFUSED
        assert rows[0]["actionable_for_customer"] == "true"
        assert "remove" in rows[0]["recommended_action"]
        # The original verdict is preserved in the bucket column.
        assert rows[0]["bucket"] == VERDICT_HARD_BOUNCE

    def test_blocked_collapses_to_rcpt_refused(self, tmp_path: Path):
        # Content/policy block: the message will not reach this
        # recipient as-sent. Spec only has 5 classes; this collapses
        # to rcpt_refused but bucket="blocked" keeps the detail.
        path = tmp_path / SMTP_EVIDENCE_REPORT_FILENAME
        write_smtp_evidence_report(
            [_row(email="a@x.com", verdict=VERDICT_BLOCKED)], path=path,
        )
        rows = _read_report(path)
        assert rows[0]["evidence_class"] == EVIDENCE_RCPT_REFUSED
        assert rows[0]["bucket"] == VERDICT_BLOCKED

    def test_complaint_collapses_to_rcpt_refused(self, tmp_path: Path):
        # Abuse complaint: terminal recipient signal.
        path = tmp_path / SMTP_EVIDENCE_REPORT_FILENAME
        write_smtp_evidence_report(
            [_row(email="a@x.com", verdict=VERDICT_COMPLAINT)], path=path,
        )
        rows = _read_report(path)
        assert rows[0]["evidence_class"] == EVIDENCE_RCPT_REFUSED
        assert rows[0]["bucket"] == VERDICT_COMPLAINT

    def test_soft_bounce_and_deferred_are_no_evidence(
        self, tmp_path: Path,
    ):
        # Transient signals — not evidence either way.
        path = tmp_path / SMTP_EVIDENCE_REPORT_FILENAME
        write_smtp_evidence_report(
            [
                _row(email="a@x.com", verdict=VERDICT_SOFT_BOUNCE),
                _row(email="b@x.com", verdict=VERDICT_DEFERRED),
            ],
            path=path,
        )
        rows = _read_report(path)
        assert rows[0]["evidence_class"] == EVIDENCE_NO_EVIDENCE
        assert rows[1]["evidence_class"] == EVIDENCE_NO_EVIDENCE
        assert rows[0]["actionable_for_customer"] == "false"

    def test_infrastructure_blocked_is_infra_blocked_not_actionable(
        self, tmp_path: Path,
    ):
        path = tmp_path / SMTP_EVIDENCE_REPORT_FILENAME
        write_smtp_evidence_report(
            [_row(
                email="a@outlook.com",
                verdict=VERDICT_INFRA_BLOCKED,
                diagnostic="550 5.7.1 messages from [192.3.105.145] block list",
                smtp_code="550",
            )],
            path=path,
        )
        rows = _read_report(path)
        assert rows[0]["evidence_class"] == EVIDENCE_INFRA_BLOCKED
        assert rows[0]["actionable_for_customer"] == "false"
        assert "re-test" in rows[0]["recommended_action"]

    def test_provider_deferred_is_provider_deferred_not_actionable(
        self, tmp_path: Path,
    ):
        path = tmp_path / SMTP_EVIDENCE_REPORT_FILENAME
        write_smtp_evidence_report(
            [_row(
                email="a@yahoo.com",
                verdict=VERDICT_PROVIDER_DEFERRED,
                diagnostic="421 [TSS04] temporarily deferred",
                smtp_code="421",
            )],
            path=path,
        )
        rows = _read_report(path)
        assert rows[0]["evidence_class"] == EVIDENCE_PROVIDER_DEFERRED
        assert rows[0]["actionable_for_customer"] == "false"

    def test_unknown_and_pending_are_no_evidence(self, tmp_path: Path):
        path = tmp_path / SMTP_EVIDENCE_REPORT_FILENAME
        write_smtp_evidence_report(
            [
                _row(email="a@x.com", verdict=VERDICT_UNKNOWN),
                _row(email="b@x.com", verdict=None, state="pending_send"),
            ],
            path=path,
        )
        rows = _read_report(path)
        assert rows[0]["evidence_class"] == EVIDENCE_NO_EVIDENCE
        assert rows[1]["evidence_class"] == EVIDENCE_NO_EVIDENCE
        assert rows[0]["actionable_for_customer"] == "false"


class TestFileFormat:
    def test_empty_input_writes_header_only(self, tmp_path: Path):
        path = tmp_path / SMTP_EVIDENCE_REPORT_FILENAME
        n = write_smtp_evidence_report([], path=path)
        assert n == 0
        rows = _read_report(path)
        assert rows == []

    def test_diagnostic_is_truncated_to_300_chars(self, tmp_path: Path):
        path = tmp_path / SMTP_EVIDENCE_REPORT_FILENAME
        long_diag = "x" * 500
        write_smtp_evidence_report(
            [_row(email="a@x.com", verdict=VERDICT_HARD_BOUNCE,
                  diagnostic=long_diag)],
            path=path,
        )
        rows = _read_report(path)
        assert len(rows[0]["smtp_reason"]) == 300

    def test_returns_row_count(self, tmp_path: Path):
        path = tmp_path / SMTP_EVIDENCE_REPORT_FILENAME
        n = write_smtp_evidence_report(
            [
                _row(email="a@x.com", verdict=VERDICT_DELIVERED),
                _row(email="b@x.com", verdict=VERDICT_HARD_BOUNCE),
                _row(email="c@x.com", verdict=VERDICT_INFRA_BLOCKED),
            ],
            path=path,
        )
        assert n == 3
