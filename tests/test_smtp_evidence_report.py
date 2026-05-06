"""V2.10.13 — smtp_evidence_report tests."""

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
    CSV_COLUMNS,
    EVIDENCE_COMPLAINT,
    EVIDENCE_CONTENT_BLOCKED,
    EVIDENCE_NO_EVIDENCE,
    EVIDENCE_RECIPIENT_ACCEPTED,
    EVIDENCE_RECIPIENT_REJECTED,
    EVIDENCE_SENDER_INFRA_BLOCKED,
    EVIDENCE_SENDER_PROVIDER_DEFERRED,
    EVIDENCE_TRANSIENT_SOFT,
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


class TestVerdictToEvidenceMapping:
    def test_delivered_is_recipient_accepted_actionable(self, tmp_path: Path):
        path = tmp_path / SMTP_EVIDENCE_REPORT_FILENAME
        write_smtp_evidence_report(
            [_row(email="a@x.com", verdict=VERDICT_DELIVERED)], path=path,
        )
        rows = _read_report(path)
        assert rows[0]["evidence_class"] == EVIDENCE_RECIPIENT_ACCEPTED
        assert rows[0]["actionable_for_customer"] == "true"

    def test_hard_bounce_is_recipient_rejected_actionable(self, tmp_path: Path):
        path = tmp_path / SMTP_EVIDENCE_REPORT_FILENAME
        write_smtp_evidence_report(
            [_row(email="a@x.com", verdict=VERDICT_HARD_BOUNCE,
                  diagnostic="550 user unknown", smtp_code="550")],
            path=path,
        )
        rows = _read_report(path)
        assert rows[0]["evidence_class"] == EVIDENCE_RECIPIENT_REJECTED
        assert rows[0]["actionable_for_customer"] == "true"
        assert "remove" in rows[0]["recommended_action"]

    def test_blocked_is_content_blocked_actionable(self, tmp_path: Path):
        path = tmp_path / SMTP_EVIDENCE_REPORT_FILENAME
        write_smtp_evidence_report(
            [_row(email="a@x.com", verdict=VERDICT_BLOCKED)], path=path,
        )
        rows = _read_report(path)
        assert rows[0]["evidence_class"] == EVIDENCE_CONTENT_BLOCKED
        assert rows[0]["actionable_for_customer"] == "true"

    def test_complaint_is_complaint_actionable(self, tmp_path: Path):
        path = tmp_path / SMTP_EVIDENCE_REPORT_FILENAME
        write_smtp_evidence_report(
            [_row(email="a@x.com", verdict=VERDICT_COMPLAINT)], path=path,
        )
        rows = _read_report(path)
        assert rows[0]["evidence_class"] == EVIDENCE_COMPLAINT
        assert rows[0]["actionable_for_customer"] == "true"

    def test_soft_bounce_and_deferred_are_transient_actionable(
        self, tmp_path: Path,
    ):
        path = tmp_path / SMTP_EVIDENCE_REPORT_FILENAME
        write_smtp_evidence_report(
            [
                _row(email="a@x.com", verdict=VERDICT_SOFT_BOUNCE),
                _row(email="b@x.com", verdict=VERDICT_DEFERRED),
            ],
            path=path,
        )
        rows = _read_report(path)
        assert rows[0]["evidence_class"] == EVIDENCE_TRANSIENT_SOFT
        assert rows[1]["evidence_class"] == EVIDENCE_TRANSIENT_SOFT
        # Transient signals are NOT actionable for customer routing.
        assert rows[0]["actionable_for_customer"] == "false"

    def test_infrastructure_blocked_is_sender_side_not_actionable(
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
        assert rows[0]["evidence_class"] == EVIDENCE_SENDER_INFRA_BLOCKED
        assert rows[0]["actionable_for_customer"] == "false"
        assert "re-test" in rows[0]["recommended_action"]

    def test_provider_deferred_is_sender_side_not_actionable(
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
        assert rows[0]["evidence_class"] == EVIDENCE_SENDER_PROVIDER_DEFERRED
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
        # Verify header is exactly the documented column set.
        with path.open("r", encoding="utf-8") as fh:
            header = next(csv.reader(fh))
        assert tuple(header) == CSV_COLUMNS

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
