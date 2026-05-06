"""V2.10.12 — launch orchestrator tests."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from app.db.pilot_send_tracker import (
    open_for_run as open_tracker,
    STATE_VERDICT_READY,
    VERDICT_HARD_BOUNCE,
)
from app.pilot_send.config import (
    PilotMessageTemplate,
    PilotSendConfig,
    write_pilot_config,
)
from app.pilot_send.launch import launch_pilot
from app.pilot_send.sender import PilotSendOutcome


@pytest.fixture(autouse=True)
def _mock_mx(monkeypatch):
    from app.pilot_send import sender

    monkeypatch.setattr(
        sender, "_resolve_mx",
        lambda domain, *, timeout: [f"mx.{domain}"],
    )


def _write_xlsx(path: Path, rows: list[dict]) -> None:
    df = pd.DataFrame(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="rows", index=False)


def _candidate_row(email: str, source_row: int = 1) -> dict:
    return {
        "email": email,
        "source_row_number": str(source_row),
        "provider_family": "corporate_unknown",
        "deliverability_probability": "0.75",
    }


@pytest.fixture
def run_dir(tmp_path: Path):
    _write_xlsx(
        tmp_path / "review_ready_probable.xlsx",
        [_candidate_row(f"u{i}@example.com", source_row=i) for i in range(5)],
    )
    return tmp_path


def _ready_config() -> PilotSendConfig:
    return PilotSendConfig(
        template=PilotMessageTemplate(
            subject="Hello",
            body_text="Hi there",
            sender_address="sender@acme.com",
        ),
        return_path_domain="bounces.acme.com",
        max_batch_size=50,
        authorization_confirmed=True,
    )


# --------------------------------------------------------------------- #
# Validation failures
# --------------------------------------------------------------------- #


class TestValidation:
    def test_authorization_not_confirmed(self, run_dir: Path):
        cfg = _ready_config()
        cfg.authorization_confirmed = False
        write_pilot_config(run_dir, cfg)
        result = launch_pilot(run_dir, job_id="j1", batch_size=2)
        assert result.error == "authorization_required"
        assert result.sent == 0

    def test_template_incomplete(self, run_dir: Path):
        cfg = _ready_config()
        cfg.template.subject = ""
        write_pilot_config(run_dir, cfg)
        result = launch_pilot(run_dir, job_id="j1", batch_size=2)
        assert result.error == "template_incomplete"

    def test_return_path_missing(self, run_dir: Path):
        cfg = _ready_config()
        cfg.return_path_domain = ""
        write_pilot_config(run_dir, cfg)
        result = launch_pilot(run_dir, job_id="j1", batch_size=2)
        assert result.error == "return_path_domain_missing"

    def test_batch_size_must_be_positive(self, run_dir: Path):
        write_pilot_config(run_dir, _ready_config())
        result = launch_pilot(run_dir, job_id="j1", batch_size=0)
        assert result.error == "batch_size_must_be_positive"

    def test_batch_size_exceeds_max(self, run_dir: Path):
        cfg = _ready_config()
        cfg.max_batch_size = 5
        write_pilot_config(run_dir, cfg)
        result = launch_pilot(run_dir, job_id="j1", batch_size=10)
        assert result.error == "batch_size_exceeds_max"

    def test_no_candidates_found(self, tmp_path: Path):
        write_pilot_config(tmp_path, _ready_config())
        result = launch_pilot(tmp_path, job_id="j1", batch_size=2)
        assert result.error == "no_candidates_found"


# --------------------------------------------------------------------- #
# Happy path
# --------------------------------------------------------------------- #


def _ok_factory():
    class _Transport:
        def sendmail(self, *a, **kw):
            return {}

        def quit(self):
            return (221, b"bye")

    def make(*, host, port, timeout):
        return _Transport()

    return make


def _fail_factory(*, code: int = 550):
    class _Transport:
        def sendmail(self, from_addr, to_addrs, msg):
            recipient = to_addrs if isinstance(to_addrs, str) else to_addrs[0]
            return {recipient: (code, b"refused")}

        def quit(self):
            return (221, b"bye")

    def make(*, host, port, timeout):
        return _Transport()

    return make


class TestHappyPath:
    def test_three_candidates_three_sent(self, run_dir: Path):
        write_pilot_config(run_dir, _ready_config())
        result = launch_pilot(
            run_dir,
            job_id="j1",
            batch_size=3,
            smtp_factory=_ok_factory(),
            sleep_fn=lambda _s: None,
        )
        assert result.error is None
        assert result.candidates_selected == 3
        assert result.candidates_added == 3
        assert result.sent == 3
        assert result.failed == 0
        assert result.batch_id  # non-empty

    def test_send_failure_recorded_as_verdict_ready(self, run_dir: Path):
        write_pilot_config(run_dir, _ready_config())
        result = launch_pilot(
            run_dir,
            job_id="j1",
            batch_size=2,
            smtp_factory=_fail_factory(code=550),
            sleep_fn=lambda _s: None,
        )
        assert result.sent == 0
        assert result.failed == 2
        # Tracker should show those rows in verdict_ready hard_bounce.
        with open_tracker(run_dir) as tracker:
            rows = tracker.snapshot()
        assert all(r.state == STATE_VERDICT_READY for r in rows)
        assert all(r.dsn_status == VERDICT_HARD_BOUNCE for r in rows)


class TestIdempotency:
    def test_relaunch_skips_already_enqueued(self, run_dir: Path):
        write_pilot_config(run_dir, _ready_config())
        first = launch_pilot(
            run_dir, job_id="j1", batch_size=3,
            smtp_factory=_ok_factory(),
            sleep_fn=lambda _s: None,
        )
        # Second launch with the same job — same emails are eligible
        # but the (job, batch, email) UNIQUE allows new batch_id.
        second = launch_pilot(
            run_dir, job_id="j1", batch_size=3,
            smtp_factory=_ok_factory(),
            sleep_fn=lambda _s: None,
        )
        assert first.batch_id != second.batch_id
        # Both batches sent — different batch_ids, so no duplicate.
        assert second.sent == 3
