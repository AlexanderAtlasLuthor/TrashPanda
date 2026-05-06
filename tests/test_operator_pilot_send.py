"""V2.10.12 — operator pilot-send endpoint tests.

Covers the 6 routes defined in ``app.operator_routes`` under
``/api/operator/jobs/{id}/pilot-send/*``:

* GET    /status
* PUT    /config
* POST   /preview
* POST   /launch
* POST   /poll-bounces
* POST   /finalize

The launch + poll endpoints touch the real
``SMTPSender`` / IMAP poller; we monkeypatch them at the
import site so no socket is opened.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app import server
from app.api_boundary import (
    JobArtifacts,
    JobResult,
    JobStatus,
)
from app.db.pilot_send_tracker import (
    open_for_run as open_tracker,
    VERDICT_DELIVERED,
    VERDICT_HARD_BOUNCE,
)
from app.pilot_send.config import (
    IMAPCredentials,
    PilotMessageTemplate,
    PilotSendConfig,
    write_pilot_config,
)


# --------------------------------------------------------------------- #
# Fixtures (mirror tests/test_operator_routes_v210.py)
# --------------------------------------------------------------------- #


@pytest.fixture()
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    server.JOB_STORE.clear()
    monkeypatch.setattr(server, "RUNTIME_ROOT", tmp_path / "runtime")
    with TestClient(server.app) as c:
        yield c
    server.JOB_STORE.clear()


def _make_run_dir(tmp_path: Path, job_id: str) -> Path:
    run_dir = tmp_path / "runtime" / "jobs" / job_id / "run_20260101_120000_abc"
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def _register_job(run_dir: Path, job_id: str) -> None:
    artifacts = JobArtifacts(run_dir=run_dir)
    started = datetime.now(timezone.utc)
    server.JOB_STORE.set_result(
        JobResult(
            job_id=job_id,
            status=JobStatus.COMPLETED,
            input_filename="synthetic.csv",
            run_dir=run_dir,
            summary=None,
            artifacts=artifacts,
            error=None,
            started_at=started,
            finished_at=started,
        )
    )


def _ready_config() -> PilotSendConfig:
    return PilotSendConfig(
        template=PilotMessageTemplate(
            subject="Hello",
            body_text="Hi",
            sender_address="sender@acme.com",
        ),
        return_path_domain="bounces.acme.com",
        max_batch_size=50,
        authorization_confirmed=True,
    )


def _write_action_xlsx(run_dir: Path, filename: str, n_rows: int) -> None:
    rows = [
        {
            "email": f"u{i}@example.com",
            "source_row_number": str(i + 1),
            "provider_family": "corporate_unknown",
            "deliverability_probability": "0.75",
        }
        for i in range(n_rows)
    ]
    df = pd.DataFrame(rows)
    with pd.ExcelWriter(run_dir / filename, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="rows", index=False)


# --------------------------------------------------------------------- #
# GET /status
# --------------------------------------------------------------------- #


class TestStatus:
    def test_no_tracker_returns_defaults(
        self, tmp_path: Path, client: TestClient
    ):
        run_dir = _make_run_dir(tmp_path, "job_a")
        _register_job(run_dir, "job_a")
        res = client.get("/api/operator/jobs/job_a/pilot-send/status")
        assert res.status_code == 200
        body = res.json()
        assert body["available"] is False
        assert body["config_ready"] is False
        assert body["counts"]["total"] == 0

    def test_with_config_reports_readiness(
        self, tmp_path: Path, client: TestClient
    ):
        run_dir = _make_run_dir(tmp_path, "job_b")
        _register_job(run_dir, "job_b")
        write_pilot_config(run_dir, _ready_config())
        res = client.get("/api/operator/jobs/job_b/pilot-send/status")
        assert res.status_code == 200
        body = res.json()
        assert body["config_ready"] is True
        assert body["authorization_confirmed"] is True

    def test_status_includes_tracker_counts(
        self, tmp_path: Path, client: TestClient
    ):
        run_dir = _make_run_dir(tmp_path, "job_c")
        _register_job(run_dir, "job_c")
        write_pilot_config(run_dir, _ready_config())
        # Manually create a tracker row.
        with open_tracker(run_dir) as tracker:
            tracker.add_candidate(
                job_id="job_c", batch_id="b1", source_row=1,
                email="x@y.com", domain="y.com",
                provider_family="corporate_unknown",
                verp_token="tok-xyz",
            )
        res = client.get("/api/operator/jobs/job_c/pilot-send/status")
        body = res.json()
        assert body["available"] is True
        assert body["counts"]["pending_send"] == 1
        assert body["counts"]["total"] == 1


# --------------------------------------------------------------------- #
# PUT /config
# --------------------------------------------------------------------- #


class TestPutConfig:
    def test_round_trip(self, tmp_path: Path, client: TestClient):
        run_dir = _make_run_dir(tmp_path, "job_cfg")
        _register_job(run_dir, "job_cfg")
        payload = {
            "template": {
                "subject": "S",
                "body_text": "B",
                "sender_address": "sender@acme.com",
            },
            "return_path_domain": "bounces.acme.com",
            "authorization_confirmed": True,
        }
        res = client.put(
            "/api/operator/jobs/job_cfg/pilot-send/config",
            json=payload,
        )
        assert res.status_code == 200
        body = res.json()
        assert body["saved"] is True
        assert body["config_ready"] is True

        # Round-trip: GET status reflects readiness.
        status = client.get("/api/operator/jobs/job_cfg/pilot-send/status")
        assert status.json()["config_ready"] is True


# --------------------------------------------------------------------- #
# POST /preview
# --------------------------------------------------------------------- #


class TestPreview:
    def test_preview_with_action_xlsx(
        self, tmp_path: Path, client: TestClient
    ):
        run_dir = _make_run_dir(tmp_path, "job_prev")
        _register_job(run_dir, "job_prev")
        _write_action_xlsx(run_dir, "review_ready_probable.xlsx", n_rows=3)
        res = client.post(
            "/api/operator/jobs/job_prev/pilot-send/preview?batch_size=10",
        )
        assert res.status_code == 200
        body = res.json()
        assert body["candidates_found"] == 3
        assert len(body["candidates"]) == 3

    def test_preview_no_files_returns_zero(
        self, tmp_path: Path, client: TestClient
    ):
        run_dir = _make_run_dir(tmp_path, "job_prev2")
        _register_job(run_dir, "job_prev2")
        res = client.post(
            "/api/operator/jobs/job_prev2/pilot-send/preview?batch_size=10",
        )
        assert res.status_code == 200
        assert res.json()["candidates_found"] == 0


# --------------------------------------------------------------------- #
# POST /launch
# --------------------------------------------------------------------- #


class _OkTransport:
    def sendmail(self, *a, **kw):
        return {}

    def quit(self):
        return (221, b"bye")


def _ok_factory(*, host, port, timeout):
    return _OkTransport()


class TestLaunch:
    def test_launch_without_authorization_returns_400(
        self, tmp_path: Path, client: TestClient
    ):
        run_dir = _make_run_dir(tmp_path, "job_l1")
        _register_job(run_dir, "job_l1")
        cfg = _ready_config()
        cfg.authorization_confirmed = False
        write_pilot_config(run_dir, cfg)
        _write_action_xlsx(run_dir, "review_ready_probable.xlsx", n_rows=1)
        res = client.post(
            "/api/operator/jobs/job_l1/pilot-send/launch?batch_size=1",
        )
        assert res.status_code == 400
        assert res.json()["error"]["error_type"] == "authorization_required"

    def test_launch_no_candidates_returns_400(
        self, tmp_path: Path, client: TestClient
    ):
        run_dir = _make_run_dir(tmp_path, "job_l2")
        _register_job(run_dir, "job_l2")
        write_pilot_config(run_dir, _ready_config())
        # No XLSX action files.
        res = client.post(
            "/api/operator/jobs/job_l2/pilot-send/launch?batch_size=2",
        )
        assert res.status_code == 400
        assert res.json()["error"]["error_type"] == "no_candidates_found"

    def test_launch_happy_path(
        self,
        tmp_path: Path,
        client: TestClient,
        monkeypatch: pytest.MonkeyPatch,
    ):
        run_dir = _make_run_dir(tmp_path, "job_l3")
        _register_job(run_dir, "job_l3")
        write_pilot_config(run_dir, _ready_config())
        _write_action_xlsx(run_dir, "review_ready_probable.xlsx", n_rows=2)

        # Mock MX resolution + the SMTP factory the SMTPSender uses
        # so no real socket is opened.
        from app.pilot_send import sender as sender_mod

        monkeypatch.setattr(
            sender_mod,
            "_resolve_mx",
            lambda domain, *, timeout: [f"mx.{domain}"],
        )

        # Patch the SMTPSender default factory by temporarily wrapping
        # the launch_pilot call from inside the route. Since we can't
        # pass smtp_factory through HTTP, we monkeypatch the module's
        # default factory.
        monkeypatch.setattr(
            sender_mod.SMTPSender, "_default_factory", staticmethod(_ok_factory),
        )

        res = client.post(
            "/api/operator/jobs/job_l3/pilot-send/launch?batch_size=2",
        )
        assert res.status_code == 200
        body = res.json()
        assert body["sent"] == 2
        assert body["failed"] == 0


# --------------------------------------------------------------------- #
# POST /poll-bounces
# --------------------------------------------------------------------- #


class TestPollBounces:
    def test_no_imap_config_returns_zero(
        self, tmp_path: Path, client: TestClient
    ):
        run_dir = _make_run_dir(tmp_path, "job_p1")
        _register_job(run_dir, "job_p1")
        # Default config has no IMAP creds.
        write_pilot_config(run_dir, _ready_config())
        # Touch tracker so file exists.
        with open_tracker(run_dir) as tracker:
            tracker.add_candidate(
                job_id="job_p1", batch_id="b1", source_row=1,
                email="x@y.com", domain="y.com",
                provider_family="corporate_unknown",
                verp_token="t1",
            )
        res = client.post(
            "/api/operator/jobs/job_p1/pilot-send/poll-bounces",
        )
        assert res.status_code == 200
        body = res.json()
        assert body["fetched"] == 0


# --------------------------------------------------------------------- #
# POST /finalize
# --------------------------------------------------------------------- #


class TestFinalize:
    def test_finalize_without_tracker_is_noop(
        self, tmp_path: Path, client: TestClient
    ):
        run_dir = _make_run_dir(tmp_path, "job_f1")
        _register_job(run_dir, "job_f1")
        res = client.post(
            "/api/operator/jobs/job_f1/pilot-send/finalize",
        )
        assert res.status_code == 200
        assert res.json()["files_written"] == {}

    def test_finalize_emits_xlsx_for_verdict_ready(
        self,
        tmp_path: Path,
        client: TestClient,
        monkeypatch: pytest.MonkeyPatch,
    ):
        run_dir = _make_run_dir(tmp_path, "job_f2")
        _register_job(run_dir, "job_f2")
        write_pilot_config(run_dir, _ready_config())

        # Skip the bounce_ingestion bridge so we don't need a feedback
        # store wired up for the test. The finalize endpoint passes
        # ``feed_bounce_ingestion=True`` by default; bypass via
        # monkeypatching the bridge.
        from app.pilot_send import finalize as finalize_mod

        monkeypatch.setattr(
            finalize_mod, "_write_ingestion_csv", lambda rows, *, path: 0,
        )

        with open_tracker(run_dir) as tracker:
            tracker.add_candidate(
                job_id="job_f2", batch_id="b1", source_row=1,
                email="ok@x.com", domain="x.com",
                provider_family="corporate_unknown", verp_token="t-ok",
            )
            tracker.add_candidate(
                job_id="job_f2", batch_id="b1", source_row=2,
                email="hard@x.com", domain="x.com",
                provider_family="corporate_unknown", verp_token="t-hard",
            )
            for row in tracker.snapshot():
                tracker.mark_sent(row.id, message_id=None)
            tracker.record_dsn("t-ok", dsn_status=VERDICT_DELIVERED)
            tracker.record_dsn("t-hard", dsn_status=VERDICT_HARD_BOUNCE)

        res = client.post(
            "/api/operator/jobs/job_f2/pilot-send/finalize",
        )
        assert res.status_code == 200
        body = res.json()
        files = body["files_written"]
        assert "delivery_verified" in files
        assert "pilot_hard_bounces" in files
        assert "updated_do_not_send" in files
        assert body["counts"]["delivered"] == 1
        assert body["counts"]["hard_bounce"] == 1
