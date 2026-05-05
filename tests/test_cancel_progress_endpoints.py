"""HTTP tests for the new /jobs/{id}/cancel and /jobs/{id}/progress endpoints."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from app import cancellation, server
from app.api_boundary import JobStatus


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SAMPLE_CSV = PROJECT_ROOT / "examples" / "sample_contacts.csv"


@pytest.fixture()
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    server.JOB_STORE.clear()
    cancellation.reset_all()
    monkeypatch.setattr(server, "RUNTIME_ROOT", tmp_path / "runtime")
    with TestClient(server.app) as test_client:
        yield test_client
    server.JOB_STORE.clear()
    cancellation.reset_all()


def _start_sample_job(client: TestClient) -> dict[str, Any]:
    with SAMPLE_CSV.open("rb") as handle:
        response = client.post(
            "/jobs",
            files={"file": (SAMPLE_CSV.name, handle, "text/csv")},
        )
    assert response.status_code == 201
    return response.json()


def _wait_for_terminal_job(client: TestClient, job_id: str) -> dict[str, Any]:
    deadline = time.time() + 20
    last: dict[str, Any] | None = None
    while time.time() < deadline:
        response = client.get(f"/jobs/{job_id}")
        assert response.status_code == 200
        last = response.json()
        if last["status"] in {JobStatus.COMPLETED, JobStatus.FAILED}:
            return last
        time.sleep(0.1)
    raise AssertionError(f"job did not finish in time: {last}")


# --------------------------------------------------------------------------- #
# /jobs/{id}/cancel                                                           #
# --------------------------------------------------------------------------- #


def test_cancel_unknown_job_returns_404(client: TestClient) -> None:
    response = client.post("/jobs/job_ghost/cancel")
    assert response.status_code == 404
    body = response.json()
    # The custom HTTP error handler nests the structured payload under
    # "error". Either ``error_type`` or ``error.error_type`` carries
    # the canonical code.
    error = body.get("error") or body
    assert error.get("error_type") == "job_not_found"


def test_cancel_terminal_job_is_noop(client: TestClient) -> None:
    started = _start_sample_job(client)
    job_id = started["job_id"]
    final = _wait_for_terminal_job(client, job_id)
    assert final["status"] in {JobStatus.COMPLETED, JobStatus.FAILED}

    response = client.post(f"/jobs/{job_id}/cancel")
    assert response.status_code == 200
    body = response.json()
    assert body["job_id"] == job_id
    assert body["cancelled"] is False
    assert "already terminal" in body["reason"]
    # Registry must remain clean for terminal jobs.
    assert cancellation.is_cancelled(job_id) is False


def test_cancel_marks_running_job_and_is_idempotent(client: TestClient) -> None:
    # Manually queue a job in the in-memory store so we can exercise
    # cancel without racing the BackgroundTask to terminal state.
    job_id = "job_test_cancel"
    queued = server._queued_result(job_id, "test.csv")
    server.JOB_STORE.create(queued)

    first = client.post(f"/jobs/{job_id}/cancel")
    assert first.status_code == 200
    body = first.json()
    assert body["cancelled"] is True
    assert cancellation.is_cancelled(job_id) is True

    second = client.post(f"/jobs/{job_id}/cancel")
    assert second.status_code == 200
    assert second.json()["cancelled"] is False  # already cancelled


# --------------------------------------------------------------------------- #
# /jobs/{id}/progress                                                         #
# --------------------------------------------------------------------------- #


def test_progress_unknown_job_returns_404(client: TestClient) -> None:
    response = client.get("/jobs/job_ghost/progress")
    assert response.status_code == 404


def test_progress_returns_status_and_no_smtp_block_without_summary(
    client: TestClient,
) -> None:
    job_id = "job_test_progress"
    queued = server._queued_result(job_id, "test.csv")
    server.JOB_STORE.create(queued)

    response = client.get(f"/jobs/{job_id}/progress")
    assert response.status_code == 200
    body = response.json()
    assert body["job_id"] == job_id
    assert body["status"] in {
        JobStatus.QUEUED,
        JobStatus.RUNNING,
        JobStatus.COMPLETED,
        JobStatus.FAILED,
    }
    assert body["cancelled"] is False
    assert body["smtp"] is None


def test_progress_surfaces_smtp_runtime_summary(
    client: TestClient, tmp_path: Path
) -> None:
    import json

    job_id = "job_test_progress_smtp"
    queued = server._queued_result(job_id, "test.csv")
    server.JOB_STORE.create(queued)

    # Create a run dir with a synthetic smtp_runtime_summary.json so the
    # progress endpoint has data to surface.
    job_output = server.RUNTIME_ROOT / "jobs" / job_id / "run_2026_01_01"
    job_output.mkdir(parents=True, exist_ok=True)
    summary_path = job_output / "smtp_runtime_summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "smtp_enabled": True,
                "smtp_dry_run": False,
                "smtp_candidates_seen": 100,
                "smtp_candidates_attempted": 42,
                "smtp_valid_count": 30,
                "smtp_invalid_count": 5,
                "smtp_inconclusive_count": 7,
                "smtp_timeout_count": 2,
                "smtp_blocked_count": 1,
            }
        ),
        encoding="utf-8",
    )

    response = client.get(f"/jobs/{job_id}/progress")
    assert response.status_code == 200
    body = response.json()
    assert body["smtp"] is not None
    assert body["smtp"]["attempted"] == 42
    assert body["smtp"]["total"] == 100
    assert body["smtp"]["valid"] == 30
    assert body["smtp"]["live"] is True
    assert 0 < body["smtp"]["ratio"] <= 1.0


# --------------------------------------------------------------------------- #
# run_probing honours cancellation                                            #
# --------------------------------------------------------------------------- #


def test_run_probing_breaks_when_cancel_returns_true() -> None:
    from app.config import SMTPProbeConfig as _CfgClass
    from app.validation_v2.smtp_integration import _Candidate, run_probing
    from app.validation_v2.smtp_probe import SMTPResult

    # Build a tiny config with rate limit fast enough to not throttle.
    cfg = _CfgClass(enabled=True, dry_run=True, rate_limit_per_second=100.0)

    candidates = [
        _Candidate(email=f"u{i}@example-corp.com", domain="example-corp.com", reason="x")
        for i in range(5)
    ]

    calls = {"n": 0}

    def fake_probe(email: str, **_kwargs: object) -> SMTPResult:
        calls["n"] += 1
        return SMTPResult(False, None, "stub", False, True)

    # Cancel after the first call.
    def cancel_check() -> bool:
        return calls["n"] >= 1

    results = run_probing(
        candidates,
        cfg,
        probe_fn=fake_probe,
        cancel_check=cancel_check,
    )
    # Loop must have broken before processing all five.
    assert len(results) <= 1
    assert calls["n"] <= 1
