"""HTTP tests for the Phase 7 FastAPI wrapper."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from app import server
from app.api_boundary import JobStatus


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SAMPLE_CSV = PROJECT_ROOT / "examples" / "sample_contacts.csv"


@pytest.fixture()
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    server.JOB_STORE.clear()
    monkeypatch.setattr(server, "RUNTIME_ROOT", tmp_path / "runtime")
    with TestClient(server.app) as test_client:
        yield test_client
    server.JOB_STORE.clear()


def _start_sample_job(client: TestClient) -> dict[str, Any]:
    with SAMPLE_CSV.open("rb") as handle:
        response = client.post(
            "/jobs",
            files={"file": (SAMPLE_CSV.name, handle, "text/csv")},
        )
    assert response.status_code == 201
    payload = response.json()
    assert payload["job_id"]
    assert payload["status"] in {
        JobStatus.QUEUED,
        JobStatus.RUNNING,
        JobStatus.COMPLETED,
        JobStatus.FAILED,
    }
    return payload


def _wait_for_terminal_job(client: TestClient, job_id: str) -> dict[str, Any]:
    deadline = time.time() + 20
    last_payload: dict[str, Any] | None = None

    while time.time() < deadline:
        response = client.get(f"/jobs/{job_id}")
        assert response.status_code == 200
        last_payload = response.json()
        if last_payload["status"] in {JobStatus.COMPLETED, JobStatus.FAILED}:
            return last_payload
        time.sleep(0.1)

    raise AssertionError(f"Job did not finish in time: {last_payload}")


def test_post_jobs_success_returns_job_id(client: TestClient) -> None:
    payload = _start_sample_job(client)

    assert payload["job_id"].startswith("job_")
    assert payload["input_filename"] == SAMPLE_CSV.name
    assert payload["error"] is None
    assert payload["started_at"]


def test_get_job_returns_consistent_payload(client: TestClient) -> None:
    started = _start_sample_job(client)
    payload = _wait_for_terminal_job(client, started["job_id"])

    assert payload["job_id"] == started["job_id"]
    assert payload["status"] == JobStatus.COMPLETED
    assert payload["summary"]["total_input_rows"] >= 1
    assert payload["artifacts"]["reports"]["processing_report_json"]
    assert payload["finished_at"]


def test_get_artifact_downloads_existing_file(client: TestClient) -> None:
    """V2.10.0.3 — ``processing_report_json`` is operator_only per
    :mod:`app.artifact_contract`, so the legacy artifact route now
    requires explicit operator context. The download itself still
    works; we just have to declare we know we're pulling a non-client
    artifact.
    """

    started = _start_sample_job(client)
    payload = _wait_for_terminal_job(client, started["job_id"])
    assert payload["status"] == JobStatus.COMPLETED

    response = client.get(
        f"/jobs/{started['job_id']}/artifacts/processing_report_json"
        "?operator=true"
    )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")
    assert "processing_report.json" in response.headers["content-disposition"]
    assert response.headers.get("x-trashpanda-audience") == "operator_only"
    assert response.content


def test_post_jobs_rejects_unsupported_extension(client: TestClient) -> None:
    response = client.post(
        "/jobs",
        files={"file": ("contacts.txt", b"email\nalice@example.com\n", "text/plain")},
    )

    assert response.status_code == 400
    payload = response.json()
    assert payload["error"]["error_type"] == "unsupported_file_type"
    assert ".csv" in payload["error"]["details"]["supported_extensions"]
    assert ".xlsx" in payload["error"]["details"]["supported_extensions"]


def test_post_jobs_rejects_missing_file(client: TestClient) -> None:
    response = client.post("/jobs", files={})

    assert response.status_code == 400
    payload = response.json()
    assert payload["error"]["error_type"] == "missing_file"
    assert payload["error"]["details"]["field"] == "file"


def test_get_job_not_found_returns_json_error(client: TestClient) -> None:
    response = client.get("/jobs/job_missing")

    assert response.status_code == 404
    payload = response.json()
    assert payload["error"]["error_type"] == "job_not_found"
    assert payload["error"]["details"]["job_id"] == "job_missing"


def test_get_logs_unknown_job_returns_404(client: TestClient) -> None:
    response = client.get("/jobs/job_ghost/logs")

    assert response.status_code == 404
    payload = response.json()
    assert payload["error"]["error_type"] == "job_not_found"
    assert payload["error"]["details"]["job_id"] == "job_ghost"


def test_get_logs_queued_job_returns_empty_or_lines(client: TestClient) -> None:
    """A freshly created job may not have a log file yet; endpoint must not 500."""
    payload = _start_sample_job(client)
    job_id = payload["job_id"]

    response = client.get(f"/jobs/{job_id}/logs")

    assert response.status_code == 200
    data = response.json()
    assert data["job_id"] == job_id
    assert isinstance(data["lines"], list)


def test_get_logs_completed_job_has_lines(client: TestClient) -> None:
    started = _start_sample_job(client)
    payload = _wait_for_terminal_job(client, started["job_id"])
    assert payload["status"] == JobStatus.COMPLETED

    response = client.get(f"/jobs/{started['job_id']}/logs?limit=50")

    assert response.status_code == 200
    data = response.json()
    assert data["job_id"] == started["job_id"]
    assert len(data["lines"]) > 0
    joined = " ".join(data["lines"])
    assert "Pipeline" in joined or "TIMING" in joined or "INFO" in joined


def test_get_artifact_not_found_returns_json_error(client: TestClient) -> None:
    started = _start_sample_job(client)
    payload = _wait_for_terminal_job(client, started["job_id"])
    assert payload["status"] == JobStatus.COMPLETED

    response = client.get(f"/jobs/{started['job_id']}/artifacts/not_real")

    assert response.status_code == 404
    payload = response.json()
    assert payload["error"]["error_type"] == "artifact_not_found"
    assert payload["error"]["details"]["key"] == "not_real"
