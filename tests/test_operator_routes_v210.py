"""V2.10.0.1 — Tests for the operator HTTP scaffold.

These tests pin the contract the future operator console will rely on:

* every operator route lives under ``/api/operator``;
* missing operator-only JSON files (smtp_runtime_summary,
  artifact_consistency, operator_review_summary, client_package_manifest)
  return a 200 + structured "missing" payload, never a 500;
* the operator review summary's missing payload always reports
  ``ready_for_client=false``, so a UI cannot infer "ready" from a
  blank state;
* responses carry ``X-TrashPanda-Audience: operator_only`` so a
  misrouted client can be detected;
* unknown ``job_id`` resolves to a structured 404, not a 500.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app import server
from app.api_boundary import (
    JobArtifacts,
    JobResult,
    JobStatus,
)


@pytest.fixture()
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """Operator routes share ``server.RUNTIME_ROOT`` resolution; pin it here."""

    server.JOB_STORE.clear()
    monkeypatch.setattr(server, "RUNTIME_ROOT", tmp_path / "runtime")
    with TestClient(server.app) as test_client:
        yield test_client
    server.JOB_STORE.clear()


def _create_run_dir(tmp_path: Path, job_id: str) -> Path:
    """Create a synthetic ``runtime/jobs/<job_id>/run_*`` directory.

    Mirrors the layout :func:`server._run_job` writes so the operator
    routes can resolve a real run_dir without needing to execute the
    pipeline. Note: we intentionally do NOT write any of the
    operator-only JSONs the routes look for — that's exactly what the
    "missing payload" tests want to exercise.
    """

    runtime_root = tmp_path / "runtime"
    job_output_dir = runtime_root / "jobs" / job_id
    run_dir = job_output_dir / "run_20260101_120000_abc123"
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def _register_completed_job(run_dir: Path, job_id: str) -> None:
    """Stash a synthetic completed JobResult in the in-memory store.

    Several operator endpoints (``POST /client-package``, the
    operator-side ``GET /jobs/{id}``) defer to ``_load_job_result`` for
    status checks. Tests that need the COMPLETED branch use this to
    avoid running the real pipeline.
    """

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


# --------------------------------------------------------------------------- #
# Namespace + audience-header smoke
# --------------------------------------------------------------------------- #


def test_operator_routes_under_operator_namespace(client: TestClient) -> None:
    """Every registered route must be reachable under /api/operator."""

    operator_paths = {
        route.path
        for route in server.app.routes
        if getattr(route, "path", "").startswith("/api/operator")
    }
    expected = {
        "/api/operator/preflight",
        "/api/operator/jobs/{job_id}",
        "/api/operator/jobs/{job_id}/smtp-runtime",
        "/api/operator/jobs/{job_id}/artifact-consistency",
        "/api/operator/jobs/{job_id}/client-package",
        "/api/operator/jobs/{job_id}/review-gate",
        "/api/operator/jobs/{job_id}/operator-review",
        "/api/operator/feedback/ingest",
        "/api/operator/feedback/preview",
    }
    missing = expected - operator_paths
    assert not missing, f"missing operator routes: {missing}"


def test_operator_response_carries_audience_header(
    client: TestClient,
    tmp_path: Path,
) -> None:
    job_id = "job_test_audience_header"
    _create_run_dir(tmp_path, job_id)

    response = client.get(f"/api/operator/jobs/{job_id}/smtp-runtime")
    assert response.status_code == 200
    assert response.headers.get("x-trashpanda-audience") == "operator_only"


# --------------------------------------------------------------------------- #
# Missing-file payloads — never 500
# --------------------------------------------------------------------------- #


def test_smtp_runtime_missing_returns_safe_payload(
    client: TestClient,
    tmp_path: Path,
) -> None:
    job_id = "job_test_smtp_missing"
    _create_run_dir(tmp_path, job_id)

    response = client.get(f"/api/operator/jobs/{job_id}/smtp-runtime")

    assert response.status_code == 200
    body = response.json()
    assert body == {"status": "missing", "available": False, "warning": True}


def test_artifact_consistency_missing_returns_safe_payload(
    client: TestClient,
    tmp_path: Path,
) -> None:
    job_id = "job_test_consistency_missing"
    _create_run_dir(tmp_path, job_id)

    response = client.get(f"/api/operator/jobs/{job_id}/artifact-consistency")

    assert response.status_code == 200
    body = response.json()
    assert body == {"status": "missing", "available": False, "warning": True}


def test_client_package_missing_returns_ready_for_client_false(
    client: TestClient,
    tmp_path: Path,
) -> None:
    job_id = "job_test_pkg_missing"
    _create_run_dir(tmp_path, job_id)

    response = client.get(f"/api/operator/jobs/{job_id}/client-package")

    assert response.status_code == 200
    body = response.json()
    assert body == {
        "status": "missing",
        "available": False,
        "ready_for_client": False,
    }


def test_operator_review_missing_returns_ready_for_client_false(
    client: TestClient,
    tmp_path: Path,
) -> None:
    job_id = "job_test_review_missing"
    _create_run_dir(tmp_path, job_id)

    response = client.get(f"/api/operator/jobs/{job_id}/operator-review")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "missing"
    assert body["available"] is False
    assert body["ready_for_client"] is False
    assert body["issues"] == [
        {
            "severity": "warn",
            "code": "operator_review_missing",
            "message": "Operator review gate has not been run yet.",
        }
    ]


# --------------------------------------------------------------------------- #
# 404 / 409 paths — structured errors, no traceback leakage
# --------------------------------------------------------------------------- #


def test_unknown_job_id_returns_structured_404(client: TestClient) -> None:
    response = client.get("/api/operator/jobs/does_not_exist/smtp-runtime")

    assert response.status_code == 404
    body = response.json()
    assert body["error"]["error_type"] == "job_not_found"
    assert body["error"]["details"]["job_id"] == "does_not_exist"


def test_review_gate_unknown_job_returns_structured_404(client: TestClient) -> None:
    """POST /review-gate against an unknown job must not 500."""

    response = client.post("/api/operator/jobs/does_not_exist/review-gate")
    assert response.status_code == 404
    body = response.json()
    assert body["error"]["error_type"] == "job_not_found"


def test_client_package_build_blocks_when_job_not_completed(
    client: TestClient,
    tmp_path: Path,
) -> None:
    """The boundary helper does not check status, so the route must."""

    job_id = "job_test_pkg_not_completed"
    run_dir = _create_run_dir(tmp_path, job_id)
    started = datetime.now(timezone.utc)
    server.JOB_STORE.set_result(
        JobResult(
            job_id=job_id,
            status=JobStatus.RUNNING,
            input_filename="synthetic.csv",
            run_dir=run_dir,
            summary=None,
            artifacts=JobArtifacts(run_dir=run_dir),
            error=None,
            started_at=started,
            finished_at=None,
        )
    )

    response = client.post(f"/api/operator/jobs/{job_id}/client-package")
    assert response.status_code == 409
    body = response.json()
    assert body["error"]["error_type"] == "job_not_completed"


# --------------------------------------------------------------------------- #
# Review-gate happy path against a completed job with no package built yet
# --------------------------------------------------------------------------- #


def test_review_gate_against_completed_job_returns_block_status(
    client: TestClient,
    tmp_path: Path,
) -> None:
    """No client package yet → gate must surface a structured block.

    We do not 500 just because the gate inputs are incomplete; the
    block is reported as an issue inside the result so the operator UI
    can render it.
    """

    job_id = "job_test_gate_block"
    run_dir = _create_run_dir(tmp_path, job_id)
    _register_completed_job(run_dir, job_id)

    response = client.post(f"/api/operator/jobs/{job_id}/review-gate")
    assert response.status_code == 200
    body = response.json()
    assert body["ready_for_client"] is False
    assert body["status"] in {"block", "warn"}
    assert isinstance(body.get("issues"), list)
    codes = {issue["code"] for issue in body["issues"]}
    assert "client_package_missing" in codes


# --------------------------------------------------------------------------- #
# Preflight body validation
# --------------------------------------------------------------------------- #


def test_preflight_rejects_missing_input_path(client: TestClient) -> None:
    response = client.post("/api/operator/preflight", json={})
    assert response.status_code == 400
    body = response.json()
    assert body["error"]["error_type"] == "missing_input_path"


def test_preflight_rejects_non_json_body(client: TestClient) -> None:
    response = client.post(
        "/api/operator/preflight",
        content=b"not json",
        headers={"content-type": "application/json"},
    )
    assert response.status_code == 400


# --------------------------------------------------------------------------- #
# Feedback ingest body validation
# --------------------------------------------------------------------------- #


def test_feedback_ingest_rejects_missing_csv_path(client: TestClient) -> None:
    response = client.post("/api/operator/feedback/ingest", json={})
    assert response.status_code == 400
    body = response.json()
    assert body["error"]["error_type"] == "missing_feedback_csv_path"


def test_feedback_ingest_rejects_nonexistent_csv(
    client: TestClient,
    tmp_path: Path,
) -> None:
    bogus = tmp_path / "no_such_feedback.csv"
    response = client.post(
        "/api/operator/feedback/ingest",
        json={"feedback_csv_path": str(bogus)},
    )
    assert response.status_code == 404
    body = response.json()
    assert body["error"]["error_type"] == "feedback_csv_not_found"


# --------------------------------------------------------------------------- #
# Feedback preview safe-payload (store missing or empty)
# --------------------------------------------------------------------------- #


def test_feedback_preview_handles_missing_store(
    client: TestClient,
    tmp_path: Path,
) -> None:
    """Pointing the preview at a non-existent store is non-fatal."""

    bogus_store = tmp_path / "no_such_store.sqlite"
    response = client.post(
        "/api/operator/feedback/preview",
        json={"feedback_store_path": str(bogus_store)},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["feedback_available"] is False
    assert isinstance(body.get("warnings"), list)
    assert "feedback_store_missing" in body["warnings"]
