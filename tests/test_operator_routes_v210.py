"""V2.10.0.1 / V2.10.0.2 — Tests for the operator HTTP scaffold.

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
* unknown ``job_id`` resolves to a structured 404, not a 500;
* the V2.10.0.2 download endpoint is the only HTTP contract that emits
  a client-safe artifact bundle, and every gate (review summary
  presence, ``ready_for_client=true``, package dir presence, manifest
  presence/readability, audience filter, path-escape check) blocks
  with a 409 + flat ``ready_for_client=false`` payload before any
  bytes leave the server.
"""

from __future__ import annotations

import io
import json
import zipfile
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


# --------------------------------------------------------------------------- #
# V2.10.0.2 — Safe client package download
# --------------------------------------------------------------------------- #


_DOWNLOAD_PATH_TEMPLATE = "/api/operator/jobs/{job_id}/client-package/download"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _setup_ready_package(
    run_dir: Path,
    *,
    summary_overrides: dict | None = None,
    manifest_overrides: dict | None = None,
    write_summary: bool = True,
    write_manifest: bool = True,
    write_package_dir: bool = True,
) -> Path:
    """Build a baseline ``ready_for_client=true`` run on disk.

    Each download test starts from this and then mutates exactly one
    gate so the failure under test is the only deviation from the
    happy path. Returns the package dir for convenience.
    """

    package_dir = run_dir / "client_delivery_package"
    if write_package_dir:
        package_dir.mkdir(parents=True, exist_ok=True)
        valid = package_dir / "valid_emails.xlsx"
        valid.write_bytes(b"FAKE_XLSX_BYTES_FOR_V2_10_0_2_TESTS")
        review = package_dir / "review_emails.xlsx"
        review.write_bytes(b"REVIEW_BYTES")

    if write_manifest and write_package_dir:
        manifest = {
            "report_version": "v2.9.6",
            "generated_at": "2026-05-05T00:00:00+00:00",
            "files_included": [
                {
                    "key": "valid_emails",
                    "filename": "valid_emails.xlsx",
                    "audience": "client_safe",
                    "size_bytes": 35,
                },
                {
                    "key": "review_emails",
                    "filename": "review_emails.xlsx",
                    "audience": "client_safe",
                    "size_bytes": 12,
                },
            ],
            "files_excluded": [],
            "warnings": [],
            "safe_count": 1,
            "review_count": 1,
            "rejected_count": 0,
        }
        if manifest_overrides:
            manifest.update(manifest_overrides)
        _write_json(
            package_dir / "client_package_manifest.json", manifest
        )

    if write_summary:
        summary = {
            "report_version": "v2.9.7",
            "ready_for_client": True,
            "status": "ready",
            "issues": [],
        }
        if summary_overrides:
            summary.update(summary_overrides)
        _write_json(run_dir / "operator_review_summary.json", summary)

    return package_dir


def _zip_namelist(content: bytes) -> set[str]:
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        return set(zf.namelist())


def test_download_blocks_when_operator_review_missing(
    client: TestClient,
    tmp_path: Path,
) -> None:
    """Test 1 — package present, summary absent: blocks with 409."""

    job_id = "job_dl_missing_review"
    run_dir = _create_run_dir(tmp_path, job_id)
    _setup_ready_package(run_dir, write_summary=False)

    response = client.get(_DOWNLOAD_PATH_TEMPLATE.format(job_id=job_id))

    assert response.status_code == 409
    body = response.json()
    assert body["error"] == "operator_review_missing"
    assert body["ready_for_client"] is False


def test_download_blocks_when_ready_for_client_false(
    client: TestClient,
    tmp_path: Path,
) -> None:
    """Test 2 — review summary says ``ready_for_client=false``."""

    job_id = "job_dl_not_ready"
    run_dir = _create_run_dir(tmp_path, job_id)
    _setup_ready_package(
        run_dir,
        summary_overrides={"ready_for_client": False, "status": "warn"},
    )

    response = client.get(_DOWNLOAD_PATH_TEMPLATE.format(job_id=job_id))

    assert response.status_code == 409
    body = response.json()
    assert body["error"] == "not_ready_for_client"
    assert body["ready_for_client"] is False
    assert body["status"] == "warn"


@pytest.mark.parametrize("status_value", ["block", "missing", None])
def test_download_blocks_other_non_ready_statuses(
    client: TestClient,
    tmp_path: Path,
    status_value: object,
) -> None:
    """Test 2b — block / missing / null status all gate the download."""

    job_id = f"job_dl_status_{status_value}"
    run_dir = _create_run_dir(tmp_path, job_id)
    overrides: dict[str, object] = {"ready_for_client": False}
    if status_value is not None:
        overrides["status"] = status_value
    else:
        overrides["status"] = None
    _setup_ready_package(run_dir, summary_overrides=overrides)

    response = client.get(_DOWNLOAD_PATH_TEMPLATE.format(job_id=job_id))

    assert response.status_code == 409
    body = response.json()
    assert body["error"] == "not_ready_for_client"
    assert body["ready_for_client"] is False


def test_download_blocks_when_ready_field_missing(
    client: TestClient,
    tmp_path: Path,
) -> None:
    """Missing ``ready_for_client`` must not be inferred as true."""

    job_id = "job_dl_ready_missing"
    run_dir = _create_run_dir(tmp_path, job_id)
    _setup_ready_package(run_dir, write_summary=False)
    _write_json(
        run_dir / "operator_review_summary.json",
        {"status": "ready"},
    )

    response = client.get(_DOWNLOAD_PATH_TEMPLATE.format(job_id=job_id))

    assert response.status_code == 409
    body = response.json()
    assert body["error"] == "not_ready_for_client"
    assert body["ready_for_client"] is False


def test_download_blocks_when_package_dir_missing(
    client: TestClient,
    tmp_path: Path,
) -> None:
    """Test 3 — review says ready but no client_delivery_package/."""

    job_id = "job_dl_pkg_missing"
    run_dir = _create_run_dir(tmp_path, job_id)
    _setup_ready_package(run_dir, write_package_dir=False)

    response = client.get(_DOWNLOAD_PATH_TEMPLATE.format(job_id=job_id))

    assert response.status_code == 409
    body = response.json()
    assert body["error"] == "client_package_missing"
    assert body["ready_for_client"] is False


def test_download_blocks_when_manifest_missing(
    client: TestClient,
    tmp_path: Path,
) -> None:
    """Test 4 — package dir exists but manifest absent."""

    job_id = "job_dl_manifest_missing"
    run_dir = _create_run_dir(tmp_path, job_id)
    package_dir = _setup_ready_package(run_dir, write_manifest=False)
    assert not (package_dir / "client_package_manifest.json").exists()

    response = client.get(_DOWNLOAD_PATH_TEMPLATE.format(job_id=job_id))

    assert response.status_code == 409
    body = response.json()
    assert body["error"] == "client_package_manifest_missing"
    assert body["ready_for_client"] is False


def test_download_blocks_when_manifest_unreadable(
    client: TestClient,
    tmp_path: Path,
) -> None:
    """Corrupt manifest must surface ``client_package_manifest_unreadable``."""

    job_id = "job_dl_manifest_unreadable"
    run_dir = _create_run_dir(tmp_path, job_id)
    package_dir = _setup_ready_package(run_dir)
    (package_dir / "client_package_manifest.json").write_text(
        "this is not json {", encoding="utf-8"
    )

    response = client.get(_DOWNLOAD_PATH_TEMPLATE.format(job_id=job_id))

    assert response.status_code == 409
    body = response.json()
    assert body["error"] == "client_package_manifest_unreadable"
    assert body["ready_for_client"] is False


@pytest.mark.parametrize(
    "bad_audience",
    ["operator_only", "technical_debug", "internal_only", "unknown", None],
)
def test_download_blocks_non_client_safe_in_manifest(
    client: TestClient,
    tmp_path: Path,
    bad_audience: object,
) -> None:
    """Test 5 — any non-client_safe entry in files_included blocks."""

    job_id = f"job_dl_audience_{bad_audience}"
    run_dir = _create_run_dir(tmp_path, job_id)
    package_dir = _setup_ready_package(run_dir)

    manifest = json.loads(
        (package_dir / "client_package_manifest.json").read_text(
            encoding="utf-8"
        )
    )
    manifest["files_included"].append(
        {
            "key": "leaked",
            "filename": "leaked_artifact.json",
            "audience": bad_audience,
            "size_bytes": 0,
        }
    )
    _write_json(package_dir / "client_package_manifest.json", manifest)

    response = client.get(_DOWNLOAD_PATH_TEMPLATE.format(job_id=job_id))

    assert response.status_code == 409
    body = response.json()
    assert body["error"] == "client_package_contains_non_client_safe"
    assert body["ready_for_client"] is False
    bad = body.get("bad_files") or []
    assert any(entry.get("filename") == "leaked_artifact.json" for entry in bad)


@pytest.mark.parametrize(
    "escaping_filename",
    [
        "../operator_review_summary.json",
        "subdir/../../escape.txt",
        "/etc/passwd",
    ],
)
def test_download_blocks_path_traversal_in_manifest(
    client: TestClient,
    tmp_path: Path,
    escaping_filename: str,
) -> None:
    """Test 6 — manifest filenames that escape the package dir block."""

    job_id = "job_dl_path_escape"
    run_dir = _create_run_dir(tmp_path, job_id)
    package_dir = _setup_ready_package(run_dir)

    manifest = json.loads(
        (package_dir / "client_package_manifest.json").read_text(
            encoding="utf-8"
        )
    )
    manifest["files_included"] = [
        {
            "key": "escape",
            "filename": escaping_filename,
            "audience": "client_safe",
            "size_bytes": 0,
        }
    ]
    _write_json(package_dir / "client_package_manifest.json", manifest)

    response = client.get(_DOWNLOAD_PATH_TEMPLATE.format(job_id=job_id))

    assert response.status_code == 409
    body = response.json()
    assert body["error"] == "client_package_path_escape"
    assert body["ready_for_client"] is False


def test_download_succeeds_for_ready_package(
    client: TestClient,
    tmp_path: Path,
) -> None:
    """Test 7 — full happy path returns a client_safe ZIP."""

    job_id = "job_dl_happy"
    run_dir = _create_run_dir(tmp_path, job_id)
    package_dir = _setup_ready_package(run_dir)

    # Sentinel files in the run_dir that MUST NOT leak into the ZIP.
    (run_dir / "smtp_runtime_summary.json").write_text(
        "{\"smtp_candidates_seen\": 1}", encoding="utf-8"
    )
    (run_dir / "artifact_consistency.json").write_text(
        "{}", encoding="utf-8"
    )
    (run_dir / "staging.sqlite3").write_bytes(b"SQLITE_PAYLOAD")
    logs_dir = run_dir / "logs"
    logs_dir.mkdir(exist_ok=True)
    (logs_dir / "run.log").write_text("log line\n", encoding="utf-8")
    temp_dir = run_dir / "temp"
    temp_dir.mkdir(exist_ok=True)
    (temp_dir / "scratch.bin").write_bytes(b"scratch")

    response = client.get(_DOWNLOAD_PATH_TEMPLATE.format(job_id=job_id))

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/zip"
    assert (
        "trashpanda_client_delivery_package"
        in response.headers["content-disposition"]
    )
    assert job_id in response.headers["content-disposition"]
    assert response.headers["x-trashpanda-audience"] == "client_safe"

    names = _zip_namelist(response.content)
    # Every package-dir file is present (including the manifest itself,
    # which is part of the package and audience-classified safe by the
    # builder).
    assert "valid_emails.xlsx" in names
    assert "review_emails.xlsx" in names
    assert "client_package_manifest.json" in names

    # And nothing from the run_dir leaks in.
    forbidden = {
        "operator_review_summary.json",
        "smtp_runtime_summary.json",
        "artifact_consistency.json",
        "staging.sqlite3",
    }
    assert names.isdisjoint(forbidden), (
        f"forbidden run-dir files leaked into ZIP: {names & forbidden}"
    )
    # No log/ or temp/ entries either.
    assert not any(
        n.startswith("logs/") or n.startswith("logs\\") for n in names
    )
    assert not any(
        n.startswith("temp/") or n.startswith("temp\\") for n in names
    )

    # Sanity: the package_dir itself isn't named in the archive — every
    # entry is package-relative.
    assert all(not n.startswith("client_delivery_package") for n in names)
    # And the resolved package dir really did contain those files.
    assert (package_dir / "valid_emails.xlsx").is_file()


def test_download_unknown_job_returns_structured_404(
    client: TestClient,
) -> None:
    """Test 8 — unknown job_id reuses ``_resolve_run_dir`` 404."""

    response = client.get(
        _DOWNLOAD_PATH_TEMPLATE.format(job_id="does_not_exist")
    )
    assert response.status_code == 404
    body = response.json()
    assert body["error"]["error_type"] == "job_not_found"
    assert body["error"]["details"]["job_id"] == "does_not_exist"
