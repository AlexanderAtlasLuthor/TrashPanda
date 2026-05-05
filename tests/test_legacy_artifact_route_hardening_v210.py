"""V2.10.0.3 — Tests for legacy artifact route hardening.

These tests pin the audience guard on:

* ``GET /jobs/{job_id}/artifacts/{key}`` — must check
  :func:`app.artifact_contract.get_artifact_audience` *after* the
  on-disk path is resolved (so 404 still wins for missing keys), and
  must default-block ``operator_only`` and ``technical_debug``;
  ``internal_only`` is always blocked, even with operator context.
* ``GET /jobs/{job_id}/artifacts/zip`` — default-blocks; only
  reachable with explicit operator context, and never advertised as
  a client deliverable.
* ``GET /results/{job_id}`` — must carry the
  ``X-TrashPanda-Delivery-Contract: not-client-delivery`` marker so a
  downstream UI cannot mistake the operator-facing results page for
  the client package.

The tests run against synthetic on-disk artifacts seeded into the
in-memory ``JOB_STORE`` so they don't need to execute the cleaning
pipeline. The V2.10.0.2 safe download endpoint
(``/api/operator/jobs/{id}/client-package/download``) is intentionally
not exercised here — its tests live in
:mod:`tests.test_operator_routes_v210`.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app import server
from app.api_boundary import (
    ClientOutputs,
    JobArtifacts,
    JobResult,
    JobStatus,
    ReportFiles,
    TechnicalCsvs,
)


# --------------------------------------------------------------------------- #
# Fixtures + helpers
# --------------------------------------------------------------------------- #


@pytest.fixture()
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """Same fixture shape as the rest of the HTTP suite."""

    server.JOB_STORE.clear()
    monkeypatch.setattr(server, "RUNTIME_ROOT", tmp_path / "runtime")
    with TestClient(server.app) as test_client:
        yield test_client
    server.JOB_STORE.clear()


def _seed_completed_job_with_artifacts(
    tmp_path: Path,
    job_id: str = "job_v2_10_0_3_legacy",
) -> dict[str, Path]:
    """Create a synthetic completed job with one artifact per audience.

    Writes real bytes for each artifact key so the audience guard's
    "path must exist on disk" precondition holds. Returns a map of
    artifact key → on-disk path so tests can also verify direct file
    access if they want to.
    """

    runtime_root = tmp_path / "runtime"
    job_output_dir = runtime_root / "jobs" / job_id
    run_dir = job_output_dir / "run_20260101_120000_legacyfx"
    run_dir.mkdir(parents=True, exist_ok=True)

    # client_safe — valid_emails.xlsx.
    valid_emails = run_dir / "valid_emails.xlsx"
    valid_emails.write_bytes(b"FAKE_XLSX_CLIENT_SAFE")

    # operator_only — processing_report.json.
    processing_json = run_dir / "processing_report.json"
    processing_json.write_text("{\"ok\": true}", encoding="utf-8")

    # technical_debug — clean_high_confidence.csv. Bytes-mode write so
    # Windows text-mode CRLF translation can't turn ``\n`` into ``\r\n``
    # and trip a byte-level assertion downstream.
    clean_csv = run_dir / "clean_high_confidence.csv"
    clean_csv.write_bytes(b"email\nalice@example.com\n")

    artifacts = JobArtifacts(
        run_dir=run_dir,
        technical_csvs=TechnicalCsvs(clean_high_confidence=clean_csv),
        client_outputs=ClientOutputs(valid_emails=valid_emails),
        reports=ReportFiles(processing_report_json=processing_json),
    )

    started = datetime.now(timezone.utc)
    server.JOB_STORE.set_result(
        JobResult(
            job_id=job_id,
            status=JobStatus.COMPLETED,
            input_filename="legacy_synthetic.csv",
            run_dir=run_dir,
            summary=None,
            artifacts=artifacts,
            error=None,
            started_at=started,
            finished_at=started,
        )
    )

    return {
        "valid_emails": valid_emails,
        "processing_report_json": processing_json,
        "clean_high_confidence": clean_csv,
        "run_dir": run_dir,
    }


# --------------------------------------------------------------------------- #
# /jobs/{id}/artifacts/{key} — audience guard
# --------------------------------------------------------------------------- #


def test_artifact_route_serves_client_safe_artifact_unchanged(
    client: TestClient,
    tmp_path: Path,
) -> None:
    job_id = "job_v2_10_0_3_legacy"
    _seed_completed_job_with_artifacts(tmp_path, job_id)

    response = client.get(f"/jobs/{job_id}/artifacts/valid_emails")

    assert response.status_code == 200
    assert response.headers["x-trashpanda-audience"] == "client_safe"
    assert response.content == b"FAKE_XLSX_CLIENT_SAFE"


def test_artifact_route_blocks_operator_only_by_default(
    client: TestClient,
    tmp_path: Path,
) -> None:
    job_id = "job_v2_10_0_3_legacy"
    _seed_completed_job_with_artifacts(tmp_path, job_id)

    response = client.get(f"/jobs/{job_id}/artifacts/processing_report_json")

    assert response.status_code == 403
    body = response.json()
    assert body["error"] == "operator_artifact_requires_operator_context"
    assert body["audience"] == "operator_only"
    assert body["key"] == "processing_report_json"


def test_artifact_route_blocks_technical_debug_by_default(
    client: TestClient,
    tmp_path: Path,
) -> None:
    job_id = "job_v2_10_0_3_legacy"
    _seed_completed_job_with_artifacts(tmp_path, job_id)

    response = client.get(f"/jobs/{job_id}/artifacts/clean_high_confidence")

    assert response.status_code == 403
    body = response.json()
    assert body["error"] == "operator_artifact_requires_operator_context"
    assert body["audience"] == "technical_debug"
    assert body["key"] == "clean_high_confidence"


@pytest.mark.parametrize(
    "operator_signal",
    [
        {"params": {"operator": "true"}, "headers": {}},
        {"params": {"operator": "1"}, "headers": {}},
        {
            "params": {},
            "headers": {"X-TrashPanda-Operator-Context": "true"},
        },
    ],
)
def test_artifact_route_allows_operator_only_with_operator_context(
    client: TestClient,
    tmp_path: Path,
    operator_signal: dict,
) -> None:
    job_id = "job_v2_10_0_3_legacy"
    _seed_completed_job_with_artifacts(tmp_path, job_id)

    response = client.get(
        f"/jobs/{job_id}/artifacts/processing_report_json",
        params=operator_signal["params"],
        headers=operator_signal["headers"],
    )

    assert response.status_code == 200
    assert response.headers["x-trashpanda-audience"] == "operator_only"
    assert response.content


def test_artifact_route_allows_technical_debug_with_operator_context(
    client: TestClient,
    tmp_path: Path,
) -> None:
    job_id = "job_v2_10_0_3_legacy"
    _seed_completed_job_with_artifacts(tmp_path, job_id)

    response = client.get(
        f"/jobs/{job_id}/artifacts/clean_high_confidence?operator=true"
    )

    assert response.status_code == 200
    assert response.headers["x-trashpanda-audience"] == "technical_debug"
    assert response.content == b"email\nalice@example.com\n"


def test_artifact_route_blocks_internal_only_even_with_operator_context(
    client: TestClient,
    tmp_path: Path,
) -> None:
    """An ``internal_only`` artifact must never be served, period.

    We seed a non-canonical artifact key so the contract's conservative
    default (``internal_only``) kicks in. Even with ``?operator=true``,
    the guard refuses.
    """

    job_id = "job_v2_10_0_3_internal"
    runtime_root = tmp_path / "runtime"
    job_output_dir = runtime_root / "jobs" / job_id
    run_dir = job_output_dir / "run_20260101_120000_internal"
    run_dir.mkdir(parents=True, exist_ok=True)

    secret_path = run_dir / "internal_secret.csv"
    secret_path.write_text("internal", encoding="utf-8")

    # The DB lookup path lets us register an arbitrary key whose
    # audience the contract resolves to internal_only.
    monkeypatch_target = "app.server._db_artifact_path"

    started = datetime.now(timezone.utc)
    server.JOB_STORE.set_result(
        JobResult(
            job_id=job_id,
            status=JobStatus.COMPLETED,
            input_filename="internal.csv",
            run_dir=run_dir,
            summary=None,
            artifacts=JobArtifacts(run_dir=run_dir),
            error=None,
            started_at=started,
            finished_at=started,
        )
    )

    # Patch the DB resolver so the unknown key resolves to our path.
    import contextlib

    @contextlib.contextmanager
    def _patched_db_artifact_path():
        original = server._db_artifact_path

        def _stub(j: str, k: str, *, visibility=None, require_exists=False):
            if j == job_id and k == "internal_secret":
                return secret_path if (
                    not require_exists or secret_path.is_file()
                ) else None
            return original(
                j, k, visibility=visibility, require_exists=require_exists
            )

        server._db_artifact_path = _stub
        try:
            yield
        finally:
            server._db_artifact_path = original

    with _patched_db_artifact_path():
        # Internal-only without operator context.
        resp_default = client.get(
            f"/jobs/{job_id}/artifacts/internal_secret"
        )
        # Internal-only WITH operator context — must still be 403.
        resp_with_op = client.get(
            f"/jobs/{job_id}/artifacts/internal_secret?operator=true"
        )
        resp_with_header = client.get(
            f"/jobs/{job_id}/artifacts/internal_secret",
            headers={"X-TrashPanda-Operator-Context": "true"},
        )

    for response in (resp_default, resp_with_op, resp_with_header):
        assert response.status_code == 403
        body = response.json()
        assert body["error"] == "artifact_not_downloadable"
        assert body["audience"] == "internal_only"
        assert body["key"] == "internal_secret"

    assert monkeypatch_target  # documentation: which symbol we patched


def test_artifact_route_missing_key_still_returns_404(
    client: TestClient,
    tmp_path: Path,
) -> None:
    """The audience guard must run AFTER path resolution.

    Otherwise an unknown key (which the contract conservatively
    classifies ``internal_only``) would silently flip from 404 to 403.
    """

    job_id = "job_v2_10_0_3_legacy"
    _seed_completed_job_with_artifacts(tmp_path, job_id)

    response = client.get(f"/jobs/{job_id}/artifacts/no_such_key")

    assert response.status_code == 404
    body = response.json()
    assert body["error"]["error_type"] == "artifact_not_found"


# --------------------------------------------------------------------------- #
# /jobs/{id}/artifacts/zip — operator-only diagnostic
# --------------------------------------------------------------------------- #


def test_artifacts_zip_blocks_by_default(
    client: TestClient,
    tmp_path: Path,
) -> None:
    job_id = "job_v2_10_0_3_legacy"
    _seed_completed_job_with_artifacts(tmp_path, job_id)

    response = client.get(f"/jobs/{job_id}/artifacts/zip")

    assert response.status_code == 403
    body = response.json()
    assert body["error"] == "legacy_zip_not_client_delivery"
    assert body["audience"] == "operator_only"
    assert body.get("delivery_contract") == "not-client-delivery"
    assert "/api/operator/jobs/" in body["message"]


def test_artifacts_zip_allowed_with_operator_context(
    client: TestClient,
    tmp_path: Path,
) -> None:
    job_id = "job_v2_10_0_3_legacy"
    _seed_completed_job_with_artifacts(tmp_path, job_id)

    response = client.get(f"/jobs/{job_id}/artifacts/zip?operator=true")

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/zip"
    assert response.headers["x-trashpanda-audience"] == "operator_only"
    assert (
        response.headers["x-trashpanda-delivery-contract"]
        == "not-client-delivery"
    )
    assert "attachment" in response.headers["content-disposition"]
    assert response.content


def test_artifacts_zip_blocked_payload_points_at_safe_endpoint(
    client: TestClient,
    tmp_path: Path,
) -> None:
    """The 403 payload must steer callers toward the V2.10.0.2 safe download.

    This is the in-payload analog of the safety-boundary inspection
    rather than a code search: any caller that sees the block gets a
    plain-text pointer to the only legitimate client delivery contract.
    """

    job_id = "job_v2_10_0_3_legacy"
    _seed_completed_job_with_artifacts(tmp_path, job_id)

    body = client.get(f"/jobs/{job_id}/artifacts/zip").json()
    assert (
        "/api/operator/jobs/{job_id}/client-package/download"
        in body["message"]
    )
    assert "ready_for_client" in body["message"]


# --------------------------------------------------------------------------- #
# /results/{id} — operator-facing marker
# --------------------------------------------------------------------------- #


def test_results_endpoint_marks_response_not_client_delivery(
    client: TestClient,
    tmp_path: Path,
) -> None:
    """The /results body and headers must explicitly disclaim client delivery."""

    job_id = "job_v2_10_0_3_legacy"
    _seed_completed_job_with_artifacts(tmp_path, job_id)

    response = client.get(f"/results/{job_id}")

    assert response.status_code == 200
    assert (
        response.headers["x-trashpanda-delivery-contract"]
        == "not-client-delivery"
    )
    assert response.headers["x-trashpanda-audience"] == "operator_only"
    body = response.json()
    assert body["delivery_contract"] == "not-client-delivery"
