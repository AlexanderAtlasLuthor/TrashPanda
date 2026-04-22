"""HTTP tests for the public API envelope (/upload, /status, /results).

Focus on the three simple endpoints described in the API spec, plus
size-limit enforcement. The existing `/jobs/*` surface is already
covered by tests/test_http_api.py; here we only exercise the new
public-facing routes.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from app import server
from app.api_boundary import JobStatus


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SAMPLE_CSV = PROJECT_ROOT / "examples" / "sample_contacts.csv"


pytestmark = pytest.mark.skipif(
    not SAMPLE_CSV.is_file(), reason=f"sample input missing at {SAMPLE_CSV}"
)


@pytest.fixture()
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """Each test gets a clean JOB_STORE and a disposable RUNTIME_ROOT."""
    server.JOB_STORE.clear()
    monkeypatch.setattr(server, "RUNTIME_ROOT", tmp_path / "runtime")
    with TestClient(server.app) as test_client:
        yield test_client
    server.JOB_STORE.clear()


def _upload_sample(client: TestClient) -> dict[str, Any]:
    with SAMPLE_CSV.open("rb") as handle:
        response = client.post(
            "/upload",
            files={"file": (SAMPLE_CSV.name, handle, "text/csv")},
        )
    assert response.status_code == 201, response.text
    return response.json()


# ─────────────────────────────────────────────────────────────────────── #
# POST /upload                                                            #
# ─────────────────────────────────────────────────────────────────────── #


class TestUploadEndpoint:
    def test_upload_returns_compact_payload(self, client: TestClient) -> None:
        payload = _upload_sample(client)
        assert set(payload.keys()) == {"job_id", "status", "input_filename"}
        assert payload["job_id"].startswith("job_")
        assert payload["input_filename"] == SAMPLE_CSV.name
        assert payload["status"] in {
            JobStatus.QUEUED, JobStatus.RUNNING, JobStatus.COMPLETED,
        }

    def test_missing_file_returns_400(self, client: TestClient) -> None:
        response = client.post("/upload")
        assert response.status_code == 400
        body = response.json()
        assert body["error"]["error_type"] == "missing_file"

    def test_unsupported_extension_returns_400(self, client: TestClient) -> None:
        response = client.post(
            "/upload",
            files={"file": ("notes.txt", b"hello", "text/plain")},
        )
        assert response.status_code == 400
        body = response.json()
        assert body["error"]["error_type"] == "unsupported_file_type"

    def test_empty_filename_is_rejected(self, client: TestClient) -> None:
        # Some clients transmit a file part with an empty filename. FastAPI
        # may strip it entirely; either way the request must not produce
        # a queued job.
        response = client.post(
            "/upload",
            files={"file": ("", b"x", "text/csv")},
        )
        assert 400 <= response.status_code < 500, response.text


class TestUploadSizeLimit:
    def test_upload_over_cap_returns_413(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Tighten the cap to 1024 bytes just for this test.
        monkeypatch.setattr(server, "MAX_UPLOAD_BYTES", 1024)
        big_blob = b"a,b,c\n" + b"x,y,z\n" * 1000  # ≫1024 bytes
        response = client.post(
            "/upload",
            files={"file": ("big.csv", big_blob, "text/csv")},
        )
        assert response.status_code == 413
        body = response.json()
        assert body["error"]["error_type"] == "payload_too_large"

    def test_partial_upload_is_cleaned_up_on_413(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
    ) -> None:
        monkeypatch.setattr(server, "MAX_UPLOAD_BYTES", 512)
        response = client.post(
            "/upload",
            files={"file": ("big.csv", b"x" * 5000, "text/csv")},
        )
        assert response.status_code == 413
        # No job should have been registered since the upload was aborted.
        uploads_dir = (tmp_path / "runtime" / "uploads")
        # Either the dir doesn't exist, or it contains no half-written file.
        if uploads_dir.is_dir():
            stray = [p for p in uploads_dir.rglob("*.csv") if p.is_file()]
            assert stray == [], f"orphaned upload on disk: {stray}"


# ─────────────────────────────────────────────────────────────────────── #
# GET /status/{job_id}                                                    #
# ─────────────────────────────────────────────────────────────────────── #


class TestStatusEndpoint:
    def test_status_after_upload_returns_expected_fields(
        self, client: TestClient,
    ) -> None:
        upload = _upload_sample(client)
        resp = client.get(f"/status/{upload['job_id']}")
        assert resp.status_code == 200
        body = resp.json()
        assert set(body.keys()) >= {
            "job_id", "status", "input_filename",
            "started_at", "finished_at", "error",
        }
        assert body["job_id"] == upload["job_id"]
        assert body["status"] in {
            JobStatus.QUEUED, JobStatus.RUNNING,
            JobStatus.COMPLETED, JobStatus.FAILED,
        }

    def test_status_eventually_reaches_terminal(self, client: TestClient) -> None:
        upload = _upload_sample(client)
        # TestClient runs BackgroundTasks synchronously after response:
        # by the time upload_sample returns, the pipeline has finished.
        resp = client.get(f"/status/{upload['job_id']}")
        body = resp.json()
        assert body["status"] in {JobStatus.COMPLETED, JobStatus.FAILED}

    def test_status_for_unknown_job_returns_404(self, client: TestClient) -> None:
        resp = client.get("/status/job_does_not_exist")
        assert resp.status_code == 404
        body = resp.json()
        assert body["error"]["error_type"] == "job_not_found"


# ─────────────────────────────────────────────────────────────────────── #
# GET /results/{job_id}                                                   #
# ─────────────────────────────────────────────────────────────────────── #


class TestResultsEndpoint:
    def test_results_include_buckets_and_summary(self, client: TestClient) -> None:
        upload = _upload_sample(client)
        resp = client.get(f"/results/{upload['job_id']}")
        assert resp.status_code == 200, resp.text
        body = resp.json()

        # Contract shape.
        assert set(body.keys()) >= {
            "job_id", "status", "summary", "buckets", "reports", "artifacts_zip",
        }
        assert body["status"] == JobStatus.COMPLETED
        assert body["artifacts_zip"].endswith("/artifacts/zip")

        # Three specified buckets, each with the expected shape.
        assert set(body["buckets"].keys()) == {
            "clean_high_confidence", "review", "invalid",
        }
        for key, entry in body["buckets"].items():
            assert set(entry.keys()) == {"count", "download_url", "filename"}
            # Counts are either an int or None (when summary was unavailable).
            assert entry["count"] is None or isinstance(entry["count"], int)
            # Download URL points at the /jobs/{id}/artifacts/... surface.
            if entry["download_url"] is not None:
                assert f"/jobs/{upload['job_id']}/artifacts/" in entry["download_url"]

    def test_results_summary_counts_are_consistent(self, client: TestClient) -> None:
        upload = _upload_sample(client)
        body = client.get(f"/results/{upload['job_id']}").json()
        summary = body["summary"]
        assert summary is not None
        # The total of the three bucket counts should never exceed total_input_rows.
        totals = [summary.get(k) or 0 for k in (
            "total_valid", "total_review", "total_invalid_or_bounce_risk",
        )]
        assert sum(totals) <= summary.get("total_input_rows", 0) + (
            summary.get("duplicates_removed") or 0
        )

    def test_results_download_urls_resolve_to_real_files(
        self, client: TestClient,
    ) -> None:
        upload = _upload_sample(client)
        body = client.get(f"/results/{upload['job_id']}").json()
        for key, entry in body["buckets"].items():
            if entry["download_url"] is None:
                continue
            head = client.get(entry["download_url"])
            assert head.status_code == 200, (
                f"{key} download_url {entry['download_url']} "
                f"returned {head.status_code}"
            )

    def test_results_for_unknown_job_returns_404(self, client: TestClient) -> None:
        resp = client.get("/results/no_such_job")
        assert resp.status_code == 404
        assert resp.json()["error"]["error_type"] == "job_not_found"

    def test_results_for_queued_job_returns_409(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Seed a job directly in the store in a non-terminal state so we
        # don't depend on race conditions between upload and pipeline.
        from datetime import datetime
        from app.api_boundary import JobResult
        fake = JobResult(
            job_id="job_queued_fixture",
            status=JobStatus.RUNNING,
            input_filename="stub.csv",
            run_dir=None, summary=None, artifacts=None, error=None,
            started_at=datetime.now(), finished_at=None,
        )
        server.JOB_STORE.create(fake)

        resp = client.get("/results/job_queued_fixture")
        assert resp.status_code == 409
        body = resp.json()
        assert body["error"]["error_type"] == "job_not_completed"
        assert body["error"]["details"]["status"] == JobStatus.RUNNING


# ─────────────────────────────────────────────────────────────────────── #
# Backwards-compatible surface                                            #
# ─────────────────────────────────────────────────────────────────────── #


def test_public_endpoints_coexist_with_existing_jobs_surface(
    client: TestClient,
) -> None:
    """Ensure the old /jobs/* routes still work after adding the new ones."""
    upload = _upload_sample(client)
    job_id = upload["job_id"]

    # /upload gave us the job; /jobs/{id} must still serve its old payload.
    legacy = client.get(f"/jobs/{job_id}")
    assert legacy.status_code == 200
    legacy_body = legacy.json()
    assert legacy_body["job_id"] == job_id
    # Full JobResult keys survive.
    assert {"status", "input_filename", "artifacts"}.issubset(legacy_body.keys())

    # /jobs (list) also still works.
    assert client.get("/jobs").status_code == 200
