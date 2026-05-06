"""V2.10.18 — HTTP route tests for /batches.

Use FastAPI's TestClient against the real ``app.server.app`` so the
startup hook that wires the batch router runs naturally. The
orchestrator is stubbed out so no real subprocess spawns.
"""

from __future__ import annotations

import io
import json
import time
import zipfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app import server, batch_routes, batches as batches_mod


@pytest.fixture
def client(tmp_path: Path, monkeypatch) -> TestClient:
    # Pin the server's runtime root + reset the lazy store so the
    # tests are isolated.
    monkeypatch.setattr(server, "RUNTIME_ROOT", tmp_path)
    monkeypatch.setattr(batch_routes, "_store", None)

    # Stub the orchestrator so it writes a "completed" status file
    # without running anything heavy.
    from scripts import auto_chunked_clean as acc

    def _stub(opts):
        (opts.output_dir / "customer_bundle").mkdir(exist_ok=True)
        (opts.output_dir / "customer_bundle" / "clean_deliverable.csv").write_text(
            "email\nuser@example.com\n", encoding="utf-8",
        )
        (opts.output_dir / batches_mod.STATUS_FILENAME).write_text(
            json.dumps({
                "started_at": "2026-05-06T20:00:00Z",
                "completed_at": "2026-05-06T20:00:01Z",
                "input_file": str(opts.input_file),
                "input_format": "csv",
                "total_rows": 1,
                "threshold_rows": opts.threshold_rows,
                "chunk_size": opts.chunk_size,
                "status": "completed",
                "chunks": [],
                "merged_at": "2026-05-06T20:00:01Z",
                "merged_counts": {
                    "clean_deliverable": 1,
                    "review_provider_limited": 0,
                    "high_risk_removed": 0,
                },
                "error": None,
            }, indent=2),
            encoding="utf-8",
        )

    monkeypatch.setattr(acc, "run", _stub)

    with TestClient(server.app) as c:
        yield c


def _wait_for_status(client: TestClient, batch_id: str, target: str, timeout: float = 2.0) -> dict:
    """Poll progress until target status reached. Stub finishes in ms."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        res = client.get(f"/batches/{batch_id}/progress")
        if res.status_code == 200:
            body = res.json()
            if body["status"] == target:
                return body
        time.sleep(0.05)
    raise AssertionError(
        f"batch {batch_id} did not reach status={target} within {timeout}s"
    )


# ---------------------------------------------------------------------------
# Upload
# ---------------------------------------------------------------------------


class TestUpload:
    def test_upload_returns_batch_id(self, client: TestClient):
        res = client.post(
            "/batches/upload",
            files={"file": ("x.csv", b"email\nuser@example.com\n", "text/csv")},
            data={"chunk_size": "25", "threshold_rows": "50"},
        )
        assert res.status_code == 200
        body = res.json()
        assert body["batch_id"].startswith("batch_")
        assert body["input_filename"] == "x.csv"
        assert body["config"]["chunk_size"] == 25

    def test_upload_rejects_unsupported_extension(self, client: TestClient):
        res = client.post(
            "/batches/upload",
            files={"file": ("x.txt", b"junk", "text/plain")},
        )
        assert res.status_code == 400
        # The server has a custom HTTPException handler that wraps the
        # detail under {error: {message, ...}}.
        body = res.json()
        message = body.get("error", {}).get("message") or body.get("detail") or ""
        assert "unsupported" in message.lower()

    def test_upload_rejects_negative_chunk_size(self, client: TestClient):
        res = client.post(
            "/batches/upload",
            files={"file": ("x.csv", b"email\n", "text/csv")},
            data={"chunk_size": "-1"},
        )
        assert res.status_code == 400


# ---------------------------------------------------------------------------
# Progress / status / list
# ---------------------------------------------------------------------------


class TestProgressEndpoint:
    def test_unknown_batch_returns_404(self, client: TestClient):
        res = client.get("/batches/batch_does_not_exist/progress")
        assert res.status_code == 404

    def test_completed_batch_progress_shape(self, client: TestClient):
        res = client.post(
            "/batches/upload",
            files={"file": ("x.csv", b"email\nu@x.com\n", "text/csv")},
        )
        batch_id = res.json()["batch_id"]
        body = _wait_for_status(client, batch_id, "completed")
        # Stable contract for the UI:
        assert {
            "batch_id", "status", "n_chunks", "n_completed",
            "n_failed", "n_running", "n_pending",
            "current_chunk_index", "merged_counts",
            "started_at", "completed_at", "error",
        }.issubset(body.keys())
        assert body["status"] == "completed"
        assert body["merged_counts"]["clean_deliverable"] == 1


class TestFullStatusEndpoint:
    def test_returns_full_status_doc(self, client: TestClient):
        res = client.post(
            "/batches/upload",
            files={"file": ("x.csv", b"email\nu@x.com\n", "text/csv")},
        )
        batch_id = res.json()["batch_id"]
        _wait_for_status(client, batch_id, "completed")

        res = client.get(f"/batches/{batch_id}")
        assert res.status_code == 200
        doc = res.json()
        # The CLI's auto_chunked_status.json shape — same fields.
        assert doc["status"] == "completed"
        assert doc["merged_counts"]["clean_deliverable"] == 1


class TestListEndpoint:
    def test_list_includes_just_uploaded_batch(self, client: TestClient):
        client.post(
            "/batches/upload",
            files={"file": ("x.csv", b"email\nu@x.com\n", "text/csv")},
        )
        res = client.get("/batches")
        assert res.status_code == 200
        body = res.json()
        assert len(body["batches"]) >= 1


# ---------------------------------------------------------------------------
# Bundle download
# ---------------------------------------------------------------------------


class TestBundleDownload:
    def test_404_when_batch_unknown(self, client: TestClient):
        res = client.get("/batches/nope/customer-bundle/download")
        assert res.status_code == 404

    def test_409_when_not_yet_complete(
        self, client: TestClient, monkeypatch,
    ):
        # Override the orchestrator stub so the batch stays in "running"
        # for this test only.
        from scripts import auto_chunked_clean as acc
        from app import batches as batches_mod

        def _running(opts):
            (opts.output_dir / batches_mod.STATUS_FILENAME).write_text(
                json.dumps({"status": "running", "chunks": []}),
                encoding="utf-8",
            )

        monkeypatch.setattr(acc, "run", _running)
        res = client.post(
            "/batches/upload",
            files={"file": ("x.csv", b"email\nu@x.com\n", "text/csv")},
        )
        batch_id = res.json()["batch_id"]
        # Wait for the stub to settle.
        time.sleep(0.1)
        res = client.get(f"/batches/{batch_id}/customer-bundle/download")
        assert res.status_code == 409

    def test_returns_zip_when_complete(self, client: TestClient):
        res = client.post(
            "/batches/upload",
            files={"file": ("x.csv", b"email\nu@x.com\n", "text/csv")},
        )
        batch_id = res.json()["batch_id"]
        _wait_for_status(client, batch_id, "completed")

        res = client.get(f"/batches/{batch_id}/customer-bundle/download")
        assert res.status_code == 200
        assert res.headers["content-type"] == "application/zip"
        zf = zipfile.ZipFile(io.BytesIO(res.content))
        names = zf.namelist()
        assert any(n.endswith("clean_deliverable.csv") for n in names)
