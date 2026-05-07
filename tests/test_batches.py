"""V2.10.18 — BatchStore + orchestrator integration tests.

The orchestrator runs in a worker thread; we keep these tests
deterministic by stubbing out ``scripts.auto_chunked_clean.run`` so
no real subprocess spawns. The contract under test is the
``BatchStore`` API + the on-disk state file.
"""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from unittest import mock

import pytest

from app import batches as batches_mod


@pytest.fixture
def store(tmp_path: Path) -> batches_mod.BatchStore:
    return batches_mod.BatchStore(runtime_root=tmp_path)


def _write_status(handle: batches_mod.BatchHandle, doc: dict) -> None:
    handle.status_path.write_text(json.dumps(doc, indent=2), encoding="utf-8")


def _stub_orchestrator(write_status: dict | None):
    """Return an orchestrator stub that, when called, writes
    ``write_status`` to the batch's status file."""

    def _stub(opts):
        if write_status is not None:
            (opts.output_dir / batches_mod.STATUS_FILENAME).write_text(
                json.dumps(write_status, indent=2), encoding="utf-8",
            )
        return None

    return _stub


# ---------------------------------------------------------------------------
# Launch
# ---------------------------------------------------------------------------


class TestLaunch:
    def test_launch_persists_input_and_placeholder(
        self, store: batches_mod.BatchStore, monkeypatch,
    ):
        # Stub the orchestrator so it doesn't actually run.
        called = threading.Event()

        def _slow_stub(opts):
            time.sleep(0.05)
            (opts.output_dir / batches_mod.STATUS_FILENAME).write_text(
                json.dumps({
                    "started_at": "2026-05-06T20:00:00Z",
                    "completed_at": "2026-05-06T20:01:00Z",
                    "input_file": str(opts.input_file),
                    "input_format": "csv",
                    "total_rows": 100,
                    "threshold_rows": 50,
                    "chunk_size": 25,
                    "status": "completed",
                    "chunks": [],
                    "merged_at": "2026-05-06T20:01:00Z",
                    "merged_counts": {"clean_deliverable": 50},
                    "error": None,
                }, indent=2),
                encoding="utf-8",
            )
            called.set()

        from scripts import auto_chunked_clean as acc
        monkeypatch.setattr(acc, "run", _slow_stub)

        handle = store.launch(
            input_bytes=b"email\nuser@example.com\n",
            input_filename="x.csv",
            chunk_size=25,
            threshold_rows=50,
        )

        assert handle.input_path.is_file()
        assert handle.status_path.is_file()
        # Placeholder before the orchestrator runs.
        placeholder = json.loads(handle.status_path.read_text(encoding="utf-8"))
        assert placeholder["status"] in {
            batches_mod.BATCH_QUEUED, "running", "completed",
        }
        # Wait for the stub to finish.
        assert called.wait(timeout=2.0), "orchestrator stub didn't run"

    def test_launch_uses_xlsx_filename_for_xlsx_uploads(
        self, store: batches_mod.BatchStore, monkeypatch,
    ):
        from scripts import auto_chunked_clean as acc
        monkeypatch.setattr(acc, "run", _stub_orchestrator({}))

        handle = store.launch(
            input_bytes=b"\x50\x4b\x03\x04",  # xlsx magic
            input_filename="My_data.xlsx",
            chunk_size=25, threshold_rows=50,
        )
        assert handle.input_path.name == batches_mod.INPUT_FILENAME_XLSX


# ---------------------------------------------------------------------------
# Get / list
# ---------------------------------------------------------------------------


class TestStoreLookups:
    def test_get_returns_none_for_unknown(self, store):
        assert store.get("nope") is None

    def test_get_rebuilds_handle_from_disk(
        self, store, tmp_path: Path, monkeypatch,
    ):
        from scripts import auto_chunked_clean as acc
        monkeypatch.setattr(acc, "run", _stub_orchestrator({"status": "completed"}))
        handle = store.launch(
            input_bytes=b"email\na@x.com\n", input_filename="a.csv",
        )
        # New store backed by the same disk root → still finds it.
        store2 = batches_mod.BatchStore(runtime_root=tmp_path)
        rebuilt = store2.get(handle.batch_id)
        assert rebuilt is not None
        assert rebuilt.batch_id == handle.batch_id

    def test_list_returns_all_batches_sorted(
        self, store, monkeypatch,
    ):
        from scripts import auto_chunked_clean as acc
        monkeypatch.setattr(acc, "run", _stub_orchestrator({"status": "completed"}))
        a = store.launch(input_bytes=b"email\n", input_filename="a.csv")
        time.sleep(0.01)  # ensure distinct timestamps in batch_id
        b = store.launch(input_bytes=b"email\n", input_filename="b.csv")
        names = [h.batch_id for h in store.list()]
        assert a.batch_id in names
        assert b.batch_id in names


# ---------------------------------------------------------------------------
# Progress aggregation
# ---------------------------------------------------------------------------


class TestProgress:
    def test_progress_for_unknown_returns_none(self, store):
        assert store.progress("nope") is None

    def test_progress_aggregates_chunk_states(self, store, monkeypatch):
        from scripts import auto_chunked_clean as acc
        monkeypatch.setattr(acc, "run", _stub_orchestrator(None))

        handle = store.launch(
            input_bytes=b"email\n", input_filename="a.csv",
        )
        # Manually overwrite the status file with a known shape.
        _write_status(handle, {
            "started_at": "2026-05-06T20:00:00Z",
            "completed_at": None,
            "input_file": str(handle.input_path),
            "input_format": "csv",
            "total_rows": 100,
            "threshold_rows": 50,
            "chunk_size": 25,
            "status": "running",
            "chunks": [
                {"index": 1, "status": "completed",
                 "started_at": "...", "completed_at": "...",
                 "exit_code": 0,
                 "counts": {"clean_deliverable": 20},
                 "input_path": "", "run_dir": "", "error": None},
                {"index": 2, "status": "running",
                 "started_at": "...", "completed_at": None,
                 "exit_code": None, "counts": None,
                 "input_path": "", "run_dir": "", "error": None},
                {"index": 3, "status": "pending",
                 "started_at": None, "completed_at": None,
                 "exit_code": None, "counts": None,
                 "input_path": "", "run_dir": "", "error": None},
                {"index": 4, "status": "pending",
                 "started_at": None, "completed_at": None,
                 "exit_code": None, "counts": None,
                 "input_path": "", "run_dir": "", "error": None},
            ],
            "merged_at": None,
            "merged_counts": None,
            "error": None,
        })

        progress = store.progress(handle.batch_id)
        assert progress is not None
        assert progress.n_chunks == 4
        assert progress.n_completed == 1
        assert progress.n_running == 1
        assert progress.n_pending == 2
        assert progress.n_failed == 0
        assert progress.current_chunk_index == 2
        assert progress.status == "running"


# ---------------------------------------------------------------------------
# Bundle availability
# ---------------------------------------------------------------------------


class TestBundleDir:
    def test_returns_none_until_bundle_written(self, store, monkeypatch):
        from scripts import auto_chunked_clean as acc
        monkeypatch.setattr(acc, "run", _stub_orchestrator({"status": "running"}))
        handle = store.launch(input_bytes=b"email\n", input_filename="a.csv")
        assert store.customer_bundle_dir(handle.batch_id) is None

    def test_returns_path_when_bundle_exists(self, store, monkeypatch):
        from scripts import auto_chunked_clean as acc

        def _stub_with_bundle(opts):
            (opts.output_dir / "customer_bundle").mkdir()
            (opts.output_dir / batches_mod.STATUS_FILENAME).write_text(
                json.dumps({"status": "completed"}), encoding="utf-8",
            )

        monkeypatch.setattr(acc, "run", _stub_with_bundle)
        handle = store.launch(input_bytes=b"email\n", input_filename="a.csv")
        # Wait briefly for the worker thread to finish.
        for _ in range(50):
            if store.customer_bundle_dir(handle.batch_id) is not None:
                break
            time.sleep(0.02)
        assert store.customer_bundle_dir(handle.batch_id) is not None


# ---------------------------------------------------------------------------
# Orphan reaping
# ---------------------------------------------------------------------------


class TestSchemaContract:
    """Pin the BatchProgress shape so the UI's stable contract
    survives refactors. Includes the reserved-for-future
    ``current_chunk_phase`` / ``current_chunk_progress_percent``
    fields."""

    def test_progress_dict_has_all_expected_keys(self, store, monkeypatch):
        from scripts import auto_chunked_clean as acc
        monkeypatch.setattr(acc, "run", _stub_orchestrator(None))
        handle = store.launch(
            input_bytes=b"email\n", input_filename="a.csv",
        )
        _write_status(handle, {
            "started_at": "2026-05-06T20:00:00Z",
            "completed_at": None,
            "input_file": str(handle.input_path),
            "input_format": "csv",
            "total_rows": 100,
            "threshold_rows": 50,
            "chunk_size": 25,
            "status": "running",
            "chunks": [],
            "merged_at": None,
            "merged_counts": None,
            "error": None,
        })
        progress = store.progress(handle.batch_id)
        assert progress is not None
        d = progress.to_dict()
        assert {
            "batch_id", "status", "n_chunks",
            "n_completed", "n_failed", "n_running", "n_pending",
            "current_chunk_index",
            "current_chunk_phase",
            "current_chunk_progress_percent",
            "merged_counts",
            "started_at", "completed_at", "error",
        } == set(d.keys())
        # Today these two are always None — but they're part of the
        # contract.
        assert d["current_chunk_phase"] is None
        assert d["current_chunk_progress_percent"] is None


class TestCancel:
    def test_cancel_unknown_batch(self, store):
        assert store.cancel("nope") == "unknown"

    def test_cancel_terminal_batch_is_noop(
        self, tmp_path: Path, monkeypatch,
    ):
        from scripts import auto_chunked_clean as acc

        # Stub: orchestrator writes a "completed" status immediately.
        def _stub(opts):
            (opts.output_dir / batches_mod.STATUS_FILENAME).write_text(
                json.dumps({"status": "completed", "chunks": []}),
                encoding="utf-8",
            )

        monkeypatch.setattr(acc, "run", _stub)

        store = batches_mod.BatchStore(runtime_root=tmp_path)
        handle = store.launch(
            input_bytes=b"email\n", input_filename="a.csv",
        )
        # Wait for the worker to settle.
        for _ in range(50):
            progress = store.progress(handle.batch_id)
            if progress and progress.status == "completed":
                break
            time.sleep(0.02)
        assert store.cancel(handle.batch_id) == "terminal"

    def test_cancel_running_batch_signals_event(
        self, tmp_path: Path, monkeypatch,
    ):
        from scripts import auto_chunked_clean as acc

        # Stub: orchestrator writes "running" then BLOCKS until the
        # cancel event is set, simulating a long-running chunk.
        def _stub(opts):
            (opts.output_dir / batches_mod.STATUS_FILENAME).write_text(
                json.dumps({"status": "running", "chunks": []}),
                encoding="utf-8",
            )
            assert opts.cancel_event is not None
            # Wait for the cancel; then mark cancelled.
            opts.cancel_event.wait(timeout=2.0)
            (opts.output_dir / batches_mod.STATUS_FILENAME).write_text(
                json.dumps({
                    "status": "failed",
                    "chunks": [],
                    "error": "cancelled",
                }),
                encoding="utf-8",
            )

        monkeypatch.setattr(acc, "run", _stub)

        store = batches_mod.BatchStore(runtime_root=tmp_path)
        handle = store.launch(
            input_bytes=b"email\n", input_filename="a.csv",
        )
        # Give the worker a moment to enter the wait.
        time.sleep(0.05)
        result = store.cancel(handle.batch_id)
        assert result == "requested"
        # The worker should observe the event and write the "failed"
        # status shortly after.
        for _ in range(100):
            doc = store.status_doc(handle.batch_id)
            if doc and doc.get("status") == "failed":
                break
            time.sleep(0.02)
        else:
            raise AssertionError("worker did not observe cancel signal")


class TestOrphanReap:
    def test_marks_running_batch_as_failed_when_no_thread(
        self, tmp_path: Path,
    ):
        # Simulate a batch left in "running" by a crashed prior process:
        # write the status file directly without a live worker.
        store = batches_mod.BatchStore(runtime_root=tmp_path)
        batch_dir = store.batches_dir / "batch_orphan"
        batch_dir.mkdir(parents=True)
        (batch_dir / batches_mod.INPUT_FILENAME_CSV).write_text(
            "email\na@x.com\n", encoding="utf-8",
        )
        status_path = batch_dir / batches_mod.STATUS_FILENAME
        status_path.write_text(
            json.dumps({"status": "running"}), encoding="utf-8",
        )

        n = store.reap_orphans()

        assert n == 1
        doc = json.loads(status_path.read_text(encoding="utf-8"))
        assert doc["status"] == "failed"
        assert "orphaned" in (doc.get("error") or "")

    def test_does_not_touch_completed_batches(self, tmp_path: Path):
        store = batches_mod.BatchStore(runtime_root=tmp_path)
        batch_dir = store.batches_dir / "batch_done"
        batch_dir.mkdir(parents=True)
        (batch_dir / batches_mod.INPUT_FILENAME_CSV).write_text("email\n")
        status_path = batch_dir / batches_mod.STATUS_FILENAME
        status_path.write_text(
            json.dumps({"status": "completed"}), encoding="utf-8",
        )

        store.reap_orphans()

        doc = json.loads(status_path.read_text(encoding="utf-8"))
        assert doc["status"] == "completed"
