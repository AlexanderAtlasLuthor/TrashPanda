"""V2.10.18 — HTTP-triggered batch jobs (Fase 2 of auto-chunked).

A batch wraps the same orchestrator that ``scripts/auto_chunked_clean``
exposes from the CLI, but runs it as a background thread inside the
FastAPI process and exposes its state file as a polling endpoint for
the UI.

Why a thread, not a subprocess at this level: each chunk inside the
batch ALREADY runs as its own subprocess (that's where OOM-safety
lives). The orchestrator itself is light — it shells out, reads
state, writes JSON. A thread is enough to host it.

Persistence
-----------

Every batch lives in ``runtime/batches/<batch_id>/`` with:

* ``input.csv`` (or ``.xlsx``) — the original upload, preserved.
* ``auto_chunked_status.json`` — the live state file (same shape
  as the CLI). Background thread writes; HTTP handlers read.
* ``_chunks/`` — per-chunk run dirs (mirrors the CLI).
* ``customer_bundle/`` — the merged final bundle (same shape).

The state file is the single source of truth. If the FastAPI process
restarts mid-batch, the in-memory ``BatchStore`` is rebuilt from the
state files on disk; any batch in ``running`` state at startup is
considered orphaned and gets marked ``failed`` with a recovery
message (operator can click "retry" / re-launch).
"""

from __future__ import annotations

import json
import logging
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from .customer_bundle import CUSTOMER_BUNDLE_DIRNAME


_LOGGER = logging.getLogger(__name__)


BATCH_DIRNAME: str = "batches"
INPUT_FILENAME_CSV: str = "input.csv"
INPUT_FILENAME_XLSX: str = "input.xlsx"
STATUS_FILENAME: str = "auto_chunked_status.json"


# Status enum mirrors scripts/auto_chunked_clean to keep the contract
# stable. We re-declare them here so consumers don't have to import
# from a scripts/ module.
BATCH_QUEUED: str = "queued"
BATCH_RUNNING: str = "running"
BATCH_COMPLETED: str = "completed"
BATCH_FAILED: str = "failed"
BATCH_PARTIAL_FAILURE: str = "partial_failure"


@dataclass(frozen=True, slots=True)
class BatchHandle:
    """A reference to a launched batch. Cheap to pass around; the
    real state lives on disk."""

    batch_id: str
    batch_dir: Path
    input_path: Path
    status_path: Path
    created_at: str


@dataclass(frozen=True, slots=True)
class BatchProgress:
    """Lightweight aggregate snapshot for the polling endpoint."""

    batch_id: str
    status: str
    n_chunks: int
    n_completed: int
    n_failed: int
    n_running: int
    n_pending: int
    current_chunk_index: int | None
    merged_counts: dict | None
    started_at: str | None
    completed_at: str | None
    error: str | None

    def to_dict(self) -> dict:
        return {
            "batch_id": self.batch_id,
            "status": self.status,
            "n_chunks": self.n_chunks,
            "n_completed": self.n_completed,
            "n_failed": self.n_failed,
            "n_running": self.n_running,
            "n_pending": self.n_pending,
            "current_chunk_index": self.current_chunk_index,
            "merged_counts": self.merged_counts,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "error": self.error,
        }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _utcnow() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _new_batch_id() -> str:
    # Short, URL-safe, sortable-by-time prefix.
    return f"batch_{datetime.now(tz=timezone.utc).strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:8]}"


def _read_status(path: Path) -> dict | None:
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def _aggregate(batch_id: str, status_doc: dict | None) -> BatchProgress:
    """Build a ``BatchProgress`` snapshot from a parsed status file."""
    if status_doc is None:
        return BatchProgress(
            batch_id=batch_id,
            status=BATCH_QUEUED,
            n_chunks=0, n_completed=0, n_failed=0,
            n_running=0, n_pending=0,
            current_chunk_index=None,
            merged_counts=None,
            started_at=None,
            completed_at=None,
            error=None,
        )
    chunks = status_doc.get("chunks") or []
    n_completed = sum(1 for c in chunks if c.get("status") == "completed")
    n_failed = sum(1 for c in chunks if c.get("status") == "failed")
    n_running = sum(1 for c in chunks if c.get("status") == "running")
    n_pending = sum(1 for c in chunks if c.get("status") == "pending")
    current_idx: int | None = None
    for c in chunks:
        if c.get("status") == "running":
            current_idx = int(c.get("index") or 0) or None
            break
    return BatchProgress(
        batch_id=batch_id,
        status=status_doc.get("status") or BATCH_QUEUED,
        n_chunks=len(chunks),
        n_completed=n_completed,
        n_failed=n_failed,
        n_running=n_running,
        n_pending=n_pending,
        current_chunk_index=current_idx,
        merged_counts=status_doc.get("merged_counts"),
        started_at=status_doc.get("started_at"),
        completed_at=status_doc.get("completed_at"),
        error=status_doc.get("error"),
    )


# ---------------------------------------------------------------------------
# Orchestrator runner
# ---------------------------------------------------------------------------


def _run_orchestrator_in_thread(
    handle: BatchHandle,
    *,
    chunk_size: int,
    threshold_rows: int,
    allow_partial: bool,
    cleanup: bool,
) -> None:
    """Body of the worker thread. Imports ``auto_chunked_clean``
    lazily so the import cost doesn't pay on every FastAPI startup."""
    try:
        from scripts.auto_chunked_clean import (
            OrchestratorOptions,
            run as run_orchestrator,
        )

        opts = OrchestratorOptions(
            input_file=handle.input_path,
            output_dir=handle.batch_dir,
            chunk_size=chunk_size,
            threshold_rows=threshold_rows,
            allow_partial=allow_partial,
            cleanup=cleanup,
        )
        run_orchestrator(opts)
    except Exception as exc:  # pragma: no cover - defensive
        _LOGGER.exception("batch %s crashed: %s", handle.batch_id, exc)
        # Best-effort: stamp the status file with the failure so the
        # UI doesn't poll forever on a dead batch.
        existing = _read_status(handle.status_path) or {}
        existing.update({
            "status": BATCH_FAILED,
            "completed_at": _utcnow(),
            "error": f"orchestrator_crash:{type(exc).__name__}:{exc}"[:500],
        })
        try:
            handle.status_path.write_text(
                json.dumps(existing, indent=2), encoding="utf-8",
            )
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


@dataclass
class BatchStore:
    """In-memory index of launched batches plus disk-backed lookups.

    Threaded launches stash their handle here so the HTTP layer can
    look them up by id. Persistence on disk is the source of truth;
    this is a cache that survives only the FastAPI process lifetime.
    """

    runtime_root: Path
    _handles: dict[str, BatchHandle] = field(default_factory=dict)
    _threads: dict[str, threading.Thread] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    @property
    def batches_dir(self) -> Path:
        return self.runtime_root / BATCH_DIRNAME

    def launch(
        self,
        *,
        input_bytes: bytes,
        input_filename: str,
        chunk_size: int = 25_000,
        threshold_rows: int = 50_000,
        allow_partial: bool = False,
        cleanup: bool = False,
    ) -> BatchHandle:
        """Persist the upload, write a placeholder status file, and
        spawn the orchestrator thread. Returns immediately."""
        batch_id = _new_batch_id()
        batch_dir = self.batches_dir / batch_id
        batch_dir.mkdir(parents=True, exist_ok=True)

        # Persist the upload.
        suffix = Path(input_filename).suffix.lower()
        if suffix in {".xlsx", ".xls"}:
            input_path = batch_dir / INPUT_FILENAME_XLSX
        else:
            input_path = batch_dir / INPUT_FILENAME_CSV
        input_path.write_bytes(input_bytes)

        # Placeholder status so polling immediately returns something
        # meaningful.
        status_path = batch_dir / STATUS_FILENAME
        placeholder = {
            "started_at": _utcnow(),
            "completed_at": None,
            "input_file": str(input_path),
            "input_format": "xlsx" if suffix in {".xlsx", ".xls"} else "csv",
            "total_rows": 0,
            "threshold_rows": threshold_rows,
            "chunk_size": chunk_size,
            "status": BATCH_QUEUED,
            "chunks": [],
            "merged_at": None,
            "merged_counts": None,
            "error": None,
        }
        status_path.write_text(json.dumps(placeholder, indent=2), encoding="utf-8")

        handle = BatchHandle(
            batch_id=batch_id,
            batch_dir=batch_dir,
            input_path=input_path,
            status_path=status_path,
            created_at=_utcnow(),
        )

        thread = threading.Thread(
            target=_run_orchestrator_in_thread,
            kwargs={
                "handle": handle,
                "chunk_size": chunk_size,
                "threshold_rows": threshold_rows,
                "allow_partial": allow_partial,
                "cleanup": cleanup,
            },
            name=f"batch-{batch_id}",
            daemon=True,
        )

        with self._lock:
            self._handles[batch_id] = handle
            self._threads[batch_id] = thread
        thread.start()
        return handle

    def get(self, batch_id: str) -> BatchHandle | None:
        with self._lock:
            handle = self._handles.get(batch_id)
        if handle is not None:
            return handle
        # Disk fallback — the FastAPI process might have restarted.
        candidate = self.batches_dir / batch_id
        if not (candidate / STATUS_FILENAME).is_file():
            return None
        # Rebuild the handle from disk.
        for cand_input in (
            candidate / INPUT_FILENAME_CSV,
            candidate / INPUT_FILENAME_XLSX,
        ):
            if cand_input.is_file():
                input_path = cand_input
                break
        else:
            return None
        rebuilt = BatchHandle(
            batch_id=batch_id,
            batch_dir=candidate,
            input_path=input_path,
            status_path=candidate / STATUS_FILENAME,
            created_at=_utcnow(),
        )
        with self._lock:
            self._handles[batch_id] = rebuilt
        return rebuilt

    def list(self) -> list[BatchHandle]:
        """List all batches present on disk (oldest first)."""
        if not self.batches_dir.is_dir():
            return []
        out: list[BatchHandle] = []
        for child in sorted(self.batches_dir.iterdir()):
            if not child.is_dir():
                continue
            handle = self.get(child.name)
            if handle is not None:
                out.append(handle)
        return out

    def progress(self, batch_id: str) -> BatchProgress | None:
        handle = self.get(batch_id)
        if handle is None:
            return None
        doc = _read_status(handle.status_path)
        return _aggregate(batch_id, doc)

    def status_doc(self, batch_id: str) -> dict | None:
        handle = self.get(batch_id)
        if handle is None:
            return None
        return _read_status(handle.status_path)

    def customer_bundle_dir(self, batch_id: str) -> Path | None:
        handle = self.get(batch_id)
        if handle is None:
            return None
        candidate = handle.batch_dir / CUSTOMER_BUNDLE_DIRNAME
        return candidate if candidate.is_dir() else None

    def reap_orphans(self) -> int:
        """Mark any on-disk batch in ``running`` (without an active
        thread) as ``failed``. Called once on FastAPI startup so a
        process restart leaves the operator a clean slate.

        Returns the number of orphans reaped."""
        n = 0
        for handle in self.list():
            doc = _read_status(handle.status_path)
            if doc is None:
                continue
            if doc.get("status") not in {BATCH_RUNNING, BATCH_QUEUED}:
                continue
            with self._lock:
                live = self._threads.get(handle.batch_id)
            if live is not None and live.is_alive():
                continue
            doc["status"] = BATCH_FAILED
            doc["completed_at"] = _utcnow()
            doc["error"] = (
                doc.get("error") or "orphaned_by_process_restart"
            )
            try:
                handle.status_path.write_text(
                    json.dumps(doc, indent=2), encoding="utf-8",
                )
            except OSError:
                pass
            n += 1
        return n


__all__ = [
    "BATCH_COMPLETED",
    "BATCH_DIRNAME",
    "BATCH_FAILED",
    "BATCH_PARTIAL_FAILURE",
    "BATCH_QUEUED",
    "BATCH_RUNNING",
    "BatchHandle",
    "BatchProgress",
    "BatchStore",
    "INPUT_FILENAME_CSV",
    "INPUT_FILENAME_XLSX",
    "STATUS_FILENAME",
]
