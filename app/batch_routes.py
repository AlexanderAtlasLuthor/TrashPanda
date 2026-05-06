"""V2.10.18 — FastAPI routes for HTTP-triggered batch jobs.

Endpoints (all under the public ``app`` — no operator auth, since
batches are just a wrapper around the same processing the user
already has access to via /jobs):

* ``POST   /batches/upload``                — start a new batch
* ``GET    /batches``                       — list batches (newest first)
* ``GET    /batches/{batch_id}``            — full status doc
* ``GET    /batches/{batch_id}/progress``   — lightweight aggregate
* ``GET    /batches/{batch_id}/customer-bundle/download``
                                            — zip of merged bundle

The router is included from ``app/server.py``. Storage lives at
``runtime/batches/<batch_id>/`` and is owned by ``app/batches.py``.
"""

from __future__ import annotations

import io
import logging
import zipfile
from pathlib import Path

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi.responses import StreamingResponse

from .batches import BATCH_COMPLETED, BATCH_PARTIAL_FAILURE, BatchStore
from .customer_bundle import CUSTOMER_BUNDLE_DIRNAME


_LOGGER = logging.getLogger(__name__)


# Sized to match the existing /jobs upload cap. The limit is enforced
# manually on read to avoid loading the whole upload before checking.
_DEFAULT_MAX_BYTES: int = 200 * 1024 * 1024  # 200 MB


router = APIRouter(prefix="/batches", tags=["batches"])


# Created lazily on first request; bound to ``runtime/`` under cwd
# unless overridden by the server's RUNTIME_ROOT.
_store: BatchStore | None = None


def get_store() -> BatchStore:
    """Lazy-initialise the batch store using the same RUNTIME_ROOT
    the rest of the server uses."""
    global _store
    if _store is None:
        from . import server  # avoid import cycles at module load
        _store = BatchStore(runtime_root=server.RUNTIME_ROOT)
        _store.batches_dir.mkdir(parents=True, exist_ok=True)
        try:
            n = _store.reap_orphans()
            if n:
                _LOGGER.info("reaped %d orphaned batch(es) at startup", n)
        except Exception:  # pragma: no cover - defensive
            _LOGGER.exception("orphan reap failed; continuing")
    return _store


def _read_upload_capped(upload: UploadFile, *, max_bytes: int) -> bytes:
    """Read the uploaded file into memory with a size cap."""
    buf = io.BytesIO()
    remaining = max_bytes
    while True:
        chunk = upload.file.read(min(1024 * 1024, remaining + 1))
        if not chunk:
            break
        if len(chunk) > remaining:
            raise HTTPException(
                status_code=413,
                detail=f"upload exceeds {max_bytes} bytes",
            )
        buf.write(chunk)
        remaining -= len(chunk)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# POST /batches/upload
# ---------------------------------------------------------------------------


@router.post("/upload")
async def upload_batch(
    file: UploadFile = File(...),
    chunk_size: int = Form(25_000),
    threshold_rows: int = Form(50_000),
    allow_partial: bool = Form(False),
    cleanup: bool = Form(False),
) -> dict:
    """Start a new batch from an uploaded CSV or XLSX file.

    The orchestrator runs in a background thread; this endpoint
    returns immediately with the new batch_id. Poll
    ``/batches/{batch_id}/progress`` for status updates.
    """
    if file.filename is None:
        raise HTTPException(status_code=400, detail="missing filename")
    suffix = Path(file.filename).suffix.lower()
    if suffix not in {".csv", ".xlsx", ".xls"}:
        raise HTTPException(
            status_code=400,
            detail=f"unsupported file extension {suffix!r}; "
                   "expected .csv / .xlsx / .xls",
        )
    if chunk_size <= 0 or threshold_rows <= 0:
        raise HTTPException(
            status_code=400,
            detail="chunk_size and threshold_rows must be positive",
        )

    data = _read_upload_capped(file, max_bytes=_DEFAULT_MAX_BYTES)

    handle = get_store().launch(
        input_bytes=data,
        input_filename=file.filename,
        chunk_size=chunk_size,
        threshold_rows=threshold_rows,
        allow_partial=allow_partial,
        cleanup=cleanup,
    )
    return {
        "batch_id": handle.batch_id,
        "batch_dir": str(handle.batch_dir),
        "input_filename": file.filename,
        "created_at": handle.created_at,
        "config": {
            "chunk_size": chunk_size,
            "threshold_rows": threshold_rows,
            "allow_partial": allow_partial,
            "cleanup": cleanup,
        },
    }


# ---------------------------------------------------------------------------
# GET /batches  (list)
# ---------------------------------------------------------------------------


@router.get("")
def list_batches() -> dict:
    """List all batches on disk, oldest-first. Returns lightweight
    progress snapshots so the UI can render a recent-batches list
    without N round-trips."""
    store = get_store()
    return {
        "batches": [
            store.progress(h.batch_id).to_dict()  # type: ignore[union-attr]
            for h in store.list()
            if store.progress(h.batch_id) is not None
        ],
    }


# ---------------------------------------------------------------------------
# GET /batches/{batch_id}
# ---------------------------------------------------------------------------


@router.get("/{batch_id}")
def get_batch(batch_id: str) -> dict:
    """Full status document for a batch (the same JSON the
    orchestrator writes to disk)."""
    doc = get_store().status_doc(batch_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="batch_not_found")
    return doc


# ---------------------------------------------------------------------------
# GET /batches/{batch_id}/progress
# ---------------------------------------------------------------------------


@router.get("/{batch_id}/progress")
def get_batch_progress(batch_id: str) -> dict:
    """Lightweight aggregate for polling. Use this from the UI on a
    short timer — it returns a fixed-size payload regardless of
    chunk count."""
    progress = get_store().progress(batch_id)
    if progress is None:
        raise HTTPException(status_code=404, detail="batch_not_found")
    return progress.to_dict()


# ---------------------------------------------------------------------------
# GET /batches/{batch_id}/customer-bundle/download
# ---------------------------------------------------------------------------


@router.get("/{batch_id}/customer-bundle/download")
def download_batch_bundle(batch_id: str) -> StreamingResponse:
    """Stream the merged customer_bundle/ as a zip.

    Returns 404 until the orchestrator has merged successfully.
    Available even on partial-failure batches as long as at least
    one chunk completed and the merge ran.
    """
    store = get_store()
    progress = store.progress(batch_id)
    if progress is None:
        raise HTTPException(status_code=404, detail="batch_not_found")
    if progress.status not in {BATCH_COMPLETED, BATCH_PARTIAL_FAILURE}:
        raise HTTPException(
            status_code=409,
            detail=f"batch not ready (status={progress.status})",
        )
    bundle_dir = store.customer_bundle_dir(batch_id)
    if bundle_dir is None:
        raise HTTPException(status_code=404, detail="bundle_not_built")

    def _stream():
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for f in sorted(bundle_dir.iterdir()):
                if f.is_file():
                    zf.write(f, arcname=f"{CUSTOMER_BUNDLE_DIRNAME}/{f.name}")
        buf.seek(0)
        yield buf.read()

    filename = f"{batch_id}_customer_bundle.zip"
    return StreamingResponse(
        _stream(),
        media_type="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
        },
    )


__all__ = ["router", "get_store"]
