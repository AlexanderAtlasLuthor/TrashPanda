"""Minimal FastAPI HTTP wrapper for TrashPanda jobs.

This module intentionally delegates all pipeline work to
``app.api_boundary.run_cleaning_job``. It only handles local HTTP concerns:
uploads, in-memory job state, JSON responses, and artifact downloads.
"""

from __future__ import annotations

import csv
import dataclasses
import io
import json
import os
import threading
import uuid
import zipfile
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import (
    BackgroundTasks,
    FastAPI,
    File,
    Form,
    HTTPException,
    Request,
    UploadFile,
)
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response

from .api_boundary import (
    JobError,
    JobErrorType,
    JobResult,
    JobStatus,
    collect_job_artifacts,
    job_result_to_dict,
    load_job_summary,
    run_cleaning_job,
)


def _load_dotenv() -> None:
    """Load ``KEY=VALUE`` pairs from ``<repo>/.env`` into ``os.environ``.

    Keeps third-party keys (GEMINI_API_KEY, ...) out of shell profiles and
    survives across restarts. Existing environment variables win over the
    file, so an explicit ``set GEMINI_API_KEY=...`` still overrides the file.
    No new dependency — a dozen lines are enough for this format.
    """
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.is_file():
        return
    try:
        for raw in env_path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            if line.startswith("export "):
                line = line[len("export "):].lstrip()
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            if not key or key in os.environ:
                continue
            # Strip matching surrounding quotes, if present.
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
                value = value[1:-1]
            os.environ[key] = value
    except OSError:
        # Best-effort: never block server startup because of .env issues.
        pass


_load_dotenv()


SUPPORTED_EXTENSIONS = {".csv", ".xlsx"}
RUNTIME_ROOT = Path("runtime").resolve()

# Upload size ceiling. Configurable via env var so deployments can tune
# it without code changes. Applied to POST /jobs AND POST /upload.
MAX_UPLOAD_MB: int = int(os.environ.get("TRASHPANDA_MAX_UPLOAD_MB", "100"))
MAX_UPLOAD_BYTES: int = MAX_UPLOAD_MB * 1024 * 1024
_UPLOAD_CHUNK_SIZE: int = 1024 * 1024  # 1 MiB streaming chunks

ARTIFACT_KEYS: dict[str, tuple[str, str]] = {
    "valid_emails": ("client_outputs", "valid_emails"),
    "review_emails": ("client_outputs", "review_emails"),
    "invalid_or_bounce_risk": ("client_outputs", "invalid_or_bounce_risk"),
    "summary_report": ("client_outputs", "summary_report"),
    "approved_original_format": ("client_outputs", "approved_original_format"),
    "clean_high_confidence": ("technical_csvs", "clean_high_confidence"),
    "review_medium_confidence": ("technical_csvs", "review_medium_confidence"),
    "removed_invalid": ("technical_csvs", "removed_invalid"),
    "processing_report_json": ("reports", "processing_report_json"),
    "processing_report_csv": ("reports", "processing_report_csv"),
    "domain_summary": ("reports", "domain_summary"),
    "typo_corrections": ("reports", "typo_corrections"),
    "duplicate_summary": ("reports", "duplicate_summary"),
}


def _new_job_id() -> str:
    return datetime.now().strftime("job_%Y%m%d_%H%M%S_") + uuid.uuid4().hex[:8]


def _error_payload(
    error_type: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "error": {
            "error_type": error_type,
            "message": message,
            "details": details or {},
        }
    }


def _raise_http_error(
    status_code: int,
    error_type: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> None:
    raise HTTPException(
        status_code=status_code,
        detail=_error_payload(error_type, message, details),
    )


class InMemoryJobStore:
    """Tiny thread-safe store for local development jobs."""

    def __init__(self) -> None:
        self._jobs: dict[str, JobResult] = {}
        self._lock = threading.RLock()

    def create(self, result: JobResult) -> None:
        with self._lock:
            self._jobs[result.job_id] = result

    def get(self, job_id: str) -> JobResult | None:
        with self._lock:
            result = self._jobs.get(job_id)
            return deepcopy(result) if result is not None else None

    def set_result(self, result: JobResult) -> None:
        with self._lock:
            self._jobs[result.job_id] = result

    def mark_running(self, job_id: str) -> None:
        with self._lock:
            result = self._jobs.get(job_id)
            if result is not None:
                result.status = JobStatus.RUNNING

    def mark_failed(
        self,
        job_id: str,
        input_filename: str,
        error: JobError,
    ) -> None:
        with self._lock:
            existing = self._jobs.get(job_id)
            started_at = existing.started_at if existing else datetime.now()
            self._jobs[job_id] = JobResult(
                job_id=job_id,
                status=JobStatus.FAILED,
                input_filename=input_filename,
                run_dir=existing.run_dir if existing else None,
                summary=None,
                artifacts=existing.artifacts if existing else None,
                error=error,
                started_at=started_at,
                finished_at=datetime.now(),
            )

    def list(self, limit: int = 20) -> list[JobResult]:
        with self._lock:
            jobs = sorted(
                self._jobs.values(),
                key=lambda j: j.started_at if j.started_at else datetime.min,
                reverse=True,
            )
        return [deepcopy(j) for j in jobs[:limit]]

    def clear(self) -> None:
        with self._lock:
            self._jobs.clear()


JOB_STORE = InMemoryJobStore()

app = FastAPI(title="TrashPanda HTTP API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


@app.exception_handler(HTTPException)
async def http_exception_handler(_request: Request, exc: HTTPException) -> JSONResponse:
    if isinstance(exc.detail, dict) and "error" in exc.detail:
        payload = exc.detail
    else:
        payload = _error_payload(
            "http_error",
            str(exc.detail),
            {"status_code": exc.status_code},
        )
    return JSONResponse(status_code=exc.status_code, content=payload, headers=exc.headers)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    _request: Request,
    exc: RequestValidationError,
) -> JSONResponse:
    # FastAPI's errors() can embed non-serialisable objects (e.g. the raw
    # ValueError that triggered validation) under "ctx". Normalise the
    # structure through json with default=str so every nested value
    # round-trips safely.
    safe_errors = json.loads(json.dumps(exc.errors(), default=str))
    return JSONResponse(
        status_code=422,
        content=_error_payload(
            "request_validation_error",
            "Request validation failed.",
            {"errors": safe_errors},
        ),
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(_request: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(
        status_code=500,
        content=_error_payload(
            JobErrorType.PIPELINE_EXECUTION_ERROR,
            "Unexpected server error.",
            {"exception_class": exc.__class__.__name__},
        ),
    )


def _safe_upload_filename(filename: str | None) -> str:
    if not filename:
        _raise_http_error(
            400,
            "missing_filename",
            "Uploaded file must include a filename.",
        )
    safe_name = filename.replace("\\", "/").split("/")[-1].strip()
    if not safe_name:
        _raise_http_error(
            400,
            "missing_filename",
            "Uploaded file must include a filename.",
        )
    return safe_name


def _validate_extension(filename: str) -> None:
    extension = Path(filename).suffix.lower()
    if extension not in SUPPORTED_EXTENSIONS:
        _raise_http_error(
            400,
            "unsupported_file_type",
            "Only .csv and .xlsx files are supported.",
            {
                "filename": filename,
                "supported_extensions": sorted(SUPPORTED_EXTENSIONS),
            },
        )


def _copy_upload_with_size_limit(
    upload: UploadFile,
    destination: Path,
    max_bytes: int | None = None,
) -> int:
    """Stream ``upload`` into ``destination`` and enforce the size cap.

    ``max_bytes`` defaults to the module-level ``MAX_UPLOAD_BYTES``,
    resolved at call time so tests can monkeypatch the cap without
    touching this function.

    Returns the total number of bytes written. Raises HTTPException(413)
    if the upload exceeds the cap, cleaning up the partial file first so
    we never leave a half-written input on disk.
    """
    effective_max = max_bytes if max_bytes is not None else MAX_UPLOAD_BYTES
    total = 0
    try:
        with destination.open("wb") as fh:
            while True:
                chunk = upload.file.read(_UPLOAD_CHUNK_SIZE)
                if not chunk:
                    break
                total += len(chunk)
                if total > effective_max:
                    fh.close()
                    try:
                        destination.unlink()
                    except OSError:
                        pass
                    _raise_http_error(
                        413,
                        "payload_too_large",
                        f"Upload exceeds maximum size ({effective_max // (1024 * 1024)} MB).",
                        {
                            "max_bytes": effective_max,
                            "uploaded_bytes_at_abort": total,
                        },
                    )
                fh.write(chunk)
    except HTTPException:
        raise
    except OSError as exc:
        try:
            destination.unlink()
        except OSError:
            pass
        _raise_http_error(
            500,
            "upload_write_failed",
            "Failed to persist uploaded file.",
            {"detail": str(exc)},
        )
    return total


def _job_paths(job_id: str) -> tuple[Path, Path]:
    uploads_dir = RUNTIME_ROOT / "uploads" / job_id
    output_root = RUNTIME_ROOT / "jobs" / job_id
    uploads_dir.mkdir(parents=True, exist_ok=True)
    output_root.mkdir(parents=True, exist_ok=True)
    return uploads_dir, output_root


def _save_job_meta(result: JobResult) -> None:
    """Persist minimal job metadata so jobs survive server restarts."""
    try:
        job_dir = RUNTIME_ROOT / "jobs" / result.job_id
        job_dir.mkdir(parents=True, exist_ok=True)
        status_value = result.status.value if hasattr(result.status, "value") else str(result.status)
        meta = {
            "job_id": result.job_id,
            "input_filename": result.input_filename,
            "status": status_value,
            "started_at": result.started_at.isoformat() if result.started_at else None,
            "finished_at": result.finished_at.isoformat() if result.finished_at else None,
        }
        (job_dir / "job_meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    except Exception:
        pass


def _read_job_meta(job_id: str) -> dict[str, Any] | None:
    meta_file = RUNTIME_ROOT / "jobs" / job_id / "job_meta.json"
    if not meta_file.is_file():
        return None
    try:
        return json.loads(meta_file.read_text(encoding="utf-8"))
    except Exception:
        return None


def _latest_run_dir(job_output_dir: Path) -> Path | None:
    if not job_output_dir.is_dir():
        return None
    candidates = sorted(
        [d for d in job_output_dir.iterdir() if d.is_dir() and d.name.startswith("run_")],
        key=lambda d: d.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def _reconstruct_job_from_disk(job_id: str) -> JobResult | None:
    """Rebuild a JobResult from disk (meta + artifacts) when JOB_STORE is empty."""
    job_output_dir = RUNTIME_ROOT / "jobs" / job_id
    meta = _read_job_meta(job_id)
    if meta is None and not job_output_dir.is_dir():
        return None

    run_dir = _latest_run_dir(job_output_dir)
    artifacts = None
    summary = None
    if run_dir is not None and _is_under(run_dir, job_output_dir):
        try:
            artifacts = collect_job_artifacts(run_dir)
        except Exception:
            artifacts = None
        try:
            summary = load_job_summary(run_dir)
        except Exception:
            summary = None

    started_at = datetime.now()
    finished_at = None
    status_str = JobStatus.COMPLETED if artifacts is not None else JobStatus.QUEUED
    input_filename = job_id

    if meta is not None:
        input_filename = meta.get("input_filename") or input_filename
        status_str = meta.get("status") or status_str
        try:
            if meta.get("started_at"):
                started_at = datetime.fromisoformat(meta["started_at"])
            if meta.get("finished_at"):
                finished_at = datetime.fromisoformat(meta["finished_at"])
        except Exception:
            pass

    result = JobResult(
        job_id=job_id,
        status=status_str,
        input_filename=input_filename,
        run_dir=run_dir,
        summary=summary,
        artifacts=artifacts,
        error=None,
        started_at=started_at,
        finished_at=finished_at,
    )
    JOB_STORE.create(result)
    return result


def _decisions_path(job_id: str) -> Path:
    return RUNTIME_ROOT / "jobs" / job_id / "review_decisions.json"


def _load_decisions(job_id: str) -> dict[str, str]:
    path = _decisions_path(job_id)
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return {k: v for k, v in data.get("decisions", {}).items() if v in ("approved", "removed")}
    except Exception:
        return {}


def _queued_result(job_id: str, input_filename: str) -> JobResult:
    return JobResult(
        job_id=job_id,
        status=JobStatus.QUEUED,
        input_filename=input_filename,
        run_dir=None,
        summary=None,
        artifacts=None,
        error=None,
        started_at=datetime.now(),
        finished_at=None,
    )


def _run_job(
    job_id: str,
    input_path: Path,
    output_root: Path,
    config_path: str | None,
) -> None:
    JOB_STORE.mark_running(job_id)
    try:
        result = run_cleaning_job(
            input_path=input_path,
            output_root=output_root,
            config_path=config_path,
            job_id=job_id,
        )
    except Exception as exc:  # pragma: no cover - defensive guard
        JOB_STORE.mark_failed(
            job_id=job_id,
            input_filename=input_path.name,
            error=JobError(
                error_type=JobErrorType.PIPELINE_EXECUTION_ERROR,
                message="Processing failed before a JobResult could be created.",
                details={"exception_class": exc.__class__.__name__},
            ),
        )
        failed = JOB_STORE.get(job_id)
        if failed is not None:
            _save_job_meta(failed)
        return

    JOB_STORE.set_result(result)
    _save_job_meta(result)


@app.get("/jobs")
def list_jobs(limit: int = 20) -> dict[str, Any]:
    safe_limit = max(1, min(limit, 100))

    in_memory = JOB_STORE.list(100)
    in_memory_ids = {j.job_id for j in in_memory}

    merged: list[dict[str, Any]] = [
        {
            "job_id": j.job_id,
            "input_filename": j.input_filename,
            "status": j.status.value if hasattr(j.status, "value") else str(j.status),
            "started_at": j.started_at.isoformat() if j.started_at else None,
            "finished_at": j.finished_at.isoformat() if j.finished_at else None,
        }
        for j in in_memory
    ]

    jobs_root = RUNTIME_ROOT / "jobs"
    if jobs_root.is_dir():
        for job_dir in jobs_root.iterdir():
            if not job_dir.is_dir() or job_dir.name in in_memory_ids:
                continue
            meta = _read_job_meta(job_dir.name)
            if meta is not None:
                merged.append(meta)
                continue
            # No meta file (e.g. pre-existing jobs). Best-effort reconstruction.
            run_dir = _latest_run_dir(job_dir)
            status = JobStatus.COMPLETED if run_dir is not None else JobStatus.QUEUED
            merged.append({
                "job_id": job_dir.name,
                "input_filename": job_dir.name,
                "status": status,
                "started_at": datetime.fromtimestamp(job_dir.stat().st_mtime).isoformat(),
                "finished_at": None,
            })

    merged.sort(key=lambda x: x.get("started_at") or "", reverse=True)
    return {"jobs": merged[:safe_limit]}


@app.post("/jobs", status_code=201)
async def create_job(
    background_tasks: BackgroundTasks,
    file: UploadFile | None = File(default=None),
    config_path: str | None = Form(default=None),
) -> dict[str, Any]:
    if file is None:
        _raise_http_error(
            400,
            "missing_file",
            "Multipart form field 'file' is required.",
            {"field": "file"},
        )

    filename = _safe_upload_filename(file.filename)
    _validate_extension(filename)

    job_id = _new_job_id()
    uploads_dir, output_root = _job_paths(job_id)
    input_path = uploads_dir / filename

    _copy_upload_with_size_limit(file, input_path)

    result = _queued_result(job_id, filename)
    JOB_STORE.create(result)
    _save_job_meta(result)
    background_tasks.add_task(_run_job, job_id, input_path, output_root, config_path)

    return job_result_to_dict(result)


# --------------------------------------------------------------------------- #
# Public API envelope (for external integrations / frontends).                #
#                                                                             #
# These routes are deliberately simpler than the /jobs/* surface used by the  #
# internal Next.js client. They reuse the job system end-to-end — no business #
# logic lives here — so any future change to the pipeline automatically flows #
# through both surfaces.                                                      #
# --------------------------------------------------------------------------- #


@app.post("/upload", status_code=201)
async def upload_file(
    background_tasks: BackgroundTasks,
    file: UploadFile | None = File(default=None),
    config_path: str | None = Form(default=None),
) -> dict[str, Any]:
    """Accept a CSV/XLSX, queue processing, return ``{job_id, status}``.

    Alias of :func:`create_job` with a smaller response payload tailored
    for simple integrations.
    """
    full = await create_job(background_tasks, file=file, config_path=config_path)
    return {
        "job_id": full["job_id"],
        "status": full["status"],
        "input_filename": full.get("input_filename"),
    }


def _status_payload(result: JobResult) -> dict[str, Any]:
    status_value = (
        result.status.value if hasattr(result.status, "value") else str(result.status)
    )
    error_payload: dict[str, Any] | None = None
    if result.error is not None:
        error_payload = {
            "error_type": result.error.error_type,
            "message": result.error.message,
        }
    return {
        "job_id": result.job_id,
        "status": status_value,
        "input_filename": result.input_filename,
        "started_at": result.started_at.isoformat() if result.started_at else None,
        "finished_at": result.finished_at.isoformat() if result.finished_at else None,
        "error": error_payload,
    }


@app.get("/status/{job_id}")
def get_status(job_id: str) -> dict[str, Any]:
    """Return compact job status: queued / running / completed / failed."""
    result = JOB_STORE.get(job_id)
    if result is None:
        result = _reconstruct_job_from_disk(job_id)
    if result is None:
        _raise_http_error(404, "job_not_found", "Job not found.", {"job_id": job_id})
    return _status_payload(result)


def _bucket_entry(
    *,
    job_id: str,
    result: JobResult,
    artifact_key: str,
    count: int | None,
) -> dict[str, Any]:
    path = _artifact_path(result, artifact_key)
    return {
        "count": count,
        "download_url": f"/jobs/{job_id}/artifacts/{artifact_key}" if path else None,
        "filename": path.name if path else None,
    }


def _summary_to_dict(result: JobResult) -> dict[str, Any] | None:
    summary = result.summary
    if summary is None:
        return None
    return {
        "total_input_rows": summary.total_input_rows,
        "total_valid": summary.total_valid,
        "total_review": summary.total_review,
        "total_invalid_or_bounce_risk": summary.total_invalid_or_bounce_risk,
        "duplicates_removed": summary.duplicates_removed,
        "typo_corrections": summary.typo_corrections,
        "disposable_emails": summary.disposable_emails,
        "placeholder_or_fake_emails": summary.placeholder_or_fake_emails,
        "role_based_emails": summary.role_based_emails,
    }


_PUBLIC_REPORT_KEYS: tuple[str, ...] = (
    "summary_report",
    "processing_report_json",
    "processing_report_csv",
    "domain_summary",
    "typo_corrections",
    "duplicate_summary",
)


@app.get("/results/{job_id}")
def get_results(job_id: str) -> dict[str, Any]:
    """Return structured results for a completed job.

    Response shape::

        {
          "job_id": ...,
          "status": "completed",
          "input_filename": ...,
          "summary": {...},
          "buckets": {
            "clean_high_confidence": {"count", "download_url", "filename"},
            "review":                 {"count", "download_url", "filename"},
            "invalid":                {"count", "download_url", "filename"}
          },
          "reports":     {"<key>": "<download_url>", ...},
          "artifacts_zip": "<download_url>"
        }

    Returns 409 if the job is not yet completed, 404 if the job id is
    unknown. The download URLs point at the canonical
    ``/jobs/{id}/artifacts/...`` routes — no results data is duplicated.
    """
    result = JOB_STORE.get(job_id)
    if result is None:
        result = _reconstruct_job_from_disk(job_id)
    if result is None:
        _raise_http_error(404, "job_not_found", "Job not found.", {"job_id": job_id})

    status_value = (
        result.status.value if hasattr(result.status, "value") else str(result.status)
    )
    if status_value != JobStatus.COMPLETED:
        _raise_http_error(
            409,
            "job_not_completed",
            "Results are only available for completed jobs.",
            {"job_id": job_id, "status": status_value},
        )

    summary_dict = _summary_to_dict(result)

    def _count(field_name: str) -> int | None:
        if summary_dict is None:
            return None
        value = summary_dict.get(field_name)
        return int(value) if value is not None else None

    buckets = {
        "clean_high_confidence": _bucket_entry(
            job_id=job_id, result=result,
            artifact_key="valid_emails",
            count=_count("total_valid"),
        ),
        "review": _bucket_entry(
            job_id=job_id, result=result,
            artifact_key="review_emails",
            count=_count("total_review"),
        ),
        "invalid": _bucket_entry(
            job_id=job_id, result=result,
            artifact_key="invalid_or_bounce_risk",
            count=_count("total_invalid_or_bounce_risk"),
        ),
    }

    reports: dict[str, str] = {}
    for key in _PUBLIC_REPORT_KEYS:
        if _artifact_path(result, key) is not None:
            reports[key] = f"/jobs/{job_id}/artifacts/{key}"

    return {
        "job_id": job_id,
        "status": status_value,
        "input_filename": result.input_filename,
        "started_at": result.started_at.isoformat() if result.started_at else None,
        "finished_at": result.finished_at.isoformat() if result.finished_at else None,
        "summary": summary_dict,
        "buckets": buckets,
        "reports": reports,
        "artifacts_zip": f"/jobs/{job_id}/artifacts/zip",
    }


@app.get("/jobs/{job_id}")
def get_job(job_id: str) -> dict[str, Any]:
    result = JOB_STORE.get(job_id)
    if result is None:
        result = _reconstruct_job_from_disk(job_id)
    if result is None:
        _raise_http_error(
            404,
            "job_not_found",
            "Job not found.",
            {"job_id": job_id},
        )
    return job_result_to_dict(result)


def _is_under(candidate: Path, root: Path) -> bool:
    try:
        candidate.resolve().relative_to(root.resolve())
    except ValueError:
        return False
    return True


def _artifact_path(result: JobResult, key: str) -> Path | None:
    if result.artifacts is None:
        return None
    mapping = ARTIFACT_KEYS.get(key)
    if mapping is None:
        return None

    group_name, attr_name = mapping
    group = getattr(result.artifacts, group_name)
    value = getattr(group, attr_name)
    if value is None:
        return None

    path = Path(value)
    if not path.is_file():
        return None
    if not _is_under(path, result.artifacts.run_dir):
        return None
    return path


def _build_zip(root: Path) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for file_path in sorted(root.rglob("*")):
            if file_path.is_file():
                zf.write(file_path, arcname=str(file_path.relative_to(root)))
    return buf.getvalue()


def _build_zip_filename(input_filename: str | None, now: datetime | None = None) -> str:
    """Build the download filename for the all-artifacts ZIP.

    Format: ``<cleaned_stem>_trashpanda_results_<YYYY-MM-DD_HH-MM>.zip``

    The stem is derived from the original uploaded filename by:
      * stripping the extension (``.csv``, ``.xlsx``, etc.)
      * lowercasing
      * replacing whitespace with underscores
      * keeping only ``[a-z0-9_-]`` characters
      * collapsing repeated underscores
      * trimming leading/trailing underscores

    If the resulting stem is empty (missing or fully sanitized away), falls
    back to ``trashpanda_results_<timestamp>.zip``.
    """
    import re

    ts = (now or datetime.now()).strftime("%Y-%m-%d_%H-%M")

    stem = ""
    if input_filename:
        raw_stem = Path(input_filename).stem
        lowered = raw_stem.lower()
        with_underscores = re.sub(r"\s+", "_", lowered)
        sanitized = re.sub(r"[^a-z0-9_-]+", "", with_underscores)
        collapsed = re.sub(r"_+", "_", sanitized).strip("_-")
        stem = collapsed

    if not stem:
        return f"trashpanda_results_{ts}.zip"
    return f"{stem}_trashpanda_results_{ts}.zip"


def _tail_log(path: Path, n: int) -> list[str]:
    """Return the last *n* non-empty lines of *path*, or [] on any I/O error."""
    try:
        with path.open("r", encoding="utf-8", errors="replace") as fh:
            all_lines = fh.readlines()
        return [ln.rstrip("\n") for ln in all_lines[-n:] if ln.strip()]
    except OSError:
        return []


_REVIEW_EXPLANATIONS: dict[str, tuple[str, str, str]] = {
    # reason -> (friendly_reason, risk, recommendation)
    "role-based": (
        "Role-based address (info@, support@, sales@)",
        "May not reach a specific person and can trigger spam filters.",
        "Approve only for broad outreach; reject for 1:1 campaigns.",
    ),
    "catch-all": (
        "Catch-all domain (accepts any address)",
        "Delivery cannot be confirmed — the domain accepts all mail.",
        "Approve only when you have a direct relationship with the contact.",
    ),
    "no-smtp": (
        "No mail server detected (missing MX record)",
        "Domain is unlikely to receive email reliably.",
        "Reject unless you can confirm the domain receives email.",
    ),
}


def _derive_flags(reason_codes: str) -> dict[str, bool]:
    rc = reason_codes.lower()
    return {
        "role_based": "role" in rc,
        "catch_all": "catch_all" in rc or "catch-all" in rc,
        "smtp_unverified": "a_fallback" in rc or "no_mx" in rc or "dns_no_nameservers" in rc,
        "typo_corrected": "typo_corrected" in rc,
        "domain_mismatch": "domain_mismatch" in rc,
    }


# --------------------------------------------------------------------------- #
# V2 field passthrough helpers (back-compat safe).                            #
# --------------------------------------------------------------------------- #

def _safe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _safe_bool(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    if s in ("true", "1", "yes", "y", "t"):
        return True
    if s in ("false", "0", "no", "n", "f"):
        return False
    return None


def _nonempty(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


# friendly copy mappings for V2 signals
_FINAL_ACTION_LABELS: dict[str, str] = {
    "auto_approve": "Auto-approved",
    "manual_review": "Manual review",
    "auto_reject": "Auto-rejected",
}

_BUCKET_FRIENDLY: dict[str, str] = {
    "valid": "Ready to send",
    "review": "Needs attention",
    "invalid": "Do not use",
    "invalid_or_bounce_risk": "Do not use",
}

_HISTORICAL_LABELS: dict[str, str] = {
    "reliable": "Historically reliable",
    "risky": "Historically risky",
    "unstable": "Historically unstable",
    "catch_all_suspected": "Catch-all suspected",
    "unknown": "No historical signal",
}


def _confidence_tier(value: float | None) -> str | None:
    if value is None:
        return None
    if value >= 0.85:
        return "high"
    if value >= 0.60:
        return "medium"
    return "low"


def _v2_passthrough(row: dict[str, str]) -> dict[str, Any]:
    """Extract V2 columns that may or may not exist in the CSV.

    Returns only keys whose underlying value is present (non-empty), so the
    UI can feature-detect and degrade gracefully on legacy runs.
    """
    out: dict[str, Any] = {}

    bucket = _nonempty(row.get("bucket_v2")) or _nonempty(row.get("preliminary_bucket"))
    if bucket:
        out["bucket_v2"] = bucket
        out["bucket_label"] = _BUCKET_FRIENDLY.get(bucket.lower(), bucket.title())

    conf_v2 = _safe_float(row.get("confidence_v2"))
    if conf_v2 is not None:
        out["confidence_v2"] = conf_v2
        tier = _confidence_tier(conf_v2)
        if tier:
            out["confidence_tier"] = tier

    # Decision layer (validation_v2.decision.aggregator)
    final_action = _nonempty(row.get("final_action"))
    if final_action:
        out["final_action"] = final_action
        out["final_action_label"] = _FINAL_ACTION_LABELS.get(
            final_action, final_action.replace("_", " ").title()
        )
    for key in ("decision_reason", "decision_note"):
        v = _nonempty(row.get(key))
        if v:
            out[key] = v
    dec_conf = _safe_float(row.get("decision_confidence"))
    if dec_conf is not None:
        out["decision_confidence"] = dec_conf

    # Deliverability signal
    deliv_prob = _safe_float(row.get("deliverability_probability"))
    if deliv_prob is not None:
        out["deliverability_probability"] = deliv_prob
    deliv_label = _nonempty(row.get("deliverability_label"))
    if deliv_label:
        out["deliverability_label"] = deliv_label
    deliv_factors = _nonempty(row.get("deliverability_factors"))
    if deliv_factors:
        out["deliverability_factors"] = deliv_factors

    # Human-readable explanation
    for key in ("human_reason", "human_risk", "human_recommendation"):
        v = _nonempty(row.get(key))
        if v:
            out[key] = v

    # Historical / reputation
    hist = _nonempty(row.get("historical_label"))
    if hist:
        out["historical_label"] = hist
        out["historical_label_friendly"] = _HISTORICAL_LABELS.get(hist.lower(), hist)
    conf_adj = _safe_bool(row.get("confidence_adjustment_applied"))
    if conf_adj is not None:
        out["confidence_adjustment_applied"] = conf_adj

    # Catch-all
    possible_catch_all = _safe_bool(row.get("possible_catch_all"))
    if possible_catch_all is not None:
        out["possible_catch_all"] = possible_catch_all
    cc = _safe_float(row.get("catch_all_confidence"))
    if cc is not None:
        out["catch_all_confidence"] = cc
    cc_reason = _nonempty(row.get("catch_all_reason"))
    if cc_reason:
        out["catch_all_reason"] = cc_reason

    # Review subclass
    subclass = _nonempty(row.get("review_subclass"))
    if subclass:
        out["review_subclass"] = subclass

    # SMTP probe
    for key in ("smtp_tested", "smtp_confirmed_valid", "smtp_suspicious"):
        b = _safe_bool(row.get(key))
        if b is not None:
            out[key] = b
    for key in ("smtp_result", "smtp_code"):
        v = _nonempty(row.get(key))
        if v:
            out[key] = v
    smtp_conf = _safe_float(row.get("smtp_confidence"))
    if smtp_conf is not None:
        out["smtp_confidence"] = smtp_conf

    # Reason codes (raw, useful for debug badges)
    rc = _nonempty(row.get("reason_codes_v2"))
    if rc:
        out["reason_codes_v2"] = rc

    return out


def _map_review_row(row: dict[str, str], index: int) -> dict[str, Any] | None:
    email = row.get("email", "").strip()
    if not email:
        return None

    row_id = row.get("id", "").strip() or str(index)
    domain = row.get("domain", "").strip() or email.split("@")[-1]

    reason_codes = row.get("reason_codes_v2", "").lower()
    if "role" in reason_codes:
        reason = "role-based"
    elif "catch_all" in reason_codes or "catch-all" in reason_codes:
        reason = "catch-all"
    else:
        reason = "no-smtp"

    try:
        conf_float = float(row.get("confidence_v2", "0") or "0")
    except (ValueError, TypeError):
        conf_float = 0.0
    confidence = "medium" if conf_float >= 0.75 else "low"

    friendly_reason, risk, recommendation = _REVIEW_EXPLANATIONS[reason]
    flags = _derive_flags(reason_codes)

    base = {
        "id": row_id,
        "email": email,
        "domain": domain,
        "reason": reason,
        "confidence": confidence,
        "classification_bucket": "Needs attention",
        "friendly_reason": friendly_reason,
        "risk": risk,
        "recommended_action": recommendation,
        "flags": flags,
    }
    # Pass through V2 intelligence when available (back-compat safe).
    base.update(_v2_passthrough(row))
    return base


@app.get("/jobs/{job_id}/review")
def get_job_review(job_id: str) -> dict[str, Any]:
    job_output_dir = RUNTIME_ROOT / "jobs" / job_id

    # Try artifact path from in-memory store first.
    csv_path: Path | None = None
    result = JOB_STORE.get(job_id)
    if result is not None:
        csv_path = _artifact_path(result, "review_medium_confidence")

    # Fallback: scan disk (handles server-restart case where JOB_STORE is empty).
    if csv_path is None:
        if not job_output_dir.is_dir():
            _raise_http_error(404, "job_not_found", "Job not found.", {"job_id": job_id})
        candidates = sorted(
            job_output_dir.glob("*/review_medium_confidence.csv"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if candidates and _is_under(candidates[0], job_output_dir):
            csv_path = candidates[0]

    if csv_path is None:
        _raise_http_error(
            404,
            "artifact_not_found",
            "Review artifact not found for this job.",
            {"job_id": job_id},
        )

    emails: list[dict[str, Any]] = []
    try:
        with csv_path.open(encoding="utf-8", newline="") as fh:
            for i, row in enumerate(csv.DictReader(fh)):
                item = _map_review_row(row, i)
                if item is not None:
                    emails.append(item)
    except OSError as exc:
        _raise_http_error(500, "artifact_read_error", "Failed to read review artifact.", {"detail": str(exc)})

    return {"job_id": job_id, "total": len(emails), "emails": emails}


@app.post("/jobs/{job_id}/ai-review")
def post_job_ai_review(job_id: str) -> dict[str, Any]:
    """Ask Gemini to stack-rank the review queue.

    Returns one `{id, decision, confidence, reasoning}` per flagged email so
    the UI can render "AI suggests Approve · 87%" badges next to each row.
    Human reviewer still has the final call.
    """
    from . import ai_review as _ai

    # Reuse the same loader as /jobs/{id}/review so signals stay in sync.
    review_payload = get_job_review(job_id)
    emails = review_payload.get("emails", [])

    try:
        result = JOB_STORE.get(job_id)
        # JobSummary is a slots dataclass — no __dict__. Use asdict() so this
        # works on every Python dataclass style.
        summary = (
            dataclasses.asdict(result.summary)
            if result is not None and getattr(result, "summary", None) is not None
            else None
        )
        suggestions = _ai.review_queue_suggestions(emails, summary)
    except _ai.AIUnavailable as exc:
        _raise_http_error(503, "ai_unavailable", str(exc))
    except HTTPException:
        raise
    except Exception as exc:  # google-genai errors, network errors, schema errors
        _raise_http_error(502, "ai_error", f"AI review failed: {exc}")

    return {
        "job_id": job_id,
        "total": len(suggestions),
        "suggestions": suggestions,
    }


@app.post("/jobs/{job_id}/ai-summary")
def post_job_ai_summary(job_id: str) -> dict[str, Any]:
    """Return a one-paragraph narrative summary of a completed job."""
    from . import ai_review as _ai

    result = JOB_STORE.get(job_id)
    if result is None or getattr(result, "summary", None) is None:
        # Fall back to the persisted job if JOB_STORE was cleared by a restart.
        job_dir = RUNTIME_ROOT / "jobs" / job_id
        if not job_dir.is_dir():
            _raise_http_error(404, "job_not_found", "Job not found.", {"job_id": job_id})
        _raise_http_error(
            409,
            "job_not_completed",
            "Summary is only available for completed jobs.",
            {"job_id": job_id},
        )

    try:
        summary = dataclasses.asdict(result.summary)
        narrative = _ai.job_summary_narrative(summary)
    except _ai.AIUnavailable as exc:
        _raise_http_error(503, "ai_unavailable", str(exc))
    except HTTPException:
        raise
    except Exception as exc:
        _raise_http_error(502, "ai_error", f"AI summary failed: {exc}")

    return {"job_id": job_id, "narrative": narrative}


@app.get("/jobs/{job_id}/review/decisions")
def get_review_decisions(job_id: str) -> dict[str, Any]:
    job_output_dir = RUNTIME_ROOT / "jobs" / job_id
    if not job_output_dir.is_dir():
        _raise_http_error(404, "job_not_found", "Job not found.", {"job_id": job_id})
    return {"job_id": job_id, "decisions": _load_decisions(job_id)}


@app.post("/jobs/{job_id}/review/decisions")
async def save_review_decisions(job_id: str, request: Request) -> dict[str, Any]:
    job_output_dir = RUNTIME_ROOT / "jobs" / job_id
    if not job_output_dir.is_dir():
        _raise_http_error(404, "job_not_found", "Job not found.", {"job_id": job_id})

    try:
        body = await request.json()
    except Exception:
        _raise_http_error(400, "invalid_body", "Request body must be JSON.")

    raw = body.get("decisions", {}) if isinstance(body, dict) else {}
    cleaned = {
        str(k): v for k, v in raw.items()
        if isinstance(k, str) and v in ("approved", "removed")
    }

    payload = {
        "job_id": job_id,
        "decisions": cleaned,
        "updated_at": datetime.now().isoformat(),
    }
    _decisions_path(job_id).write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return {"job_id": job_id, "saved": len(cleaned)}


def _collect_approved_refs(
    run_dir: Path,
    decisions: dict[str, str],
) -> dict[str, set[int]]:
    """Build {source_file: {source_row_number,...}} for final-approved rows.

    Includes all clean_high_confidence rows plus review rows decided "approved".
    """
    approved: dict[str, set[int]] = {}

    def _absorb(csv_path: Path, only_approved: bool) -> None:
        if not csv_path.is_file():
            return
        try:
            with csv_path.open(encoding="utf-8", newline="") as fh:
                for row in csv.DictReader(fh):
                    if only_approved:
                        rid = row.get("id", "").strip()
                        if decisions.get(rid) != "approved":
                            continue
                    src = (row.get("source_file") or "").strip()
                    try:
                        rn = int(row.get("source_row_number") or 0)
                    except (ValueError, TypeError):
                        continue
                    if not src or rn <= 0:
                        continue
                    approved.setdefault(src, set()).add(rn)
        except OSError:
            return

    _absorb(run_dir / "clean_high_confidence.csv", only_approved=False)
    _absorb(run_dir / "review_medium_confidence.csv", only_approved=True)
    return approved


@app.get("/jobs/{job_id}/review/export")
def get_review_export(job_id: str) -> Response:
    """Final approved XLSX: clean_high_confidence + manually-approved review."""
    import pandas as pd

    job_output_dir = RUNTIME_ROOT / "jobs" / job_id
    if not job_output_dir.is_dir():
        _raise_http_error(404, "job_not_found", "Job not found.", {"job_id": job_id})

    run_dir = _latest_run_dir(job_output_dir)
    if run_dir is None or not _is_under(run_dir, job_output_dir):
        _raise_http_error(404, "no_run_dir", "No run directory found.", {"job_id": job_id})

    decisions = _load_decisions(job_id)
    approved = _collect_approved_refs(run_dir, decisions)

    if not approved:
        _raise_http_error(
            404,
            "no_approved_rows",
            "No approved rows available for export.",
            {"job_id": job_id},
        )

    uploads_dir = RUNTIME_ROOT / "uploads" / job_id
    if not uploads_dir.is_dir():
        _raise_http_error(404, "no_uploads", "Original input files not found.", {"job_id": job_id})

    path_by_name: dict[str, Path] = {p.name: p for p in uploads_dir.iterdir() if p.is_file()}

    frames: list[Any] = []
    for src_name in sorted(approved):
        orig_path = path_by_name.get(src_name)
        if orig_path is None or not orig_path.is_file():
            continue
        try:
            if orig_path.suffix.lower() == ".csv":
                orig_df = pd.read_csv(orig_path, dtype=str, keep_default_na=False, na_filter=False)
            else:
                orig_df = pd.read_excel(orig_path, dtype=str)
        except Exception:
            continue

        valid_indices = sorted(rn - 2 for rn in approved[src_name] if 0 <= rn - 2 < len(orig_df))
        if not valid_indices:
            continue
        frames.append(orig_df.iloc[valid_indices].reset_index(drop=True))

    if not frames:
        _raise_http_error(
            404,
            "no_rows_extracted",
            "Could not extract rows from any original input.",
            {"job_id": job_id},
        )

    combined = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        combined.to_excel(writer, sheet_name="final_approved", index=False)

    filename = f"final_approved_{job_id}.xlsx"
    return Response(
        content=buf.getvalue(),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# --------------------------------------------------------------------------- #
# /jobs/{id}/insights — V2 Deliverability Intelligence aggregate + row-level  #
# feed. Back-compat: v2_available=False for legacy runs, UI will render an    #
# empty-state panel.                                                          #
# --------------------------------------------------------------------------- #

_V2_MARKER_COLS = (
    "bucket_v2",
    "confidence_v2",
    "final_action",
    "deliverability_probability",
    "human_reason",
    "possible_catch_all",
    "smtp_tested",
)


def _row_iter(csv_path: Path):
    try:
        with csv_path.open(encoding="utf-8", newline="") as fh:
            for row in csv.DictReader(fh):
                yield row
    except OSError:
        return


def _build_insight_row(row: dict[str, str], index: int, source: str) -> dict[str, Any] | None:
    email = (row.get("email") or "").strip()
    if not email:
        return None
    row_id = (row.get("id") or "").strip() or f"{source}-{index}"
    domain = (row.get("domain") or "").strip() or email.split("@")[-1]
    item: dict[str, Any] = {
        "id": row_id,
        "email": email,
        "domain": domain,
        "source": source,  # "valid" | "review" | "invalid"
    }
    item.update(_v2_passthrough(row))
    # Also include basic reason codes for table chips
    rc = (row.get("reason_codes_v2") or row.get("reason_codes") or "").strip()
    if rc:
        item["reason_codes"] = rc
    return item


@app.get("/jobs/{job_id}/insights")
def get_job_insights(job_id: str) -> dict[str, Any]:
    """Aggregate V2 deliverability intelligence for a completed job.

    Reads clean_high_confidence.csv, review_medium_confidence.csv and
    removed_invalid.csv. Returns per-row V2 fields + roll-up counts + domain
    intelligence. Sets ``v2_available=False`` for legacy runs.
    """
    job_output_dir = RUNTIME_ROOT / "jobs" / job_id
    if not job_output_dir.is_dir():
        _raise_http_error(404, "job_not_found", "Job not found.", {"job_id": job_id})

    run_dir = _latest_run_dir(job_output_dir)
    if run_dir is None or not _is_under(run_dir, job_output_dir):
        return {
            "job_id": job_id,
            "v2_available": False,
            "totals": {"all": 0, "valid": 0, "review": 0, "invalid": 0},
            "rows": [],
            "domain_intelligence": {
                "reliable": [], "risky": [], "unstable": [], "catch_all_suspected": [],
            },
            "confidence_tiers": {"high": 0, "medium": 0, "low": 0, "unknown": 0},
            "final_actions": {},
            "catch_all_count": 0,
            "smtp_tested_count": 0,
            "smtp_suspicious_count": 0,
        }

    sources = [
        ("valid", run_dir / "clean_high_confidence.csv"),
        ("review", run_dir / "review_medium_confidence.csv"),
        ("invalid", run_dir / "removed_invalid.csv"),
    ]

    rows: list[dict[str, Any]] = []
    v2_available = False
    source_counts = {"valid": 0, "review": 0, "invalid": 0}

    conf_tiers = {"high": 0, "medium": 0, "low": 0, "unknown": 0}
    final_actions: dict[str, int] = {}
    catch_all_count = 0
    smtp_tested_count = 0
    smtp_suspicious_count = 0

    # domain aggregation: domain -> {count, deliv_sum, deliv_n, label_counts}
    domain_stats: dict[str, dict[str, Any]] = {}

    for source_name, path in sources:
        if not path.is_file() or not _is_under(path, job_output_dir):
            continue
        for idx, raw in enumerate(_row_iter(path)):
            if not v2_available:
                for marker in _V2_MARKER_COLS:
                    if (raw.get(marker) or "").strip():
                        v2_available = True
                        break
            item = _build_insight_row(raw, idx, source_name)
            if item is None:
                continue
            rows.append(item)
            source_counts[source_name] += 1

            # aggregate
            tier = item.get("confidence_tier")
            if tier in conf_tiers:
                conf_tiers[tier] += 1
            else:
                conf_tiers["unknown"] += 1

            fa = item.get("final_action")
            if fa:
                final_actions[fa] = final_actions.get(fa, 0) + 1

            if item.get("possible_catch_all"):
                catch_all_count += 1
            if item.get("smtp_tested"):
                smtp_tested_count += 1
            if item.get("smtp_suspicious"):
                smtp_suspicious_count += 1

            d = item["domain"].lower()
            ds = domain_stats.setdefault(
                d,
                {
                    "domain": d,
                    "count": 0,
                    "deliv_sum": 0.0,
                    "deliv_n": 0,
                    "historical": {},
                    "catch_all": 0,
                    "smtp_suspicious": 0,
                    "invalid": 0,
                    "valid": 0,
                    "review": 0,
                },
            )
            ds["count"] += 1
            ds[source_name] += 1
            dp = item.get("deliverability_probability")
            if isinstance(dp, (int, float)):
                ds["deliv_sum"] += float(dp)
                ds["deliv_n"] += 1
            hl = item.get("historical_label")
            if hl:
                ds["historical"][hl] = ds["historical"].get(hl, 0) + 1
            if item.get("possible_catch_all"):
                ds["catch_all"] += 1
            if item.get("smtp_suspicious"):
                ds["smtp_suspicious"] += 1

    # classify domains
    reliable: list[dict[str, Any]] = []
    risky: list[dict[str, Any]] = []
    unstable: list[dict[str, Any]] = []
    catch_all_suspected: list[dict[str, Any]] = []

    for ds in domain_stats.values():
        avg_deliv = (ds["deliv_sum"] / ds["deliv_n"]) if ds["deliv_n"] else None
        hist = ds["historical"]
        primary_hist = max(hist, key=hist.get) if hist else None
        entry = {
            "domain": ds["domain"],
            "count": ds["count"],
            "avg_deliverability": round(avg_deliv, 3) if avg_deliv is not None else None,
            "historical_label": primary_hist,
            "catch_all_count": ds["catch_all"],
            "smtp_suspicious_count": ds["smtp_suspicious"],
            "valid": ds["valid"],
            "review": ds["review"],
            "invalid": ds["invalid"],
        }
        # classification (priority order)
        if primary_hist == "catch_all_suspected" or (ds["count"] >= 3 and ds["catch_all"] / max(ds["count"], 1) >= 0.5):
            catch_all_suspected.append(entry)
        elif primary_hist == "risky" or (avg_deliv is not None and avg_deliv < 0.4 and ds["count"] >= 2):
            risky.append(entry)
        elif primary_hist == "unstable" or ds["smtp_suspicious"] > 0:
            unstable.append(entry)
        elif primary_hist == "reliable" or (avg_deliv is not None and avg_deliv >= 0.8):
            reliable.append(entry)

    # top-N per list, by volume
    def _top(lst: list[dict[str, Any]], n: int = 15) -> list[dict[str, Any]]:
        return sorted(lst, key=lambda e: e["count"], reverse=True)[:n]

    total_all = sum(source_counts.values())
    return {
        "job_id": job_id,
        "v2_available": v2_available,
        "totals": {"all": total_all, **source_counts},
        "confidence_tiers": conf_tiers,
        "final_actions": final_actions,
        "catch_all_count": catch_all_count,
        "smtp_tested_count": smtp_tested_count,
        "smtp_suspicious_count": smtp_suspicious_count,
        "domain_intelligence": {
            "reliable": _top(reliable),
            "risky": _top(risky),
            "unstable": _top(unstable),
            "catch_all_suspected": _top(catch_all_suspected),
        },
        "rows": rows,
    }


@app.get("/jobs/{job_id}/typo-corrections")
def get_typo_corrections(job_id: str) -> dict[str, Any]:
    job_output_dir = RUNTIME_ROOT / "jobs" / job_id
    if not job_output_dir.is_dir():
        _raise_http_error(404, "job_not_found", "Job not found.", {"job_id": job_id})

    run_dir = _latest_run_dir(job_output_dir)
    if run_dir is None:
        return {"job_id": job_id, "total": 0, "corrections": []}

    csv_path = run_dir / "typo_corrections.csv"
    if not csv_path.is_file() or not _is_under(csv_path, job_output_dir):
        return {"job_id": job_id, "total": 0, "corrections": []}

    corrections: list[dict[str, str]] = []
    try:
        with csv_path.open(encoding="utf-8", newline="") as fh:
            for row in csv.DictReader(fh):
                orig = (row.get("typo_original_domain") or "").strip()
                corrected = (row.get("corrected_domain") or "").strip()
                email = (row.get("email") or "").strip()
                if orig and corrected and orig != corrected:
                    corrections.append({
                        "original": orig,
                        "corrected": corrected,
                        "email": email,
                    })
    except OSError:
        return {"job_id": job_id, "total": 0, "corrections": []}

    return {"job_id": job_id, "total": len(corrections), "corrections": corrections}


@app.get("/jobs/{job_id}/logs")
def get_job_logs(job_id: str, limit: int = 20) -> dict[str, Any]:
    result = JOB_STORE.get(job_id)
    if result is None:
        _raise_http_error(404, "job_not_found", "Job not found.", {"job_id": job_id})

    job_output_dir = RUNTIME_ROOT / "jobs" / job_id
    if not job_output_dir.is_dir():
        return {"job_id": job_id, "lines": []}

    # Glob for the run sub-directory (name is non-deterministic at job start time).
    candidates = sorted(
        job_output_dir.glob("*/logs/run.log"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        return {"job_id": job_id, "lines": []}

    log_path = candidates[0]
    if not _is_under(log_path, job_output_dir):
        return {"job_id": job_id, "lines": []}

    safe_limit = max(1, min(limit, 200))
    return {"job_id": job_id, "lines": _tail_log(log_path, safe_limit)}


def _media_type_for(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return "text/csv"
    if suffix == ".json":
        return "application/json"
    if suffix == ".xlsx":
        return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    return "application/octet-stream"


@app.get("/jobs/{job_id}/artifacts/zip")
def get_artifacts_zip(job_id: str) -> Response:
    result = JOB_STORE.get(job_id)
    if result is None:
        _raise_http_error(404, "job_not_found", "Job not found.", {"job_id": job_id})
    if result.status != JobStatus.COMPLETED:
        _raise_http_error(
            409,
            "job_not_completed",
            "ZIP download is only available for completed jobs.",
            {"status": result.status.value if hasattr(result.status, "value") else str(result.status)},
        )

    job_output_dir = RUNTIME_ROOT / "jobs" / job_id
    if not job_output_dir.is_dir():
        _raise_http_error(404, "no_artifacts", "No output directory found.", {"job_id": job_id})

    run_dirs = sorted(
        [d for d in job_output_dir.iterdir() if d.is_dir()],
        key=lambda d: d.stat().st_mtime,
        reverse=True,
    )
    if not run_dirs:
        _raise_http_error(404, "no_artifacts", "No run directory found.", {"job_id": job_id})

    run_dir = run_dirs[0]
    if not _is_under(run_dir, job_output_dir):
        _raise_http_error(403, "forbidden", "Path traversal detected.", {})

    zip_bytes = _build_zip(run_dir)
    download_name = _build_zip_filename(result.input_filename)
    return Response(
        content=zip_bytes,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{download_name}"'},
    )


@app.get("/jobs/{job_id}/artifacts/{key}")
def get_artifact(job_id: str, key: str) -> FileResponse:
    result = JOB_STORE.get(job_id)
    if result is None:
        _raise_http_error(
            404,
            "job_not_found",
            "Job not found.",
            {"job_id": job_id},
        )

    path = _artifact_path(result, key)
    if path is None:
        _raise_http_error(
            404,
            "artifact_not_found",
            "Artifact not found for this job.",
            {
                "job_id": job_id,
                "key": key,
                "supported_keys": sorted(ARTIFACT_KEYS),
            },
        )

    return FileResponse(
        path=path,
        filename=path.name,
        media_type=_media_type_for(path),
    )
