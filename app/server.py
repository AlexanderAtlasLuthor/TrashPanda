"""Minimal FastAPI HTTP wrapper for TrashPanda jobs.

This module intentionally delegates all pipeline work to
``app.api_boundary.run_cleaning_job``. It only handles local HTTP concerns:
uploads, in-memory job state, JSON responses, and artifact downloads.
"""

from __future__ import annotations

import shutil
import threading
import uuid
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
from fastapi.responses import FileResponse, JSONResponse

from .api_boundary import (
    JobError,
    JobErrorType,
    JobResult,
    JobStatus,
    job_result_to_dict,
    run_cleaning_job,
)


SUPPORTED_EXTENSIONS = {".csv", ".xlsx"}
RUNTIME_ROOT = Path("runtime").resolve()

ARTIFACT_KEYS: dict[str, tuple[str, str]] = {
    "valid_emails": ("client_outputs", "valid_emails"),
    "review_emails": ("client_outputs", "review_emails"),
    "invalid_or_bounce_risk": ("client_outputs", "invalid_or_bounce_risk"),
    "summary_report": ("client_outputs", "summary_report"),
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
    return JSONResponse(
        status_code=422,
        content=_error_payload(
            "request_validation_error",
            "Request validation failed.",
            {"errors": exc.errors()},
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


def _job_paths(job_id: str) -> tuple[Path, Path]:
    uploads_dir = RUNTIME_ROOT / "uploads" / job_id
    output_root = RUNTIME_ROOT / "jobs" / job_id
    uploads_dir.mkdir(parents=True, exist_ok=True)
    output_root.mkdir(parents=True, exist_ok=True)
    return uploads_dir, output_root


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
        return

    JOB_STORE.set_result(result)


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

    with input_path.open("wb") as destination:
        shutil.copyfileobj(file.file, destination)

    result = _queued_result(job_id, filename)
    JOB_STORE.create(result)
    background_tasks.add_task(_run_job, job_id, input_path, output_root, config_path)

    return job_result_to_dict(result)


@app.get("/jobs/{job_id}")
def get_job(job_id: str) -> dict[str, Any]:
    result = JOB_STORE.get(job_id)
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


def _media_type_for(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return "text/csv"
    if suffix == ".json":
        return "application/json"
    if suffix == ".xlsx":
        return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    return "application/octet-stream"


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
