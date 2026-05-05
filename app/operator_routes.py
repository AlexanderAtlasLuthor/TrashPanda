"""V2.10.0.1 / V2.10.0.2 — Operator-only HTTP routes.

This module wraps the V2.9 boundary helpers in :mod:`app.api_boundary`
under a dedicated ``/api/operator`` namespace. It exists so the future
operator console (Next.js, V2.10.1+) can reach preflight, the
client-package builder, the operator review gate, the SMTP / artifact
consistency JSON files, and the feedback bridge without going through
the legacy ``/jobs/{id}/...`` surface used by clients today.

NOTE — operator endpoints are NOT a client delivery contract. Anything
classified ``operator_only``, ``technical_debug`` or ``internal_only``
by :mod:`app.artifact_contract` may surface here. Do not link these
routes from any client-facing UI surface, and do not treat their
responses as audience-filtered.

The one exception is ``GET /jobs/{id}/client-package/download``
(V2.10.0.2): the response body is the audience-filtered client
package, so it carries ``X-TrashPanda-Audience: client_safe`` and is
gated behind ``operator_review_summary.ready_for_client === true``.

This module deliberately does NOT:

* harden the legacy ``/jobs/{id}/artifacts/...`` routes
  (deferred to V2.10.0.3),
* introduce auth / role gating (deferred to V2.10.x),
* add multipart upload to the feedback / preflight endpoints
  (deferred to V2.10.6 — JSON-with-paths is accepted for now),
* touch any frontend code (deferred to V2.10.1+).
"""

from __future__ import annotations

import io
import json
import logging
import zipfile
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request, Response

from .api_boundary import (
    JobStatus,
    build_client_package_for_job,
    build_feedback_domain_intel_preview_for_job,
    ingest_bounce_feedback,
    job_result_to_dict,
    run_operator_review_for_job,
    run_rollout_preflight,
)
from .config import load_config, resolve_project_paths


_LOGGER = logging.getLogger(__name__)


# Filenames that the operator routes read directly. Kept here (rather
# than re-importing module-private constants) so this module stays
# decoupled from V2.9 internals.
_SMTP_RUNTIME_SUMMARY_FILENAME = "smtp_runtime_summary.json"
_ARTIFACT_CONSISTENCY_FILENAME = "artifact_consistency.json"
_OPERATOR_REVIEW_SUMMARY_FILENAME = "operator_review_summary.json"
_CLIENT_PACKAGE_DIR_NAME = "client_delivery_package"
_CLIENT_PACKAGE_MANIFEST_FILENAME = "client_package_manifest.json"


# Standard "missing" payloads. Returned with HTTP 200 so the operator
# UI can render an empty state instead of a hard error. Each endpoint
# customises ``status_code`` only when the operator action is genuinely
# impossible (e.g. unknown job_id).
_MISSING_GENERIC: dict[str, Any] = {
    "status": "missing",
    "available": False,
    "warning": True,
}

_MISSING_OPERATOR_REVIEW: dict[str, Any] = {
    "status": "missing",
    "ready_for_client": False,
    "available": False,
    "issues": [
        {
            "severity": "warn",
            "code": "operator_review_missing",
            "message": "Operator review gate has not been run yet.",
        }
    ],
}

_MISSING_CLIENT_PACKAGE: dict[str, Any] = {
    "status": "missing",
    "available": False,
    "ready_for_client": False,
}


# Sent on every operator response so a misrouted client cannot mistake
# this surface for the client delivery contract. Defense-in-depth only;
# the real audience filter lives in :mod:`app.artifact_contract`.
_OPERATOR_AUDIENCE_HEADERS: dict[str, str] = {
    "X-TrashPanda-Audience": "operator_only",
}


router = APIRouter(prefix="/api/operator", tags=["operator"])


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


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


def _runtime_root() -> Path:
    """Resolve the runtime root from :mod:`app.server` at call time.

    Looking it up lazily lets the existing test fixture
    ``monkeypatch.setattr(server, 'RUNTIME_ROOT', tmp_path / 'runtime')``
    work for these routes too.
    """

    from . import server  # local import: avoids any startup cycle

    return server.RUNTIME_ROOT


def _resolve_run_dir(job_id: str) -> Path:
    """Return the latest ``run_*`` dir for ``job_id``.

    Resolution order:

    1. ``RUNTIME_ROOT/jobs/<job_id>/run_*`` — the canonical layout used by
       :func:`app.server._run_job`.
    2. ``JobResult.run_dir`` — covers boundary callers that recorded a
       run directory outside ``RUNTIME_ROOT`` (e.g. direct
       :func:`run_cleaning_job` users).

    Raises ``404 job_not_found`` if neither resolution succeeds, or
    ``409 run_dir_unavailable`` if the job exists but never wrote a run.
    The path is always resolved, never returned as a relative path.
    """

    from . import server

    job_output_dir = (_runtime_root() / "jobs" / job_id).resolve()
    if job_output_dir.is_dir():
        run_dir = server._latest_run_dir(job_output_dir)
        if run_dir is not None and server._is_under(run_dir, job_output_dir):
            return run_dir.resolve()

    # Fallback to the in-memory / DB JobResult path. Useful for tests
    # and for boundary-only callers that bypass the queue entirely.
    result = server._load_job_result(job_id)
    if result is None and not job_output_dir.is_dir():
        _raise_http_error(
            404,
            "job_not_found",
            "Job not found.",
            {"job_id": job_id},
        )
    if result is not None and result.run_dir is not None:
        candidate = Path(result.run_dir).resolve()
        if candidate.is_dir():
            return candidate

    _raise_http_error(
        409,
        "run_dir_unavailable",
        "Job has no run directory yet.",
        {"job_id": job_id},
    )
    # Unreachable; satisfies type checkers.
    raise AssertionError("unreachable")


def _read_json_or_missing(
    path: Path,
    missing_payload: dict[str, Any],
) -> dict[str, Any]:
    """Best-effort JSON read with a stable 'missing' fallback.

    * File absent  → return a copy of ``missing_payload``.
    * File present but unreadable / not JSON → return the missing
      payload with ``status`` flipped to ``unreadable`` so the operator
      UI can render a distinct warning. No traceback is exposed.
    """

    if not path.is_file():
        return dict(missing_payload)
    try:
        text = path.read_text(encoding="utf-8")
        data = json.loads(text)
    except Exception as exc:  # pragma: no cover - defensive guard
        _LOGGER.warning("Failed to read operator JSON %s: %s", path, exc)
        return {**missing_payload, "status": "unreadable", "available": False}
    if not isinstance(data, dict):
        # Operator JSONs are always objects; anything else is treated as
        # corruption from the operator UI's point of view.
        return {**missing_payload, "status": "unreadable", "available": False}
    return data


def _resolve_feedback_store_path(config_path: str | None) -> Path | None:
    """Resolve the V2.7 bounce-outcome store path from config.

    Returns ``None`` if ``bounce_ingestion`` is not configured. The
    feedback preview and ingest endpoints must surface that as a
    structured warning, not a 500.
    """

    project_paths = resolve_project_paths()
    cfg = load_config(
        config_path=Path(config_path) if config_path else None,
        base_dir=project_paths.project_root,
    )
    bounce_cfg = getattr(cfg, "bounce_ingestion", None)
    if bounce_cfg is None:
        return None
    raw = Path(getattr(bounce_cfg, "store_path"))
    if not raw.is_absolute():
        raw = (project_paths.project_root / raw).resolve()
    return raw


def _operator_response(
    payload: dict[str, Any],
    status_code: int = 200,
):
    """Wrap a JSON-friendly dict in a ``JSONResponse`` with audience header."""

    from fastapi.responses import JSONResponse

    return JSONResponse(
        status_code=status_code,
        content=payload,
        headers=_OPERATOR_AUDIENCE_HEADERS,
    )


# --------------------------------------------------------------------------- #
# Endpoints
# --------------------------------------------------------------------------- #


@router.post("/preflight")
async def operator_preflight(request: Request) -> Any:
    """Run the V2.9.2 rollout preflight against a path on disk.

    JSON body shape::

        {
          "input_path": "<required str>",
          "output_dir": "<optional str>",
          "config_path": "<optional str>",
          "operator_confirmed_large_run": false,
          "smtp_port_verified": false
        }

    Multipart upload is intentionally NOT supported here yet
    (deferred to V2.10.6 alongside the operator-UI upload component).
    Operators / smoke tests pass a path that already exists on disk.
    """

    try:
        body = await request.json()
    except Exception:
        _raise_http_error(400, "invalid_body", "Request body must be JSON.")

    if not isinstance(body, dict):
        _raise_http_error(400, "invalid_body", "Request body must be a JSON object.")

    input_path = body.get("input_path")
    if not isinstance(input_path, str) or not input_path.strip():
        _raise_http_error(
            400,
            "missing_input_path",
            "Field 'input_path' is required (string).",
            {"field": "input_path"},
        )

    output_dir = body.get("output_dir")
    config_path = body.get("config_path")
    confirmed = bool(body.get("operator_confirmed_large_run", False))
    smtp_verified = bool(body.get("smtp_port_verified", False))

    try:
        result = run_rollout_preflight(
            input_path,
            output_dir=output_dir,
            config_path=config_path,
            operator_confirmed_large_run=confirmed,
            smtp_port_verified=smtp_verified,
        )
    except Exception as exc:  # pragma: no cover - defensive
        _LOGGER.exception("Preflight execution failed for %s", input_path)
        _raise_http_error(
            500,
            "preflight_failed",
            "Preflight execution failed.",
            {"exception_class": exc.__class__.__name__},
        )

    return _operator_response(result)


@router.get("/jobs/{job_id}")
def operator_get_job(job_id: str) -> Any:
    """Operator-side view of a job. Wraps the existing job loader.

    Reuses :func:`app.server._load_job_result` so this surface stays
    consistent with the legacy ``/jobs/{id}`` payload. Marked
    operator-only via response header — it does not synthesise any new
    audience filtering and is not a delivery contract.
    """

    from . import server

    result = server._load_job_result(job_id)
    if result is None:
        _raise_http_error(
            404,
            "job_not_found",
            "Job not found.",
            {"job_id": job_id},
        )
    return _operator_response(job_result_to_dict(result))


@router.get("/jobs/{job_id}/smtp-runtime")
def operator_get_smtp_runtime(job_id: str) -> Any:
    """Read ``<run_dir>/smtp_runtime_summary.json`` (operator_only)."""

    run_dir = _resolve_run_dir(job_id)
    payload = _read_json_or_missing(
        run_dir / _SMTP_RUNTIME_SUMMARY_FILENAME,
        _MISSING_GENERIC,
    )
    return _operator_response(payload)


@router.get("/jobs/{job_id}/artifact-consistency")
def operator_get_artifact_consistency(job_id: str) -> Any:
    """Read ``<run_dir>/artifact_consistency.json`` (operator_only)."""

    run_dir = _resolve_run_dir(job_id)
    payload = _read_json_or_missing(
        run_dir / _ARTIFACT_CONSISTENCY_FILENAME,
        _MISSING_GENERIC,
    )
    return _operator_response(payload)


@router.post("/jobs/{job_id}/client-package")
def operator_build_client_package(job_id: str) -> Any:
    """Build the V2.9.6 client delivery package for ``job_id``.

    Requires the job to have reached a terminal completed state — the
    boundary helper does not enforce status, so we do it here. The
    builder filters strictly through the V2.9.5 audience contract.
    """

    from . import server

    result = server._load_job_result(job_id)
    if result is None:
        _raise_http_error(
            404,
            "job_not_found",
            "Job not found.",
            {"job_id": job_id},
        )

    status_value = (
        result.status.value if hasattr(result.status, "value") else str(result.status)
    )
    if status_value != JobStatus.COMPLETED:
        _raise_http_error(
            409,
            "job_not_completed",
            "Client package can only be built after the job completes.",
            {"job_id": job_id, "status": status_value},
        )

    run_dir = _resolve_run_dir(job_id)
    try:
        payload = build_client_package_for_job(run_dir)
    except Exception as exc:  # pragma: no cover - defensive
        _LOGGER.exception("Client package build failed for %s", job_id)
        _raise_http_error(
            500,
            "client_package_failed",
            "Client package build failed.",
            {"exception_class": exc.__class__.__name__},
        )
    return _operator_response(payload)


@router.get("/jobs/{job_id}/client-package")
def operator_get_client_package_manifest(job_id: str) -> Any:
    """Read the most recent ``client_package_manifest.json``.

    Returns a stable ``ready_for_client=false`` payload when the
    package directory or manifest is absent. Never builds the package.
    """

    run_dir = _resolve_run_dir(job_id)
    manifest_path = (
        run_dir / _CLIENT_PACKAGE_DIR_NAME / _CLIENT_PACKAGE_MANIFEST_FILENAME
    )
    payload = _read_json_or_missing(manifest_path, _MISSING_CLIENT_PACKAGE)
    return _operator_response(payload)


# --------------------------------------------------------------------------- #
# V2.10.0.2 — Safe client package download
# --------------------------------------------------------------------------- #


_CLIENT_SAFE_AUDIENCE = "client_safe"
_CLIENT_DOWNLOAD_AUDIENCE_HEADERS: dict[str, str] = {
    "X-TrashPanda-Audience": _CLIENT_SAFE_AUDIENCE,
}


def _download_blocked_response(
    error: str,
    message: str,
    *,
    extra: dict[str, Any] | None = None,
) -> Any:
    """Return a 409 with the V2.10.0.2 flat ``ready_for_client=false`` shape.

    The download endpoint deliberately does NOT use the nested
    ``{"error": {"error_type": ...}}`` envelope used elsewhere — the
    operator UI must be able to branch on a single top-level
    ``ready_for_client`` flag without unwrapping. Every blocking gate
    in :func:`operator_download_client_package` funnels through here.
    """

    from fastapi.responses import JSONResponse

    payload: dict[str, Any] = {
        "error": error,
        "message": message,
        "ready_for_client": False,
    }
    if extra:
        payload.update(extra)
    return JSONResponse(status_code=409, content=payload)


def _manifest_filename_escapes_package(
    filename: Any,
    package_dir_resolved: Path,
    package_dir: Path,
) -> bool:
    """Return True iff ``filename`` would resolve outside ``package_dir``.

    Rejects three classes of escape:

    * absolute paths (``/etc/passwd``, ``C:\\...``) — always outside;
    * any segment equal to ``..`` — caught before ``resolve()`` so
      Windows path semantics cannot mask it;
    * a resolved path whose ``relative_to(package_dir_resolved)`` raises
      — covers symlink-style escapes the manifest could smuggle.
    """

    if not isinstance(filename, str) or not filename:
        # Unknown shape: treat as escape so the operator notices.
        return True
    candidate_rel = Path(filename)
    if candidate_rel.is_absolute():
        return True
    if any(part == ".." for part in candidate_rel.parts):
        return True
    try:
        resolved = (package_dir / filename).resolve()
        resolved.relative_to(package_dir_resolved)
    except (OSError, ValueError):
        return True
    return False


@router.get("/jobs/{job_id}/client-package/download")
def operator_download_client_package(job_id: str) -> Any:
    """Stream the safe client delivery package as a ZIP.

    This is the **only** HTTP contract that emits client-safe artifacts
    in bulk. Every gate below must pass before any bytes are returned;
    failures yield ``409 Conflict`` with a flat
    ``ready_for_client=false`` payload (see
    :func:`_download_blocked_response`).

    Gates, in order:

    1. ``operator_review_summary.json`` exists and is valid JSON;
    2. ``ready_for_client`` is exactly ``true``;
    3. ``client_delivery_package/`` exists as a directory;
    4. ``client_package_manifest.json`` exists and is valid JSON;
    5. every ``files_included`` entry has ``audience == "client_safe"``;
    6. every ``files_included.filename`` resolves inside the package
       directory (no ``..``, no absolute paths, no symlink escapes).

    The ZIP body contains only files physically present under
    ``client_delivery_package/``. Operator-only run-dir artifacts
    (``operator_review_summary.json``, ``smtp_runtime_summary.json``,
    ``artifact_consistency.json``, ``staging.sqlite3``, ``logs/``,
    ``temp/``) are never included because the walk is rooted at the
    package dir, not the run dir.
    """

    run_dir = _resolve_run_dir(job_id)

    summary_path = run_dir / _OPERATOR_REVIEW_SUMMARY_FILENAME
    if not summary_path.is_file():
        return _download_blocked_response(
            "operator_review_missing",
            "Operator review gate has not been run yet.",
        )

    try:
        summary_data: Any = json.loads(summary_path.read_text(encoding="utf-8"))
    except Exception as exc:
        _LOGGER.warning(
            "operator_review_summary.json unreadable for %s: %s", job_id, exc
        )
        return _download_blocked_response(
            "operator_review_missing",
            "Operator review gate has not been run yet.",
        )
    if not isinstance(summary_data, dict):
        return _download_blocked_response(
            "operator_review_missing",
            "Operator review gate has not been run yet.",
        )

    if summary_data.get("ready_for_client") is not True:
        return _download_blocked_response(
            "not_ready_for_client",
            "Client package cannot be downloaded unless ready_for_client is true.",
            extra={"status": summary_data.get("status")},
        )

    package_dir = run_dir / _CLIENT_PACKAGE_DIR_NAME
    if not package_dir.is_dir():
        return _download_blocked_response(
            "client_package_missing",
            "client_delivery_package does not exist. "
            "Build the client package first.",
        )

    manifest_path = package_dir / _CLIENT_PACKAGE_MANIFEST_FILENAME
    if not manifest_path.is_file():
        return _download_blocked_response(
            "client_package_manifest_missing",
            "Client package manifest is missing.",
        )

    try:
        manifest_data: Any = json.loads(
            manifest_path.read_text(encoding="utf-8")
        )
    except Exception as exc:
        _LOGGER.warning(
            "client_package_manifest.json unreadable for %s: %s", job_id, exc
        )
        return _download_blocked_response(
            "client_package_manifest_unreadable",
            "Client package manifest could not be read safely.",
        )
    if not isinstance(manifest_data, dict):
        return _download_blocked_response(
            "client_package_manifest_unreadable",
            "Client package manifest could not be read safely.",
        )

    files_included = manifest_data.get("files_included") or []
    if not isinstance(files_included, list):
        return _download_blocked_response(
            "client_package_manifest_unreadable",
            "Client package manifest could not be read safely.",
        )

    bad_files: list[dict[str, Any]] = []
    for entry in files_included:
        if not isinstance(entry, dict):
            bad_files.append({"filename": None, "audience": None})
            continue
        audience = entry.get("audience")
        if audience != _CLIENT_SAFE_AUDIENCE:
            bad_files.append(
                {
                    "filename": entry.get("filename"),
                    "audience": audience,
                }
            )
    if bad_files:
        return _download_blocked_response(
            "client_package_contains_non_client_safe",
            "Client package contains non-client-safe artifacts and "
            "cannot be downloaded.",
            extra={"bad_files": bad_files},
        )

    package_dir_resolved = package_dir.resolve()
    for entry in files_included:
        if not isinstance(entry, dict):
            continue
        if _manifest_filename_escapes_package(
            entry.get("filename"), package_dir_resolved, package_dir
        ):
            return _download_blocked_response(
                "client_package_path_escape",
                "Client package manifest references a path outside the "
                "package directory.",
            )

    # Build the ZIP from the package dir only. The walk is rooted under
    # ``package_dir``, so run-dir-level files (operator_review_summary,
    # smtp_runtime_summary, artifact_consistency, staging.sqlite3, logs/,
    # temp/) are excluded by construction.
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for file_path in sorted(package_dir.rglob("*")):
            if not file_path.is_file():
                continue
            try:
                resolved = file_path.resolve()
                resolved.relative_to(package_dir_resolved)
            except (OSError, ValueError):
                # Defense-in-depth: a symlink under the package dir that
                # escapes is silently dropped. The manifest gate above is
                # the primary control; this guards against on-disk drift.
                continue
            arcname = str(file_path.relative_to(package_dir))
            zf.write(file_path, arcname=arcname)
    zip_bytes = buf.getvalue()

    download_name = f"trashpanda_client_delivery_package_{job_id}.zip"
    headers = {
        "Content-Disposition": f'attachment; filename="{download_name}"',
        **_CLIENT_DOWNLOAD_AUDIENCE_HEADERS,
    }
    return Response(
        content=zip_bytes,
        media_type="application/zip",
        headers=headers,
    )


@router.post("/jobs/{job_id}/review-gate")
def operator_run_review_gate(job_id: str) -> Any:
    """Run the V2.9.7 operator review gate for ``job_id``.

    Always returns the gate result (including ``ready_for_client`` and
    structured ``issues``); a missing client package becomes a
    block-severity issue inside the result rather than an HTTP error.
    """

    run_dir = _resolve_run_dir(job_id)
    try:
        payload = run_operator_review_for_job(run_dir)
    except Exception as exc:  # pragma: no cover - defensive
        _LOGGER.exception("Operator review gate failed for %s", job_id)
        _raise_http_error(
            500,
            "operator_review_failed",
            "Operator review gate execution failed.",
            {"exception_class": exc.__class__.__name__},
        )
    return _operator_response(payload)


@router.get("/jobs/{job_id}/operator-review")
def operator_get_review_summary(job_id: str) -> Any:
    """Read ``<run_dir>/operator_review_summary.json``.

    When absent, returns the canonical ``ready_for_client=false``
    missing payload so the operator UI can render an explicit "not
    yet run" state without inferring it from a 404.
    """

    run_dir = _resolve_run_dir(job_id)
    payload = _read_json_or_missing(
        run_dir / _OPERATOR_REVIEW_SUMMARY_FILENAME,
        _MISSING_OPERATOR_REVIEW,
    )
    return _operator_response(payload)


@router.post("/feedback/ingest")
async def operator_feedback_ingest(request: Request) -> Any:
    """Ingest a V2.7 bounce-outcome CSV into the feedback store.

    JSON body shape::

        {
          "feedback_csv_path": "<required str>",
          "config_path": "<optional str>"
        }

    Multipart upload is deferred to V2.10.6. The CSV must already exist
    on the server. Per-row failures (missing email, unknown outcome, …)
    are counted in the boundary's ``IngestionSummary``; ingestion-level
    failures populate ``error`` instead of raising.
    """

    try:
        body = await request.json()
    except Exception:
        _raise_http_error(400, "invalid_body", "Request body must be JSON.")
    if not isinstance(body, dict):
        _raise_http_error(400, "invalid_body", "Request body must be a JSON object.")

    feedback_csv_path = body.get("feedback_csv_path")
    if not isinstance(feedback_csv_path, str) or not feedback_csv_path.strip():
        _raise_http_error(
            400,
            "missing_feedback_csv_path",
            "Field 'feedback_csv_path' is required (string).",
            {"field": "feedback_csv_path"},
        )

    csv_path = Path(feedback_csv_path)
    if not csv_path.is_file():
        _raise_http_error(
            404,
            "feedback_csv_not_found",
            "Feedback CSV does not exist on the server.",
            {"feedback_csv_path": str(csv_path)},
        )

    config_path = body.get("config_path")
    payload = ingest_bounce_feedback(csv_path, config_path=config_path)
    return _operator_response(payload)


@router.get("/feedback/preview")
def operator_feedback_preview_get(config_path: str | None = None) -> Any:
    """Compute the V2.9.8 feedback → domain intel preview (read-mode)."""

    return _feedback_preview_payload(config_path=config_path)


@router.post("/feedback/preview")
async def operator_feedback_preview_post(request: Request) -> Any:
    """Compute the V2.9.8 feedback → domain intel preview (write-mode).

    Optional JSON body::

        {
          "feedback_store_path": "<optional override str>",
          "config_path":         "<optional str>",
          "output_dir":          "<optional str — when set, the JSON
                                  preview is also written here>"
        }
    """

    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {}

    return _feedback_preview_payload(
        feedback_store_path_override=body.get("feedback_store_path"),
        config_path=body.get("config_path"),
        output_dir=body.get("output_dir"),
    )


def _feedback_preview_payload(
    *,
    feedback_store_path_override: str | None = None,
    config_path: str | None = None,
    output_dir: str | None = None,
) -> Any:
    """Shared implementation for the GET / POST feedback-preview routes."""

    if isinstance(feedback_store_path_override, str) and feedback_store_path_override.strip():
        store_path: Path | None = Path(feedback_store_path_override)
    else:
        store_path = _resolve_feedback_store_path(config_path)

    if store_path is None:
        return _operator_response(
            {
                "feedback_available": False,
                "warnings": ["bounce_ingestion_not_configured"],
                "records": [],
                "total_domains": 0,
                "total_observations": 0,
                "known_good_count": 0,
                "known_risky_count": 0,
                "cold_start_count": 0,
                "unknown_count": 0,
            }
        )

    try:
        payload = build_feedback_domain_intel_preview_for_job(
            store_path,
            output_dir=output_dir,
        )
    except Exception as exc:  # pragma: no cover - defensive
        _LOGGER.exception("Feedback preview failed for %s", store_path)
        _raise_http_error(
            500,
            "feedback_preview_failed",
            "Feedback preview execution failed.",
            {"exception_class": exc.__class__.__name__},
        )
    return _operator_response(payload)


__all__ = [
    "router",
]
