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

from fastapi import APIRouter, Depends, HTTPException, Request, Response

from .auth import require_operator_token
from .api_boundary import (
    JobStatus,
    build_client_package_for_job,
    build_feedback_domain_intel_preview_for_job,
    ingest_bounce_feedback,
    job_result_to_dict,
    run_operator_review_for_job,
    run_rollout_preflight,
)
from .artifact_contract import is_safe_only_artifact
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

# V2.10.8.3 — safe-only partial delivery contract.
_SAFE_ONLY_NOTE_FILENAME = "SAFE_ONLY_DELIVERY_NOTE.txt"
_SAFE_ONLY_OVERRIDE_HEADER = "X-TrashPanda-Operator-Override"
_SAFE_ONLY_OVERRIDE_VALUE = "safe-only"
_SAFE_ONLY_DELIVERY_MODE = "safe_only"
_SAFE_ONLY_DELIVERY_LABEL = "safe_only_partial"


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


router = APIRouter(
    prefix="/api/operator",
    tags=["operator"],
    # Apply bearer-token auth to every operator endpoint. Production
    # deployments configure ``TRASHPANDA_OPERATOR_TOKEN`` (or its
    # plural ``TRASHPANDA_OPERATOR_TOKENS`` for rotation) and the
    # dependency rejects requests without a matching value. When the
    # env var is unset (local dev, the existing test suite) the
    # dependency is a no-op so behaviour is unchanged.
    dependencies=[Depends(require_operator_token)],
)


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


# --------------------------------------------------------------------------- #
# V2.10.8.3 — Safe-only partial client package download
# --------------------------------------------------------------------------- #


@router.get("/jobs/{job_id}/client-package/download-safe-only")
def operator_download_safe_only_client_package(
    job_id: str,
    request: Request,
) -> Any:
    """Stream the *safe-only* partial client delivery package as a ZIP.

    Distinct from the full download endpoint: this is the partial-
    delivery channel exposed when ``ready_for_client=false`` but the
    operator review gate flagged ``ready_for_client_partial=true`` with
    ``partial_delivery_mode="safe_only"``. The response carries only the
    artifacts in ``manifest.safe_only_delivery.files_included`` — a
    *strict subset* of the package's ``client_safe`` files, never
    ``review_emails``, ``invalid_or_bounce_risk``, ``duplicate_emails``,
    or ``hard_fail_emails`` (see :mod:`app.artifact_contract`).

    Gates, in order — every failure yields ``409 Conflict`` with a flat
    payload containing the appropriate ``ready_for_client`` and
    ``ready_for_client_partial`` flags so the operator UI can branch
    without unwrapping:

    1. ``operator_review_summary.json`` exists and is valid JSON;
    2. ``ready_for_client`` is not ``True`` (full-ready ⇒ use the
       standard endpoint);
    3. ``ready_for_client_partial`` is exactly ``True``;
    4. ``partial_delivery_mode`` is exactly ``"safe_only"``;
    5. the request carries ``X-TrashPanda-Operator-Override: safe-only``;
    6. ``client_delivery_package/`` exists;
    7. ``client_package_manifest.json`` exists and is valid JSON;
    8. ``manifest.safe_only_delivery`` is supported, references the
       expected note filename, and has a non-empty ``files_included``;
    9. ``SAFE_ONLY_DELIVERY_NOTE.txt`` exists physically on disk;
    10. every entry has ``audience == "client_safe"``;
    11. every entry passes :func:`is_safe_only_artifact` (subset gate);
    12. every entry resolves inside the package directory;
    13. every entry exists on disk.

    The ZIP is built ONLY from the manifest's
    ``safe_only_delivery.files_included`` — no ``rglob`` walk —
    so review/rejected XLSXs and ``client_package_manifest.json``
    are excluded by construction.
    """

    run_dir = _resolve_run_dir(job_id)

    # --- 1) Read operator review summary ------------------------------- #
    summary_path = run_dir / _OPERATOR_REVIEW_SUMMARY_FILENAME
    if not summary_path.is_file():
        return _download_blocked_response(
            "operator_review_missing",
            "Operator review summary is required before safe-only download.",
            extra={"ready_for_client_partial": False},
        )

    try:
        summary_data: Any = json.loads(summary_path.read_text(encoding="utf-8"))
    except Exception as exc:
        _LOGGER.warning(
            "operator_review_summary.json unreadable for %s: %s", job_id, exc
        )
        return _download_blocked_response(
            "operator_review_unreadable",
            "Operator review summary could not be read before safe-only download.",
            extra={"ready_for_client_partial": False},
        )
    if not isinstance(summary_data, dict):
        return _download_blocked_response(
            "operator_review_unreadable",
            "Operator review summary could not be read before safe-only download.",
            extra={"ready_for_client_partial": False},
        )

    # --- 2) Full-ready runs use the standard endpoint ------------------ #
    if summary_data.get("ready_for_client") is True:
        return _download_blocked_response(
            "safe_only_not_required",
            "Full delivery is already ready; use the standard client "
            "package download endpoint.",
            extra={
                "ready_for_client": True,
                "ready_for_client_partial": False,
            },
        )

    # --- 3) Partial readiness must be on ------------------------------- #
    if summary_data.get("ready_for_client_partial") is not True:
        return _download_blocked_response(
            "safe_only_unavailable",
            "Safe-only partial delivery is not available for this run.",
            extra={"ready_for_client_partial": False},
        )

    # --- 4) Delivery mode must be safe_only ---------------------------- #
    if summary_data.get("partial_delivery_mode") != _SAFE_ONLY_DELIVERY_MODE:
        return _download_blocked_response(
            "safe_only_mode_invalid",
            "Safe-only partial delivery mode is not enabled for this run.",
            extra={"ready_for_client_partial": True},
        )

    # --- 5) Operator override header ----------------------------------- #
    # Header name is matched case-insensitively by HTTP/Starlette; the
    # *value* must match exactly per the V2.10.8.3 contract.
    override_value = request.headers.get(_SAFE_ONLY_OVERRIDE_HEADER)
    if override_value != _SAFE_ONLY_OVERRIDE_VALUE:
        return _download_blocked_response(
            "safe_only_override_required",
            "Safe-only partial delivery requires explicit operator override.",
            extra={"ready_for_client_partial": True},
        )

    # --- 6) Package directory ------------------------------------------ #
    package_dir = run_dir / _CLIENT_PACKAGE_DIR_NAME
    if not package_dir.is_dir():
        return _download_blocked_response(
            "client_package_missing",
            "Client delivery package is missing.",
            extra={"ready_for_client_partial": True},
        )

    # --- 7) Manifest --------------------------------------------------- #
    manifest_path = package_dir / _CLIENT_PACKAGE_MANIFEST_FILENAME
    if not manifest_path.is_file():
        return _download_blocked_response(
            "client_package_manifest_missing",
            "Client package manifest is missing.",
            extra={"ready_for_client_partial": True},
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
            "Client package manifest could not be read.",
            extra={"ready_for_client_partial": True},
        )
    if not isinstance(manifest_data, dict):
        return _download_blocked_response(
            "client_package_manifest_unreadable",
            "Client package manifest could not be read.",
            extra={"ready_for_client_partial": True},
        )

    # --- 8) safe_only_delivery block ----------------------------------- #
    safe_only = manifest_data.get("safe_only_delivery")
    safe_only_files: Any = None
    if isinstance(safe_only, dict):
        safe_only_files = safe_only.get("files_included")
    if (
        not isinstance(safe_only, dict)
        or safe_only.get("supported") is not True
        or safe_only.get("note_filename") != _SAFE_ONLY_NOTE_FILENAME
        or not isinstance(safe_only_files, list)
        or not safe_only_files
    ):
        return _download_blocked_response(
            "safe_only_manifest_missing",
            "Safe-only delivery manifest block is missing or unsupported.",
            extra={"ready_for_client_partial": True},
        )

    # --- 9) The note must exist physically on disk --------------------- #
    note_path = package_dir / _SAFE_ONLY_NOTE_FILENAME
    if not note_path.is_file():
        return _download_blocked_response(
            "safe_only_note_missing",
            "SAFE_ONLY_DELIVERY_NOTE.txt is required for safe-only delivery.",
            extra={"ready_for_client_partial": True},
        )

    # --- 10) audience == client_safe ----------------------------------- #
    bad_audience: list[dict[str, Any]] = []
    for entry in safe_only_files:
        if not isinstance(entry, dict):
            bad_audience.append({"filename": None, "audience": None})
            continue
        if entry.get("audience") != _CLIENT_SAFE_AUDIENCE:
            bad_audience.append(
                {
                    "filename": entry.get("filename"),
                    "audience": entry.get("audience"),
                }
            )
    if bad_audience:
        return _download_blocked_response(
            "safe_only_contains_non_client_safe",
            "Safe-only package contains non-client-safe artifacts.",
            extra={
                "ready_for_client_partial": True,
                "bad_files": bad_audience,
            },
        )

    # --- 11) Subset gate: must pass is_safe_only_artifact -------------- #
    bad_subset: list[dict[str, Any]] = []
    for entry in safe_only_files:
        if not isinstance(entry, dict):
            continue
        token = entry.get("key") or entry.get("filename") or ""
        if not is_safe_only_artifact(str(token)):
            bad_subset.append(
                {"key": entry.get("key"), "filename": entry.get("filename")}
            )
    if bad_subset:
        return _download_blocked_response(
            "safe_only_contains_non_safe_only",
            "Safe-only package contains artifacts outside the safe-only "
            "allowlist.",
            extra={
                "ready_for_client_partial": True,
                "bad_files": bad_subset,
            },
        )

    # --- 12) Path-escape gate ------------------------------------------ #
    package_dir_resolved = package_dir.resolve()
    for entry in safe_only_files:
        if not isinstance(entry, dict):
            continue
        if _manifest_filename_escapes_package(
            entry.get("filename"), package_dir_resolved, package_dir
        ):
            return _download_blocked_response(
                "safe_only_path_escape",
                "Safe-only manifest contains a filename outside the "
                "package directory.",
                extra={"ready_for_client_partial": True},
            )

    # --- 13) Every listed file must exist on disk ---------------------- #
    missing_files: list[str] = []
    for entry in safe_only_files:
        if not isinstance(entry, dict):
            continue
        filename = entry.get("filename")
        if not isinstance(filename, str):
            continue
        if not (package_dir / filename).is_file():
            missing_files.append(filename)
    if missing_files:
        return _download_blocked_response(
            "safe_only_file_missing",
            "Safe-only manifest references a missing file.",
            extra={
                "ready_for_client_partial": True,
                "missing_files": missing_files,
            },
        )

    # --- Build the safe-only ZIP from the manifest list ONLY ----------- #
    # No directory walk: the ZIP carries strictly what the manifest
    # advertises. ``client_package_manifest.json`` is therefore excluded
    # by construction unless explicitly listed (which by V2.10.8.2 it is
    # not). Arcnames are prefixed with ``client_delivery_package/`` so
    # the operator can extract the partial delivery alongside an
    # existing full package without name collisions.
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for entry in safe_only_files:
            if not isinstance(entry, dict):
                continue
            filename = entry.get("filename")
            if not isinstance(filename, str) or not filename:
                continue
            file_path = package_dir / filename
            try:
                resolved = file_path.resolve()
                resolved.relative_to(package_dir_resolved)
            except (OSError, ValueError):
                # Defense-in-depth: should be caught above.
                continue
            arcname = f"{_CLIENT_PACKAGE_DIR_NAME}/{filename}"
            zf.write(file_path, arcname=arcname)
    zip_bytes = buf.getvalue()

    download_name = f"trashpanda_safe_only_client_package_{job_id}.zip"
    headers = {
        "Content-Disposition": f'attachment; filename="{download_name}"',
        "X-TrashPanda-Audience": _CLIENT_SAFE_AUDIENCE,
        "X-TrashPanda-Delivery-Mode": _SAFE_ONLY_DELIVERY_LABEL,
        "X-TrashPanda-Ready-For-Client": "false",
        "X-TrashPanda-Ready-For-Client-Partial": "true",
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


# --------------------------------------------------------------------------- #
# Client bundle — the "Send to client" endpoint                                #
#                                                                              #
# A pragmatic, opinionated endpoint built on top of the existing review-gate   #
# / client-package machinery. The goal is to collapse the three-step operator  #
# workflow (review-gate → build-package → download) into a single GET request  #
# the UI can wire to one giant button.                                         #
#                                                                              #
# Behaviour:                                                                   #
#   1. Auto-runs the review gate if needed (idempotent).                       #
#   2. Auto-builds the client delivery package if missing.                     #
#   3. Returns a *minimal* ZIP containing only what the customer should        #
#      receive: the PRIMARY artifact, README_CLIENT.txt, summary_report.xlsx.  #
#      No technical CSVs, no operator-only summaries, no manifest noise.       #
#   4. When the gate is WARN/BLOCK but the package has at least one safe row   #
#      we still return a ZIP — the README is updated to flag the partial       #
#      delivery so the operator can defend the lower count to the client.     #
#   5. When literally nothing safe exists, returns 409 with a human reason.    #
#                                                                              #
# This endpoint is *additive*: the legacy /client-package/download endpoint    #
# stays untouched for the operator/auditor flow.                               #
# --------------------------------------------------------------------------- #


_CLIENT_BUNDLE_PRIMARY_KEYS: tuple[str, ...] = (
    "approved_original_format",
    "valid_emails",
)
_CLIENT_BUNDLE_SUPPORT_FILES: tuple[str, ...] = (
    "README_CLIENT.txt",
    "summary_report.xlsx",
    "SAFE_ONLY_DELIVERY_NOTE.txt",
)


def _bundle_filename(run_dir: Path, job_id: str) -> str:
    """Build a friendly ZIP filename: ``<input>_clean_<YYYY-MM-DD>.zip``."""

    from datetime import datetime as _dt

    stem = "trashpanda"
    job_meta = run_dir.parent / "job_meta.json"
    if job_meta.is_file():
        try:
            meta = json.loads(job_meta.read_text(encoding="utf-8"))
            input_filename = meta.get("input_filename")
            if isinstance(input_filename, str) and input_filename.strip():
                base = input_filename.rsplit("/", 1)[-1].rsplit("\\", 1)[-1]
                if "." in base:
                    base = base.rsplit(".", 1)[0]
                stem = "".join(
                    c if (c.isalnum() or c in "-_") else "_"
                    for c in base
                ).strip("_") or stem
        except Exception:  # pragma: no cover - filename is cosmetic only
            pass
    today = _dt.utcnow().strftime("%Y-%m-%d")
    return f"{stem}_clean_{today}.zip"


def _read_manifest(package_dir: Path) -> dict[str, Any] | None:
    manifest_path = package_dir / _CLIENT_PACKAGE_MANIFEST_FILENAME
    if not manifest_path.is_file():
        return None
    try:
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def _ensure_review_summary(run_dir: Path) -> dict[str, Any] | None:
    """Run the operator review gate if its summary file is missing."""

    summary_path = run_dir / _OPERATOR_REVIEW_SUMMARY_FILENAME
    if summary_path.is_file():
        try:
            data = json.loads(summary_path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
        except Exception:
            pass
    try:
        return run_operator_review_for_job(run_dir)
    except Exception as exc:  # pragma: no cover - defensive
        _LOGGER.warning(
            "client-bundle: failed to run review gate for %s: %s", run_dir, exc
        )
        return None


def _ensure_client_package(run_dir: Path) -> Path | None:
    """Build the client delivery package if it's missing. Returns its path."""

    package_dir = run_dir / _CLIENT_PACKAGE_DIR_NAME
    if package_dir.is_dir() and (package_dir / _CLIENT_PACKAGE_MANIFEST_FILENAME).is_file():
        return package_dir
    try:
        build_client_package_for_job(run_dir)
    except Exception as exc:  # pragma: no cover - defensive
        _LOGGER.warning(
            "client-bundle: failed to build client package for %s: %s", run_dir, exc
        )
        return None
    return package_dir if package_dir.is_dir() else None


def _resolve_primary_filename(manifest: dict[str, Any]) -> str | None:
    primary = manifest.get("primary_artifact")
    if isinstance(primary, dict):
        filename = primary.get("filename")
        if isinstance(filename, str) and filename:
            return filename
    files = manifest.get("files_included") or []
    for preferred in _CLIENT_BUNDLE_PRIMARY_KEYS:
        for entry in files:
            if isinstance(entry, dict) and entry.get("key") == preferred:
                filename = entry.get("filename")
                if isinstance(filename, str) and filename:
                    return filename
    return None


def _bundle_blocked_response(error: str, message: str) -> Response:
    return Response(
        content=json.dumps(
            {
                "error": error,
                "message": message,
                "ready_for_client": False,
            }
        ),
        media_type="application/json",
        status_code=409,
    )


@router.get("/jobs/{job_id}/client-bundle/download")
def operator_download_client_bundle(job_id: str) -> Any:
    """One-click "Send to client" download.

    Auto-runs the review gate and builds the client package if needed,
    then ships a curated ZIP containing just the customer-facing files
    (PRIMARY artifact + README + summary). Reduces the legacy three-
    step operator flow to a single GET request.
    """

    run_dir = _resolve_run_dir(job_id)

    review = _ensure_review_summary(run_dir)
    if review is None:
        return _bundle_blocked_response(
            "review_gate_unavailable",
            "Review gate could not be evaluated for this job. The pipeline "
            "may still be running, or the artifact contract is incomplete.",
        )

    package_dir = _ensure_client_package(run_dir)
    if package_dir is None:
        return _bundle_blocked_response(
            "client_package_missing",
            "Client package could not be built for this job.",
        )

    manifest = _read_manifest(package_dir)
    if manifest is None:
        return _bundle_blocked_response(
            "client_package_manifest_missing",
            "Client package manifest is missing or unreadable.",
        )

    primary_filename = _resolve_primary_filename(manifest)
    safe_count = int(manifest.get("safe_count") or 0)
    if primary_filename is None or safe_count <= 0:
        return _bundle_blocked_response(
            "no_safe_rows",
            "This job has no rows we are willing to recommend for sending. "
            "Re-run with extra-strict filtering or fix the input list.",
        )

    ready = bool(review.get("ready_for_client"))
    delivery_mode = "full" if ready else "safe_only_partial"

    # Walk the package dir and pick only the PRIMARY + small support set.
    selected: list[Path] = []
    package_dir_resolved = package_dir.resolve()
    for filename in [primary_filename, *_CLIENT_BUNDLE_SUPPORT_FILES]:
        candidate = package_dir / filename
        if not candidate.is_file():
            continue
        try:
            candidate.resolve().relative_to(package_dir_resolved)
        except (OSError, ValueError):
            continue
        selected.append(candidate)

    # If somehow the primary went missing between the manifest read and
    # the disk walk, fail loud rather than ship an empty ZIP.
    if not any(p.name == primary_filename for p in selected):
        return _bundle_blocked_response(
            "primary_artifact_unavailable",
            "Primary artifact disappeared between manifest build and "
            "download.",
        )

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for path in selected:
            zf.write(path, arcname=path.name)
    download_name = _bundle_filename(run_dir, job_id)

    headers = {
        "Content-Disposition": f'attachment; filename="{download_name}"',
        "X-TrashPanda-Audience": _CLIENT_SAFE_AUDIENCE,
        "X-TrashPanda-Delivery-Mode": delivery_mode,
        "X-TrashPanda-Ready-For-Client": "true" if ready else "false",
    }
    return Response(
        content=buf.getvalue(),
        media_type="application/zip",
        headers=headers,
    )


@router.get("/jobs/{job_id}/client-bundle/summary")
def operator_get_client_bundle_summary(job_id: str) -> Any:
    """Summary used by the UI's "Send to client" panel.

    Mirrors what the bundle download would contain *without* actually
    streaming bytes. Lets the UI render the giant button + counts +
    "Send is partial" disclaimer before the operator clicks download.
    """

    run_dir = _resolve_run_dir(job_id)

    review = _ensure_review_summary(run_dir) or {}
    package_dir = _ensure_client_package(run_dir)
    manifest = _read_manifest(package_dir) if package_dir else None

    safe_count = int((manifest or {}).get("safe_count") or 0)
    review_count = int((manifest or {}).get("review_count") or 0)
    rejected_count = int((manifest or {}).get("rejected_count") or 0)
    primary_filename = (
        _resolve_primary_filename(manifest) if manifest else None
    )
    ready = bool(review.get("ready_for_client"))
    available = bool(primary_filename and safe_count > 0)
    download_name = _bundle_filename(run_dir, job_id) if available else None

    return _operator_response(
        {
            "available": available,
            "ready_for_client": ready,
            "delivery_mode": "full" if ready else "safe_only_partial",
            "primary_filename": primary_filename,
            "download_filename": download_name,
            "safe_count": safe_count,
            "review_count": review_count,
            "rejected_count": rejected_count,
            "issues": review.get("issues") or [],
        }
    )


# --------------------------------------------------------------------------- #
# Extra Strict Offline — re-clean entry point                                  #
#                                                                              #
# Runs the offline extra-strict filter on a finished job's run dir and ships   #
# the produced 6-file bundle (PRIMARY xlsx + review + removed + rejected +    #
# summary.txt + README) as a single ZIP. Uses the same in-process function as #
# scripts/extra_strict_clean.py — no shell-out, no extra dependency surface.   #
# --------------------------------------------------------------------------- #


@router.get("/jobs/{job_id}/extra-strict/download")
def operator_download_extra_strict(job_id: str) -> Any:
    """Run Extra Strict Offline on a finished job and stream the ZIP.

    Idempotent: if ``run_dir/extra_strict/`` already has the artifacts
    they're shipped as-is; otherwise the cleaner is run first. Useful
    for the "customer reported bounces — re-clean stricter" flow.
    """

    from .extra_strict_clean import (
        ExtraStrictConfig,
        run_extra_strict_clean,
    )

    run_dir = _resolve_run_dir(job_id)

    try:
        result = run_extra_strict_clean(
            run_dir, config=ExtraStrictConfig(),
        )
    except FileNotFoundError as exc:
        return _bundle_blocked_response(
            "run_dir_missing",
            f"Job run directory is not available: {exc}",
        )
    except Exception as exc:  # pragma: no cover - defensive
        _LOGGER.exception(
            "extra-strict cleaner failed for %s: %s", job_id, exc
        )
        return _bundle_blocked_response(
            "extra_strict_failed",
            "Extra Strict Offline cleaner crashed mid-run.",
        )

    files: list[Path] = [
        result.primary_xlsx,
        result.review_xlsx,
        result.removed_xlsx,
        result.rejected_xlsx,
        result.summary_txt,
        result.readme_txt,
    ]
    files = [p for p in files if p.is_file()]
    if not files:
        return _bundle_blocked_response(
            "extra_strict_empty",
            "Extra Strict run produced no files.",
        )

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for path in files:
            zf.write(path, arcname=path.name)
    download_name = _bundle_filename(run_dir, job_id).replace(
        "_clean_", "_extrastrict_"
    )
    headers = {
        "Content-Disposition": f'attachment; filename="{download_name}"',
        "X-TrashPanda-Audience": _CLIENT_SAFE_AUDIENCE,
        "X-TrashPanda-Bundle-Mode": "extra_strict",
    }
    return Response(
        content=buf.getvalue(),
        media_type="application/zip",
        headers=headers,
    )


__all__ = [
    "router",
]
