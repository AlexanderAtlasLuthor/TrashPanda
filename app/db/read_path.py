"""Transitional read-path helpers for DB-first legacy job access.

Every public function in this module is **non-blocking** when the database is
unavailable: the first thing each does is consult :func:`is_db_available`,
which is a cheap TTL-cached probe. If the DB is down the call returns the
neutral fallback (``None``) immediately and request handlers can proceed with
the legacy JSON path.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from .models import Artifact, Job
from .session import is_db_available, log_db_failure, session_scope


LOGGER = logging.getLogger(__name__)

LEGACY_JOB_SOURCE_PREFIX = "legacy_job_id:"


def _legacy_job_source(legacy_job_id: str) -> str:
    return f"{LEGACY_JOB_SOURCE_PREFIX}{legacy_job_id}"


def _legacy_job_id_from_source(source: str | None, fallback_job_id: str) -> str:
    if source and source.startswith(LEGACY_JOB_SOURCE_PREFIX):
        return source[len(LEGACY_JOB_SOURCE_PREFIX):]
    return fallback_job_id


def _serialize_job(job: Job) -> dict[str, Any]:
    return {
        "job_id": _legacy_job_id_from_source(job.source, str(job.id)),
        "db_job_id": job.id,
        "input_filename": job.input_filename,
        "status": job.status,
        "queued_at": job.queued_at,
        "started_at": job.started_at,
        "completed_at": job.completed_at,
        "failed_at": job.failed_at,
        "cancelled_at": job.cancelled_at,
        "created_at": job.created_at,
        "updated_at": job.updated_at,
        "finished_at": job.completed_at or job.failed_at or job.cancelled_at,
        "summary": {
            "total_input_rows": job.summary_total_input_rows,
            "total_valid": job.summary_total_valid,
            "total_review": job.summary_total_review,
            "total_invalid_or_bounce_risk": job.summary_total_invalid_or_bounce_risk,
            "duplicates_removed": job.summary_duplicates_removed,
            "typo_corrections": job.summary_typo_corrections,
            "disposable_emails": job.summary_disposable_emails,
            "placeholder_or_fake_emails": job.summary_placeholder_or_fake_emails,
            "role_based_emails": job.summary_role_based_emails,
        },
        "error": (
            {
                "error_type": job.error_type,
                "message": job.error_message,
                "details": job.error_details,
            }
            if job.error_type or job.error_message
            else None
        ),
    }


def _serialize_artifact(artifact: Artifact) -> dict[str, Any]:
    return {
        "artifact_key": artifact.artifact_key,
        "artifact_group": artifact.artifact_group,
        "visibility": artifact.visibility,
        "display_filename": artifact.display_filename,
        "storage_key": artifact.storage_key,
        "storage_location": artifact.storage_location,
        "content_kind": artifact.content_kind,
        "content_type": artifact.content_type,
        "size_bytes": artifact.size_bytes,
        "status": artifact.status,
        "created_at": artifact.created_at,
        "registered_at": artifact.registered_at,
    }


def load_job_record(legacy_job_id: str) -> dict[str, Any] | None:
    if not is_db_available():
        return None
    try:
        with session_scope() as session:
            job = session.scalar(select(Job).where(Job.source == _legacy_job_source(legacy_job_id)))
            if job is None:
                return None
            return _serialize_job(job)
    except SQLAlchemyError as exc:
        log_db_failure(
            "Skipping DB job load for legacy job %s: %s",
            legacy_job_id,
            exc,
        )
        return None
    except Exception as exc:  # pragma: no cover - defence in depth
        LOGGER.warning(
            "Skipping DB job load for legacy job %s: %s",
            legacy_job_id,
            exc,
            exc_info=True,
        )
        return None


def list_job_records(limit: int) -> list[dict[str, Any]] | None:
    if not is_db_available():
        return None
    try:
        with session_scope() as session:
            jobs = session.scalars(
                select(Job)
                .where(Job.source.like(f"{LEGACY_JOB_SOURCE_PREFIX}%"))
                .order_by(Job.created_at.desc())
                .limit(limit)
            ).all()
            return [_serialize_job(job) for job in jobs]
    except SQLAlchemyError as exc:
        log_db_failure("Skipping DB job list load: %s", exc)
        return None
    except Exception as exc:  # pragma: no cover - defence in depth
        LOGGER.warning("Skipping DB job list load: %s", exc, exc_info=True)
        return None


def load_artifact_record(
    legacy_job_id: str,
    artifact_key: str,
    *,
    visibility: str | None = None,
) -> dict[str, Any] | None:
    if not is_db_available():
        return None
    try:
        with session_scope() as session:
            job = session.scalar(select(Job).where(Job.source == _legacy_job_source(legacy_job_id)))
            if job is None:
                return None

            stmt = select(Artifact).where(
                Artifact.job_id == job.id,
                Artifact.artifact_key == artifact_key,
                Artifact.status.in_(("available", "registered")),
            )
            if visibility is not None:
                stmt = stmt.where(Artifact.visibility == visibility)

            artifact = session.scalar(stmt)
            if artifact is None:
                return None
            return _serialize_artifact(artifact)
    except SQLAlchemyError as exc:
        log_db_failure(
            "Skipping DB artifact load for legacy job %s (%s): %s",
            legacy_job_id,
            artifact_key,
            exc,
        )
        return None
    except Exception as exc:  # pragma: no cover - defence in depth
        LOGGER.warning(
            "Skipping DB artifact load for legacy job %s (%s): %s",
            legacy_job_id,
            artifact_key,
            exc,
            exc_info=True,
        )
        return None


def load_artifact_records(
    legacy_job_id: str,
    *,
    visibility: str | None = None,
) -> list[dict[str, Any]] | None:
    if not is_db_available():
        return None
    try:
        with session_scope() as session:
            job = session.scalar(select(Job).where(Job.source == _legacy_job_source(legacy_job_id)))
            if job is None:
                return None

            stmt = select(Artifact).where(
                Artifact.job_id == job.id,
                Artifact.status.in_(("available", "registered")),
            )
            if visibility is not None:
                stmt = stmt.where(Artifact.visibility == visibility)

            artifacts = session.scalars(stmt.order_by(Artifact.created_at.asc())).all()
            return [_serialize_artifact(artifact) for artifact in artifacts]
    except SQLAlchemyError as exc:
        log_db_failure(
            "Skipping DB artifact list load for legacy job %s: %s",
            legacy_job_id,
            exc,
        )
        return None
    except Exception as exc:  # pragma: no cover - defence in depth
        LOGGER.warning(
            "Skipping DB artifact list load for legacy job %s: %s",
            legacy_job_id,
            exc,
            exc_info=True,
        )
        return None
