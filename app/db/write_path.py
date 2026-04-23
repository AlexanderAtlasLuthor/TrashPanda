"""Transitional write-path helpers for bridging legacy jobs into PostgreSQL.

Every public entry point in this module is **non-blocking** when the DB is
unavailable. We gate each call with :func:`is_db_available` (a cheap,
TTL-cached ``SELECT 1``) so request handlers never pay the libpq
connect-timeout when Postgres is down — they get an instant ``None`` /
no-op fallback and continue with the legacy JSON path.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from .models import (
    ActorKind,
    Artifact,
    ArtifactContentKind,
    ArtifactStatus,
    ArtifactVisibility,
    AuditEvent,
    EventSeverity,
    Job,
    JobEvent,
    JobEventType,
    JobStatus,
    Membership,
    MembershipRole,
    MembershipStatus,
    Organization,
    OrganizationStatus,
    ReviewDecision,
    UploadedFile,
    UploadedFileContentKind,
    UploadedFileStatus,
    User,
    UserStatus,
)
from .session import is_db_available, log_db_failure, session_scope


LOGGER = logging.getLogger(__name__)

TEMP_DEFAULT_USER_EMAIL = "temporary-backend-user@trashpanda.local"
TEMP_DEFAULT_USER_NAME = "Temporary Backend User"
TEMP_DEFAULT_ORG_SLUG = "temporary-default-org"
TEMP_DEFAULT_ORG_NAME = "Temporary Default Organization"
LEGACY_JOB_SOURCE_PREFIX = "legacy_job_id:"

_CONTENT_TYPE_BY_SUFFIX = {
    ".csv": "text/csv",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".json": "application/json",
    ".zip": "application/zip",
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _legacy_job_source(legacy_job_id: str) -> str:
    return f"{LEGACY_JOB_SOURCE_PREFIX}{legacy_job_id}"


def _legacy_storage_key(legacy_job_id: str, filename: str) -> str:
    return f"legacy-upload:{legacy_job_id}:{filename}"


def _content_kind_for(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return UploadedFileContentKind.CSV.value
    return UploadedFileContentKind.XLSX.value


def _content_type_for(path: Path) -> str:
    return _CONTENT_TYPE_BY_SUFFIX.get(path.suffix.lower(), "application/octet-stream")


def _artifact_content_kind_for(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return ArtifactContentKind.CSV.value
    if suffix == ".xlsx":
        return ArtifactContentKind.XLSX.value
    if suffix == ".json":
        return ArtifactContentKind.JSON.value
    if suffix == ".zip":
        return ArtifactContentKind.ZIP.value
    if suffix == ".log":
        return ArtifactContentKind.LOG.value
    if suffix == ".sqlite":
        return ArtifactContentKind.SQLITE.value
    return ArtifactContentKind.OTHER.value


def _get_job_by_legacy_id(session, legacy_job_id: str) -> Job | None:
    return session.scalar(select(Job).where(Job.source == _legacy_job_source(legacy_job_id)))


def _summary_value(summary: Any, name: str) -> int:
    if summary is None:
        return 0
    value = getattr(summary, name, 0)
    if value is None:
        return 0
    return int(value)


def _ensure_temporary_user(session) -> User:
    user = session.scalar(select(User).where(User.email == TEMP_DEFAULT_USER_EMAIL))
    if user is not None:
        return user

    user = User(
        email=TEMP_DEFAULT_USER_EMAIL,
        display_name=TEMP_DEFAULT_USER_NAME,
        status=UserStatus.ACTIVE.value,
    )
    session.add(user)
    session.flush()
    return user


def _ensure_temporary_organization(session, user: User) -> Organization:
    organization = session.scalar(
        select(Organization).where(Organization.slug == TEMP_DEFAULT_ORG_SLUG)
    )
    if organization is not None:
        return organization

    organization = Organization(
        name=TEMP_DEFAULT_ORG_NAME,
        slug=TEMP_DEFAULT_ORG_SLUG,
        status=OrganizationStatus.ACTIVE.value,
        created_by_user_id=user.id,
    )
    session.add(organization)
    session.flush()
    return organization


def _ensure_temporary_membership(session, user: User, organization: Organization) -> None:
    membership = session.scalar(
        select(Membership).where(
            Membership.user_id == user.id,
            Membership.organization_id == organization.id,
        )
    )
    if membership is not None:
        return

    session.add(
        Membership(
            user_id=user.id,
            organization_id=organization.id,
            role=MembershipRole.OWNER.value,
            status=MembershipStatus.ACTIVE.value,
            accepted_at=_utc_now(),
        )
    )


def _ensure_temporary_identity(session) -> tuple[Organization, User]:
    user = _ensure_temporary_user(session)
    organization = _ensure_temporary_organization(session, user)
    _ensure_temporary_membership(session, user, organization)
    session.flush()
    return organization, user


def persist_queued_job_and_upload(
    legacy_job_id: str,
    input_path: Path,
    queued_at: datetime | None = None,
) -> uuid.UUID | None:
    """Persist a queued job and its uploaded file without changing legacy flow."""

    if not is_db_available():
        return None
    queued_time = queued_at or _utc_now()
    try:
        with session_scope() as session:
            organization, user = _ensure_temporary_identity(session)
            source = _legacy_job_source(legacy_job_id)

            job = _get_job_by_legacy_id(session, legacy_job_id)
            if job is None:
                job = Job(
                    organization_id=organization.id,
                    created_by_user_id=user.id,
                    status=JobStatus.QUEUED.value,
                    input_filename=input_path.name,
                    queued_at=queued_time,
                    source=source,
                )
                session.add(job)
                session.flush()
                session.add(
                    JobEvent(
                        organization_id=organization.id,
                        job_id=job.id,
                        event_type=JobEventType.JOB_CREATED.value,
                        message="Job created and queued from a legacy upload request.",
                        severity=EventSeverity.INFO.value,
                        actor_kind=ActorKind.USER.value,
                        actor_user_id=user.id,
                        occurred_at=queued_time,
                        event_metadata={
                            "legacy_job_id": legacy_job_id,
                            "input_filename": input_path.name,
                        },
                    )
                )

            storage_key = _legacy_storage_key(legacy_job_id, input_path.name)
            uploaded_file = session.scalar(
                select(UploadedFile).where(UploadedFile.storage_key == storage_key)
            )
            if uploaded_file is None:
                session.add(
                    UploadedFile(
                        organization_id=organization.id,
                        job_id=job.id,
                        uploaded_by_user_id=user.id,
                        original_filename=input_path.name,
                        stored_filename=input_path.name,
                        storage_key=storage_key,
                        storage_location=str(input_path.resolve()),
                        content_kind=_content_kind_for(input_path),
                        content_type=_content_type_for(input_path),
                        size_bytes=input_path.stat().st_size,
                        status=UploadedFileStatus.UPLOADED.value,
                        uploaded_at=queued_time,
                    )
                )

            return job.id
    except SQLAlchemyError as exc:
        log_db_failure(
            "Skipping DB persistence for queued legacy job %s: %s",
            legacy_job_id,
            exc,
        )
        return None
    except Exception as exc:  # pragma: no cover - defence in depth
        LOGGER.warning(
            "Skipping DB persistence for queued legacy job %s: %s",
            legacy_job_id,
            exc,
            exc_info=True,
        )
        return None


def mark_job_running(legacy_job_id: str, started_at: datetime | None = None) -> None:
    """Best-effort bridge that marks the DB job as running."""

    if not is_db_available():
        return
    started_time = started_at or _utc_now()
    try:
        with session_scope() as session:
            job = _get_job_by_legacy_id(session, legacy_job_id)
            if job is None:
                return
            if job.status == JobStatus.RUNNING.value:
                return

            job.status = JobStatus.RUNNING.value
            job.started_at = started_time
            if job.queued_at is None:
                job.queued_at = started_time

            session.add(
                JobEvent(
                    organization_id=job.organization_id,
                    job_id=job.id,
                    event_type=JobEventType.JOB_STARTED.value,
                    message="Legacy background processing started.",
                    severity=EventSeverity.INFO.value,
                    actor_kind=ActorKind.WORKER.value,
                    actor_user_id=None,
                    occurred_at=started_time,
                    event_metadata={"legacy_job_id": legacy_job_id},
                )
            )
    except SQLAlchemyError as exc:
        log_db_failure(
            "Skipping DB running-state update for legacy job %s: %s",
            legacy_job_id,
            exc,
        )
    except Exception as exc:  # pragma: no cover - defence in depth
        LOGGER.warning(
            "Skipping DB running-state update for legacy job %s: %s",
            legacy_job_id,
            exc,
            exc_info=True,
        )


def mark_job_completed(
    legacy_job_id: str,
    *,
    started_at: datetime | None = None,
    completed_at: datetime | None = None,
    summary: Any = None,
) -> None:
    """Best-effort bridge that marks the DB job as completed."""

    if not is_db_available():
        return
    completed_time = completed_at or _utc_now()
    try:
        with session_scope() as session:
            job = _get_job_by_legacy_id(session, legacy_job_id)
            if job is None:
                return
            if job.status == JobStatus.COMPLETED.value and job.completed_at is not None:
                return

            job.status = JobStatus.COMPLETED.value
            job.started_at = job.started_at or started_at or job.queued_at or completed_time
            job.completed_at = completed_time
            job.failed_at = None
            job.cancelled_at = None
            job.cancelled_by_user_id = None
            job.error_type = None
            job.error_message = None
            job.error_details = None
            job.summary_total_input_rows = _summary_value(summary, "total_input_rows")
            job.summary_total_valid = _summary_value(summary, "total_valid")
            job.summary_total_review = _summary_value(summary, "total_review")
            job.summary_total_invalid_or_bounce_risk = _summary_value(
                summary,
                "total_invalid_or_bounce_risk",
            )
            job.summary_duplicates_removed = _summary_value(summary, "duplicates_removed")
            job.summary_typo_corrections = _summary_value(summary, "typo_corrections")
            job.summary_disposable_emails = _summary_value(summary, "disposable_emails")
            job.summary_placeholder_or_fake_emails = _summary_value(
                summary,
                "placeholder_or_fake_emails",
            )
            job.summary_role_based_emails = _summary_value(summary, "role_based_emails")

            session.add(
                JobEvent(
                    organization_id=job.organization_id,
                    job_id=job.id,
                    event_type=JobEventType.JOB_COMPLETED.value,
                    message="Legacy background processing completed successfully.",
                    severity=EventSeverity.INFO.value,
                    actor_kind=ActorKind.WORKER.value,
                    actor_user_id=None,
                    occurred_at=completed_time,
                    event_metadata={
                        "legacy_job_id": legacy_job_id,
                        "input_filename": job.input_filename,
                    },
                )
            )
    except SQLAlchemyError as exc:
        log_db_failure(
            "Skipping DB completion update for legacy job %s: %s",
            legacy_job_id,
            exc,
        )
    except Exception as exc:  # pragma: no cover - defence in depth
        LOGGER.warning(
            "Skipping DB completion update for legacy job %s: %s",
            legacy_job_id,
            exc,
            exc_info=True,
        )


def mark_job_failed(
    legacy_job_id: str,
    *,
    started_at: datetime | None = None,
    failed_at: datetime | None = None,
    error_type: str | None = None,
    error_message: str | None = None,
    error_details: dict[str, Any] | None = None,
) -> None:
    """Best-effort bridge that marks the DB job as failed."""

    if not is_db_available():
        return
    failed_time = failed_at or _utc_now()
    try:
        with session_scope() as session:
            job = _get_job_by_legacy_id(session, legacy_job_id)
            if job is None:
                return
            if job.status == JobStatus.COMPLETED.value:
                return
            if job.status == JobStatus.FAILED.value and job.failed_at is not None:
                return

            job.status = JobStatus.FAILED.value
            job.started_at = job.started_at or started_at or job.queued_at or failed_time
            job.failed_at = failed_time
            job.completed_at = None
            job.cancelled_at = None
            job.cancelled_by_user_id = None
            job.error_type = error_type or "pipeline_execution_error"
            job.error_message = error_message or "Legacy background processing failed."
            job.error_details = error_details

            session.add(
                JobEvent(
                    organization_id=job.organization_id,
                    job_id=job.id,
                    event_type=JobEventType.JOB_FAILED.value,
                    message=job.error_message,
                    severity=EventSeverity.ERROR.value,
                    actor_kind=ActorKind.WORKER.value,
                    actor_user_id=None,
                    occurred_at=failed_time,
                    event_metadata={
                        "legacy_job_id": legacy_job_id,
                        "error_type": job.error_type,
                        "error_details": error_details or {},
                    },
                )
            )
    except SQLAlchemyError as exc:
        log_db_failure(
            "Skipping DB failure update for legacy job %s: %s",
            legacy_job_id,
            exc,
        )
    except Exception as exc:  # pragma: no cover - defence in depth
        LOGGER.warning(
            "Skipping DB failure update for legacy job %s: %s",
            legacy_job_id,
            exc,
            exc_info=True,
        )


def register_job_artifacts(
    legacy_job_id: str,
    artifact_records: list[dict[str, Any]],
    *,
    registered_at: datetime | None = None,
) -> None:
    """Best-effort bridge that registers generated artifacts in PostgreSQL."""

    if not is_db_available():
        return
    registered_time = registered_at or _utc_now()
    try:
        with session_scope() as session:
            job = _get_job_by_legacy_id(session, legacy_job_id)
            if job is None:
                return

            for record in artifact_records:
                artifact_key = str(record["artifact_key"])
                existing = session.scalar(
                    select(Artifact).where(
                        Artifact.job_id == job.id,
                        Artifact.artifact_key == artifact_key,
                    )
                )
                if existing is not None:
                    continue

                visibility = str(record["visibility"])
                artifact_status = (
                    ArtifactStatus.AVAILABLE.value
                    if visibility == ArtifactVisibility.CUSTOMER.value
                    else ArtifactStatus.REGISTERED.value
                )

                artifact = Artifact(
                    organization_id=job.organization_id,
                    job_id=job.id,
                    artifact_key=artifact_key,
                    artifact_group=str(record["artifact_group"]),
                    visibility=visibility,
                    display_filename=str(record["display_filename"]),
                    storage_key=str(record["storage_key"]),
                    storage_location=str(record["storage_location"]),
                    content_kind=_artifact_content_kind_for(Path(str(record["storage_location"]))),
                    content_type=str(record["content_type"]),
                    size_bytes=int(record["size_bytes"]),
                    status=artifact_status,
                    generated_at=registered_time,
                    registered_at=registered_time,
                )
                session.add(artifact)
                session.flush()

                session.add(
                    JobEvent(
                        organization_id=job.organization_id,
                        job_id=job.id,
                        event_type=JobEventType.ARTIFACT_REGISTERED.value,
                        message=f"Registered artifact: {artifact.display_filename}",
                        severity=EventSeverity.INFO.value,
                        actor_kind=ActorKind.WORKER.value,
                        actor_user_id=None,
                        occurred_at=registered_time,
                        event_metadata={
                            "legacy_job_id": legacy_job_id,
                            "artifact_id": str(artifact.id),
                            "artifact_key": artifact.artifact_key,
                            "visibility": artifact.visibility,
                            "status": artifact.status,
                        },
                    )
                )
    except SQLAlchemyError as exc:
        log_db_failure(
            "Skipping artifact registration for legacy job %s: %s",
            legacy_job_id,
            exc,
        )
    except Exception as exc:  # pragma: no cover - defence in depth
        LOGGER.warning(
            "Skipping artifact registration for legacy job %s: %s",
            legacy_job_id,
            exc,
            exc_info=True,
        )


def load_review_decisions(legacy_job_id: str) -> dict[str, str] | None:
    """Return DB-backed review decisions for a legacy job, if present."""

    if not is_db_available():
        return None
    try:
        with session_scope() as session:
            job = _get_job_by_legacy_id(session, legacy_job_id)
            if job is None:
                return None

            rows = session.scalars(
                select(ReviewDecision).where(ReviewDecision.job_id == job.id)
            ).all()
            if not rows:
                return None

            return {row.review_item_id: row.decision for row in rows}
    except SQLAlchemyError as exc:
        log_db_failure(
            "Skipping DB review-decision load for legacy job %s: %s",
            legacy_job_id,
            exc,
        )
        return None
    except Exception as exc:  # pragma: no cover - defence in depth
        LOGGER.warning(
            "Skipping DB review-decision load for legacy job %s: %s",
            legacy_job_id,
            exc,
            exc_info=True,
        )
        return None


def save_review_decisions(
    legacy_job_id: str,
    decisions: dict[str, str],
    *,
    decided_at: datetime | None = None,
) -> None:
    """Best-effort bridge that mirrors review decisions into PostgreSQL."""

    if not is_db_available():
        return
    decided_time = decided_at or _utc_now()
    try:
        with session_scope() as session:
            job = _get_job_by_legacy_id(session, legacy_job_id)
            if job is None:
                return

            user = _ensure_temporary_user(session)
            existing_rows = session.scalars(
                select(ReviewDecision).where(ReviewDecision.job_id == job.id)
            ).all()
            existing_by_item = {row.review_item_id: row for row in existing_rows}

            for review_item_id, decision in decisions.items():
                existing = existing_by_item.get(review_item_id)
                action = "review_decision_created"
                previous_decision = None

                if existing is None:
                    existing = ReviewDecision(
                        organization_id=job.organization_id,
                        job_id=job.id,
                        review_item_id=review_item_id,
                        decision=decision,
                        decided_by_user_id=user.id,
                        decided_at=decided_time,
                        source="legacy_json_bridge",
                    )
                    session.add(existing)
                    session.flush()
                else:
                    action = "review_decision_updated"
                    previous_decision = existing.decision
                    existing.previous_decision = previous_decision
                    existing.decision = decision
                    existing.decided_by_user_id = user.id
                    existing.decided_at = decided_time

                session.add(
                    AuditEvent(
                        organization_id=job.organization_id,
                        actor_kind=ActorKind.USER.value,
                        actor_user_id=user.id,
                        action=action,
                        resource_kind="review_decision",
                        resource_id=existing.id,
                        outcome="success",
                        occurred_at=decided_time,
                        event_metadata={
                            "legacy_job_id": legacy_job_id,
                            "review_item_id": review_item_id,
                            "previous_decision": previous_decision,
                            "decision": decision,
                        },
                    )
                )

            current_review_ids = set(decisions)
            for stale in existing_rows:
                if stale.review_item_id not in current_review_ids:
                    session.delete(stale)
    except SQLAlchemyError as exc:
        log_db_failure(
            "Skipping DB review-decision save for legacy job %s: %s",
            legacy_job_id,
            exc,
        )
    except Exception as exc:  # pragma: no cover - defence in depth
        LOGGER.warning(
            "Skipping DB review-decision save for legacy job %s: %s",
            legacy_job_id,
            exc,
            exc_info=True,
        )
