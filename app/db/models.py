"""SQLAlchemy models for the SaaS v1 persistence layer."""

from __future__ import annotations

import enum
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import INET, JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class StringEnum(enum.Enum):
    """Enum whose value is the canonical persisted string."""

    def __str__(self) -> str:
        return str(self.value)


class UserStatus(StringEnum):
    ACTIVE = "active"
    DISABLED = "disabled"
    DELETED = "deleted"


class OrganizationStatus(StringEnum):
    ACTIVE = "active"
    SUSPENDED = "suspended"
    DELETED = "deleted"


class MembershipRole(StringEnum):
    OWNER = "owner"
    ADMIN = "admin"
    MEMBER = "member"


class MembershipStatus(StringEnum):
    INVITED = "invited"
    ACTIVE = "active"
    DISABLED = "disabled"
    REMOVED = "removed"


class JobStatus(StringEnum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"
    DELETED = "deleted"


class UploadedFileStatus(StringEnum):
    INITIATED = "initiated"
    UPLOADED = "uploaded"
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    PROCESSING_LOCKED = "processing_locked"
    PROCESSED = "processed"
    EXPIRED = "expired"
    DELETED = "deleted"


class UploadedFileContentKind(StringEnum):
    CSV = "csv"
    XLSX = "xlsx"


class ArtifactStatus(StringEnum):
    GENERATED = "generated"
    REGISTERED = "registered"
    AVAILABLE = "available"
    EXPIRED = "expired"
    DELETED = "deleted"


class ArtifactVisibility(StringEnum):
    CUSTOMER = "customer"
    INTERNAL = "internal"


class ArtifactGroup(StringEnum):
    CLIENT_OUTPUT = "client_output"
    TECHNICAL_CSV = "technical_csv"
    REPORT = "report"
    INTERNAL = "internal"


class ArtifactContentKind(StringEnum):
    CSV = "csv"
    XLSX = "xlsx"
    JSON = "json"
    ZIP = "zip"
    LOG = "log"
    SQLITE = "sqlite"
    OTHER = "other"


class ReviewDecisionValue(StringEnum):
    APPROVED = "approved"
    REMOVED = "removed"


class ActorKind(StringEnum):
    USER = "user"
    SYSTEM = "system"
    WORKER = "worker"
    SUPPORT = "support"


class JobEventType(StringEnum):
    JOB_CREATED = "job_created"
    JOB_QUEUED = "job_queued"
    JOB_STARTED = "job_started"
    JOB_COMPLETED = "job_completed"
    JOB_FAILED = "job_failed"
    JOB_CANCELLED = "job_cancelled"
    ARTIFACT_GENERATED = "artifact_generated"
    ARTIFACT_REGISTERED = "artifact_registered"
    REVIEW_READY = "review_ready"


class EventSeverity(StringEnum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class AuditOutcome(StringEnum):
    SUCCESS = "success"
    FAILURE = "failure"
    DENIED = "denied"


class AuditAction(StringEnum):
    FILE_UPLOADED = "file_uploaded"
    JOB_CREATED = "job_created"
    JOB_VIEWED = "job_viewed"
    REVIEW_DECISION_CREATED = "review_decision_created"
    REVIEW_DECISION_UPDATED = "review_decision_updated"
    ARTIFACT_DOWNLOADED = "artifact_downloaded"
    ACCESS_DENIED = "access_denied"
    MEMBERSHIP_CREATED = "membership_created"
    MEMBERSHIP_UPDATED = "membership_updated"


class AuditResourceKind(StringEnum):
    USER = "user"
    ORGANIZATION = "organization"
    MEMBERSHIP = "membership"
    JOB = "job"
    UPLOADED_FILE = "uploaded_file"
    ARTIFACT = "artifact"
    REVIEW_DECISION = "review_decision"


class UsageEventType(StringEnum):
    JOB_CREATED = "job_created"
    FILE_UPLOADED = "file_uploaded"
    ROWS_PROCESSED = "rows_processed"
    ARTIFACT_GENERATED = "artifact_generated"
    ARTIFACT_DOWNLOADED = "artifact_downloaded"


class UsageUnit(StringEnum):
    JOB = "job"
    FILE = "file"
    ROW = "row"
    ARTIFACT = "artifact"
    BYTE = "byte"
    DOWNLOAD = "download"


class UUIDPrimaryKeyMixin:
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class SoftDeleteMixin:
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class User(UUIDPrimaryKeyMixin, TimestampMixin, SoftDeleteMixin, Base):
    __tablename__ = "users"
    __table_args__ = (
        CheckConstraint(
            "status IN ('active', 'disabled', 'deleted')",
            name="status_valid",
        ),
        CheckConstraint(
            "((status = 'deleted' AND deleted_at IS NOT NULL) OR (status <> 'deleted'))",
            name="deleted_at_matches_status",
        ),
        Index("idx_users_status", "status"),
    )

    email: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    display_name: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)
    email_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_login_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    avatar_url: Mapped[str | None] = mapped_column(Text)
    timezone: Mapped[str | None] = mapped_column(String)
    locale: Mapped[str | None] = mapped_column(String)


class Organization(UUIDPrimaryKeyMixin, TimestampMixin, SoftDeleteMixin, Base):
    __tablename__ = "organizations"
    __table_args__ = (
        CheckConstraint(
            "status IN ('active', 'suspended', 'deleted')",
            name="status_valid",
        ),
        CheckConstraint(
            "((status = 'deleted' AND deleted_at IS NOT NULL) OR (status <> 'deleted'))",
            name="deleted_at_matches_status",
        ),
        Index("idx_organizations_status", "status"),
        Index("idx_organizations_created_at", "created_at"),
    )

    name: Mapped[str] = mapped_column(String, nullable=False)
    slug: Mapped[str | None] = mapped_column(String, unique=True)
    status: Mapped[str] = mapped_column(String, nullable=False)
    created_by_user_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
    )
    billing_email: Mapped[str | None] = mapped_column(String)
    logo_url: Mapped[str | None] = mapped_column(Text)
    settings: Mapped[dict[str, Any] | None] = mapped_column(JSONB)


class Membership(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "memberships"
    __table_args__ = (
        UniqueConstraint("user_id", "organization_id", name="uq_memberships_user_org"),
        UniqueConstraint("organization_id", "id", name="uq_memberships_org_id_id"),
        CheckConstraint(
            "role IN ('owner', 'admin', 'member')",
            name="role_valid",
        ),
        CheckConstraint(
            "status IN ('invited', 'active', 'disabled', 'removed')",
            name="status_valid",
        ),
        CheckConstraint(
            "((status = 'removed' AND removed_at IS NOT NULL) OR (status <> 'removed'))",
            name="removed_at_matches_status",
        ),
        Index("idx_memberships_organization_user", "organization_id", "user_id"),
        Index("idx_memberships_organization_status", "organization_id", "status"),
        Index("idx_memberships_user", "user_id"),
    )

    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="RESTRICT"),
        nullable=False,
    )
    organization_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="RESTRICT"),
        nullable=False,
    )
    role: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)
    invited_by_user_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
    )
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    removed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_active_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Job(UUIDPrimaryKeyMixin, TimestampMixin, SoftDeleteMixin, Base):
    __tablename__ = "jobs"
    __table_args__ = (
        UniqueConstraint("organization_id", "id", name="uq_jobs_org_id_id"),
        CheckConstraint(
            "status IN ('queued', 'running', 'completed', 'failed', 'cancelled', 'expired', 'deleted')",
            name="status_valid",
        ),
        CheckConstraint(
            "("
            "(summary_total_input_rows IS NULL OR summary_total_input_rows >= 0) AND "
            "(summary_total_valid IS NULL OR summary_total_valid >= 0) AND "
            "(summary_total_review IS NULL OR summary_total_review >= 0) AND "
            "(summary_total_invalid_or_bounce_risk IS NULL OR summary_total_invalid_or_bounce_risk >= 0) AND "
            "(summary_duplicates_removed IS NULL OR summary_duplicates_removed >= 0) AND "
            "(summary_typo_corrections IS NULL OR summary_typo_corrections >= 0) AND "
            "(summary_disposable_emails IS NULL OR summary_disposable_emails >= 0) AND "
            "(summary_placeholder_or_fake_emails IS NULL OR summary_placeholder_or_fake_emails >= 0) AND "
            "(summary_role_based_emails IS NULL OR summary_role_based_emails >= 0)"
            ")",
            name="summary_nonnegative",
        ),
        CheckConstraint(
            "("
            "(status = 'queued' AND queued_at IS NOT NULL AND completed_at IS NULL AND failed_at IS NULL AND cancelled_at IS NULL) "
            "OR "
            "(status = 'running' AND queued_at IS NOT NULL AND started_at IS NOT NULL AND completed_at IS NULL AND failed_at IS NULL AND cancelled_at IS NULL) "
            "OR "
            "(status = 'completed' AND queued_at IS NOT NULL AND started_at IS NOT NULL AND completed_at IS NOT NULL AND failed_at IS NULL AND cancelled_at IS NULL AND error_type IS NULL AND error_message IS NULL AND summary_total_input_rows IS NOT NULL AND summary_total_valid IS NOT NULL AND summary_total_review IS NOT NULL AND summary_total_invalid_or_bounce_risk IS NOT NULL) "
            "OR "
            "(status = 'failed' AND failed_at IS NOT NULL AND completed_at IS NULL AND cancelled_at IS NULL AND error_type IS NOT NULL AND error_message IS NOT NULL) "
            "OR "
            "(status = 'cancelled' AND cancelled_at IS NOT NULL AND completed_at IS NULL AND failed_at IS NULL) "
            "OR "
            "(status = 'expired' AND completed_at IS NOT NULL) "
            "OR "
            "(status = 'deleted' AND deleted_at IS NOT NULL)"
            ")",
            name="lifecycle_valid",
        ),
        Index("idx_jobs_organization_created_at", "organization_id", "created_at"),
        Index("idx_jobs_organization_status_created_at", "organization_id", "status", "created_at"),
        Index("idx_jobs_created_by_user_created_at", "created_by_user_id", "created_at"),
        Index("idx_jobs_organization_completed_at", "organization_id", "completed_at"),
        Index("idx_jobs_organization_failed_at", "organization_id", "failed_at"),
    )

    organization_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="RESTRICT"),
        nullable=False,
    )
    created_by_user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="RESTRICT"),
        nullable=False,
    )
    status: Mapped[str] = mapped_column(String, nullable=False)
    input_filename: Mapped[str] = mapped_column(String, nullable=False)
    queued_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    failed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    cancelled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    cancelled_by_user_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
    )
    error_type: Mapped[str | None] = mapped_column(String)
    error_message: Mapped[str | None] = mapped_column(Text)
    error_details: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    summary_total_input_rows: Mapped[int | None] = mapped_column(BigInteger)
    summary_total_valid: Mapped[int | None] = mapped_column(BigInteger)
    summary_total_review: Mapped[int | None] = mapped_column(BigInteger)
    summary_total_invalid_or_bounce_risk: Mapped[int | None] = mapped_column(BigInteger)
    summary_duplicates_removed: Mapped[int | None] = mapped_column(BigInteger)
    summary_typo_corrections: Mapped[int | None] = mapped_column(BigInteger)
    summary_disposable_emails: Mapped[int | None] = mapped_column(BigInteger)
    summary_placeholder_or_fake_emails: Mapped[int | None] = mapped_column(BigInteger)
    summary_role_based_emails: Mapped[int | None] = mapped_column(BigInteger)
    name: Mapped[str | None] = mapped_column(String)
    description: Mapped[str | None] = mapped_column(Text)
    engine_version: Mapped[str | None] = mapped_column(String)
    config_version: Mapped[str | None] = mapped_column(String)
    source: Mapped[str | None] = mapped_column(String)
    priority: Mapped[int | None] = mapped_column(Integer)


class UploadedFile(UUIDPrimaryKeyMixin, TimestampMixin, SoftDeleteMixin, Base):
    __tablename__ = "uploaded_files"
    __table_args__ = (
        UniqueConstraint("storage_key", name="uq_uploaded_files_storage_key"),
        UniqueConstraint("organization_id", "id", name="uq_uploaded_files_org_id_id"),
        ForeignKeyConstraint(
            ["organization_id", "job_id"],
            ["jobs.organization_id", "jobs.id"],
            ondelete="RESTRICT",
            name="fk_uploaded_files_org_job_jobs",
        ),
        CheckConstraint(
            "status IN ('initiated', 'uploaded', 'accepted', 'rejected', 'processing_locked', 'processed', 'expired', 'deleted')",
            name="status_valid",
        ),
        CheckConstraint(
            "content_kind IN ('csv', 'xlsx')",
            name="content_kind_valid",
        ),
        CheckConstraint("size_bytes >= 0", name="size_nonnegative"),
        CheckConstraint(
            "(row_count_estimate IS NULL OR row_count_estimate >= 0)",
            name="row_count_nonnegative",
        ),
        CheckConstraint(
            "("
            "(status = 'initiated') "
            "OR "
            "(status = 'uploaded' AND uploaded_at IS NOT NULL) "
            "OR "
            "(status = 'accepted' AND uploaded_at IS NOT NULL AND accepted_at IS NOT NULL AND rejected_at IS NULL AND rejection_reason IS NULL) "
            "OR "
            "(status = 'rejected' AND rejected_at IS NOT NULL AND rejection_reason IS NOT NULL) "
            "OR "
            "(status = 'processing_locked' AND uploaded_at IS NOT NULL AND accepted_at IS NOT NULL) "
            "OR "
            "(status = 'processed' AND uploaded_at IS NOT NULL AND accepted_at IS NOT NULL) "
            "OR "
            "(status = 'expired' AND uploaded_at IS NOT NULL) "
            "OR "
            "(status = 'deleted' AND deleted_at IS NOT NULL)"
            ")",
            name="lifecycle_valid",
        ),
        Index("idx_uploaded_files_organization_job", "organization_id", "job_id"),
        Index("idx_uploaded_files_job", "job_id"),
        Index("idx_uploaded_files_organization_created_at", "organization_id", "created_at"),
        Index("idx_uploaded_files_organization_status_created_at", "organization_id", "status", "created_at"),
    )

    organization_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="RESTRICT"),
        nullable=False,
    )
    job_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    uploaded_by_user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="RESTRICT"),
        nullable=False,
    )
    original_filename: Mapped[str] = mapped_column(String, nullable=False)
    stored_filename: Mapped[str] = mapped_column(String, nullable=False)
    storage_key: Mapped[str] = mapped_column(String, nullable=False)
    storage_location: Mapped[str] = mapped_column(String, nullable=False)
    content_kind: Mapped[str] = mapped_column(String, nullable=False)
    content_type: Mapped[str] = mapped_column(String, nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)
    uploaded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    rejected_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    rejection_reason: Mapped[str | None] = mapped_column(Text)
    checksum: Mapped[str | None] = mapped_column(String)
    row_count_estimate: Mapped[int | None] = mapped_column(BigInteger)
    scan_status: Mapped[str | None] = mapped_column(String)


class Artifact(UUIDPrimaryKeyMixin, TimestampMixin, SoftDeleteMixin, Base):
    __tablename__ = "artifacts"
    __table_args__ = (
        UniqueConstraint("job_id", "artifact_key", name="uq_artifacts_job_artifact_key"),
        UniqueConstraint("storage_key", name="uq_artifacts_storage_key"),
        UniqueConstraint("organization_id", "id", name="uq_artifacts_org_id_id"),
        ForeignKeyConstraint(
            ["organization_id", "job_id"],
            ["jobs.organization_id", "jobs.id"],
            ondelete="RESTRICT",
            name="fk_artifacts_org_job_jobs",
        ),
        CheckConstraint(
            "status IN ('generated', 'registered', 'available', 'expired', 'deleted')",
            name="status_valid",
        ),
        CheckConstraint(
            "visibility IN ('customer', 'internal')",
            name="visibility_valid",
        ),
        CheckConstraint(
            "artifact_group IN ('client_output', 'technical_csv', 'report', 'internal')",
            name="artifact_group_valid",
        ),
        CheckConstraint(
            "content_kind IN ('csv', 'xlsx', 'json', 'zip', 'log', 'sqlite', 'other')",
            name="content_kind_valid",
        ),
        CheckConstraint(
            "(size_bytes IS NULL OR size_bytes >= 0)",
            name="size_nonnegative",
        ),
        CheckConstraint(
            "(download_count IS NULL OR download_count >= 0)",
            name="download_count_nonnegative",
        ),
        CheckConstraint(
            "("
            "(status = 'generated' AND generated_at IS NOT NULL) "
            "OR "
            "(status = 'registered' AND generated_at IS NOT NULL AND registered_at IS NOT NULL) "
            "OR "
            "(status = 'available' AND generated_at IS NOT NULL AND registered_at IS NOT NULL AND storage_key IS NOT NULL AND storage_location IS NOT NULL AND content_type IS NOT NULL AND size_bytes IS NOT NULL AND visibility = 'customer') "
            "OR "
            "(status = 'expired' AND generated_at IS NOT NULL) "
            "OR "
            "(status = 'deleted' AND deleted_at IS NOT NULL)"
            ")",
            name="lifecycle_valid",
        ),
        Index("idx_artifacts_organization_job", "organization_id", "job_id"),
        Index("idx_artifacts_organization_job_visibility", "organization_id", "job_id", "visibility"),
        Index("idx_artifacts_organization_status_created_at", "organization_id", "status", "created_at"),
        Index("idx_artifacts_organization_created_at", "organization_id", "created_at"),
    )

    organization_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="RESTRICT"),
        nullable=False,
    )
    job_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    artifact_key: Mapped[str] = mapped_column(String, nullable=False)
    artifact_group: Mapped[str] = mapped_column(String, nullable=False)
    visibility: Mapped[str] = mapped_column(String, nullable=False)
    display_filename: Mapped[str] = mapped_column(String, nullable=False)
    storage_key: Mapped[str | None] = mapped_column(String)
    storage_location: Mapped[str | None] = mapped_column(String)
    content_kind: Mapped[str] = mapped_column(String, nullable=False)
    content_type: Mapped[str | None] = mapped_column(String)
    size_bytes: Mapped[int | None] = mapped_column(BigInteger)
    status: Mapped[str] = mapped_column(String, nullable=False)
    generated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    registered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    checksum: Mapped[str | None] = mapped_column(String)
    download_count: Mapped[int | None] = mapped_column(BigInteger)
    last_downloaded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    generated_by_stage: Mapped[str | None] = mapped_column(String)


class ReviewDecision(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "review_decisions"
    __table_args__ = (
        UniqueConstraint("job_id", "review_item_id", name="uq_review_decisions_job_review_item"),
        ForeignKeyConstraint(
            ["organization_id", "job_id"],
            ["jobs.organization_id", "jobs.id"],
            ondelete="RESTRICT",
            name="fk_review_decisions_org_job_jobs",
        ),
        CheckConstraint(
            "decision IN ('approved', 'removed')",
            name="decision_valid",
        ),
        CheckConstraint(
            "(previous_decision IS NULL OR previous_decision IN ('approved', 'removed'))",
            name="previous_decision_valid",
        ),
        Index("idx_review_decisions_organization_job", "organization_id", "job_id"),
        Index("idx_review_decisions_organization_job_updated_at", "organization_id", "job_id", "updated_at"),
        Index("idx_review_decisions_decided_by_user_decided_at", "decided_by_user_id", "decided_at"),
        Index("idx_review_decisions_organization_decided_at", "organization_id", "decided_at"),
    )

    organization_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="RESTRICT"),
        nullable=False,
    )
    job_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    review_item_id: Mapped[str] = mapped_column(String, nullable=False)
    decision: Mapped[str] = mapped_column(String, nullable=False)
    decided_by_user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="RESTRICT"),
        nullable=False,
    )
    decided_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    comment: Mapped[str | None] = mapped_column(Text)
    previous_decision: Mapped[str | None] = mapped_column(String)
    source: Mapped[str | None] = mapped_column(String)
    exported_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class JobEvent(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "job_events"
    __table_args__ = (
        ForeignKeyConstraint(
            ["organization_id", "job_id"],
            ["jobs.organization_id", "jobs.id"],
            ondelete="RESTRICT",
            name="fk_job_events_org_job_jobs",
        ),
        CheckConstraint(
            "event_type IN ('job_created', 'job_queued', 'job_started', 'job_completed', 'job_failed', 'job_cancelled', 'artifact_generated', 'artifact_registered', 'review_ready')",
            name="event_type_valid",
        ),
        CheckConstraint(
            "severity IN ('info', 'warning', 'error')",
            name="severity_valid",
        ),
        CheckConstraint(
            "actor_kind IN ('user', 'system', 'worker', 'support')",
            name="actor_kind_valid",
        ),
        CheckConstraint(
            "(((actor_kind IN ('user', 'support')) AND actor_user_id IS NOT NULL) OR (actor_kind IN ('system', 'worker')))",
            name="actor_user_matches_kind",
        ),
        CheckConstraint(
            "(duration_ms IS NULL OR duration_ms >= 0)",
            name="duration_nonnegative",
        ),
        Index("idx_job_events_organization_job_occurred_at", "organization_id", "job_id", "occurred_at"),
        Index("idx_job_events_job_occurred_at", "job_id", "occurred_at"),
        Index("idx_job_events_organization_occurred_at", "organization_id", "occurred_at"),
        Index("idx_job_events_organization_type_occurred_at", "organization_id", "event_type", "occurred_at"),
        Index("idx_job_events_organization_severity_occurred_at", "organization_id", "severity", "occurred_at"),
    )

    organization_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="RESTRICT"),
        nullable=False,
    )
    job_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    event_type: Mapped[str] = mapped_column(String, nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    severity: Mapped[str] = mapped_column(String, nullable=False)
    actor_kind: Mapped[str] = mapped_column(String, nullable=False)
    actor_user_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
    )
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    event_metadata: Mapped[dict[str, Any] | None] = mapped_column("metadata", JSONB)
    worker_id: Mapped[str | None] = mapped_column(String)
    duration_ms: Mapped[int | None] = mapped_column(BigInteger)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class AuditEvent(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "audit_events"
    __table_args__ = (
        CheckConstraint(
            "actor_kind IN ('user', 'system', 'worker', 'support')",
            name="actor_kind_valid",
        ),
        CheckConstraint(
            "(((actor_kind IN ('user', 'support')) AND actor_user_id IS NOT NULL) OR (actor_kind IN ('system', 'worker')))",
            name="actor_user_matches_kind",
        ),
        CheckConstraint(
            "action IN ('file_uploaded', 'job_created', 'job_viewed', 'review_decision_created', 'review_decision_updated', 'artifact_downloaded', 'access_denied', 'membership_created', 'membership_updated')",
            name="action_valid",
        ),
        CheckConstraint(
            "outcome IN ('success', 'failure', 'denied')",
            name="outcome_valid",
        ),
        CheckConstraint(
            "resource_kind IN ('user', 'organization', 'membership', 'job', 'uploaded_file', 'artifact', 'review_decision')",
            name="resource_kind_valid",
        ),
        Index("idx_audit_events_organization_occurred_at", "organization_id", "occurred_at"),
        Index("idx_audit_events_organization_actor_occurred_at", "organization_id", "actor_user_id", "occurred_at"),
        Index("idx_audit_events_organization_resource_occurred_at", "organization_id", "resource_kind", "resource_id", "occurred_at"),
        Index("idx_audit_events_organization_action_occurred_at", "organization_id", "action", "occurred_at"),
        Index("idx_audit_events_organization_outcome_occurred_at", "organization_id", "outcome", "occurred_at"),
    )

    organization_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="RESTRICT"),
        nullable=False,
    )
    actor_kind: Mapped[str] = mapped_column(String, nullable=False)
    actor_user_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
    )
    action: Mapped[str] = mapped_column(String, nullable=False)
    resource_kind: Mapped[str] = mapped_column(String, nullable=False)
    resource_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    outcome: Mapped[str] = mapped_column(String, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ip_address: Mapped[str | None] = mapped_column(INET)
    user_agent: Mapped[str | None] = mapped_column(Text)
    request_id: Mapped[str | None] = mapped_column(String)
    reason: Mapped[str | None] = mapped_column(Text)
    event_metadata: Mapped[dict[str, Any] | None] = mapped_column("metadata", JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class UsageEvent(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "usage_events"
    __table_args__ = (
        ForeignKeyConstraint(
            ["organization_id", "job_id"],
            ["jobs.organization_id", "jobs.id"],
            ondelete="RESTRICT",
            name="fk_usage_events_org_job_jobs",
        ),
        ForeignKeyConstraint(
            ["organization_id", "uploaded_file_id"],
            ["uploaded_files.organization_id", "uploaded_files.id"],
            ondelete="RESTRICT",
            name="fk_usage_events_org_uploaded_file_uploaded_files",
        ),
        ForeignKeyConstraint(
            ["organization_id", "artifact_id"],
            ["artifacts.organization_id", "artifacts.id"],
            ondelete="RESTRICT",
            name="fk_usage_events_org_artifact_artifacts",
        ),
        CheckConstraint(
            "event_type IN ('job_created', 'file_uploaded', 'rows_processed', 'artifact_generated', 'artifact_downloaded')",
            name="event_type_valid",
        ),
        CheckConstraint(
            "unit IN ('job', 'file', 'row', 'artifact', 'byte', 'download')",
            name="unit_valid",
        ),
        CheckConstraint(
            "quantity >= 0",
            name="quantity_nonnegative",
        ),
        Index("idx_usage_events_organization_occurred_at", "organization_id", "occurred_at"),
        Index("idx_usage_events_organization_type_occurred_at", "organization_id", "event_type", "occurred_at"),
        Index("idx_usage_events_organization_occurred_at_type", "organization_id", "occurred_at", "event_type"),
        Index("idx_usage_events_job", "job_id"),
        Index("idx_usage_events_artifact", "artifact_id"),
        Index("idx_usage_events_uploaded_file", "uploaded_file_id"),
    )

    organization_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="RESTRICT"),
        nullable=False,
    )
    event_type: Mapped[str] = mapped_column(String, nullable=False)
    quantity: Mapped[int] = mapped_column(BigInteger, nullable=False)
    unit: Mapped[str] = mapped_column(String, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    actor_user_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
    )
    job_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    uploaded_file_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    artifact_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    event_metadata: Mapped[dict[str, Any] | None] = mapped_column("metadata", JSONB)
    source: Mapped[str | None] = mapped_column(String)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
