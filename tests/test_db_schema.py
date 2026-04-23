from __future__ import annotations

from sqlalchemy.dialects.postgresql import dialect as postgres_dialect
from sqlalchemy.schema import CreateTable

from app.db.base import Base
from app.db.config import get_database_settings
from app.db.models import Artifact, Job, Membership, ReviewDecision, UploadedFile


def test_db_metadata_contains_expected_tables() -> None:
    expected = {
        "users",
        "organizations",
        "memberships",
        "jobs",
        "uploaded_files",
        "artifacts",
        "review_decisions",
        "job_events",
        "audit_events",
        "usage_events",
    }
    assert expected.issubset(Base.metadata.tables)


def test_default_database_url_is_postgres_driver_url(monkeypatch) -> None:
    for key in (
        "TRASHPANDA_DATABASE_URL",
        "TRASHPANDA_DB_HOST",
        "TRASHPANDA_DB_PORT",
        "TRASHPANDA_DB_NAME",
        "TRASHPANDA_DB_USER",
        "TRASHPANDA_DB_PASSWORD",
    ):
        monkeypatch.delenv(key, raising=False)

    get_database_settings.cache_clear()
    settings = get_database_settings()
    assert settings.url == "postgresql+psycopg://postgres:postgres@localhost:5432/trashpanda"


def test_core_uniqueness_constraints_exist() -> None:
    membership_constraints = {constraint.name for constraint in Membership.__table__.constraints}
    artifact_constraints = {constraint.name for constraint in Artifact.__table__.constraints}
    review_constraints = {constraint.name for constraint in ReviewDecision.__table__.constraints}
    assert "uq_memberships_user_org" in membership_constraints
    assert "uq_artifacts_job_artifact_key" in artifact_constraints
    assert "uq_review_decisions_job_review_item" in review_constraints


def test_postgres_ddl_compiles_for_core_tables() -> None:
    ddl = str(CreateTable(Job.__table__).compile(dialect=postgres_dialect()))
    assert "CREATE TABLE jobs" in ddl
    assert "FOREIGN KEY" in ddl

    uploaded_ddl = str(CreateTable(UploadedFile.__table__).compile(dialect=postgres_dialect()))
    assert "CREATE TABLE uploaded_files" in uploaded_ddl

