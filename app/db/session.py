"""Engine and session helpers for the SaaS persistence layer."""

from __future__ import annotations

from collections.abc import Generator, Iterator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from .config import DatabaseSettings, get_database_settings


_ENGINE: Engine | None = None
_SESSION_FACTORY: sessionmaker[Session] | None = None


def _engine_kwargs(settings: DatabaseSettings) -> dict[str, object]:
    kwargs: dict[str, object] = {
        "echo": settings.echo,
        "pool_pre_ping": settings.pool_pre_ping,
    }
    if not settings.url.startswith("sqlite"):
        kwargs["pool_size"] = settings.pool_size
        kwargs["max_overflow"] = settings.max_overflow
    return kwargs


def create_db_engine(settings: DatabaseSettings | None = None) -> Engine:
    """Build a new SQLAlchemy engine from resolved settings."""

    resolved = settings or get_database_settings()
    return create_engine(resolved.url, **_engine_kwargs(resolved))


def get_engine() -> Engine:
    """Return the process-wide SQLAlchemy engine singleton."""

    global _ENGINE
    if _ENGINE is None:
        _ENGINE = create_db_engine()
    return _ENGINE


def get_session_factory() -> sessionmaker[Session]:
    """Return the process-wide configured session factory."""

    global _SESSION_FACTORY
    if _SESSION_FACTORY is None:
        _SESSION_FACTORY = sessionmaker(
            bind=get_engine(),
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
            class_=Session,
        )
    return _SESSION_FACTORY


def get_db_session() -> Generator[Session, None, None]:
    """Yield a database session suitable for future dependency injection."""

    session = get_session_factory()()
    try:
        yield session
    finally:
        session.close()


@contextmanager
def session_scope() -> Iterator[Session]:
    """Provide a transactional session scope."""

    session = get_session_factory()()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def dispose_engine() -> None:
    """Dispose the cached engine and clear the session factory."""

    global _ENGINE, _SESSION_FACTORY
    if _ENGINE is not None:
        _ENGINE.dispose()
    _ENGINE = None
    _SESSION_FACTORY = None

