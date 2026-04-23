"""Engine and session helpers for the SaaS persistence layer.

Resilience contract
-------------------

Request handlers MUST NOT block on a dead/slow database. This module enforces
that at two levels:

1. **Engine-level timeouts.** The engine is built with a short libpq
   ``connect_timeout`` and a short SQLAlchemy ``pool_timeout`` so any single
   DB call fails in a couple of seconds instead of the libpq default
   (~30 s on Windows for silent drops).

2. **Availability gate.** :func:`is_db_available` is a cheap, TTL-cached
   ``SELECT 1`` probe. DB-path helpers short-circuit on it so they don't even
   attempt to open a connection when the database is known-bad. The gate is
   safe to call on every request.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Generator, Iterator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from .config import DatabaseSettings, get_database_settings


LOGGER = logging.getLogger(__name__)

_ENGINE: Engine | None = None
_SESSION_FACTORY: sessionmaker[Session] | None = None


# ── Engine construction ─────────────────────────────────────────────────────


def _engine_kwargs(settings: DatabaseSettings) -> dict[str, object]:
    kwargs: dict[str, object] = {
        "echo": settings.echo,
        "pool_pre_ping": settings.pool_pre_ping,
    }
    if settings.url.startswith("sqlite"):
        # SQLite uses StaticPool / SingletonThreadPool — no pool timeouts or
        # libpq options apply. Leave it alone.
        return kwargs

    kwargs["pool_size"] = settings.pool_size
    kwargs["max_overflow"] = settings.max_overflow
    kwargs["pool_timeout"] = settings.pool_timeout_seconds
    kwargs["pool_recycle"] = settings.pool_recycle_seconds

    # psycopg (v3) and psycopg2 both accept ``connect_timeout`` as a libpq
    # connection option. Setting this is what actually stops requests from
    # hanging on a dead DB — ``pool_timeout`` alone doesn't help on the very
    # first connect because the pool starts empty.
    kwargs["connect_args"] = {"connect_timeout": settings.connect_timeout_seconds}
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
    reset_db_availability_cache()


# ── Availability probe ──────────────────────────────────────────────────────
#
# Cheap ``SELECT 1`` round-trip with a TTL-cached result. Callers use this to
# avoid blocking on a dead DB — if the probe says "unavailable", skip the DB
# call entirely and fall back to the legacy JSON path.

_AVAIL_LOCK = threading.Lock()
_AVAIL_STATE: dict[str, object] = {
    "result": None,        # bool | None — None = never checked
    "checked_at": 0.0,     # monotonic seconds
    "last_logged_at": 0.0,
    "last_logged_state": None,
}

# How long a probe result is trusted before we re-check. Short enough to
# recover quickly when the DB comes back, long enough that a tight request
# loop doesn't probe on every call.
_AVAIL_TTL_SECONDS = 5.0

# Minimum interval between identical log lines. Keeps the log readable when
# the DB is down for a long time.
_LOG_THROTTLE_SECONDS = 60.0


def _log_availability_change(is_up: bool, reason: str | None) -> None:
    """Log DB up/down transitions with built-in throttling.

    The first "down" transition is logged immediately; subsequent identical
    states are silent until ``_LOG_THROTTLE_SECONDS`` elapses. Recovery is
    always logged so operators see when the DB came back.
    """

    now = time.monotonic()
    last_state = _AVAIL_STATE.get("last_logged_state")
    last_at = float(_AVAIL_STATE.get("last_logged_at") or 0.0)

    if last_state == is_up and (now - last_at) < _LOG_THROTTLE_SECONDS:
        return

    if is_up:
        if last_state is False:
            LOGGER.info("Database is reachable again; re-enabling DB-backed paths.")
        # First-ever "up" is not worth logging — that's the normal state.
    else:
        LOGGER.warning(
            "Database is unreachable (%s); skipping DB-backed paths until it recovers.",
            reason or "unknown",
        )

    _AVAIL_STATE["last_logged_state"] = is_up
    _AVAIL_STATE["last_logged_at"] = now


def _probe_database() -> tuple[bool, str | None]:
    """Run a single ``SELECT 1`` and return ``(ok, reason_if_down)``."""

    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True, None
    except SQLAlchemyError as exc:
        cause = exc.__cause__ or exc
        return False, f"{type(cause).__name__}: {cause}".strip()
    except Exception as exc:  # pragma: no cover - defence in depth
        return False, f"{type(exc).__name__}: {exc}".strip()


def is_db_available() -> bool:
    """Return ``True`` if a cheap ``SELECT 1`` currently succeeds.

    Results are cached for ``_AVAIL_TTL_SECONDS`` so a tight request loop
    probes at most once per window. The probe itself is bounded by the
    engine's ``connect_timeout`` and ``pool_timeout`` so it can't hang.

    Never raises. Safe to call from any request path.
    """

    now = time.monotonic()
    with _AVAIL_LOCK:
        cached = _AVAIL_STATE.get("result")
        checked_at = float(_AVAIL_STATE.get("checked_at") or 0.0)
        if cached is not None and (now - checked_at) < _AVAIL_TTL_SECONDS:
            return bool(cached)

    ok, reason = _probe_database()

    with _AVAIL_LOCK:
        _AVAIL_STATE["result"] = ok
        _AVAIL_STATE["checked_at"] = time.monotonic()
        _log_availability_change(ok, reason)

    return ok


def reset_db_availability_cache() -> None:
    """Clear the availability cache. Useful after configuration changes."""

    with _AVAIL_LOCK:
        _AVAIL_STATE["result"] = None
        _AVAIL_STATE["checked_at"] = 0.0
        _AVAIL_STATE["last_logged_state"] = None
        _AVAIL_STATE["last_logged_at"] = 0.0


def log_db_failure(message: str, *args: object) -> None:
    """Rate-limited ``logger.warning`` for DB-layer failures.

    Shares throttling state with :func:`is_db_available` so repeated failures
    coming from different call sites don't flood the log.
    """

    now = time.monotonic()
    last_at = float(_AVAIL_STATE.get("last_logged_at") or 0.0)
    if (
        (now - last_at) < _LOG_THROTTLE_SECONDS
        and _AVAIL_STATE.get("last_logged_state") is False
    ):
        return
    LOGGER.warning(message, *args)
    _AVAIL_STATE["last_logged_state"] = False
    _AVAIL_STATE["last_logged_at"] = now

