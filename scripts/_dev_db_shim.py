"""Dev-only SQLite shim for end-to-end DB validation scripts.

Production runs against PostgreSQL; the schema in :mod:`app.db.models` uses
``JSONB`` and ``INET`` (Postgres-specific types) on purpose. We do **not**
weaken those types — production stays Postgres-first.

But for fast local validation (``scripts/e2e_with_db.py`` and similar) it
is convenient to back the schema with an in-process SQLite file so a dev
or CI machine can exercise the full DB write/read path without provisioning
PostgreSQL. SQLite has no ``JSONB`` or ``INET`` type, so we install a
narrow, well-documented compile-time alias that maps:

    sqlalchemy.dialects.postgresql.JSONB  -> sqlalchemy.JSON
    sqlalchemy.dialects.postgresql.INET   -> sqlalchemy.String

These aliases are applied **only** when this module is imported, and only
before any model is imported. They never run inside the application
process — production code does not import :mod:`scripts`.

Usage
-----

    # Top of any dev validation script, BEFORE importing app.db.*:
    from scripts._dev_db_shim import enable_sqlite_compat
    enable_sqlite_compat()

    import os
    os.environ["TRASHPANDA_DATABASE_URL"] = "sqlite:///./dev.sqlite"
    from app.db.init_db import init_db
    init_db()

If you forget to call this before importing ``app.db.models`` against
SQLite, you will get ``CompileError: ... type 'JSONB' ...`` — that error
is intentional and tells you the shim was not installed in time.
"""

from __future__ import annotations

import os

_SHIM_INSTALLED = False


def enable_sqlite_compat() -> None:
    """Install the JSONB/INET → JSON/String aliases for SQLite.

    Idempotent. Safe to call multiple times. Has no effect on production
    code paths (those never import this module).
    """
    global _SHIM_INSTALLED
    if _SHIM_INSTALLED:
        return

    import sqlalchemy
    import sqlalchemy.dialects.postgresql as pg

    # Both aliases are storage-only: JSONB → JSON keeps round-trippable
    # JSON, INET → String keeps the IP literal as text. Neither is suitable
    # for production, which is why this lives under scripts/ and not app/.
    pg.JSONB = sqlalchemy.JSON  # type: ignore[assignment]
    pg.INET = sqlalchemy.String  # type: ignore[assignment]

    _SHIM_INSTALLED = True


def configure_sqlite_url(db_path: str) -> str:
    """Set ``TRASHPANDA_DATABASE_URL`` to a SQLite file and return the URL.

    Convenience wrapper used by validation scripts so the env var spelling
    stays in one place.
    """
    url = f"sqlite:///{db_path}"
    os.environ["TRASHPANDA_DATABASE_URL"] = url
    return url
