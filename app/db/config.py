"""Environment-driven database settings for the SaaS persistence layer."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


def _load_dotenv() -> None:
    """Load KEY=VALUE pairs from ``.env`` if present.

    Mirrors the project's existing no-dependency dotenv pattern so DB utilities
    can run standalone (for example, ``python -m app.db.init_db``) without
    relying on the HTTP server bootstrap.
    """

    project_root = Path(__file__).resolve().parents[2]
    for filename in (".env", ".env.local"):
        env_path = project_root / filename
        if not env_path.is_file():
            continue
        try:
            for raw in env_path.read_text(encoding="utf-8").splitlines():
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                if line.startswith("export "):
                    line = line[len("export "):].lstrip()
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip()
                if not key or key in os.environ:
                    continue
                if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
                    value = value[1:-1]
                os.environ[key] = value
        except OSError:
            continue


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True, slots=True)
class DatabaseSettings:
    """Resolved database settings for the SaaS persistence layer."""

    url: str
    echo: bool
    pool_size: int
    max_overflow: int
    pool_pre_ping: bool


def _build_default_database_url() -> str:
    host = os.environ.get("TRASHPANDA_DB_HOST", "localhost")
    port = os.environ.get("TRASHPANDA_DB_PORT", "5432")
    name = os.environ.get("TRASHPANDA_DB_NAME", "trashpanda")
    user = os.environ.get("TRASHPANDA_DB_USER", "postgres")
    password = os.environ.get("TRASHPANDA_DB_PASSWORD", "postgres")
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{name}"


@lru_cache(maxsize=1)
def get_database_settings() -> DatabaseSettings:
    """Return cached database settings resolved from the environment."""

    _load_dotenv()
    url = os.environ.get("TRASHPANDA_DATABASE_URL", "").strip() or _build_default_database_url()
    return DatabaseSettings(
        url=url,
        echo=_env_bool("TRASHPANDA_DB_ECHO", False),
        pool_size=int(os.environ.get("TRASHPANDA_DB_POOL_SIZE", "10")),
        max_overflow=int(os.environ.get("TRASHPANDA_DB_MAX_OVERFLOW", "20")),
        pool_pre_ping=_env_bool("TRASHPANDA_DB_POOL_PRE_PING", True),
    )

