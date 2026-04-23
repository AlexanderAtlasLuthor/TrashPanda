"""Human-friendly dependency checks for the PostgreSQL persistence layer."""

from __future__ import annotations

import importlib.util
import sys


def _missing_modules() -> list[str]:
    required = {
        "sqlalchemy": "sqlalchemy",
        "psycopg": "psycopg[binary]",
    }
    missing: list[str] = []
    for module_name, package_name in required.items():
        if importlib.util.find_spec(module_name) is None:
            missing.append(package_name)
    return missing


def _install_command() -> str:
    return f'"{sys.executable}" -m pip install -r requirements.txt'


def ensure_database_dependencies() -> None:
    """Raise a clear startup error when DB runtime packages are missing."""

    missing = _missing_modules()
    if not missing:
        return

    package_list = ", ".join(missing)
    raise ModuleNotFoundError(
        "TrashPanda's PostgreSQL persistence layer requires additional Python packages.\n"
        f"Missing package(s): {package_list}\n"
        f"Active Python: {sys.executable}\n"
        "Install the backend dependencies with:\n"
        f"  {_install_command()}\n"
        "Or install just the DB packages with:\n"
        '  python -m pip install sqlalchemy "psycopg[binary]"'
    )
