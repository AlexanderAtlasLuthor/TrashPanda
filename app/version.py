"""Runtime version info.

Single source of truth for "which commit is this process running?".
Used by:

* ``/version`` HTTP endpoint — operators / the UI compare against
  ``main`` on GitHub to detect deploy drift (the May 2026 incident
  where a fix was on ``main`` for days while the VPS still ran
  pre-merge code).
* Backend startup logs — first line of the log identifies the
  running revision so operators don't have to guess from timestamps.

Resolution order (first hit wins):

1. ``VERSION`` file at the repo root (preferred — written by the
   deploy script on each pull, so the answer is correct even when
   the repo isn't a git checkout, e.g. inside a container).
2. ``git rev-parse HEAD`` against the source tree (fallback for
   dev environments).
3. ``unknown`` sentinel — the process is running, but the source
   identity is opaque. Surfaced honestly rather than silently
   misreported.

The module is import-time cheap: no shell call until ``get_version``
is actually called, and the result is cached.
"""

from __future__ import annotations

import logging
import os
import subprocess
from dataclasses import asdict, dataclass
from functools import lru_cache
from pathlib import Path


_LOGGER = logging.getLogger(__name__)


VERSION_FILENAME: str = "VERSION"
VERSION_UNKNOWN: str = "unknown"


@dataclass(frozen=True, slots=True)
class VersionInfo:
    """Identity of the running revision."""

    commit: str
    short_commit: str
    branch: str
    dirty: bool
    source: str  # "version_file" | "git" | "unknown"

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _repo_root() -> Path:
    """The TrashPanda repo root. ``app/`` is one level below it."""
    return Path(__file__).resolve().parent.parent


def _read_version_file() -> str | None:
    candidates = (
        Path.cwd() / VERSION_FILENAME,
        _repo_root() / VERSION_FILENAME,
    )
    for candidate in candidates:
        try:
            if candidate.is_file():
                text = candidate.read_text(encoding="utf-8").strip()
                if text:
                    return text.split()[0]  # tolerate trailing whitespace/comments
        except OSError:
            continue
    return None


def _git_call(args: list[str], *, cwd: Path) -> str | None:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return None
    if result.returncode != 0:
        return None
    out = result.stdout.strip()
    return out or None


@lru_cache(maxsize=1)
def get_version() -> VersionInfo:
    """Resolve the running revision. Cached for the life of the process.

    Set ``TRASHPANDA_VERSION_OVERRIDE`` to short-circuit detection
    (useful in containers and for tests)."""
    override = os.environ.get("TRASHPANDA_VERSION_OVERRIDE")
    if override:
        commit = override.strip()
        return VersionInfo(
            commit=commit,
            short_commit=commit[:7] or VERSION_UNKNOWN,
            branch="",
            dirty=False,
            source="env_override",
        )

    file_sha = _read_version_file()
    if file_sha:
        return VersionInfo(
            commit=file_sha,
            short_commit=file_sha[:7],
            branch="",
            dirty=False,
            source="version_file",
        )

    repo = _repo_root()
    git_sha = _git_call(["rev-parse", "HEAD"], cwd=repo)
    if git_sha:
        branch = _git_call(["rev-parse", "--abbrev-ref", "HEAD"], cwd=repo) or ""
        porcelain = _git_call(["status", "--porcelain"], cwd=repo)
        dirty = bool(porcelain)
        return VersionInfo(
            commit=git_sha,
            short_commit=git_sha[:7],
            branch=branch,
            dirty=dirty,
            source="git",
        )

    return VersionInfo(
        commit=VERSION_UNKNOWN,
        short_commit=VERSION_UNKNOWN,
        branch="",
        dirty=False,
        source="unknown",
    )


def reset_cache() -> None:
    """Clear the cached resolution. Tests use this."""
    get_version.cache_clear()


def log_startup_banner(logger: logging.Logger | None = None) -> None:
    """Emit a single startup line identifying the running revision.

    Called from the FastAPI app startup so the first thing in the
    backend log answers "what code is this?". No-op on resolution
    failure (the banner is informational, never blocking).
    """
    log = logger or _LOGGER
    info = get_version()
    extra = ""
    if info.dirty:
        extra = " (dirty)"
    if info.branch:
        extra += f" branch={info.branch}"
    log.info(
        "trashpanda revision: %s [%s]%s",
        info.short_commit, info.source, extra,
    )


__all__ = [
    "VERSION_FILENAME",
    "VERSION_UNKNOWN",
    "VersionInfo",
    "get_version",
    "log_startup_banner",
    "reset_cache",
]
