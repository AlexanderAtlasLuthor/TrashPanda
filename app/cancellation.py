"""Process-wide cancellation registry for in-flight jobs.

Threads (e.g. SMTP probe loops, BackgroundTasks) consult
:func:`is_cancelled` periodically and unwind cleanly when their job has
been marked for cancellation via :func:`cancel`. The registry is kept
intentionally simple: a single ``set`` of job IDs guarded by a lock.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Final

_LOCK: Final = threading.Lock()
_CANCELLED: set[str] = set()


def cancel(job_id: str) -> bool:
    """Mark ``job_id`` as cancelled. Returns True if the flag was newly set."""
    job_id = (job_id or "").strip()
    if not job_id:
        return False
    with _LOCK:
        if job_id in _CANCELLED:
            return False
        _CANCELLED.add(job_id)
        return True


def is_cancelled(job_id: str | None) -> bool:
    if not job_id:
        return False
    with _LOCK:
        return job_id in _CANCELLED


def clear(job_id: str) -> None:
    if not job_id:
        return
    with _LOCK:
        _CANCELLED.discard(job_id)


def reset_all() -> None:
    """Test helper. Drop every cancellation flag."""
    with _LOCK:
        _CANCELLED.clear()


def make_cancel_check(job_id: str | None) -> Callable[[], bool]:
    """Return a 0-arg callable that reports whether ``job_id`` was cancelled."""
    if not job_id:
        return lambda: False
    return lambda: is_cancelled(job_id)


class JobCancelled(RuntimeError):
    """Raised by long-running loops when a cancellation flag is observed."""

    def __init__(self, job_id: str | None = None) -> None:
        super().__init__(f"job cancelled: {job_id or '<unknown>'}")
        self.job_id = job_id


__all__ = [
    "JobCancelled",
    "cancel",
    "clear",
    "is_cancelled",
    "make_cancel_check",
    "reset_all",
]
