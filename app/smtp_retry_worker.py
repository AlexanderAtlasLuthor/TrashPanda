"""V2.10.11 — Retry queue drain worker.

Reads ``runtime/jobs/<job_id>/<run_id>/smtp_retry_queue.sqlite``
files written by :class:`app.engine.stages.SMTPVerificationStage`
and re-probes each pending row that is due. Designed to run on a
short cron / systemd-timer cadence (15min) so real-world
greylisting (which clears in 5-30min for legitimate senders)
gets a second chance without operator action.

Operator-controlled scope
-------------------------

The worker does **not** drain every queue file it finds. Each job's
queue carries an ``auto_retry_enabled`` flag (in
``smtp_retry_queue_config.json``) set by the operator at upload
time. When ``False`` (the default), the worker leaves the queue
alone — only the operator-triggered ``POST
/jobs/{id}/retry-queue/run`` endpoint drains it.

This intentional gating prevents two failure modes:

1. The worker probing a job whose customer never asked for
   retries, burning rate-limit budget and exposing the operator's
   IP to additional MX-side fingerprinting.
2. A previously-delivered bundle silently changing content when
   the worker rescues rows out of review. Re-classification of
   the client package is **always** operator-triggered via the
   separate ``finalize`` endpoint.

CLI entry point
---------------

    python -m app.smtp_retry_worker [--once] [--runtime-root PATH]

* ``--once`` runs a single drain pass and exits — used by tests
  and by the systemd timer.
* Without ``--once`` the worker loops with
  ``drain_interval_seconds`` between passes.
* ``--runtime-root`` overrides the default ``runtime/jobs``
  location.

The worker is offline-friendly: tests inject a fake probe via
:func:`drain_run_queue(probe_fn=...)`. The default probe is
``app.validation_v2.smtp_probe.probe_email_smtplib``.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable

from .db.smtp_retry_queue import (
    DEFAULT_RETRY_SCHEDULE_MINUTES,
    DEFAULT_TTL_HOURS,
    SMTP_RETRY_CONFIG_FILENAME,
    SMTP_RETRY_QUEUE_FILENAME,
    SMTPRetryQueue,
    STATE_EXHAUSTED,
    STATE_PENDING,
    STATE_SUCCEEDED,
    open_for_run,
)
from .engine.stages.smtp_verification import (
    SMTP_STATUS_BLOCKED,
    SMTP_STATUS_TEMP_FAIL,
    SMTP_STATUS_TIMEOUT,
    SMTP_STATUS_VALID,
    normalize_smtp_status,
)
from .validation_v2.smtp_probe import SMTPResult, probe_email_smtplib


_LOGGER = logging.getLogger(__name__)


DEFAULT_DRAIN_INTERVAL_SECONDS: int = 900  # 15min — matches the README example.
DEFAULT_DRAIN_BATCH_SIZE: int = 100
DEFAULT_PROBE_TIMEOUT_SECONDS: float = 6.0


# Statuses that close the loop on a queue row — the row exits to
# ``succeeded`` regardless of whether the verdict is "good news"
# (valid) or "bad news" (invalid). Anything else means the probe
# stayed inconclusive and the row is rescheduled (or exhausted).
_TERMINAL_STATUSES: frozenset[str] = frozenset({
    SMTP_STATUS_VALID,
    "invalid",
    "catch_all_possible",
})


@dataclass(slots=True)
class DrainResult:
    """Aggregate counts from a single ``drain_run_queue`` call."""

    queue_path: Path
    auto_retry_enabled: bool
    expired: int = 0
    probed: int = 0
    succeeded: int = 0
    rescheduled: int = 0
    exhausted: int = 0


# --------------------------------------------------------------------------- #
# Per-run config (auto_retry flag persistence)
# --------------------------------------------------------------------------- #


def read_retry_config(run_dir: str | Path) -> dict:
    """Load ``smtp_retry_queue_config.json`` from ``run_dir``.

    Missing / malformed file → ``{"auto_retry_enabled": False}``.
    The dict is intentionally untyped (free-form) so adding
    additional keys later doesn't break older queues.
    """
    path = Path(run_dir) / SMTP_RETRY_CONFIG_FILENAME
    if not path.is_file():
        return {"auto_retry_enabled": False}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        _LOGGER.warning("retry config unreadable at %s, defaulting off", path)
        return {"auto_retry_enabled": False}


def write_retry_config(run_dir: str | Path, *, auto_retry_enabled: bool) -> Path:
    """Persist the per-run retry config and return the path."""
    path = Path(run_dir) / SMTP_RETRY_CONFIG_FILENAME
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"auto_retry_enabled": bool(auto_retry_enabled)}),
        encoding="utf-8",
    )
    return path


# --------------------------------------------------------------------------- #
# Drain
# --------------------------------------------------------------------------- #


def _default_probe(email: str, *, timeout: float) -> SMTPResult:
    return probe_email_smtplib(email, timeout=timeout)


def drain_run_queue(
    run_dir: str | Path,
    *,
    probe_fn: Callable[..., SMTPResult] | None = None,
    schedule_minutes: tuple[int, ...] = DEFAULT_RETRY_SCHEDULE_MINUTES,
    ttl_hours: int = DEFAULT_TTL_HOURS,
    batch_size: int = DEFAULT_DRAIN_BATCH_SIZE,
    probe_timeout_seconds: float = DEFAULT_PROBE_TIMEOUT_SECONDS,
    require_auto_retry: bool = True,
    now: datetime | None = None,
) -> DrainResult:
    """Drain due rows from one run's retry queue.

    Parameters
    ----------
    run_dir:
        Path to the per-run output directory.
    probe_fn:
        Override for the live SMTP probe — tests pass a fake. The
        default uses :func:`probe_email_smtplib`.
    require_auto_retry:
        When True (the worker default), the drain skips queues whose
        ``auto_retry_enabled`` flag is False. The on-demand HTTP
        endpoint passes ``False`` so it can drain regardless of the
        flag.

    Returns a :class:`DrainResult` describing what changed. Never
    raises on a missing queue file — returns a ``DrainResult`` with
    zero counts.
    """
    run_dir_path = Path(run_dir)
    queue_path = run_dir_path / SMTP_RETRY_QUEUE_FILENAME
    config = read_retry_config(run_dir_path)
    auto_retry = bool(config.get("auto_retry_enabled"))

    result = DrainResult(
        queue_path=queue_path,
        auto_retry_enabled=auto_retry,
    )

    if not queue_path.is_file():
        return result
    if require_auto_retry and not auto_retry:
        return result

    fn = probe_fn or _default_probe
    ts = now or datetime.now(tz=timezone.utc)

    with closing(open_for_run(run_dir_path)) as queue:
        result.expired = queue.expire_old(
            ttl=timedelta(hours=ttl_hours), now=ts,
        )
        rows = queue.claim_pending(now=ts, limit=batch_size)
        for row in rows:
            try:
                probe_result = fn(row.email, timeout=probe_timeout_seconds)
            except Exception as exc:  # pragma: no cover - defensive
                _LOGGER.warning(
                    "retry probe failed for %s: %s", row.email, exc,
                )
                queue.reschedule(
                    row.id,
                    schedule_minutes=schedule_minutes,
                    last_status="error",
                    last_response_message=str(exc)[:200],
                    now=ts,
                )
                result.probed += 1
                result.rescheduled += 1
                continue

            status = normalize_smtp_status(probe_result)
            response_code = (
                int(probe_result.response_code)
                if probe_result.response_code is not None
                else None
            )
            response_message = probe_result.response_message or ""

            result.probed += 1
            if status in _TERMINAL_STATUSES:
                queue.mark_succeeded(
                    row.id,
                    last_status=status,
                    last_response_code=response_code,
                    last_response_message=response_message,
                    now=ts,
                )
                result.succeeded += 1
            else:
                new_state = queue.reschedule(
                    row.id,
                    schedule_minutes=schedule_minutes,
                    last_status=status,
                    last_response_code=response_code,
                    last_response_message=response_message,
                    now=ts,
                )
                if new_state == STATE_EXHAUSTED:
                    result.exhausted += 1
                else:
                    result.rescheduled += 1

    return result


def iter_run_dirs(runtime_root: str | Path) -> Iterable[Path]:
    """Yield every ``run_dir`` under a runtime root.

    Layout matches the existing operator routes:
    ``runtime/jobs/<job_id>/<run_id>/``. Directories without a
    ``smtp_retry_queue.sqlite`` are skipped silently.
    """
    root = Path(runtime_root)
    if not root.is_dir():
        return
    for job_dir in root.iterdir():
        if not job_dir.is_dir():
            continue
        for run_dir in job_dir.iterdir():
            if not run_dir.is_dir():
                continue
            if (run_dir / SMTP_RETRY_QUEUE_FILENAME).is_file():
                yield run_dir


def drain_all(
    runtime_root: str | Path,
    *,
    probe_fn: Callable[..., SMTPResult] | None = None,
    require_auto_retry: bool = True,
    now: datetime | None = None,
) -> list[DrainResult]:
    """Drain every queue under ``runtime_root`` once."""
    return [
        drain_run_queue(
            run_dir,
            probe_fn=probe_fn,
            require_auto_retry=require_auto_retry,
            now=now,
        )
        for run_dir in iter_run_dirs(runtime_root)
    ]


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m app.smtp_retry_worker",
        description=(
            "Drain SMTP retry queues for jobs with auto_retry_enabled=true."
        ),
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one drain pass and exit (default: loop forever).",
    )
    parser.add_argument(
        "--runtime-root",
        default="runtime/jobs",
        help="Root of the per-job runtime tree (default: runtime/jobs).",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=DEFAULT_DRAIN_INTERVAL_SECONDS,
        help=(
            "Seconds between drain passes when not --once "
            f"(default: {DEFAULT_DRAIN_INTERVAL_SECONDS})."
        ),
    )
    return parser


def _main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    args = _build_parser().parse_args(argv)
    while True:
        results = drain_all(args.runtime_root)
        for r in results:
            if r.probed:
                _LOGGER.info(
                    "drained %s: probed=%s succeeded=%s rescheduled=%s "
                    "exhausted=%s expired=%s",
                    r.queue_path,
                    r.probed,
                    r.succeeded,
                    r.rescheduled,
                    r.exhausted,
                    r.expired,
                )
        if args.once:
            return 0
        time.sleep(args.interval_seconds)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(_main())


__all__ = [
    "DEFAULT_DRAIN_BATCH_SIZE",
    "DEFAULT_DRAIN_INTERVAL_SECONDS",
    "DEFAULT_PROBE_TIMEOUT_SECONDS",
    "DrainResult",
    "drain_all",
    "drain_run_queue",
    "iter_run_dirs",
    "read_retry_config",
    "write_retry_config",
]
