"""V2.10.12 — pilot bounce-poller CLI entry point.

Usage::

    python -m app.pilot_send.bounce_poller --once \
        --runtime-root /root/trashpanda/runtime/jobs

Designed for invocation from a systemd timer; see
``deploy/trashpanda-pilot-bounce-poller.{service,timer}``.

The CLI scans every per-run directory under ``--runtime-root`` and
calls :func:`app.pilot_send.bounce_poller.poll_bounces` against each
that has a ``pilot_send_tracker.sqlite`` file. Runs that haven't yet
configured IMAP credentials or where the password env var is unset
are silently skipped — the poller short-circuits internally.
"""

from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path

from .bounce_poller import poll_bounces


_LOGGER = logging.getLogger("app.pilot_send")


_DEFAULT_INTERVAL_SECONDS: int = 900  # 15 minutes — matches retry worker.


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m app.pilot_send.bounce_poller",
        description=(
            "Drain pilot send bounce mailboxes and apply DSN verdicts."
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
        help="Root of per-job runtime directories (default: runtime/jobs).",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=_DEFAULT_INTERVAL_SECONDS,
        help=(
            "Seconds between drain passes when not --once "
            f"(default: {_DEFAULT_INTERVAL_SECONDS})."
        ),
    )
    return parser


def _iter_run_dirs(root: Path):
    if not root.is_dir():
        return
    for job_dir in root.iterdir():
        if not job_dir.is_dir():
            continue
        for run_dir in job_dir.iterdir():
            if not run_dir.is_dir():
                continue
            if (run_dir / "pilot_send_tracker.sqlite").is_file():
                yield run_dir


def _drain_all(runtime_root: Path) -> int:
    """Run one pass over every eligible run dir. Returns total
    matched DSNs across all queues."""
    total_matched = 0
    for run_dir in _iter_run_dirs(runtime_root):
        try:
            result = poll_bounces(run_dir)
        except Exception as exc:  # pragma: no cover - defensive
            _LOGGER.warning("poll_bounces failed for %s: %s", run_dir, exc)
            continue
        if result.fetched > 0:
            _LOGGER.info(
                "drained %s: fetched=%s parsed=%s matched=%s",
                run_dir, result.fetched, result.parsed, result.matched,
            )
            total_matched += result.matched
    return total_matched


def _main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(message)s",
    )
    args = _build_parser().parse_args(argv)
    runtime_root = Path(args.runtime_root)
    while True:
        _drain_all(runtime_root)
        if args.once:
            return 0
        time.sleep(int(args.interval_seconds))


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(_main())
