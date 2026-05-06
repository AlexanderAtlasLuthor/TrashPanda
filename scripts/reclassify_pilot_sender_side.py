"""Re-classify pilot rows mis-labeled as hard_bounce/blocked/etc. when
the real cause was a sender-side rejection (infrastructure_blocked or
provider_deferred).

Why this exists
---------------

Pilots run before the May 2026 sender-side classifier fix collapsed
Microsoft "[ip] weren't sent ... block list" 5xx and Yahoo TSS04 421
into ``hard_bounce`` / ``soft_bounce`` / ``blocked``. Those rows
poisoned the customer's ``do_not_send`` list with addresses that may
be perfectly valid — the rejection was about the sending IP, not the
recipient.

This script walks an existing ``pilot_send_tracker.sqlite``, re-runs
the new ``_INFRA_BLOCK_PATTERNS`` / ``_PROVIDER_DEFER_PATTERNS`` over
each row's stored ``dsn_diagnostic``, and updates ``dsn_status`` in
place when it matches.

Usage
-----

Dry-run (default — no writes, prints report):

    python -m scripts.reclassify_pilot_sender_side \\
        runtime/jobs/<JOB_ID>

Apply (write changes + re-run finalize so XLSX outputs reflect them):

    python -m scripts.reclassify_pilot_sender_side \\
        runtime/jobs/<JOB_ID> --apply

The tracker file is backed up to
``pilot_send_tracker.sqlite.bak-<timestamp>`` before any write.
"""

from __future__ import annotations

import argparse
import shutil
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

# Allow running as `python scripts/reclassify_pilot_sender_side.py ...`
# without setting PYTHONPATH.
if __name__ == "__main__" and __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.db.pilot_send_tracker import (  # noqa: E402
    PILOT_TRACKER_FILENAME,
    VERDICT_HARD_BOUNCE,
    VERDICT_INFRA_BLOCKED,
    VERDICT_PROVIDER_DEFERRED,
    VERDICT_SOFT_BOUNCE,
    VERDICT_BLOCKED,
    VERDICT_DEFERRED,
    VERDICT_UNKNOWN,
)
from app.pilot_send.bounce_parser import (  # noqa: E402
    _is_infra_block,
    _is_provider_deferred,
)


# Verdicts that the old classifier could have produced incorrectly
# from a sender-side diagnostic. We scan only these — we never
# "downgrade" delivered/complaint/etc.
_RECLASSIFIABLE_VERDICTS: frozenset[str] = frozenset({
    VERDICT_HARD_BOUNCE,
    VERDICT_BLOCKED,
    VERDICT_SOFT_BOUNCE,
    VERDICT_DEFERRED,
    VERDICT_UNKNOWN,
})


def _new_verdict_for(diagnostic: str) -> str | None:
    """Return the new verdict if the diagnostic matches a sender-side
    pattern, else ``None`` (leave the row untouched)."""
    if not diagnostic:
        return None
    if _is_infra_block(diagnostic):
        return VERDICT_INFRA_BLOCKED
    if _is_provider_deferred(diagnostic):
        return VERDICT_PROVIDER_DEFERRED
    return None


def reclassify(
    tracker_path: Path,
    *,
    apply: bool,
) -> dict[str, object]:
    """Walk the tracker DB and report (and optionally apply) updates.

    Returns a summary dict with counts per (old_verdict → new_verdict).
    """
    if not tracker_path.is_file():
        raise FileNotFoundError(tracker_path)

    moves: dict[tuple[str, str], int] = {}
    examples: dict[tuple[str, str], list[tuple[str, str]]] = {}
    update_rows: list[tuple[str, int]] = []

    with sqlite3.connect(str(tracker_path)) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            "SELECT id, email, dsn_status, dsn_diagnostic "
            "FROM pilot_send_tracker "
            "WHERE dsn_status IN ({})".format(
                ",".join("?" * len(_RECLASSIFIABLE_VERDICTS))
            ),
            tuple(_RECLASSIFIABLE_VERDICTS),
        )
        for row in cursor.fetchall():
            old = row["dsn_status"]
            diag = row["dsn_diagnostic"] or ""
            new = _new_verdict_for(diag)
            if new is None or new == old:
                continue
            key = (old, new)
            moves[key] = moves.get(key, 0) + 1
            examples.setdefault(key, [])
            if len(examples[key]) < 3:
                examples[key].append((row["email"], diag[:120]))
            update_rows.append((new, row["id"]))

        if apply and update_rows:
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            backup = tracker_path.with_name(
                f"{tracker_path.name}.bak-{ts}"
            )
            shutil.copy2(tracker_path, backup)
            now_iso = datetime.now(timezone.utc).isoformat()
            conn.executemany(
                "UPDATE pilot_send_tracker "
                "SET dsn_status = ?, updated_at = ? "
                "WHERE id = ?",
                [(new, now_iso, rid) for new, rid in update_rows],
            )
            conn.commit()
        else:
            backup = None

    return {
        "tracker_path": str(tracker_path),
        "applied": apply,
        "backup_path": str(backup) if apply and update_rows else None,
        "moves": {f"{o}->{n}": c for (o, n), c in sorted(moves.items())},
        "total_rows_changed": sum(moves.values()),
        "examples": {
            f"{o}->{n}": ex for (o, n), ex in sorted(examples.items())
        },
    }


def _print_report(report: dict[str, object]) -> None:
    print(f"Tracker:        {report['tracker_path']}")
    print(f"Mode:           {'APPLY' if report['applied'] else 'DRY-RUN'}")
    if report["backup_path"]:
        print(f"Backup:         {report['backup_path']}")
    print(f"Rows reclassified: {report['total_rows_changed']}")
    if not report["moves"]:
        print("No matching rows. Nothing to do.")
        return
    print()
    print("Move counts:")
    for key, count in report["moves"].items():
        print(f"  {key:50s} {count:5d}")
    print()
    print("Examples (first 3 per move):")
    for key, examples in report["examples"].items():
        print(f"  [{key}]")
        for email, diag in examples:
            print(f"    {email:40s}  {diag}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Re-classify pilot rows mis-labeled as hard_bounce/blocked "
            "when the cause was a sender-side rejection."
        ),
    )
    parser.add_argument(
        "run_dir",
        type=Path,
        help=(
            "Pilot run directory containing "
            f"{PILOT_TRACKER_FILENAME} (e.g. runtime/jobs/<JOB_ID>)."
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Write changes to the tracker. Without this flag the "
            "script prints a dry-run report only."
        ),
    )
    parser.add_argument(
        "--also-finalize",
        action="store_true",
        help=(
            "After --apply, re-run finalize_pilot to regenerate the "
            "XLSX outputs with the corrected verdicts."
        ),
    )
    args = parser.parse_args(argv)

    tracker_path = args.run_dir / PILOT_TRACKER_FILENAME
    report = reclassify(tracker_path, apply=args.apply)
    _print_report(report)

    if args.apply and args.also_finalize and report["total_rows_changed"]:
        print()
        print("Re-running finalize_pilot ...")
        from app.pilot_send.finalize import finalize_pilot

        result = finalize_pilot(args.run_dir)
        print(f"  files_written: {len(result.files_written)}")
        for name, path in sorted(result.files_written.items()):
            print(f"    {name:35s} {path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
