"""Import reputation snapshots into the sender_reputation SQLite store.

Supports:

* ``--snds <csv>``    Microsoft SNDS CSV (downloaded from
                     sendersupport.olc.protection.outlook.com/snds/).
* ``--manual``        Single ad-hoc snapshot from CLI flags. Useful
                     for "we just got delisted" notes or for marking
                     an RBL listing.

Examples
--------

Import a fresh SNDS dump::

    python -m scripts.import_reputation_snapshot --snds snds_2026-05-06.csv

Record a manual yellow flag::

    python -m scripts.import_reputation_snapshot --manual \\
        --ip 192.3.105.145 --status yellow \\
        --notes "Microsoft S3150 in pilot, delist requested"
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

if __name__ == "__main__" and __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.sender_reputation import (  # noqa: E402
    ALL_SOURCES,
    ALL_STATUSES,
    ReputationSnapshot,
    SOURCE_MANUAL,
    import_snds_csv,
    open_store,
    record_snapshot,
)


def _do_snds(args: argparse.Namespace) -> int:
    captured_at = (
        datetime.fromisoformat(args.captured_at)
        if args.captured_at
        else datetime.now(tz=timezone.utc)
    )
    if captured_at.tzinfo is None:
        captured_at = captured_at.replace(tzinfo=timezone.utc)
    with open_store(args.db) as conn:
        n = import_snds_csv(conn, args.snds, captured_at=captured_at)
    print(f"snds: imported {n} row(s) from {args.snds}")
    return 0


def _do_manual(args: argparse.Namespace) -> int:
    if not args.ip:
        raise SystemExit("--manual requires --ip")
    if args.status not in ALL_STATUSES:
        raise SystemExit(
            f"--status must be one of {sorted(ALL_STATUSES)}"
        )
    if args.source not in ALL_SOURCES:
        raise SystemExit(
            f"--source must be one of {sorted(ALL_SOURCES)}"
        )
    captured_at = (
        datetime.fromisoformat(args.captured_at)
        if args.captured_at
        else datetime.now(tz=timezone.utc)
    )
    if captured_at.tzinfo is None:
        captured_at = captured_at.replace(tzinfo=timezone.utc)
    snap = ReputationSnapshot(
        ip=args.ip.strip(),
        source=args.source,
        captured_at=captured_at,
        status=args.status,
        score=args.score,
        complaint_rate=args.complaint_rate,
        notes=args.notes or "",
    )
    with open_store(args.db) as conn:
        record_snapshot(conn, snap)
    print(f"manual: recorded {snap.source}/{snap.status} for {snap.ip}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Import sender reputation snapshots (SNDS CSV or manual)."
        ),
    )
    parser.add_argument(
        "--db",
        help=(
            "Path to the SQLite store (default: "
            "runtime/reputation/sender_reputation.sqlite)."
        ),
    )
    parser.add_argument(
        "--captured-at",
        help=(
            "ISO-8601 timestamp for the snapshot. Default: now (UTC)."
        ),
    )

    sub = parser.add_subparsers(dest="mode", required=True)

    snds_p = sub.add_parser("snds", help="Import a Microsoft SNDS CSV.")
    snds_p.add_argument("snds", help="Path to the SNDS CSV file.")

    man_p = sub.add_parser("manual", help="Record a single manual snapshot.")
    man_p.add_argument("--ip", required=True, help="Sending IP.")
    man_p.add_argument(
        "--source", default=SOURCE_MANUAL,
        help=f"One of {sorted(ALL_SOURCES)}. Default: manual.",
    )
    man_p.add_argument(
        "--status", required=True,
        help=f"One of {sorted(ALL_STATUSES)}.",
    )
    man_p.add_argument(
        "--score", type=float, default=None,
        help="Numeric reputation score (source-dependent).",
    )
    man_p.add_argument(
        "--complaint-rate", type=float, default=None,
        help="Complaint rate as a decimal (e.g. 0.0045 = 0.45%%).",
    )
    man_p.add_argument(
        "--notes", default="",
        help="Free-form note (e.g. 'delist requested').",
    )

    args = parser.parse_args(argv)
    if args.mode == "snds":
        return _do_snds(args)
    if args.mode == "manual":
        return _do_manual(args)
    parser.error("unknown mode")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
