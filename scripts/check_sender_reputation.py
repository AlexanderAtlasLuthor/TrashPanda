"""CLI: print the current reputation gate decision for a sending IP.

Use this before launching a pilot to see whether the IP is healthy
across all tracked sources. Exit code:

* 0  green or unknown  (safe to proceed)
* 1  yellow            (safe but operator should monitor)
* 2  red               (NOT safe — resolve before pilot)

Example::

    python -m scripts.check_sender_reputation --ip 192.3.105.145
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

if __name__ == "__main__" and __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.sender_reputation import (  # noqa: E402
    DEFAULT_FRESHNESS_HOURS,
    is_safe_to_pilot,
    latest_per_source,
    open_store,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Print the reputation gate decision for a sending IP.",
    )
    parser.add_argument("--ip", required=True, help="Sending IP.")
    parser.add_argument("--db", help="SQLite store path.")
    parser.add_argument(
        "--freshness-hours",
        type=int,
        default=DEFAULT_FRESHNESS_HOURS,
        help=f"Snapshots older than this are stale. Default: {DEFAULT_FRESHNESS_HOURS}h.",
    )
    parser.add_argument(
        "--json", action="store_true",
        help="Output JSON instead of human-readable text.",
    )
    args = parser.parse_args(argv)

    with open_store(args.db) as conn:
        decision = is_safe_to_pilot(
            conn, args.ip, freshness_hours=args.freshness_hours,
        )
        snapshots = latest_per_source(conn, args.ip)

    if args.json:
        payload = {
            "ip": decision.ip,
            "safe": decision.safe,
            "overall_status": decision.overall_status,
            "reasons": list(decision.reasons),
            "snapshots": {
                source: {
                    "captured_at": snap.captured_at.isoformat(),
                    "status": snap.status,
                    "score": snap.score,
                    "complaint_rate": snap.complaint_rate,
                    "notes": snap.notes,
                }
                for source, snap in snapshots.items()
            },
        }
        print(json.dumps(payload, indent=2))
    else:
        print(f"IP:             {decision.ip}")
        print(f"Overall:        {decision.overall_status}")
        print(f"Safe to pilot:  {decision.safe}")
        print("Reasons:")
        for reason in decision.reasons:
            print(f"  - {reason}")

    if decision.overall_status == "red":
        return 2
    if decision.overall_status == "yellow":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
