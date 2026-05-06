"""Regenerate <run_dir>/customer_bundle/ for an existing run.

Use this when:

* You ran a job and the customer_bundle was generated with an older
  version of the code (e.g. before review_*.xlsx files were folded
  into clean_deliverable).
* You want to refresh the bundle after the operator manually moved
  rows around in the V2 review queue.
* The pilot finished and you just want the customer-facing files.

The script doesn't re-run the pipeline. It only re-reads whatever
artifacts already exist in the run directory and rewrites
``customer_bundle/`` in place.

Example::

    python -m scripts.regenerate_customer_bundle runtime/jobs/pilot-001
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

if __name__ == "__main__" and __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.customer_bundle import emit_customer_bundle  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Regenerate the customer_bundle/ for an existing run.",
    )
    parser.add_argument(
        "run_dir",
        type=Path,
        help="Per-run output directory (e.g. runtime/jobs/<JOB_ID>).",
    )
    parser.add_argument(
        "--quiet", action="store_true",
        help="Suppress the per-bucket count summary.",
    )
    args = parser.parse_args(argv)

    if not args.run_dir.is_dir():
        raise SystemExit(f"run_dir not found: {args.run_dir}")

    result = emit_customer_bundle(args.run_dir)

    if not args.quiet:
        print(json.dumps({
            "bundle_dir": str(result.bundle_dir),
            "counts": result.counts,
            "files": {k: str(v) for k, v in result.files_written.items()},
        }, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
