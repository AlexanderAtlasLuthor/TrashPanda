#!/usr/bin/env python3
"""CLI wrapper for the Emergency Extra Strict Offline clean.

Usage
-----
    python -m scripts.extra_strict_clean <run_dir> [--probability-threshold 0.75]
    python -m scripts.extra_strict_clean --job-id <job_id>

When ``--job-id`` is supplied the latest run directory under
``runtime/jobs/<job_id>/`` is resolved automatically.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Allow running as a script (``python scripts/extra_strict_clean.py``)
# without installing the package.
_HERE = Path(__file__).resolve().parent
_PROJECT_ROOT = _HERE.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from app.extra_strict_clean import (  # noqa: E402
    ExtraStrictConfig,
    run_extra_strict_clean,
)


def _latest_run_dir(job_output_dir: Path) -> Path | None:
    if not job_output_dir.is_dir():
        return None
    candidates = [
        d for d in job_output_dir.iterdir() if d.is_dir() and d.name != "uploads"
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _resolve_run_dir(args: argparse.Namespace) -> Path:
    if args.run_dir is not None:
        path = Path(args.run_dir)
        if not path.is_dir():
            raise SystemExit(f"run_dir not found: {path}")
        return path

    if args.job_id is None:
        raise SystemExit("either <run_dir> or --job-id is required")

    job_root = _PROJECT_ROOT / "runtime" / "jobs" / args.job_id
    latest = _latest_run_dir(job_root)
    if latest is None:
        raise SystemExit(
            f"no run directory under {job_root} (has the job actually finished?)"
        )
    return latest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Apply the Extra Strict Offline policy on top of a finished "
            "TrashPanda run and emit a single clean_final_extra_strict.xlsx "
            "plus removed/review companions."
        )
    )
    parser.add_argument(
        "run_dir",
        nargs="?",
        help=(
            "Path to a finished run directory containing "
            "clean_high_confidence.csv, review_medium_confidence.csv, "
            "removed_invalid.csv."
        ),
    )
    parser.add_argument(
        "--job-id",
        help="Resolve run_dir automatically from runtime/jobs/<job_id>/",
    )
    parser.add_argument(
        "--probability-threshold",
        type=float,
        default=0.75,
        help="Minimum deliverability probability for a row to survive (default 0.75).",
    )
    parser.add_argument(
        "--allow-medium-risk",
        action="store_true",
        help="Keep medium-risk-domain rows in the primary deliverable.",
    )
    parser.add_argument(
        "--allow-role-based",
        action="store_true",
        help="Keep role-based addresses (info@, support@, ...) in the deliverable.",
    )
    parser.add_argument(
        "--output-subdir",
        default="extra_strict",
        help="Subdirectory under run_dir to write artifacts into (default: extra_strict).",
    )

    args = parser.parse_args(argv)

    run_dir = _resolve_run_dir(args)
    config = ExtraStrictConfig(
        min_deliverability_probability=float(args.probability_threshold),
        high_risk_domain_excluded=True,
        medium_risk_domain_excluded=not args.allow_medium_risk,
        role_based_excluded=not args.allow_role_based,
        output_subdir=args.output_subdir,
    )

    result = run_extra_strict_clean(run_dir, config=config)
    print(f"run_dir:         {run_dir}")
    print(f"output_dir:      {result.out_dir}")
    print(f"total_rows:      {result.total_rows}")
    for tier, count in sorted(result.counts.items()):
        print(f"  {tier:20s} {count}")
    print(f"PRIMARY:         {result.primary_xlsx}")
    print(f"review_catchall: {result.review_xlsx}")
    print(f"removed:         {result.removed_xlsx}")
    print(f"rejected:        {result.rejected_xlsx}")
    print(f"summary:         {result.summary_txt}")
    print(f"readme:          {result.readme_txt}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
