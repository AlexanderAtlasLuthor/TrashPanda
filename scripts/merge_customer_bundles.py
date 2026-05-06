"""Merge customer_bundle/ outputs from multiple run dirs into one.

Use this when:

* The pipeline OOM'd on a large input and you split it into smaller
  jobs to avoid running out of memory. Each job produces its own
  customer_bundle/; this script consolidates them so the customer
  receives a single coherent deliverable.
* You ran a pilot batch and a subsequent backfill batch separately
  and want one combined bundle.

What it does:

* Concatenates the four CSVs from each input bundle:
  clean_deliverable.csv, review_provider_limited.csv,
  high_risk_removed.csv, smtp_evidence_report.csv.
* De-duplicates by email (case-insensitive). First occurrence wins,
  so feed the bundles in order of trust (most recent / best-evidence
  first).
* Resolves contradictions across bundles: if an email is in
  high_risk_removed in any bundle, it's removed from clean_deliverable
  in the merged output. Same rule for review_provider_limited.
* Emits a fresh README_CUSTOMER.md with the combined counts and the
  list of source bundles.

Example::

    python -m scripts.merge_customer_bundles \\
        runtime/jobs/100k-part-1 \\
        runtime/jobs/100k-part-2 \\
        --output-dir runtime/jobs/100k-merged
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

if __name__ == "__main__" and __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd  # noqa: E402

from app.customer_bundle import (  # noqa: E402
    CLEAN_DELIVERABLE_CSV,
    CUSTOMER_BUNDLE_DIRNAME,
    CUSTOMER_README,
    HIGH_RISK_REMOVED_CSV,
    REVIEW_PROVIDER_LIMITED_CSV,
)
from app.pilot_send.evidence import SMTP_EVIDENCE_REPORT_FILENAME  # noqa: E402


_ALL_FILES: tuple[str, ...] = (
    CLEAN_DELIVERABLE_CSV,
    REVIEW_PROVIDER_LIMITED_CSV,
    HIGH_RISK_REMOVED_CSV,
    SMTP_EVIDENCE_REPORT_FILENAME,
)


def _read_csv_safely(path: Path) -> pd.DataFrame:
    if not path.is_file() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(
            path, dtype=str, keep_default_na=False, na_filter=False,
        )
    except (pd.errors.EmptyDataError, ValueError):
        return pd.DataFrame()


def _concat_unique(frames: list[pd.DataFrame], *, on: str = "email") -> pd.DataFrame:
    non_empty = [f for f in frames if not f.empty]
    if not non_empty:
        return pd.DataFrame()
    combined = pd.concat(non_empty, ignore_index=True, sort=False)
    if on not in combined.columns:
        return combined
    norm = combined[on].astype(str).str.strip().str.lower()
    keep = ~norm.duplicated(keep="first")
    return combined.loc[keep].reset_index(drop=True)


def _email_set(df: pd.DataFrame) -> set[str]:
    if df.empty or "email" not in df.columns:
        return set()
    return set(df["email"].astype(str).str.strip().str.lower())


def _exclude_emails(df: pd.DataFrame, excluded: set[str]) -> pd.DataFrame:
    if df.empty or "email" not in df.columns or not excluded:
        return df
    lower = df["email"].astype(str).str.strip().str.lower()
    return df.loc[~lower.isin(excluded)].reset_index(drop=True)


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if df.empty:
        df = pd.DataFrame(columns=["email"])
    df.to_csv(path, index=False)


def _readme(
    sources: list[Path],
    counts: dict[str, int],
) -> str:
    when = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    lines = [
        "# Email cleaning — merged customer bundle",
        "",
        f"Merged at: {when}",
        "",
        "## Source bundles",
        "",
    ]
    for src in sources:
        lines.append(f"* `{src}`")
    lines.extend([
        "",
        "## Counts",
        "",
        f"* `clean_deliverable.csv`: {counts['clean_deliverable']:,} rows",
        f"* `review_provider_limited.csv`: {counts['review_provider_limited']:,} rows",
        f"* `high_risk_removed.csv`: {counts['high_risk_removed']:,} rows",
        f"* `smtp_evidence_report.csv`: {counts['smtp_evidence_report']:,} rows",
        "",
        "## How the merge resolved overlaps",
        "",
        "* De-duplicated by email (case-insensitive). First occurrence",
        "  across the input bundles wins.",
        "* Contradiction rule: any email that appears in",
        "  `high_risk_removed.csv` in ANY input bundle is excluded from",
        "  the merged `clean_deliverable.csv` and the merged",
        "  `review_provider_limited.csv` — a removed verdict in any",
        "  source is the strongest signal.",
        "* Same rule one level down: emails in",
        "  `review_provider_limited.csv` from any bundle are excluded",
        "  from the merged `clean_deliverable.csv`.",
        "",
        "## What we promise / what we don't",
        "",
        "Same as each source bundle. We removed obviously-bad rows",
        "(syntax / dead domain / disposable / role / spam pattern /",
        "bad-reputation domain) and separated risk. We did NOT",
        "individually verify every mailbox via SMTP — that's not",
        "economical at this volume.",
        "",
        "Use `clean_deliverable.csv` for the campaign. Decide separately",
        "whether to also include `review_provider_limited.csv`. Do NOT",
        "send to `high_risk_removed.csv`.",
    ])
    return "\n".join(lines) + "\n"


def merge_bundles(
    sources: list[Path],
    *,
    output_dir: Path,
) -> dict[str, int]:
    """Merge the customer_bundle/ from each ``sources`` run dir into
    ``output_dir/customer_bundle/``. Returns per-file row counts."""
    bundle_dirs: list[Path] = []
    for src in sources:
        candidate = src / CUSTOMER_BUNDLE_DIRNAME
        if not candidate.is_dir():
            # The user might have passed the bundle dir directly.
            if (src / CLEAN_DELIVERABLE_CSV).is_file():
                candidate = src
            else:
                raise FileNotFoundError(
                    f"no customer_bundle/ inside {src} "
                    f"(and {src} doesn't look like a bundle dir either)"
                )
        bundle_dirs.append(candidate)

    out_bundle = output_dir / CUSTOMER_BUNDLE_DIRNAME
    out_bundle.mkdir(parents=True, exist_ok=True)

    # Collect frames per file.
    per_file: dict[str, list[pd.DataFrame]] = {f: [] for f in _ALL_FILES}
    for bdir in bundle_dirs:
        for fname in _ALL_FILES:
            df = _read_csv_safely(bdir / fname)
            per_file[fname].append(df)

    clean = _concat_unique(per_file[CLEAN_DELIVERABLE_CSV])
    review = _concat_unique(per_file[REVIEW_PROVIDER_LIMITED_CSV])
    removed = _concat_unique(per_file[HIGH_RISK_REMOVED_CSV])
    evidence = _concat_unique(per_file[SMTP_EVIDENCE_REPORT_FILENAME])

    # Contradiction resolution: any bundle saying "removed" wins; any
    # saying "review" beats "clean".
    removed_emails = _email_set(removed)
    review_emails = _email_set(review)

    clean = _exclude_emails(clean, removed_emails | review_emails)
    review = _exclude_emails(review, removed_emails)

    counts = {
        "clean_deliverable": int(len(clean)),
        "review_provider_limited": int(len(review)),
        "high_risk_removed": int(len(removed)),
        "smtp_evidence_report": int(len(evidence)),
    }

    _write_csv(clean, out_bundle / CLEAN_DELIVERABLE_CSV)
    _write_csv(review, out_bundle / REVIEW_PROVIDER_LIMITED_CSV)
    _write_csv(removed, out_bundle / HIGH_RISK_REMOVED_CSV)
    _write_csv(evidence, out_bundle / SMTP_EVIDENCE_REPORT_FILENAME)

    (out_bundle / CUSTOMER_README).write_text(
        _readme(bundle_dirs, counts), encoding="utf-8",
    )
    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Merge customer_bundle/ outputs from multiple run dirs."
        ),
    )
    parser.add_argument(
        "run_dirs", nargs="+", type=Path,
        help="Run directories to merge (each must contain customer_bundle/).",
    )
    parser.add_argument(
        "--output-dir", "-o", type=Path, required=True,
        help="Where to write the merged customer_bundle/.",
    )
    args = parser.parse_args(argv)

    counts = merge_bundles(args.run_dirs, output_dir=args.output_dir)
    print(json.dumps({
        "output": str(args.output_dir / CUSTOMER_BUNDLE_DIRNAME),
        "counts": counts,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
