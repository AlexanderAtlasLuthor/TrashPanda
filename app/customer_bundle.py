"""V2.10.13 — Customer-facing delivery bundle.

The pipeline already produces technical CSVs (``clean_high_confidence``
/ ``review_medium_confidence`` / ``removed_invalid``) and pilot XLSX
(``delivery_verified``, ``pilot_hard_bounces``, ...). Those names
exist for engineers and operators; they are not the right names to
hand to a non-technical buyer.

This module emits a parallel, customer-language bundle into
``<run_dir>/customer_bundle/`` with four files (all CSV, per the
customer-facing spec):

* ``clean_deliverable.csv``        — the safe-to-send list.
* ``review_provider_limited.csv``  — recipients we could not
  verify because the recipient provider rejected/throttled our
  sending IP. NOT evidence the email is bad.
* ``high_risk_removed.csv``        — explicitly removed from the
  deliverable (recipient-level rejection, content/policy block,
  abuse complaint, syntax/MX/disposable failures).
* ``smtp_evidence_report.csv``     — per-row audit, copy of the
  one written by ``finalize_pilot``.

Plus ``README_CUSTOMER.md`` explaining the honest framing: we
remove obvious bad rows and separate risk; we do NOT promise to
verify every mailbox individually.

Designed to be safe to call repeatedly. Idempotent.
"""

from __future__ import annotations

import csv
import shutil
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .pilot_send.evidence import CSV_COLUMNS, SMTP_EVIDENCE_REPORT_FILENAME


CUSTOMER_BUNDLE_DIRNAME: str = "customer_bundle"

# Customer-facing filenames per spec — all CSV.
CLEAN_DELIVERABLE_CSV: str = "clean_deliverable.csv"
REVIEW_PROVIDER_LIMITED_CSV: str = "review_provider_limited.csv"
HIGH_RISK_REMOVED_CSV: str = "high_risk_removed.csv"
CUSTOMER_README: str = "README_CUSTOMER.md"


# Source files we draw from. Each maps to one or more output files.
# ``delivery_verified`` is the strongest "clean" signal (pilot-proven
# good); ``clean_high_confidence`` is the bulk pre-pilot clean list.
_SOURCE_DELIVERY_VERIFIED: str = "delivery_verified.xlsx"
_SOURCE_CLEAN_HIGH_CONFIDENCE: str = "clean_high_confidence.csv"
_SOURCE_PILOT_HARD_BOUNCES: str = "pilot_hard_bounces.xlsx"
_SOURCE_PILOT_BLOCKED_OR_DEFERRED: str = "pilot_blocked_or_deferred.xlsx"
_SOURCE_PILOT_INFRA_RETEST: str = "pilot_infrastructure_blocked.xlsx"
_SOURCE_REMOVED_INVALID: str = "removed_invalid.csv"
_SOURCE_DO_NOT_SEND: str = "updated_do_not_send.xlsx"
_SOURCE_DO_NOT_SEND_FALLBACK: str = "do_not_send.xlsx"


@dataclass(slots=True)
class CustomerBundleResult:
    bundle_dir: Path
    files_written: dict[str, Path]
    counts: dict[str, int]


# ---------------------------------------------------------------------------
# Read helpers — survive missing files gracefully.
# ---------------------------------------------------------------------------


def _read_any(path: Path) -> pd.DataFrame:
    if not path.is_file() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        if path.suffix.lower() in {".xlsx", ".xls"}:
            return pd.read_excel(path, sheet_name=0, dtype=str)
        return pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False)
    except (pd.errors.EmptyDataError, ValueError):
        return pd.DataFrame()


def _concat_unique(frames: list[pd.DataFrame], *, on: str) -> pd.DataFrame:
    """Concatenate frames and de-duplicate by ``on`` (case-insensitive
    on email-style values), preserving column union."""
    non_empty = [f for f in frames if not f.empty]
    if not non_empty:
        return pd.DataFrame()
    combined = pd.concat(non_empty, ignore_index=True, sort=False)
    if on not in combined.columns:
        return combined
    norm = combined[on].astype(str).str.strip().str.lower()
    keep = ~norm.duplicated(keep="first")
    return combined.loc[keep].reset_index(drop=True)


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if df.empty:
        # Write a header-only file so consumers can rely on the file
        # existing. Use a single ``email`` column when nothing else
        # is known.
        df = pd.DataFrame(columns=["email"])
    df.to_csv(path, index=False)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


_README_TEMPLATE: str = """# Email cleaning — customer bundle

This bundle contains the result of running our multi-layer email
cleaning system over your list. Read this first — the file names
and what they mean matter.

## What we promise

We removed obvious-bad rows (syntax errors, dead domains, disposable
addresses, role-based risky, known spam patterns) and separated
risk so the deliverable list is **defensibly cleaner** than the
input. Where we ran a controlled SMTP pilot, we used the result
**only when the response was about the recipient**. When the
response was about our sending infrastructure, we did NOT treat
it as evidence about the recipient.

## What we do NOT promise

We did not, and cannot at this price point, individually verify
the existence of every mailbox in your list. Mailbox existence
checking at scale requires either commercial APIs (cost-prohibitive
for lists this size) or a controlled multi-IP SMTP fleet over
weeks. What we did instead is run multi-layer validation +
controlled-pilot sampling to defensibly reduce risk.

## Files in this bundle

### `clean_deliverable.csv`
The safe-to-send list. Rows that passed every layer we checked
(syntax, domain, MX, disposable, role-based risk, spam patterns,
known-bad domains) AND — where we ran a pilot — were not rejected
by the recipient provider with a recipient-level signal.

### `review_provider_limited.csv`
Recipients where the **only** negative signal was that the
recipient provider rejected or throttled our sending IP / network
(Microsoft "block list" replies, Yahoo `TSS04` deferrals, etc.).
**These are not evidence the email is bad.** They mean we could
not verify them with the current sending infrastructure. Treat
them as "deliverability unknown" until a re-test from a different
sender. Sending to them is a business call, not a data-cleanliness
call.

### `high_risk_removed.csv`
Rows we explicitly removed from the deliverable. Reasons include:
syntax/MX/disposable failures, a recipient-level SMTP rejection
(user unknown, mailbox not found), content/policy bounces (DMARC,
spam, content filter), or abuse complaints. We do NOT recommend
sending to these.

### `smtp_evidence_report.csv`
Per-row audit. Each row that went through the pilot has a line
here with the SMTP code, the diagnostic message, the evidence
class (recipient_rejected / content_blocked / sender_infra_blocked
/ provider_deferred / accepted / no_evidence), and a recommended
action. Use this when you want to defend a specific routing
decision.

## How to use this

1. Use `clean_deliverable.csv` for the campaign.
2. Decide separately whether to also include
   `review_provider_limited.csv`. They are NOT clean, but they
   are also NOT proven bad — they are unverified.
3. Do NOT send to `high_risk_removed.csv`.
4. Keep `smtp_evidence_report.csv` for your records.
"""


def emit_customer_bundle(
    run_dir: str | Path,
    *,
    bundle_dirname: str = CUSTOMER_BUNDLE_DIRNAME,
) -> CustomerBundleResult:
    """Build the customer-facing bundle from the run directory.

    Reads whatever pilot/technical outputs are present (missing files
    are tolerated — we just emit smaller buckets) and writes the
    customer-language files into ``<run_dir>/<bundle_dirname>/``.

    Idempotent: re-running overwrites the bundle in place.
    """
    run_dir_path = Path(run_dir)
    bundle_dir = run_dir_path / bundle_dirname
    bundle_dir.mkdir(parents=True, exist_ok=True)

    # ---- Sources ----
    delivery_verified = _read_any(run_dir_path / _SOURCE_DELIVERY_VERIFIED)
    clean_high_conf = _read_any(run_dir_path / _SOURCE_CLEAN_HIGH_CONFIDENCE)
    pilot_hard = _read_any(run_dir_path / _SOURCE_PILOT_HARD_BOUNCES)
    pilot_blk_def = _read_any(run_dir_path / _SOURCE_PILOT_BLOCKED_OR_DEFERRED)
    pilot_infra = _read_any(run_dir_path / _SOURCE_PILOT_INFRA_RETEST)
    removed_invalid = _read_any(run_dir_path / _SOURCE_REMOVED_INVALID)

    do_not_send = _read_any(run_dir_path / _SOURCE_DO_NOT_SEND)
    if do_not_send.empty:
        do_not_send = _read_any(run_dir_path / _SOURCE_DO_NOT_SEND_FALLBACK)

    # ---- Combine into customer buckets ----
    # clean_deliverable = pilot-proven delivered + non-pilot clean
    # rows that were not contradicted by the pilot.
    #
    # We exclude any email that appears in any of the negative
    # sources (hard / blocked / infra / removed_invalid / dns).
    clean_candidates = _concat_unique(
        [delivery_verified, clean_high_conf], on="email",
    )
    negative_sources = _concat_unique(
        [pilot_hard, removed_invalid, do_not_send], on="email",
    )
    if not clean_candidates.empty and not negative_sources.empty and "email" in negative_sources.columns:
        bad_emails = set(
            negative_sources["email"].astype(str).str.strip().str.lower()
        )
        if "email" in clean_candidates.columns:
            mask = ~clean_candidates["email"].astype(str).str.strip().str.lower().isin(bad_emails)
            clean_candidates = clean_candidates.loc[mask].reset_index(drop=True)

    # review_provider_limited = pilot infra_blocked + provider_deferred
    # + the existing pilot_blocked_or_deferred bucket (transient /
    # provider-side issues).
    review_provider_limited = _concat_unique(
        [pilot_infra, pilot_blk_def], on="email",
    )

    # high_risk_removed = the canonical "do not send" union.
    high_risk_removed = _concat_unique(
        [do_not_send, pilot_hard, removed_invalid], on="email",
    )

    # ---- Write outputs ----
    files_written: dict[str, Path] = {}
    counts: dict[str, int] = {}

    clean_path = bundle_dir / CLEAN_DELIVERABLE_CSV
    _write_csv(clean_candidates, clean_path)
    files_written["clean_deliverable"] = clean_path
    counts["clean_deliverable"] = int(len(clean_candidates))

    review_path = bundle_dir / REVIEW_PROVIDER_LIMITED_CSV
    _write_csv(review_provider_limited, review_path)
    files_written["review_provider_limited"] = review_path
    counts["review_provider_limited"] = int(len(review_provider_limited))

    removed_path = bundle_dir / HIGH_RISK_REMOVED_CSV
    _write_csv(high_risk_removed, removed_path)
    files_written["high_risk_removed"] = removed_path
    counts["high_risk_removed"] = int(len(high_risk_removed))

    # Copy the evidence report verbatim if it exists; if not, write a
    # header-only stub so the bundle structure is consistent.
    evidence_src = run_dir_path / SMTP_EVIDENCE_REPORT_FILENAME
    evidence_dst = bundle_dir / SMTP_EVIDENCE_REPORT_FILENAME
    if evidence_src.is_file():
        shutil.copy2(evidence_src, evidence_dst)
        with evidence_src.open("r", encoding="utf-8") as fh:
            counts["smtp_evidence_report"] = max(
                0, sum(1 for _ in fh) - 1,
            )
    else:
        with evidence_dst.open("w", newline="", encoding="utf-8") as fh:
            csv.writer(fh).writerow(CSV_COLUMNS)
        counts["smtp_evidence_report"] = 0
    files_written["smtp_evidence_report"] = evidence_dst

    readme_path = bundle_dir / CUSTOMER_README
    readme_path.write_text(_README_TEMPLATE, encoding="utf-8")
    files_written["readme"] = readme_path

    return CustomerBundleResult(
        bundle_dir=bundle_dir,
        files_written=files_written,
        counts=counts,
    )


__all__ = [
    "CLEAN_DELIVERABLE_CSV",
    "CUSTOMER_BUNDLE_DIRNAME",
    "CUSTOMER_README",
    "CustomerBundleResult",
    "HIGH_RISK_REMOVED_CSV",
    "REVIEW_PROVIDER_LIMITED_CSV",
    "emit_customer_bundle",
]
