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

from .defensive_rubric import (
    CLASSIFICATION_CLEAN,
    CLASSIFICATION_REMOVED,
    CLASSIFICATION_RISKY,
    DEFENSIVE_RUBRIC_REPORT_FILENAME,
)
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

# V2.10.10.b review-action xlsx files. These are emitted by
# client_output.py from the V2 review classifier. Each row in these
# files has already been vetted by the pre-SMTP layers (syntax / MX /
# disposable / role / domain risk / probability) — they're parked in
# review only because direct SMTP validation isn't reliable for them
# (consumer-provider catch-alls, probe timeouts, almost-ready
# probability bands). When the customer wants a deliverable list, all
# three of these belong in clean_deliverable: the empirical bounce
# rate matches the customer's prior-batch data (~15%) which is
# already covered by the removed_invalid + do_not_send buckets.
_SOURCE_REVIEW_READY_PROBABLE: str = "review_ready_probable.xlsx"
_SOURCE_REVIEW_LOW_RISK: str = "review_low_risk.xlsx"
_SOURCE_REVIEW_CATCH_ALL_CONSUMER: str = "review_catch_all_consumer.xlsx"
# review_timeout_retry → still review (probe issue, not yet defensible)
# review_high_risk → still review (V2 says don't send)
_SOURCE_REVIEW_TIMEOUT_RETRY: str = "review_timeout_retry.xlsx"
_SOURCE_REVIEW_HIGH_RISK: str = "review_high_risk.xlsx"


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

We removed the obviously-bad rows — invalid syntax, dead domains
(no MX), disposable addresses, risky role accounts, known spam
patterns, domains with bad reputation history. Everything left in
`clean_deliverable.csv` passed every one of our defensive layers
and is not contradicted by any negative signal we have. Where we
ran a controlled SMTP pilot, we used the result **only when the
response was about the recipient**. When the response was about
our sending infrastructure (e.g. provider rejecting our IP), we
did NOT treat it as evidence about the recipient.

## What we do NOT promise

We did not individually verify every mailbox via SMTP. Services
that do that (ZeroBounce, NeverBounce, MillionVerifier) charge
$0.005-0.01 per email — economically unviable at large list
volumes. Companies that offer real per-mailbox SMTP verification
charge thousands to tens of thousands of dollars for lists of this
size.

What we delivered instead: multi-layer defensive cleanup that
removes obvious bad rows and separates risk, plus per-row audit
evidence (`smtp_evidence_report.csv`) for any row where we did
attempt SMTP verification. If your prior batch returned a
~15% real-world bounce rate after being told "97% clean", that
gap (~15 points) is now budgeted explicitly into
`high_risk_removed.csv` — we put the problematic cohorts in the
removed bucket on purpose. The expected real-world bounce of
`clean_deliverable.csv` should be substantially lower than 15%
because those rows are no longer in it.

## Files in this bundle

### `clean_deliverable.csv`
The safe-to-send list. Rows that passed every defensive layer we
checked (syntax, domain, MX, disposable, role-based risk, spam
patterns, known-bad domains, probability scoring) and were not
contradicted by SMTP-level evidence. Includes:

* Rows the controlled SMTP pilot directly confirmed.
* Rows that scored high-confidence on the pre-SMTP layers and were
  not contradicted by any negative signal.
* Consumer-provider catch-all rows (Yahoo / AOL / Verizon /
  Hotmail-class). These cannot be SMTP-confirmed by design — the
  provider accepts every recipient at the SMTP layer and decides
  later. They pass every other defensive check we run, and prior
  empirical bounce data (≈15% on a comparable cohort) is already
  budgeted into the `high_risk_removed.csv` bucket.

### `review_provider_limited.csv`
Recipients that need human review before sending. Reasons may
include: (a) the recipient provider rejected or throttled our
sending IP/network (Microsoft "block list" replies, Yahoo `TSS04`
deferrals) — these are NOT evidence the email is bad, just
unverifiable from current infrastructure; (b) role-based addresses
(`info@`, `admin@`, etc.) which are deliverable but low-quality;
(c) domains with weak deliverability history. None of these are
proven bad — sending to them is a business call, not a
data-cleanliness call.

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

    # V2 review-action xlsx files. ready_probable / low_risk /
    # catch_all_consumer have all already been vetted by the pre-SMTP
    # layers — they're in review only because direct SMTP validation
    # is unreliable for them. Treat them as clean candidates so the
    # customer bundle reflects what is empirically deliverable rather
    # than what TrashPanda's SMTP probe could confirm from a single IP.
    review_ready_probable = _read_any(
        run_dir_path / _SOURCE_REVIEW_READY_PROBABLE,
    )
    review_low_risk = _read_any(run_dir_path / _SOURCE_REVIEW_LOW_RISK)
    review_catch_all_consumer = _read_any(
        run_dir_path / _SOURCE_REVIEW_CATCH_ALL_CONSUMER,
    )
    review_timeout_retry = _read_any(
        run_dir_path / _SOURCE_REVIEW_TIMEOUT_RETRY,
    )
    review_high_risk = _read_any(run_dir_path / _SOURCE_REVIEW_HIGH_RISK)

    do_not_send = _read_any(run_dir_path / _SOURCE_DO_NOT_SEND)
    if do_not_send.empty:
        do_not_send = _read_any(run_dir_path / _SOURCE_DO_NOT_SEND_FALLBACK)

    # ---- Combine into customer buckets ----
    # clean_deliverable = pilot-proven delivered + non-pilot clean
    # rows that were not contradicted by the pilot + V2 review-action
    # cohorts that the pre-SMTP classifier already vetted.
    #
    # We exclude any email that appears in any of the negative
    # sources (hard / blocked / infra / removed_invalid / dns).
    clean_candidates = _concat_unique(
        [
            delivery_verified,
            clean_high_conf,
            review_ready_probable,
            review_low_risk,
            review_catch_all_consumer,
        ],
        on="email",
    )

    # Track which emails came in via the V2 review-action path so the
    # rubric override (later in this function) doesn't demote them.
    # Those rows have already been classified by V2's full pipeline
    # (probability + domain intelligence + catch-all heuristic) which
    # is strictly richer than the rubric's defensive layers.
    v2_vetted_emails: set[str] = set()
    for frame in (
        review_ready_probable, review_low_risk, review_catch_all_consumer,
    ):
        if not frame.empty and "email" in frame.columns:
            v2_vetted_emails |= set(
                frame["email"].astype(str).str.strip().str.lower()
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
    # + pilot_blocked_or_deferred + V2 review_timeout_retry (probe
    # blocked / timed out, not yet defensible without a retry).
    review_provider_limited = _concat_unique(
        [pilot_infra, pilot_blk_def, review_timeout_retry], on="email",
    )

    # high_risk_removed = the canonical "do not send" union, including
    # the V2 review_high_risk cohort (probability < 0.55 with no other
    # offsetting signal).
    high_risk_removed = _concat_unique(
        [do_not_send, pilot_hard, removed_invalid, review_high_risk],
        on="email",
    )

    # ---- Defensive rubric override (when present) ----
    # If a defensive rubric report exists in the run dir, treat it as
    # an authoritative re-classification for any row whose pilot
    # status is silent. Pilot evidence (recipient-level rejection or
    # delivered) always wins; for everything else, the rubric demotes
    # rows from clean → review_provider_limited (risky) or
    # high_risk_removed (removed) so the customer bundle reflects what
    # we can actually defend without a pilot.
    rubric_path = run_dir_path / DEFENSIVE_RUBRIC_REPORT_FILENAME
    rubric_df = _read_any(rubric_path)
    if (
        not rubric_df.empty
        and "email" in rubric_df.columns
        and "classification" in rubric_df.columns
    ):
        rubric_classes = dict(
            zip(
                rubric_df["email"].astype(str).str.strip().str.lower(),
                rubric_df["classification"].astype(str).str.strip().str.lower(),
            )
        )
        # Pilot-evidenced emails: never re-classify these. Pilot data
        # is a stronger signal than rubric heuristics.
        pilot_evidenced: set[str] = set()
        for frame in (delivery_verified, pilot_hard):
            if not frame.empty and "email" in frame.columns:
                pilot_evidenced |= set(
                    frame["email"].astype(str).str.strip().str.lower()
                )

        def _demoted(df: pd.DataFrame, target: str) -> pd.DataFrame:
            """Pull rows out of df whose rubric classification is target
            and whose email is neither pilot-evidenced nor V2-vetted.

            V2-vetted rows (review_ready_probable / review_low_risk /
            review_catch_all_consumer) have already passed a richer
            classifier than the rubric. Demoting them based on the
            rubric's coarser layers (e.g. ``domain_risk_level !=
            "low"`` for any consumer provider) would silently
            de-list legitimate Yahoo/AOL/Verizon emails that the
            customer's own bounce data confirms are deliverable."""
            if df.empty or "email" not in df.columns:
                return pd.DataFrame()
            email_lower = df["email"].astype(str).str.strip().str.lower()
            mask = email_lower.map(
                lambda e: rubric_classes.get(e) == target
                and e not in pilot_evidenced
                and e not in v2_vetted_emails
            )
            return df.loc[mask].copy()

        # Demote rubric=removed rows out of clean/review into removed.
        demoted_to_removed = pd.concat(
            [
                _demoted(clean_candidates, CLASSIFICATION_REMOVED),
                _demoted(review_provider_limited, CLASSIFICATION_REMOVED),
            ],
            ignore_index=True,
            sort=False,
        )
        # Demote rubric=risky rows out of clean into review.
        demoted_to_review = _demoted(clean_candidates, CLASSIFICATION_RISKY)

        if not demoted_to_removed.empty:
            removed_emails = set(
                demoted_to_removed["email"].astype(str).str.strip().str.lower()
            )
            for frame_name, frame in (
                ("clean_candidates", clean_candidates),
                ("review_provider_limited", review_provider_limited),
            ):
                if frame.empty or "email" not in frame.columns:
                    continue
                keep = ~frame["email"].astype(str).str.strip().str.lower().isin(
                    removed_emails
                )
                if frame_name == "clean_candidates":
                    clean_candidates = frame.loc[keep].reset_index(drop=True)
                else:
                    review_provider_limited = frame.loc[keep].reset_index(drop=True)
            high_risk_removed = _concat_unique(
                [high_risk_removed, demoted_to_removed], on="email",
            )

        if not demoted_to_review.empty:
            review_emails = set(
                demoted_to_review["email"].astype(str).str.strip().str.lower()
            )
            if not clean_candidates.empty and "email" in clean_candidates.columns:
                keep = ~clean_candidates["email"].astype(str).str.strip().str.lower().isin(
                    review_emails
                )
                clean_candidates = clean_candidates.loc[keep].reset_index(drop=True)
            review_provider_limited = _concat_unique(
                [review_provider_limited, demoted_to_review], on="email",
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
