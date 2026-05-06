"""V2.10.13 — Per-row SMTP evidence report.

The pilot emits several xlsx buckets (delivered / hard_bounce /
blocked / infra_blocked / provider_deferred / ...). Each tells the
operator *what bucket* a row landed in, but none of them shows the
*evidence class* — i.e. is this evidence about the recipient, about
the sender, or about nothing at all?

This module produces ``smtp_evidence_report.csv``, a flat per-row
audit file that the operator (and the customer, if they ask) can
read to defend each routing decision. Schema:

================================  ==============================================================
column                            meaning
================================  ==============================================================
email                             The address that was probed.
bucket                            The internal ``dsn_status`` / pilot bucket
                                  (e.g. ``hard_bounce``, ``infrastructure_blocked``).
smtp_code                         Raw SMTP / DSN status code.
smtp_reason                       Diagnostic text (truncated to 300 chars).
evidence_class                    One of: ``rcpt_refused`` | ``infra_blocked`` |
                                  ``provider_deferred`` | ``accepted`` | ``no_evidence``.
domain                            Lowercased domain (extra column, not in core spec).
provider_family                   Provider family (extra column, not in core spec).
actionable_for_customer           ``true`` iff the evidence is about the recipient (safe to act
                                  on). ``false`` for sender-side rejections and "no evidence".
recommended_action                Short string the operator can read at a glance.
================================  ==============================================================

The file is always written next to the existing pilot xlsx outputs.
Empty pilot → empty report (header only).
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

from ..db.pilot_send_tracker import (
    PilotRow,
    VERDICT_BLOCKED,
    VERDICT_COMPLAINT,
    VERDICT_DEFERRED,
    VERDICT_DELIVERED,
    VERDICT_HARD_BOUNCE,
    VERDICT_INFRA_BLOCKED,
    VERDICT_PROVIDER_DEFERRED,
    VERDICT_SOFT_BOUNCE,
    VERDICT_UNKNOWN,
)


SMTP_EVIDENCE_REPORT_FILENAME: str = "smtp_evidence_report.csv"


# ---------------------------------------------------------------------------
# Evidence taxonomy — exactly the 5 classes in the customer-facing spec.
# ---------------------------------------------------------------------------

EVIDENCE_RCPT_REFUSED: str = "rcpt_refused"
EVIDENCE_ACCEPTED: str = "accepted"
EVIDENCE_INFRA_BLOCKED: str = "infra_blocked"
EVIDENCE_PROVIDER_DEFERRED: str = "provider_deferred"
EVIDENCE_NO_EVIDENCE: str = "no_evidence"

ALL_EVIDENCE_CLASSES: tuple[str, ...] = (
    EVIDENCE_RCPT_REFUSED,
    EVIDENCE_ACCEPTED,
    EVIDENCE_INFRA_BLOCKED,
    EVIDENCE_PROVIDER_DEFERRED,
    EVIDENCE_NO_EVIDENCE,
)


# Whether each evidence class can be acted on by the customer (i.e.
# is this row evidence about the recipient?). Sender-side classes
# (infra_blocked / provider_deferred) and no_evidence are NOT
# actionable.
_ACTIONABLE: frozenset[str] = frozenset({
    EVIDENCE_RCPT_REFUSED,
    EVIDENCE_ACCEPTED,
})


# Map the 8 internal pilot verdicts onto the 5 spec evidence classes.
# Rationale for each collapse:
#   * blocked (content/policy) → rcpt_refused: the message will not
#     reach this recipient as-sent; functionally a refusal of this
#     recipient for this message. The bucket column preserves the
#     finer "blocked" detail for anyone who needs it.
#   * complaint                 → rcpt_refused: terminal recipient
#     signal — must not send to them again.
#   * soft_bounce / deferred    → no_evidence: transient signals are
#     not evidence either way.
_VERDICT_TO_EVIDENCE: dict[str, str] = {
    VERDICT_DELIVERED: EVIDENCE_ACCEPTED,
    VERDICT_HARD_BOUNCE: EVIDENCE_RCPT_REFUSED,
    VERDICT_BLOCKED: EVIDENCE_RCPT_REFUSED,
    VERDICT_COMPLAINT: EVIDENCE_RCPT_REFUSED,
    VERDICT_SOFT_BOUNCE: EVIDENCE_NO_EVIDENCE,
    VERDICT_DEFERRED: EVIDENCE_NO_EVIDENCE,
    VERDICT_INFRA_BLOCKED: EVIDENCE_INFRA_BLOCKED,
    VERDICT_PROVIDER_DEFERRED: EVIDENCE_PROVIDER_DEFERRED,
    VERDICT_UNKNOWN: EVIDENCE_NO_EVIDENCE,
}


_RECOMMENDED_ACTION: dict[str, str] = {
    EVIDENCE_ACCEPTED: "deliver",
    EVIDENCE_RCPT_REFUSED: "remove from list",
    EVIDENCE_INFRA_BLOCKED: (
        "do not act on recipient — re-test from clean sender IP"
    ),
    EVIDENCE_PROVIDER_DEFERRED: (
        "do not act on recipient — provider throttled our sender"
    ),
    EVIDENCE_NO_EVIDENCE: "leave in review until verdict arrives",
}


@dataclass(frozen=True, slots=True)
class EvidenceRow:
    email: str
    bucket: str
    smtp_code: str
    smtp_reason: str
    evidence_class: str
    domain: str
    provider_family: str
    actionable_for_customer: bool
    recommended_action: str


def _classify_row(row: PilotRow) -> EvidenceRow:
    verdict = row.dsn_status or ""
    evidence_class = _VERDICT_TO_EVIDENCE.get(verdict, EVIDENCE_NO_EVIDENCE)
    if not verdict:
        # Pending / sent / expired without DSN — leave as no_evidence.
        evidence_class = EVIDENCE_NO_EVIDENCE
    return EvidenceRow(
        email=row.email,
        bucket=verdict or row.state or "",
        smtp_code=row.dsn_smtp_code or "",
        smtp_reason=(row.dsn_diagnostic or "")[:300],
        evidence_class=evidence_class,
        domain=row.domain,
        provider_family=row.provider_family or "corporate_unknown",
        actionable_for_customer=evidence_class in _ACTIONABLE,
        recommended_action=_RECOMMENDED_ACTION.get(evidence_class, ""),
    )


# Column order matches the customer-facing spec
# (``email | bucket | smtp_code | smtp_reason | evidence_class``)
# first, then operational extras.
CSV_COLUMNS: tuple[str, ...] = (
    "email",
    "bucket",
    "smtp_code",
    "smtp_reason",
    "evidence_class",
    "domain",
    "provider_family",
    "actionable_for_customer",
    "recommended_action",
)


def write_smtp_evidence_report(
    rows: list[PilotRow],
    *,
    path: Path,
) -> int:
    """Write ``smtp_evidence_report.csv`` to ``path``. Returns count
    of rows written (excluding header). Always writes the file, even
    when ``rows`` is empty (header-only file makes auditing easy)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(CSV_COLUMNS)
        for pilot_row in rows:
            ev = _classify_row(pilot_row)
            writer.writerow([
                ev.email,
                ev.bucket,
                ev.smtp_code,
                ev.smtp_reason,
                ev.evidence_class,
                ev.domain,
                ev.provider_family,
                "true" if ev.actionable_for_customer else "false",
                ev.recommended_action,
            ])
            written += 1
    return written


__all__ = [
    "ALL_EVIDENCE_CLASSES",
    "CSV_COLUMNS",
    "EVIDENCE_ACCEPTED",
    "EVIDENCE_INFRA_BLOCKED",
    "EVIDENCE_NO_EVIDENCE",
    "EVIDENCE_PROVIDER_DEFERRED",
    "EVIDENCE_RCPT_REFUSED",
    "EvidenceRow",
    "SMTP_EVIDENCE_REPORT_FILENAME",
    "write_smtp_evidence_report",
]
