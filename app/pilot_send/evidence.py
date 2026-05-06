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
domain                            Lowercased domain.
provider_family                   yahoo_family / microsoft_family / corporate_unknown / ...
pilot_verdict                     The internal ``dsn_status`` (e.g. ``hard_bounce``).
evidence_class                    The honest classification — see EVIDENCE_* constants below.
actionable_for_customer           ``true`` iff the evidence is about the recipient (safe to act
                                  on). ``false`` for sender-side rejections and "no evidence".
smtp_code                         Raw SMTP / DSN status code.
smtp_reason                       Diagnostic text (truncated to 300 chars).
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
# Evidence taxonomy
# ---------------------------------------------------------------------------

# Recipient-level evidence: the destination MX explicitly addressed
# the recipient. Safe for the customer to act on.
EVIDENCE_RECIPIENT_REJECTED: str = "recipient_rejected"
EVIDENCE_RECIPIENT_ACCEPTED: str = "recipient_accepted"
EVIDENCE_TRANSIENT_SOFT: str = "transient_soft"
EVIDENCE_COMPLAINT: str = "complaint"

# Content / policy evidence: still a recipient-side rejection but the
# reason is content/policy (DMARC, spam, content filter), not the
# mailbox per se. Marked actionable because the message would not
# arrive to that recipient as-sent.
EVIDENCE_CONTENT_BLOCKED: str = "content_blocked"

# Sender-side evidence: the rejection describes our IP / network /
# reputation, not the recipient. NOT actionable for the customer.
EVIDENCE_SENDER_INFRA_BLOCKED: str = "sender_infra_blocked"
EVIDENCE_SENDER_PROVIDER_DEFERRED: str = "sender_provider_deferred"

# No evidence yet (pending / sent without DSN / expired without
# DSN / unparseable). Operator should not draw a conclusion.
EVIDENCE_NO_EVIDENCE: str = "no_evidence"


# Whether each evidence class can be acted on by the customer (i.e.
# is this row evidence about the recipient?).
_ACTIONABLE: frozenset[str] = frozenset({
    EVIDENCE_RECIPIENT_REJECTED,
    EVIDENCE_RECIPIENT_ACCEPTED,
    EVIDENCE_CONTENT_BLOCKED,
    EVIDENCE_COMPLAINT,
})


_VERDICT_TO_EVIDENCE: dict[str, str] = {
    VERDICT_DELIVERED: EVIDENCE_RECIPIENT_ACCEPTED,
    VERDICT_HARD_BOUNCE: EVIDENCE_RECIPIENT_REJECTED,
    VERDICT_SOFT_BOUNCE: EVIDENCE_TRANSIENT_SOFT,
    VERDICT_DEFERRED: EVIDENCE_TRANSIENT_SOFT,
    VERDICT_BLOCKED: EVIDENCE_CONTENT_BLOCKED,
    VERDICT_COMPLAINT: EVIDENCE_COMPLAINT,
    VERDICT_INFRA_BLOCKED: EVIDENCE_SENDER_INFRA_BLOCKED,
    VERDICT_PROVIDER_DEFERRED: EVIDENCE_SENDER_PROVIDER_DEFERRED,
    VERDICT_UNKNOWN: EVIDENCE_NO_EVIDENCE,
}


_RECOMMENDED_ACTION: dict[str, str] = {
    EVIDENCE_RECIPIENT_ACCEPTED: "deliver",
    EVIDENCE_RECIPIENT_REJECTED: "remove from list",
    EVIDENCE_CONTENT_BLOCKED: "remove from list (content/policy)",
    EVIDENCE_COMPLAINT: "remove from list (abuse complaint)",
    EVIDENCE_TRANSIENT_SOFT: "retry later",
    EVIDENCE_SENDER_INFRA_BLOCKED: (
        "do not act on recipient — re-test from clean sender IP"
    ),
    EVIDENCE_SENDER_PROVIDER_DEFERRED: (
        "do not act on recipient — provider throttled our sender"
    ),
    EVIDENCE_NO_EVIDENCE: "leave in review until verdict arrives",
}


@dataclass(frozen=True, slots=True)
class EvidenceRow:
    email: str
    domain: str
    provider_family: str
    pilot_verdict: str
    evidence_class: str
    actionable_for_customer: bool
    smtp_code: str
    smtp_reason: str
    recommended_action: str


def _classify_row(row: PilotRow) -> EvidenceRow:
    verdict = row.dsn_status or ""
    evidence_class = _VERDICT_TO_EVIDENCE.get(verdict, EVIDENCE_NO_EVIDENCE)
    if not verdict:
        # Pending / sent / expired without DSN — leave as no_evidence.
        evidence_class = EVIDENCE_NO_EVIDENCE
    return EvidenceRow(
        email=row.email,
        domain=row.domain,
        provider_family=row.provider_family or "corporate_unknown",
        pilot_verdict=verdict or row.state or "",
        evidence_class=evidence_class,
        actionable_for_customer=evidence_class in _ACTIONABLE,
        smtp_code=row.dsn_smtp_code or "",
        smtp_reason=(row.dsn_diagnostic or "")[:300],
        recommended_action=_RECOMMENDED_ACTION.get(evidence_class, ""),
    )


CSV_COLUMNS: tuple[str, ...] = (
    "email",
    "domain",
    "provider_family",
    "pilot_verdict",
    "evidence_class",
    "actionable_for_customer",
    "smtp_code",
    "smtp_reason",
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
                ev.domain,
                ev.provider_family,
                ev.pilot_verdict,
                ev.evidence_class,
                "true" if ev.actionable_for_customer else "false",
                ev.smtp_code,
                ev.smtp_reason,
                ev.recommended_action,
            ])
            written += 1
    return written


__all__ = [
    "CSV_COLUMNS",
    "EVIDENCE_COMPLAINT",
    "EVIDENCE_CONTENT_BLOCKED",
    "EVIDENCE_NO_EVIDENCE",
    "EVIDENCE_RECIPIENT_ACCEPTED",
    "EVIDENCE_RECIPIENT_REJECTED",
    "EVIDENCE_SENDER_INFRA_BLOCKED",
    "EVIDENCE_SENDER_PROVIDER_DEFERRED",
    "EVIDENCE_TRANSIENT_SOFT",
    "EvidenceRow",
    "SMTP_EVIDENCE_REPORT_FILENAME",
    "write_smtp_evidence_report",
]
