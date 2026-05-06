"""V2.10.12 — Pilot batch candidate selector.

Reads the action-oriented review XLSX files emitted by
:mod:`app.client_output` (V2.10.10.b / V2.10.11) and picks rows
that are appropriate to send a real message to. The selector is
intentionally narrow:

* ``ready_probable`` — Tier 2, just below auto_approve. Top
  candidate.
* ``review_low_risk`` — high-probability cold-start B2B. Strong
  candidate.
* ``review_timeout_retry`` — operationally inconclusive. The pilot
  send is essentially a "real" retry from a different egress.
* ``review_catch_all_consumer`` — Yahoo / AOL / Verizon-class.
  Pilot send is the only way to know.

Rows in ``review_high_risk`` and ``do_not_send`` are NEVER
selected — there's already evidence not to send to them.

The selector is pure: takes a run directory, returns a list of
candidates without touching the tracker. The launch endpoint
calls the selector, then writes each selected row into the
tracker.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


# Action keys that are eligible for a pilot send. Order is by
# rescatability descending — the selector fills the batch from the
# top of the priority list before falling through.
_ELIGIBLE_ACTIONS: tuple[str, ...] = (
    "ready_probable",
    "low_risk",
    "timeout_retry",
    "catch_all_consumer",
)

# Filename per action, mirroring ``app.client_output.REVIEW_ACTION_FILES``.
_ACTION_FILES: dict[str, str] = {
    "ready_probable": "review_ready_probable.xlsx",
    "low_risk": "review_low_risk.xlsx",
    "timeout_retry": "review_timeout_retry.xlsx",
    "catch_all_consumer": "review_catch_all_consumer.xlsx",
}


@dataclass(frozen=True, slots=True)
class PilotCandidate:
    source_row: int
    email: str
    domain: str
    provider_family: str
    action: str
    deliverability_probability: float


def _read_action_file(run_dir: Path, filename: str) -> pd.DataFrame:
    path = run_dir / filename
    if not path.is_file():
        return pd.DataFrame()
    try:
        return pd.read_excel(path, sheet_name=0, dtype=str)
    except Exception:
        return pd.DataFrame()


def _domain_for(email: str) -> str:
    if not email or "@" not in email:
        return ""
    return email.rsplit("@", 1)[-1].strip().lower()


def _coerce_int(value: object, *, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_float(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def select_candidates(
    run_dir: str | Path,
    *,
    batch_size: int,
    actions: tuple[str, ...] = _ELIGIBLE_ACTIONS,
) -> list[PilotCandidate]:
    """Return up to ``batch_size`` candidates from the run's action files.

    Reads each eligible action file in priority order and accumulates
    rows until the batch is full. Rows missing an email or domain are
    silently dropped — the launch endpoint validates that the batch
    is non-empty before committing.

    Pure: never raises, never opens the tracker, never sends. The
    caller persists the selection.
    """
    run_dir_path = Path(run_dir)
    if batch_size <= 0:
        return []

    out: list[PilotCandidate] = []
    seen_emails: set[str] = set()
    for action in actions:
        if len(out) >= batch_size:
            break
        filename = _ACTION_FILES.get(action)
        if filename is None:
            continue
        df = _read_action_file(run_dir_path, filename)
        if df.empty:
            continue
        for _, row in df.iterrows():
            if len(out) >= batch_size:
                break
            email = str(row.get("email") or "").strip().lower()
            if not email or "@" not in email:
                continue
            if email in seen_emails:
                continue
            seen_emails.add(email)
            domain = _domain_for(email)
            out.append(
                PilotCandidate(
                    source_row=_coerce_int(row.get("source_row_number"), default=0),
                    email=email,
                    domain=domain,
                    provider_family=str(
                        row.get("provider_family") or "corporate_unknown"
                    ).lower(),
                    action=action,
                    deliverability_probability=_coerce_float(
                        row.get("deliverability_probability")
                    ),
                )
            )
    return out


__all__ = [
    "PilotCandidate",
    "select_candidates",
]
