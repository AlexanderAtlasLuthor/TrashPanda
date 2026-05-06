"""V2.10.14 — Pre-SMTP defensive rubric.

The pipeline already runs all the pre-SMTP layers (syntax, MX,
disposable, role, domain risk) but its routing decisions intermix
those signals with SMTP and probability scoring. For sub-budget
cleanups (no SMTP pilot, or SMTP unavailable) we want a simpler,
explicit per-layer pass/fail rubric so each routing decision is
defensible to the customer in plain language.

This module is a **post-processor**. It reads stage-produced columns
on rows already materialized in the technical CSVs and emits:

* Per-row ``RubricRow`` with each layer's pass/fail + an overall
  classification: ``clean`` / ``risky`` / ``removed``.
* ``defensive_rubric_report.csv`` next to the technical outputs:
  one row per email, columns per layer + classification + reason.
* A grouping helper that returns ``{email -> RubricRow}`` so the
  customer bundle can route by classification without having to
  re-read the rubric.

Layers and policy (single source of truth — change here):

  syntax       fail → removed
  mx           fail → removed
  disposable   fail → removed
  role         fail → risky (role accounts deliver but are low-quality)
  domain_risk  high  → risky
               low   → pass
               medium / unknown / cold_start → risky

Anything else → clean.

The rubric does NOT consult ``smtp_*`` columns. It is the honest
"what we can defend without a pilot" view.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass, asdict
from pathlib import Path

import pandas as pd


DEFENSIVE_RUBRIC_REPORT_FILENAME: str = "defensive_rubric_report.csv"


# Classifications.
CLASSIFICATION_CLEAN: str = "clean"
CLASSIFICATION_RISKY: str = "risky"
CLASSIFICATION_REMOVED: str = "removed"

ALL_CLASSIFICATIONS: tuple[str, ...] = (
    CLASSIFICATION_CLEAN,
    CLASSIFICATION_RISKY,
    CLASSIFICATION_REMOVED,
)


# Layer names — keep in sync with column emission order.
LAYER_SYNTAX: str = "syntax"
LAYER_MX: str = "mx"
LAYER_DISPOSABLE: str = "disposable"
LAYER_ROLE: str = "role"
LAYER_DOMAIN_RISK: str = "domain_risk"

ALL_LAYERS: tuple[str, ...] = (
    LAYER_SYNTAX,
    LAYER_MX,
    LAYER_DISPOSABLE,
    LAYER_ROLE,
    LAYER_DOMAIN_RISK,
)


# Stage-produced columns we read. Ground truth confirmed against:
#   app/engine/stages/email_processing.py  (syntax_valid)
#   app/engine/stages/enrichment.py        (has_mx_record, score_reasons,
#                                           client_reason)
#   app/engine/stages/domain_intelligence.py  (domain_risk_level)
_COLUMN_SYNTAX: str = "syntax_valid"
_COLUMN_MX: str = "has_mx_record"
_COLUMN_SCORE_REASONS: str = "score_reasons"
_COLUMN_CLIENT_REASON: str = "client_reason"
_COLUMN_DOMAIN_RISK: str = "domain_risk_level"


@dataclass(frozen=True, slots=True)
class RubricRow:
    email: str
    syntax_pass: bool
    mx_pass: bool
    disposable_pass: bool   # True == not disposable
    role_pass: bool         # True == not a role account
    domain_risk_pass: bool  # True == low risk
    classification: str
    reason: str             # human-readable summary

    def to_csv_dict(self) -> dict[str, str]:
        return {
            "email": self.email,
            f"{LAYER_SYNTAX}_pass": _bool_str(self.syntax_pass),
            f"{LAYER_MX}_pass": _bool_str(self.mx_pass),
            f"{LAYER_DISPOSABLE}_pass": _bool_str(self.disposable_pass),
            f"{LAYER_ROLE}_pass": _bool_str(self.role_pass),
            f"{LAYER_DOMAIN_RISK}_pass": _bool_str(self.domain_risk_pass),
            "classification": self.classification,
            "reason": self.reason,
        }


CSV_COLUMNS: tuple[str, ...] = (
    "email",
    f"{LAYER_SYNTAX}_pass",
    f"{LAYER_MX}_pass",
    f"{LAYER_DISPOSABLE}_pass",
    f"{LAYER_ROLE}_pass",
    f"{LAYER_DOMAIN_RISK}_pass",
    "classification",
    "reason",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _bool_str(value: bool) -> str:
    return "true" if value else "false"


def _coerce_bool(raw: object) -> bool:
    """Pipeline outputs round-trip through CSV which stringifies bools."""
    if isinstance(raw, bool):
        return raw
    if raw is None:
        return False
    s = str(raw).strip().lower()
    return s in {"1", "true", "t", "yes", "y"}


def _has_token(value: object, token: str) -> bool:
    """``score_reasons`` is a pipe-separated string. Match a token
    case-insensitively and exactly (no substring false-positives)."""
    if value is None:
        return False
    s = str(value).strip().lower()
    if not s:
        return False
    parts = {p.strip() for p in s.replace(",", "|").split("|") if p.strip()}
    return token.lower() in parts


# ---------------------------------------------------------------------------
# Per-row classification
# ---------------------------------------------------------------------------


def classify_row(row: dict) -> RubricRow:
    """Apply the rubric to one materialized pipeline row.

    ``row`` is the dict-of-strings shape that pandas produces when
    reading the technical CSVs back. All inputs are coerced safely;
    missing columns count as a fail (conservative)."""
    email = str(row.get("email") or "").strip()

    # Syntax / MX are direct booleans.
    syntax_pass = _coerce_bool(row.get(_COLUMN_SYNTAX))
    mx_pass = _coerce_bool(row.get(_COLUMN_MX))

    # Disposable: tokenised in score_reasons or named in client_reason.
    score_reasons = row.get(_COLUMN_SCORE_REASONS)
    client_reason = str(row.get(_COLUMN_CLIENT_REASON) or "").lower()
    disposable_flag = (
        _has_token(score_reasons, "disposable")
        or "disposable" in client_reason
        or "temporary" in client_reason  # "Temporary/disposable email"
    )
    disposable_pass = not disposable_flag

    # Role: tokenised in score_reasons.
    role_flag = _has_token(score_reasons, "role_account")
    role_pass = not role_flag

    # Domain risk: only ``low`` is an unconditional pass. Treat
    # ``unknown`` / blank as risky (we don't know → don't promise).
    risk_level = str(row.get(_COLUMN_DOMAIN_RISK) or "").strip().lower()
    domain_risk_pass = risk_level == "low"

    # Classify. Order matters — removal trumps risky.
    failures: list[str] = []
    if not syntax_pass:
        failures.append("syntax")
    if not mx_pass:
        failures.append("no_mx")
    if disposable_flag:
        failures.append("disposable")

    if failures:
        classification = CLASSIFICATION_REMOVED
        reason = "removed: " + ", ".join(failures)
    else:
        risky: list[str] = []
        if role_flag:
            risky.append("role_account")
        if not domain_risk_pass:
            risky.append(f"domain_risk={risk_level or 'unknown'}")
        if risky:
            classification = CLASSIFICATION_RISKY
            reason = "risky: " + ", ".join(risky)
        else:
            classification = CLASSIFICATION_CLEAN
            reason = "all defensive layers pass"

    return RubricRow(
        email=email,
        syntax_pass=syntax_pass,
        mx_pass=mx_pass,
        disposable_pass=disposable_pass,
        role_pass=role_pass,
        domain_risk_pass=domain_risk_pass,
        classification=classification,
        reason=reason,
    )


# ---------------------------------------------------------------------------
# Whole-run classification
# ---------------------------------------------------------------------------


_TECHNICAL_CSVS: tuple[str, ...] = (
    "clean_high_confidence.csv",
    "review_medium_confidence.csv",
    "removed_invalid.csv",
)


def _read_csv_safely(path: Path) -> pd.DataFrame:
    if not path.is_file() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False)
    except (pd.errors.EmptyDataError, ValueError):
        return pd.DataFrame()


def classify_run(run_dir: str | Path) -> dict[str, RubricRow]:
    """Read the technical CSVs in ``run_dir`` and classify every row.

    Returns ``{email_lower -> RubricRow}``. If the same email appears in
    multiple technical CSVs (shouldn't happen in normal pipeline runs,
    but happens during re-runs), the first occurrence wins."""
    run_path = Path(run_dir)
    out: dict[str, RubricRow] = {}
    for filename in _TECHNICAL_CSVS:
        df = _read_csv_safely(run_path / filename)
        if df.empty:
            continue
        for record in df.to_dict("records"):
            email = str(record.get("email") or "").strip().lower()
            if not email or email in out:
                continue
            out[email] = classify_row(record)
    return out


def write_rubric_report(
    rubric: dict[str, RubricRow],
    *,
    path: Path,
) -> int:
    """Write ``defensive_rubric_report.csv`` to ``path``.

    Always writes the file (header-only when empty). Returns count of
    rows written (excluding header)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(CSV_COLUMNS))
        writer.writeheader()
        for row in rubric.values():
            writer.writerow(row.to_csv_dict())
            written += 1
    return written


def emit_rubric(run_dir: str | Path) -> tuple[Path, dict[str, RubricRow]]:
    """End-to-end: classify the run and write the report. Returns
    ``(report_path, rubric_dict)`` for downstream consumers (e.g. the
    customer bundle)."""
    rubric = classify_run(run_dir)
    path = Path(run_dir) / DEFENSIVE_RUBRIC_REPORT_FILENAME
    write_rubric_report(rubric, path=path)
    return path, rubric


__all__ = [
    "ALL_CLASSIFICATIONS",
    "ALL_LAYERS",
    "CLASSIFICATION_CLEAN",
    "CLASSIFICATION_REMOVED",
    "CLASSIFICATION_RISKY",
    "CSV_COLUMNS",
    "DEFENSIVE_RUBRIC_REPORT_FILENAME",
    "LAYER_DISPOSABLE",
    "LAYER_DOMAIN_RISK",
    "LAYER_MX",
    "LAYER_ROLE",
    "LAYER_SYNTAX",
    "RubricRow",
    "classify_row",
    "classify_run",
    "emit_rubric",
    "write_rubric_report",
]
