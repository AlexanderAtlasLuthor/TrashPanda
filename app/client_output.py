"""Phase 2 - Client Output Layer.

Converts the three technical materialized CSVs
(``clean_high_confidence.csv``, ``review_medium_confidence.csv``,
``removed_invalid.csv``) plus the ``processing_report.json`` into a
business-friendly deliverable set:

- ``valid_emails.xlsx``
- ``review_emails.xlsx``
- ``invalid_or_bounce_risk.xlsx``
- ``summary_report.xlsx``

This module is a pure export/add-on layer on top of the existing
pipeline outputs. It does **not** modify scoring, DNS, validation, or
any pipeline decision logic. Technical CSVs are preserved as-is.
"""

from __future__ import annotations

import json
import logging
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd


# ---------------------------------------------------------------------------
# Bucket / label configuration
# ---------------------------------------------------------------------------

# Internal (pipeline) bucket -> (client-facing label, output xlsx name,
# recommended_action, source CSV name, default client_reason fallback).
#
# V2.5 — the three legacy workbooks are now V2-driven:
#   * ``valid_emails.xlsx``           = V2 ``auto_approve`` only.
#   * ``review_emails.xlsx``          = V2 ``manual_review``.
#   * ``invalid_or_bounce_risk.xlsx`` = V2 ``auto_reject`` plus
#                                       duplicates and V1 hard fails
#                                       (legacy union; backward compat).
# The two new V2 semantic workbooks below split the union so customers
# can see duplicates and hard fails as their own cohorts.
BUCKET_CONFIG: dict[str, dict[str, str]] = {
    "clean_high_confidence": {
        "client_status": "valid",
        "xlsx_name": "valid_emails.xlsx",
        "csv_name": "clean_high_confidence.csv",
        "recommended_action": "Ready to use",
        "default_reason": "Valid email",
    },
    "review_medium_confidence": {
        "client_status": "review",
        "xlsx_name": "review_emails.xlsx",
        "csv_name": "review_medium_confidence.csv",
        "recommended_action": "Review before use",
        "default_reason": "Needs manual review",
    },
    "removed_invalid": {
        "client_status": "invalid_or_bounce_risk",
        "xlsx_name": "invalid_or_bounce_risk.xlsx",
        "csv_name": "removed_invalid.csv",
        "recommended_action": "Do not use",
        "default_reason": "Invalid email",
    },
}


# V2.5 supplementary workbooks. Each is a strict subset of
# ``removed_invalid.csv`` carved out for V2 semantics; the parent
# workbook still contains the union for legacy consumers.
SUPPLEMENTARY_BUCKET_CONFIG: dict[str, dict[str, str]] = {
    "removed_duplicates": {
        "client_status": "duplicate",
        "xlsx_name": "duplicate_emails.xlsx",
        "csv_name": "removed_duplicates.csv",
        "recommended_action": "Duplicate of another row",
        "default_reason": "Duplicate email",
    },
    "removed_hard_fail": {
        "client_status": "hard_fail",
        "xlsx_name": "hard_fail_emails.xlsx",
        "csv_name": "removed_hard_fail.csv",
        "recommended_action": "Do not use",
        "default_reason": "Failed structural validation",
    },
}


# V2.10.10 — Review-bucket subdivisions.
#
# ``review_emails.xlsx`` carries every ``manual_review`` row, but the
# 793-row WY_small audit showed the bucket mixes very different
# operator-action profiles (cold-start B2B that is rescatable vs.
# catch-all consumer that is not vs. high-risk domain that is
# effectively do-not-send). The pipeline already records a precise
# ``decision_reason`` per row; this config splits the same data into
# subset XLSXs the operator and customer can act on independently.
#
# Each subdivision is a strict subset of ``review_emails.xlsx`` —
# the parent workbook stays intact for legacy/manifest consumers and
# the subdivision XLSXs are emitted *in addition*. Subsets that would
# be empty are skipped silently.
#
# Keys are the file stems used in the package + artifact contract; the
# matching ``decision_reason`` values come from the canonical V2.4 /
# V2.6 vocabulary in :mod:`app.v2_decision_policy` and the SMTP/catch-
# all reason codes emitted there.
REVIEW_SUBDIVISIONS: dict[str, dict[str, Any]] = {
    "review_cold_start_b2b": {
        "xlsx_name": "review_cold_start_b2b.xlsx",
        "sheet_name": "review_cold_start_b2b",
        "decision_reasons": frozenset({"cold_start_no_smtp_valid"}),
        "operator_note": (
            "Unconfirmed B2B / unknown domains. Often rescatable with a "
            "second-pass / live SMTP retry from a warmed sender IP."
        ),
    },
    "review_smtp_inconclusive": {
        "xlsx_name": "review_smtp_inconclusive.xlsx",
        "sheet_name": "review_smtp_inconclusive",
        "decision_reasons": frozenset({
            "smtp_blocked",
            "smtp_timeout",
            "smtp_temp_fail",
            "smtp_error",
            "smtp_unconfirmed_for_candidate",
        }),
        "operator_note": (
            "MX records exist but SMTP did not return a confirmed verdict "
            "(blocked, timeout, transient failure, or dry-run). Retry "
            "with a different egress before sending."
        ),
    },
    "review_catch_all": {
        "xlsx_name": "review_catch_all.xlsx",
        "sheet_name": "review_catch_all",
        "decision_reasons": frozenset({
            "catch_all_possible",
            "catch_all_confirmed",
        }),
        "operator_note": (
            "Catch-all providers (often Yahoo / AOL / Verizon-class). "
            "Cannot be confirmed automatically without sending. Treat as "
            "spam-trap risk."
        ),
    },
    "review_medium_probability": {
        "xlsx_name": "review_medium_probability.xlsx",
        "sheet_name": "review_medium_probability",
        "decision_reasons": frozenset({"medium_probability"}),
        "operator_note": (
            "Probability between 0.50 and 0.80. Mixed signals — review "
            "case by case before sending."
        ),
    },
    "review_domain_high_risk": {
        "xlsx_name": "review_domain_high_risk.xlsx",
        "sheet_name": "review_domain_high_risk",
        "decision_reasons": frozenset({"domain_high_risk"}),
        "operator_note": (
            "Disposable, suspicious-shape, or otherwise high-risk "
            "domain. Effectively do-not-send unless you have separate "
            "evidence the address is real."
        ),
    },
}


# Client column profile: ordered list of (client_column_name, candidate
# source columns in priority order). Any candidate missing in the CSV
# is silently skipped. If none of the candidates exist, the column is
# omitted entirely from the client export.
CLIENT_COLUMN_PROFILE: list[tuple[str, tuple[str, ...]]] = [
    ("email", ("email",)),
    ("normalized_email", ("email_normalized",)),
    ("first_name", ("first_name", "fname", "firstname", "nombre", "given_name")),
    ("last_name", ("last_name", "lname", "lastname", "apellido", "surname", "family_name")),
    ("phone", ("phone", "phone_number", "telefono", "mobile", "cell")),
    ("company", ("company", "empresa", "organization", "org")),
    ("city", ("city", "ciudad")),
    ("state", ("state", "estado", "province", "region")),
]


# V2.5 — additional verification columns appended to every client
# workbook so review/risk consumers can see *why* a row landed where it
# did. Each tuple is ``(client_column_name, candidate_source_columns)``.
# Missing candidates are silently skipped, so workbooks generated from
# pre-V2 outputs (without these columns) still render.
V2_VERIFICATION_COLUMNS: list[tuple[str, tuple[str, ...]]] = [
    ("final_action", ("final_action",)),
    ("decision_reason", ("decision_reason",)),
    ("decision_confidence", ("decision_confidence",)),
    ("deliverability_probability", ("deliverability_probability",)),
    ("smtp_status", ("smtp_status",)),
    ("smtp_confirmed_valid", ("smtp_confirmed_valid",)),
    ("catch_all_status", ("catch_all_status",)),
    ("catch_all_flag", ("catch_all_flag",)),
    ("final_output_reason", ("final_output_reason",)),
]


# Patterns used to bucket rows in the summary breakdown by reason type.
# Matching is case-insensitive substring match against ``client_reason``.
REASON_PATTERNS: dict[str, tuple[str, ...]] = {
    "disposable": ("disposable", "temporary/disposable"),
    "placeholder_or_fake": ("placeholder", "fake"),
    "role_based": ("role-based", "role based"),
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_client_reason(row: pd.Series, default_reason: str) -> str:
    """Return a non-empty client_reason, falling back to the bucket default.

    Special case: if the technical ``final_output_reason`` marks the row
    as a removed duplicate, surface that explicitly to the client —
    otherwise duplicates show up with an empty client_reason.
    """
    final_reason = row.get("final_output_reason")
    if final_reason is not None and str(final_reason).strip() == "removed_duplicate":
        return "Duplicate email"

    value = row.get("client_reason")
    if value is None:
        return default_reason
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none"}:
        return default_reason
    return text


def _build_client_frame(
    df: pd.DataFrame,
    *,
    client_status: str,
    recommended_action: str,
    default_reason: str,
) -> pd.DataFrame:
    """Project the technical frame onto the client column profile.

    V2.5 — appends the V2 verification columns
    (:data:`V2_VERIFICATION_COLUMNS`) when present in the source frame
    so review/risk workbooks expose ``final_action``, ``decision_reason``,
    SMTP/catch-all status, etc., to the client. Missing columns are
    silently skipped so workbooks generated from pre-V2 outputs still
    render correctly.
    """
    out = pd.DataFrame(index=df.index)

    for client_col, candidates in CLIENT_COLUMN_PROFILE:
        for cand in candidates:
            if cand in df.columns:
                out[client_col] = df[cand]
                break

    out["status"] = client_status
    if "client_reason" in df.columns or "final_output_reason" in df.columns:
        out["client_reason"] = df.apply(
            lambda r: _resolve_client_reason(r, default_reason), axis=1
        )
    else:
        out["client_reason"] = default_reason
    out["recommended_action"] = recommended_action

    # V2.5 — append V2 verification columns (skipped if not present
    # upstream, so pre-V2 jobs still produce sensible workbooks).
    for client_col, candidates in V2_VERIFICATION_COLUMNS:
        if client_col in out.columns:
            continue
        for cand in candidates:
            if cand in df.columns:
                out[client_col] = df[cand]
                break

    return out


def _write_xlsx(df: pd.DataFrame, path: Path, sheet_name: str = "emails") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)


def _read_csv_safe(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


_ENCODING_FALLBACKS = ("utf-8-sig", "utf-8", "cp1252", "latin-1")


def _read_original_file(path: Path) -> pd.DataFrame:
    """Read the original client input file (CSV or XLSX) preserving all columns."""
    if path.suffix.lower() == ".xlsx":
        return pd.read_excel(path, dtype=str, keep_default_na=False)
    for enc in _ENCODING_FALLBACKS:
        try:
            return pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False, encoding=enc)
        except (UnicodeDecodeError, pd.errors.EmptyDataError):
            continue
    raise ValueError(f"Cannot decode original input file with any supported encoding: {path}")


def _count_reason(df: pd.DataFrame, patterns: tuple[str, ...]) -> int:
    if df.empty or "client_reason" not in df.columns:
        return 0
    col = df["client_reason"].astype(str).str.lower()
    mask = pd.Series(False, index=df.index)
    for pat in patterns:
        mask = mask | col.str.contains(pat.lower(), regex=False, na=False)
    return int(mask.sum())


# V2.5 — counts derived from V2 verification columns. Used by the
# summary report to surface SMTP / catch-all / unknown cohort sizes
# alongside the legacy bucket totals.

_TRUTHY_STRINGS: frozenset[str] = frozenset({"1", "true", "t", "yes", "y"})


def _count_truthy(df: pd.DataFrame, column: str) -> int:
    """Count rows where ``column`` evaluates to a truthy CSV value.

    CSV values are strings (or stringified ``True``/``False``); a
    truthy match is any of ``1 / true / yes / y / t``. Missing column
    or empty frame returns zero.
    """
    if df.empty or column not in df.columns:
        return 0
    col = df[column].astype(str).str.strip().str.lower()
    return int(col.isin(_TRUTHY_STRINGS).sum())


def _count_unverified(df: pd.DataFrame) -> int:
    """Count rows lacking confirmed-valid SMTP evidence.

    A row is *unverified* when its ``smtp_status`` is one of
    ``not_tested``, ``unknown``, ``error``, ``timeout``, ``temp_fail``,
    ``blocked`` (i.e. anything other than ``valid`` / ``invalid`` /
    ``catch_all_possible``). Missing column or empty frame → zero.
    """
    if df.empty or "smtp_status" not in df.columns:
        return 0
    col = df["smtp_status"].astype(str).str.strip().str.lower()
    unverified = {"not_tested", "unknown", "error", "timeout", "temp_fail", "blocked"}
    return int(col.isin(unverified).sum())


def _load_processing_report(run_dir: Path) -> dict[str, Any]:
    path = run_dir / "processing_report.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_client_outputs(
    run_dir: Path,
    logger: logging.Logger | None = None,
) -> dict[str, Path]:
    """Write the four client-facing XLSX deliverables.

    Reads the three technical CSVs materialized by the pipeline plus
    ``processing_report.json`` and emits:

    - ``valid_emails.xlsx``
    - ``review_emails.xlsx``
    - ``invalid_or_bounce_risk.xlsx``
    - ``summary_report.xlsx``

    Returns a mapping of logical name → output path for the files that
    were actually written. Never raises on missing optional inputs;
    errors are logged and the offending file is skipped.
    """
    run_dir = Path(run_dir)
    log = logger or logging.getLogger(__name__)
    written: dict[str, Path] = {}

    # Load each bucket into a client-shaped frame and write its XLSX.
    client_frames: dict[str, pd.DataFrame] = {}
    for bucket, cfg in BUCKET_CONFIG.items():
        csv_path = run_dir / cfg["csv_name"]
        raw = _read_csv_safe(csv_path)
        client_df = _build_client_frame(
            raw,
            client_status=cfg["client_status"],
            recommended_action=cfg["recommended_action"],
            default_reason=cfg["default_reason"],
        )
        client_frames[bucket] = client_df

        xlsx_path = run_dir / cfg["xlsx_name"]
        try:
            _write_xlsx(client_df, xlsx_path, sheet_name=cfg["client_status"])
            written[cfg["client_status"]] = xlsx_path
            log.info(
                "Client export written | bucket=%s rows=%s file=%s",
                bucket, len(client_df), xlsx_path.name,
            )
        except Exception as exc:  # pragma: no cover - defensive I/O guard
            log.warning("Failed to write %s: %s", xlsx_path, exc)

    # V2.10.10 — split the review bucket by ``decision_reason`` so the
    # operator-action profile is legible without re-reading the full
    # workbook. The parent ``review_emails.xlsx`` stays intact; these
    # subdivisions are emitted in addition.
    review_frame = client_frames.get("review_medium_confidence", pd.DataFrame())
    if not review_frame.empty and "decision_reason" in review_frame.columns:
        for sub_key, sub_cfg in REVIEW_SUBDIVISIONS.items():
            mask = review_frame["decision_reason"].astype(str).isin(
                sub_cfg["decision_reasons"]
            )
            sub_df = review_frame.loc[mask].copy()
            if sub_df.empty:
                continue
            xlsx_path = run_dir / sub_cfg["xlsx_name"]
            try:
                _write_xlsx(sub_df, xlsx_path, sheet_name=sub_cfg["sheet_name"])
                written[sub_key] = xlsx_path
                log.info(
                    "Review subdivision written | key=%s rows=%s file=%s",
                    sub_key, len(sub_df), xlsx_path.name,
                )
            except Exception as exc:  # pragma: no cover - defensive I/O guard
                log.warning("Failed to write %s: %s", xlsx_path, exc)

    # V2.5 — supplementary workbooks split the legacy ``invalid_or_bounce_risk``
    # cohort into duplicates and hard fails so customers can address each
    # cleanly. Failures are non-fatal: the legacy union workbook still
    # contains every removed row.
    for bucket, cfg in SUPPLEMENTARY_BUCKET_CONFIG.items():
        csv_path = run_dir / cfg["csv_name"]
        if not csv_path.is_file() or csv_path.stat().st_size == 0:
            continue
        raw = _read_csv_safe(csv_path)
        client_df = _build_client_frame(
            raw,
            client_status=cfg["client_status"],
            recommended_action=cfg["recommended_action"],
            default_reason=cfg["default_reason"],
        )
        client_frames[bucket] = client_df

        xlsx_path = run_dir / cfg["xlsx_name"]
        try:
            _write_xlsx(client_df, xlsx_path, sheet_name=cfg["client_status"])
            written[cfg["client_status"]] = xlsx_path
            log.info(
                "V2.5 client export written | bucket=%s rows=%s file=%s",
                bucket, len(client_df), xlsx_path.name,
            )
        except Exception as exc:  # pragma: no cover - defensive I/O guard
            log.warning("Failed to write %s: %s", xlsx_path, exc)

    # Summary report.
    try:
        summary_path = _write_summary_report(run_dir, client_frames)
        written["summary"] = summary_path
        log.info("Client summary written | file=%s", summary_path.name)
    except Exception as exc:  # pragma: no cover - defensive I/O guard
        log.warning("Failed to write summary_report.xlsx: %s", exc)

    return written


def generate_approved_original_format(
    run_dir: Path,
    input_paths: list[Path],
    logger: logging.Logger | None = None,
) -> Path | None:
    """Write approved_original_format.xlsx with approved rows in the original column layout.

    Reads source_file + source_row_number from clean_high_confidence.csv only,
    then cross-references the original input files to extract those exact rows
    with all original columns preserved.

    Only "Ready to send" (high_confidence) rows are included.
    Review and invalid rows are excluded.

    Returns the written path on success, None if skipped.
    Logs warnings and returns None on any error; never raises.
    """
    run_dir = Path(run_dir)
    log = logger or logging.getLogger(__name__)

    # --- Collect approved (source_file → set of source_row_numbers) ---
    # Only clean_high_confidence rows qualify as "approved / ready to send".
    approved: dict[str, set[int]] = {}
    for csv_name in ("clean_high_confidence.csv",):
        df = _read_csv_safe(run_dir / csv_name)
        if df.empty or "source_file" not in df.columns or "source_row_number" not in df.columns:
            continue
        for _, row in df.iterrows():
            src = str(row["source_file"]).strip()
            try:
                rn = int(row["source_row_number"])
            except (ValueError, TypeError):
                continue
            approved.setdefault(src, set()).add(rn)

    if not approved:
        log.warning("approved_original_format: no high_confidence rows found in pipeline outputs")
        return None

    # --- Build name → path lookup for the original input files ---
    path_by_name: dict[str, Path] = {p.name: p for p in input_paths}

    # --- Extract approved rows from each original file ---
    frames: list[pd.DataFrame] = []
    for src_name in sorted(approved):
        row_nums = approved[src_name]
        orig_path = path_by_name.get(src_name)
        if orig_path is None or not orig_path.is_file():
            log.warning(
                "approved_original_format: original input not found for source_file=%r", src_name
            )
            continue
        try:
            orig_df = _read_original_file(orig_path)
        except Exception as exc:
            log.warning("approved_original_format: cannot read %r: %s", src_name, exc)
            continue

        # source_row_number starts at 2 (row 1 = header, row 2 = first data row).
        # Translate to zero-based pandas iloc: pandas_index = source_row_number - 2.
        valid_indices = sorted(rn - 2 for rn in row_nums if 0 <= rn - 2 < len(orig_df))
        if not valid_indices:
            continue
        frames.append(orig_df.iloc[valid_indices].reset_index(drop=True))

    if not frames:
        log.warning(
            "approved_original_format: could not extract rows from any original input"
        )
        return None

    combined = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
    out_path = run_dir / "approved_original_format.xlsx"
    try:
        _write_xlsx(combined, out_path, sheet_name="approved")
        log.info(
            "approved_original_format written | rows=%s file=%s",
            len(combined), out_path.name,
        )
        return out_path
    except Exception as exc:
        log.warning("approved_original_format: failed to write XLSX: %s", exc)
        return None


def _write_summary_report(
    run_dir: Path,
    client_frames: dict[str, pd.DataFrame],
) -> Path:
    """Write ``summary_report.xlsx`` with totals + reason breakdown."""
    report = _load_processing_report(run_dir)

    valid_df = client_frames.get("clean_high_confidence", pd.DataFrame())
    review_df = client_frames.get("review_medium_confidence", pd.DataFrame())
    invalid_df = client_frames.get("removed_invalid", pd.DataFrame())

    total_valid = len(valid_df)
    total_review = len(review_df)
    total_invalid = len(invalid_df)

    total_input = int(
        report.get("total_rows_processed")
        or report.get("total_rows")
        or (total_valid + total_review + total_invalid)
    )
    duplicates_removed = int(
        report.get("total_duplicates_removed")
        or report.get("total_duplicate_rows")
        or 0
    )
    typo_corrections = int(report.get("total_typo_corrections") or 0)

    # Reason-based counts derived from the already-filtered client frames
    # so they always agree with the exported XLSX files.
    combined = pd.concat(
        [df for df in (valid_df, review_df, invalid_df) if not df.empty],
        ignore_index=True,
    ) if any(not df.empty for df in (valid_df, review_df, invalid_df)) else pd.DataFrame()

    disposable_count = _count_reason(invalid_df, REASON_PATTERNS["disposable"])
    placeholder_count = _count_reason(invalid_df, REASON_PATTERNS["placeholder_or_fake"])
    role_based_count = _count_reason(review_df, REASON_PATTERNS["role_based"])
    # Role-based may also end up in invalid in some configs; include both.
    role_based_count += _count_reason(invalid_df, REASON_PATTERNS["role_based"])

    # V2.5 — V2-aware counts derived from the materialized CSVs.
    duplicates_df = client_frames.get("removed_duplicates", pd.DataFrame())
    hard_fail_df = client_frames.get("removed_hard_fail", pd.DataFrame())
    safe_approved_count = total_valid  # auto_approve only.
    manual_review_count = total_review
    duplicate_count = len(duplicates_df)
    hard_fail_count = len(hard_fail_df)
    # Auto-reject is the legacy invalid bucket minus duplicates and
    # hard fails (which are now also surfaced separately).
    rejected_count = max(0, total_invalid - duplicate_count - hard_fail_count)
    smtp_verified_count = _count_truthy(valid_df, "smtp_confirmed_valid")
    catch_all_risk_count = _count_truthy(combined, "catch_all_flag") if not combined.empty else 0
    unknown_or_unverified_count = _count_unverified(combined)

    totals_rows = [
        ("total_input_rows", total_input),
        ("total_valid", total_valid),
        ("total_review", total_review),
        ("total_invalid_or_bounce_risk", total_invalid),
        ("duplicates_removed", duplicates_removed),
        ("typo_corrections", typo_corrections),
        ("disposable_emails", disposable_count),
        ("placeholder_or_fake_emails", placeholder_count),
        ("role_based_emails", role_based_count),
        # V2.5 — V2-semantic counts.
        ("safe_approved_count", safe_approved_count),
        ("manual_review_count", manual_review_count),
        ("rejected_count", rejected_count),
        ("duplicate_count", duplicate_count),
        ("hard_fail_count", hard_fail_count),
        ("smtp_verified_count", smtp_verified_count),
        ("catch_all_risk_count", catch_all_risk_count),
        ("unknown_or_unverified_count", unknown_or_unverified_count),
    ]
    totals_df = pd.DataFrame(totals_rows, columns=["metric", "value"])

    # Breakdown by (status, client_reason).
    if not combined.empty and "client_reason" in combined.columns:
        counter: Counter[tuple[str, str]] = Counter(
            zip(
                combined["status"].astype(str).tolist(),
                combined["client_reason"].astype(str).tolist(),
            )
        )
        breakdown_rows = [
            {"status": status, "client_reason": reason, "count": count}
            for (status, reason), count in sorted(counter.items())
        ]
    else:
        breakdown_rows = []
    breakdown_df = pd.DataFrame(
        breakdown_rows, columns=["status", "client_reason", "count"]
    )

    # V2.10.10 — breakdown by decision_reason.
    #
    # ``breakdown_by_reason`` (above) groups by the human-readable
    # ``client_reason`` text, which is useful for end-customers but
    # collapses across the V2 decision vocabulary. This third sheet
    # reuses the same combined frame and groups by ``final_action`` x
    # ``decision_reason`` so an operator can see at a glance how the
    # review bucket subdivides — and how many ``manual_review`` rows
    # came from cold-start vs catch-all vs medium-probability.
    if (
        not combined.empty
        and "decision_reason" in combined.columns
        and "final_action" in combined.columns
    ):
        decision_counter: Counter[tuple[str, str]] = Counter(
            zip(
                combined["final_action"].astype(str).tolist(),
                combined["decision_reason"].astype(str).tolist(),
            )
        )
        decision_breakdown_rows = [
            {"final_action": action, "decision_reason": reason, "count": count}
            for (action, reason), count in sorted(decision_counter.items())
        ]
    else:
        decision_breakdown_rows = []
    decision_breakdown_df = pd.DataFrame(
        decision_breakdown_rows,
        columns=["final_action", "decision_reason", "count"],
    )

    out_path = run_dir / "summary_report.xlsx"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        totals_df.to_excel(writer, sheet_name="totals", index=False)
        breakdown_df.to_excel(writer, sheet_name="breakdown_by_reason", index=False)
        decision_breakdown_df.to_excel(
            writer, sheet_name="breakdown_by_decision_reason", index=False
        )
    return out_path
