"""V2.9.6 — Client delivery package builder.

Assembles a clean, audience-filtered package directory from the
artifacts of a run directory. Filtering uses the V2.9.5 artifact
classification contract (``app.artifact_contract``) as the single
source of truth. Anything not explicitly ``client_safe`` is excluded —
including legacy reports that the operator results route still
exposes.

This module is delivery-side only. It does **not**:

* change V2 classification logic,
* change SMTP / catch-all / domain intelligence behaviour,
* change export routing or report generation,
* redesign server routes,
* implement an operator review gate,
* perform any network activity.

Outputs:

* a clean directory of ``client_safe`` files copied from the run dir,
* ``client_package_manifest.json`` describing what was included,
  excluded, and any warnings the builder raised.
"""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .artifact_contract import (
    ARTIFACT_AUDIENCE_CLIENT_SAFE,
    ARTIFACT_AUDIENCE_INTERNAL_ONLY,
    get_artifact_audience,
    is_client_safe_artifact,
    is_safe_only_artifact,
)
from .atomic_io import atomic_write_json


_REPORT_VERSION = "v2.9.6"
_DEFAULT_PACKAGE_SUBDIR = "client_delivery_package"
_MANIFEST_FILENAME = "client_package_manifest.json"

# V2.10.8.2 — safe-only partial delivery anchor file. Generated only
# when the run has both safe rows and at least one
# review/rejected row, so the future safe-only download endpoint has
# a self-describing artifact to ship alongside the safe XLSXs.
_SAFE_ONLY_NOTE_FILENAME = "SAFE_ONLY_DELIVERY_NOTE.txt"
_SAFE_ONLY_NOTE_KEY = "safe_only_delivery_note"

# Always-on README that tells the customer which file to use first.
# Generalises the older safe-only-only note: every package now ships
# a one-page client guide regardless of safe-only status.
_README_FILENAME = "README_CLIENT.txt"
_README_KEY = "client_readme"

# Preferred PRIMARY artifact, in order of preference. The first one
# present in ``files_included`` wins. ``approved_original_format``
# preserves the customer's input columns and is therefore the most
# useful "single deliverable"; ``valid_emails`` falls back when the
# original-format export was suppressed.
_PRIMARY_KEY_PREFERENCES: tuple[str, ...] = (
    "approved_original_format",
    "valid_emails",
)


_README_BODY_TEMPLATE = """\
TrashPanda — Client delivery package
====================================

USE THIS FILE FIRST:
  {primary_filename}

It contains the rows we are willing to recommend for immediate use.
Each row carries explanatory columns so you can see *why* each
address survived the pipeline.

Other files in this package
---------------------------
  valid_emails.xlsx
      Verified addresses with explanatory columns
      (final_action, smtp_status, deliverability_probability,
      catch_all_flag, recommended_action, …).

  review_emails.xlsx
      Addresses that need a manual review before sending. Often
      catch-all providers (Yahoo / AOL / Verizon-class) where no
      automated system can confirm deliverability without sending.

  invalid_or_bounce_risk.xlsx
      Addresses we recommend NOT sending to (hard syntax failures,
      MX failures, duplicates, history-flagged risky domains).

  duplicate_emails.xlsx, hard_fail_emails.xlsx
      Subsets of invalid_or_bounce_risk.xlsx broken out by reason.

  approved_original_format.xlsx
      The valid rows in the same column layout as your original
      upload, ready to drop straight back into your campaign tool.

  summary_report.xlsx
      Counts and category breakdown for this run.

  SAFE_ONLY_DELIVERY_NOTE.txt
      Only present for partial-delivery runs. Read it before sending.

If you saw a high bounce rate on a previous delivery, also ask for
the "Extra Strict Offline" XLSX — same data, more aggressive filter
that separates Yahoo/AOL automatically.
"""


def _resolve_primary_key(files_included: list["ClientPackageFile"]) -> str | None:
    keys_present = {f.key for f in files_included}
    for preferred in _PRIMARY_KEY_PREFERENCES:
        if preferred in keys_present:
            return preferred
    return None

# Filenames whose row counts get surfaced in the manifest. The XLSX
# convention is set by ``app.client_output._write_xlsx`` — first sheet
# holds the email rows with a header.
_COUNT_FILES: dict[str, str] = {
    "safe_count": "valid_emails.xlsx",
    "review_count": "review_emails.xlsx",
    "rejected_count": "invalid_or_bounce_risk.xlsx",
}

# V2.10.10 — per-subdivision row counts surfaced under the manifest's
# ``review_breakdown`` block. Each entry is a strict subset of
# ``review_emails.xlsx``; a subdivision file may legitimately be
# absent (zero rows of that decision_reason) — the resolver returns
# ``None`` for missing files, which the manifest serializes as ``null``.
_REVIEW_BREAKDOWN_FILES: dict[str, str] = {
    "review_cold_start_b2b": "review_cold_start_b2b.xlsx",
    "review_smtp_inconclusive": "review_smtp_inconclusive.xlsx",
    "review_catch_all": "review_catch_all.xlsx",
    "review_medium_probability": "review_medium_probability.xlsx",
    "review_domain_high_risk": "review_domain_high_risk.xlsx",
}


# V2.10.10.b — action-oriented review classification (the rescue
# view: "what should I do with this row"). Counts feed the manifest's
# ``review_action_breakdown`` block. ``second_pass_candidates`` is the
# rolled-up union of ``low_risk`` + ``timeout_retry`` and is surfaced
# alongside the per-action counts. Order mirrors the UI's render
# order — rescatability descending.
_REVIEW_ACTION_BREAKDOWN_FILES: dict[str, str] = {
    "review_low_risk": "review_low_risk.xlsx",
    "review_timeout_retry": "review_timeout_retry.xlsx",
    "review_catch_all_consumer": "review_catch_all_consumer.xlsx",
    "review_high_risk": "review_high_risk.xlsx",
    "do_not_send": "do_not_send.xlsx",
    "second_pass_candidates": "second_pass_candidates.xlsx",
}


# --------------------------------------------------------------------------- #
# Result model
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class ClientPackageFile:
    """One file copied into the client delivery package."""

    key: str
    source_path: Path
    package_path: Path
    audience: str
    size_bytes: int


@dataclass(frozen=True)
class ClientPackageWarning:
    """Non-fatal issue raised while building the package."""

    code: str
    message: str


@dataclass(frozen=True)
class ClientPackageResult:
    """Return value of :func:`build_client_delivery_package`."""

    package_dir: Path
    manifest_path: Path
    files_included: tuple[ClientPackageFile, ...]
    files_excluded: tuple[dict[str, str], ...]
    warnings: tuple[ClientPackageWarning, ...]
    safe_count: int | None
    review_count: int | None
    rejected_count: int | None
    review_breakdown: dict[str, int | None] = field(default_factory=dict)
    review_action_breakdown: dict[str, int | None] = field(default_factory=dict)
    source_run_dir: Path = field(default_factory=lambda: Path("."))
    generated_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-friendly dict (paths as strings, no dataclasses)."""
        return {
            "report_version": _REPORT_VERSION,
            "generated_at": self.generated_at,
            "source_run_dir": str(self.source_run_dir),
            "package_dir": str(self.package_dir),
            "manifest_path": str(self.manifest_path),
            "files_included": [
                {
                    "key": f.key,
                    "filename": f.package_path.name,
                    "source_path": str(f.source_path),
                    "package_path": str(f.package_path),
                    "audience": f.audience,
                    "size_bytes": int(f.size_bytes),
                }
                for f in self.files_included
            ],
            "files_excluded": [dict(item) for item in self.files_excluded],
            "warnings": [
                {"code": w.code, "message": w.message} for w in self.warnings
            ],
            "safe_count": self.safe_count,
            "review_count": self.review_count,
            "rejected_count": self.rejected_count,
            "review_breakdown": dict(self.review_breakdown),
            "review_action_breakdown": dict(self.review_action_breakdown),
        }


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _key_for_filename(filename: str) -> str:
    """Return the artifact key implied by ``filename`` (stem without extension)."""
    last = filename.rsplit("/", 1)[-1]
    if "." in last:
        return last.rsplit(".", 1)[0]
    return last


def _try_count_rows(path: Path) -> tuple[int | None, ClientPackageWarning | None]:
    """Best-effort row count for an XLSX file. Never raises."""
    if not path.is_file():
        return None, None
    try:
        # Local import keeps this module importable without pandas in
        # contexts where it's not needed (e.g. light-weight tooling).
        import pandas as pd  # type: ignore

        df = pd.read_excel(path, sheet_name=0)
        return int(len(df.index)), None
    except Exception as exc:  # pragma: no cover - defensive guard
        return None, ClientPackageWarning(
            code="count_unavailable",
            message=f"Failed to read row count from {path.name}: {exc!s}",
        )


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# --------------------------------------------------------------------------- #
# Builder
# --------------------------------------------------------------------------- #


def build_client_delivery_package(
    run_dir: str | Path,
    output_dir: str | Path | None = None,
) -> ClientPackageResult:
    """Build a client delivery package from ``run_dir``.

    Parameters
    ----------
    run_dir:
        Directory containing the pipeline run artifacts.
    output_dir:
        Optional override for the package directory. Defaults to
        ``<run_dir>/client_delivery_package``.

    Returns
    -------
    ClientPackageResult
        Structured report of what was packaged. Always returns; never
        raises on per-file failures (those become warnings).
    """
    run_dir_path = Path(run_dir).resolve()
    if output_dir is None:
        package_dir = (run_dir_path / _DEFAULT_PACKAGE_SUBDIR).resolve()
    else:
        package_dir = Path(output_dir).resolve()
    package_dir.mkdir(parents=True, exist_ok=True)

    files_included: list[ClientPackageFile] = []
    files_excluded: list[dict[str, str]] = []
    warnings: list[ClientPackageWarning] = []

    # Walk top-level entries in run_dir. Subdirectories are not
    # client-safe and are recorded as excluded without recursion.
    for entry in sorted(run_dir_path.iterdir(), key=lambda p: p.name):
        # Never include the package dir itself.
        try:
            if entry.resolve() == package_dir:
                continue
        except OSError:  # pragma: no cover - resolve race
            continue

        if entry.is_dir():
            files_excluded.append(
                {
                    "key": "",
                    "filename": f"{entry.name}/",
                    "audience": ARTIFACT_AUDIENCE_INTERNAL_ONLY,
                    "reason": "not_client_safe",
                }
            )
            continue

        filename = entry.name
        # Skip the manifest if it already exists from a previous build.
        if filename == _MANIFEST_FILENAME:
            continue

        # Source of truth: the V2.9.5 artifact contract.
        if is_client_safe_artifact(filename):
            key = _key_for_filename(filename)
            audience = get_artifact_audience(filename)
            dest = package_dir / filename
            try:
                shutil.copy2(entry, dest)
                size = dest.stat().st_size
            except Exception as exc:  # pragma: no cover - defensive
                warnings.append(
                    ClientPackageWarning(
                        code="copy_failed",
                        message=f"Failed to copy {filename} into package: {exc!s}",
                    )
                )
                continue
            files_included.append(
                ClientPackageFile(
                    key=key,
                    source_path=entry,
                    package_path=dest,
                    audience=audience,
                    size_bytes=int(size),
                )
            )
        else:
            files_excluded.append(
                {
                    "key": _key_for_filename(filename),
                    "filename": filename,
                    "audience": get_artifact_audience(filename),
                    "reason": "not_client_safe",
                }
            )

    # Approved original format presence warning.
    has_approved_original = any(
        f.key == "approved_original_format" for f in files_included
    )
    if not has_approved_original:
        warnings.append(
            ClientPackageWarning(
                code="approved_original_format_absent",
                message=(
                    "approved_original_format.xlsx was not present; "
                    "package includes available client-safe outputs only."
                ),
            )
        )

    # Counts read from the package copies (best-effort).
    counts: dict[str, int | None] = {}
    for count_key, filename in _COUNT_FILES.items():
        count, warn = _try_count_rows(package_dir / filename)
        counts[count_key] = count
        if warn is not None:
            warnings.append(warn)

    # V2.10.10 — review-bucket subdivision row counts. A missing file
    # (zero rows for that ``decision_reason``) returns ``None`` so the
    # manifest serializes a ``null`` rather than a misleading zero.
    review_breakdown_counts: dict[str, int | None] = {}
    for sub_key, filename in _REVIEW_BREAKDOWN_FILES.items():
        sub_count, sub_warn = _try_count_rows(package_dir / filename)
        review_breakdown_counts[sub_key] = sub_count
        if sub_warn is not None:
            warnings.append(sub_warn)

    # V2.10.10.b — action-oriented review classification counts.
    review_action_counts: dict[str, int | None] = {}
    for action_key, filename in _REVIEW_ACTION_BREAKDOWN_FILES.items():
        action_count, action_warn = _try_count_rows(package_dir / filename)
        review_action_counts[action_key] = action_count
        if action_warn is not None:
            warnings.append(action_warn)

    # ---- V2.10.8.2: safe-only partial delivery note --------------------- #
    # Only synthesize the note when partial delivery actually applies:
    # at least one safe row, and at least one row that the full client
    # package would carry but a safe-only delivery must drop.
    safe_count_int = int(counts["safe_count"] or 0)
    review_count_int = int(counts["review_count"] or 0)
    rejected_count_int = int(counts["rejected_count"] or 0)
    partial_applies = safe_count_int > 0 and (
        review_count_int > 0 or rejected_count_int > 0
    )

    note_path = package_dir / _SAFE_ONLY_NOTE_FILENAME
    # Keep on-disk state in sync with the manifest contract: a stale
    # note from a previous build must not survive a rebuild whose
    # counts no longer satisfy the partial-applies rule.
    if note_path.exists() and not partial_applies:
        try:
            note_path.unlink()
        except OSError:  # pragma: no cover - defensive
            pass

    if partial_applies:
        note_body = (
            "This is a safe-only partial delivery package.\n"
            "\n"
            "The full run is NOT ready_for_client.\n"
            "Only SMTP-confirmed safe rows are included.\n"
            "Review, catch-all, inconclusive, rejected, technical, debug, "
            "and internal artifacts are excluded.\n"
            "\n"
            f"safe_count: {safe_count_int}\n"
            f"review_count: {review_count_int}\n"
            f"rejected_count: {rejected_count_int}\n"
            "delivery_mode: safe_only_partial\n"
        )
        note_path.write_text(note_body, encoding="utf-8")
        files_included.append(
            ClientPackageFile(
                key=_SAFE_ONLY_NOTE_KEY,
                source_path=note_path,
                package_path=note_path,
                audience=ARTIFACT_AUDIENCE_CLIENT_SAFE,
                size_bytes=int(note_path.stat().st_size),
            )
        )

    # Safe-only delivery manifest block. ``files_included`` here is a
    # *strict subset* of the main files_included filtered by the
    # safe-only allowlist (see app.artifact_contract). Counts are
    # always the actual run counts, regardless of supported state.
    safe_only_files = [
        {
            "key": f.key,
            "filename": f.package_path.name,
            "audience": f.audience,
            "size_bytes": int(f.size_bytes),
        }
        for f in files_included
        if is_safe_only_artifact(f.package_path.name)
    ]
    safe_only_block: dict[str, Any] = {
        "supported": bool(partial_applies),
        "note_filename": _SAFE_ONLY_NOTE_FILENAME if partial_applies else None,
        "files_included": safe_only_files if partial_applies else [],
        "safe_count": safe_count_int,
        "review_count": review_count_int,
        "rejected_count": rejected_count_int,
    }

    # ---- Always-on README and PRIMARY artifact pointer ------------------
    primary_key = _resolve_primary_key(files_included)
    primary_filename: str | None = None
    if primary_key is not None:
        for f in files_included:
            if f.key == primary_key:
                primary_filename = f.package_path.name
                break

    readme_path = package_dir / _README_FILENAME
    readme_body = _README_BODY_TEMPLATE.format(
        primary_filename=primary_filename or "valid_emails.xlsx"
    )
    try:
        readme_path.write_text(readme_body, encoding="utf-8")
        files_included.append(
            ClientPackageFile(
                key=_README_KEY,
                source_path=readme_path,
                package_path=readme_path,
                audience=ARTIFACT_AUDIENCE_CLIENT_SAFE,
                size_bytes=int(readme_path.stat().st_size),
            )
        )
    except OSError as exc:  # pragma: no cover - defensive guard
        warnings.append(
            ClientPackageWarning(
                code="readme_write_failed",
                message=f"Failed to write {_README_FILENAME}: {exc!s}",
            )
        )

    primary_block: dict[str, Any] = {
        "key": primary_key,
        "filename": primary_filename,
        "label": "Recommended download",
        "reason": (
            "approved_original_format preserves the customer's columns"
            if primary_key == "approved_original_format"
            else "valid_emails carries the explanatory trashpanda_* columns"
            if primary_key == "valid_emails"
            else None
        ),
    }

    generated_at = _utc_now_iso()

    manifest = {
        "report_version": _REPORT_VERSION,
        "generated_at": generated_at,
        "source_run_dir": str(run_dir_path),
        "package_dir": str(package_dir),
        "primary_artifact": primary_block,
        "readme_filename": _README_FILENAME,
        "files_included": [
            {
                "key": f.key,
                "filename": f.package_path.name,
                "audience": f.audience,
                "size_bytes": int(f.size_bytes),
                "primary": (f.key == primary_key) if primary_key else False,
            }
            for f in files_included
        ],
        "files_excluded": [dict(item) for item in files_excluded],
        "warnings": [{"code": w.code, "message": w.message} for w in warnings],
        "safe_count": counts["safe_count"],
        "review_count": counts["review_count"],
        "rejected_count": counts["rejected_count"],
        "review_breakdown": review_breakdown_counts,
        "review_action_breakdown": review_action_counts,
        "safe_only_delivery": safe_only_block,
    }
    manifest_path = package_dir / _MANIFEST_FILENAME
    atomic_write_json(manifest_path, manifest)

    return ClientPackageResult(
        package_dir=package_dir,
        manifest_path=manifest_path,
        files_included=tuple(files_included),
        files_excluded=tuple(files_excluded),
        warnings=tuple(warnings),
        safe_count=counts["safe_count"],
        review_count=counts["review_count"],
        rejected_count=counts["rejected_count"],
        review_breakdown=dict(review_breakdown_counts),
        review_action_breakdown=dict(review_action_counts),
        source_run_dir=run_dir_path,
        generated_at=generated_at,
    )


__all__ = [
    "ClientPackageFile",
    "ClientPackageResult",
    "ClientPackageWarning",
    "build_client_delivery_package",
]
