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
    ARTIFACT_AUDIENCE_INTERNAL_ONLY,
    get_artifact_audience,
    is_client_safe_artifact,
)


_REPORT_VERSION = "v2.9.6"
_DEFAULT_PACKAGE_SUBDIR = "client_delivery_package"
_MANIFEST_FILENAME = "client_package_manifest.json"

# Filenames whose row counts get surfaced in the manifest. The XLSX
# convention is set by ``app.client_output._write_xlsx`` — first sheet
# holds the email rows with a header.
_COUNT_FILES: dict[str, str] = {
    "safe_count": "valid_emails.xlsx",
    "review_count": "review_emails.xlsx",
    "rejected_count": "invalid_or_bounce_risk.xlsx",
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

    generated_at = _utc_now_iso()

    manifest = {
        "report_version": _REPORT_VERSION,
        "generated_at": generated_at,
        "source_run_dir": str(run_dir_path),
        "package_dir": str(package_dir),
        "files_included": [
            {
                "key": f.key,
                "filename": f.package_path.name,
                "audience": f.audience,
                "size_bytes": int(f.size_bytes),
            }
            for f in files_included
        ],
        "files_excluded": [dict(item) for item in files_excluded],
        "warnings": [{"code": w.code, "message": w.message} for w in warnings],
        "safe_count": counts["safe_count"],
        "review_count": counts["review_count"],
        "rejected_count": counts["rejected_count"],
    }
    manifest_path = package_dir / _MANIFEST_FILENAME
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    return ClientPackageResult(
        package_dir=package_dir,
        manifest_path=manifest_path,
        files_included=tuple(files_included),
        files_excluded=tuple(files_excluded),
        warnings=tuple(warnings),
        safe_count=counts["safe_count"],
        review_count=counts["review_count"],
        rejected_count=counts["rejected_count"],
        source_run_dir=run_dir_path,
        generated_at=generated_at,
    )


__all__ = [
    "ClientPackageFile",
    "ClientPackageResult",
    "ClientPackageWarning",
    "build_client_delivery_package",
]
