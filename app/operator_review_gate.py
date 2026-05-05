"""V2.9.7 — Operator review gate.

Evaluates whether a built client delivery package is safe to mark as
ready for delivery. Combines:

* the V2.9.6 client package (filesystem + manifest),
* V2.9.4 artifact consistency metadata,
* V2.9.3 SMTP runtime summary,
* V2.8 deliverability summary,

and produces an operator-facing readiness decision plus structured
issues. Writes ``operator_review_summary.json`` into the run directory.

This module is decision/reporting only. It does **not**:

* change V2 classification logic,
* change SMTP / catch-all / domain intelligence behaviour,
* change export routing or report generation,
* change client package filtering,
* perform any network activity.

The summary file is classified ``operator_only`` by the V2.9.5
artifact contract, so the V2.9.6 client package builder excludes it.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .artifact_contract import is_client_safe_artifact


# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #


_REPORT_VERSION = "v2.9.7"
_DEFAULT_PACKAGE_SUBDIR = "client_delivery_package"
_PACKAGE_MANIFEST_FILENAME = "client_package_manifest.json"
_OPERATOR_SUMMARY_FILENAME = "operator_review_summary.json"
_ARTIFACT_CONSISTENCY_FILENAME = "artifact_consistency.json"
_SMTP_RUNTIME_FILENAME = "smtp_runtime_summary.json"
_V2_SUMMARY_FILENAME = "v2_deliverability_summary.json"

SMTP_COVERAGE_THRESHOLD: float = 0.80

SEVERITY_BLOCK = "block"
SEVERITY_WARN = "warn"

STATUS_READY = "ready"
STATUS_WARN = "warn"
STATUS_BLOCK = "block"


# --------------------------------------------------------------------------- #
# Result models
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class OperatorReviewIssue:
    """One observation raised during the review gate evaluation."""

    severity: str  # "warn" | "block"
    code: str
    message: str


@dataclass(frozen=True)
class OperatorReviewResult:
    """Structured review-gate decision for a run."""

    ready_for_client: bool
    status: str  # "ready" | "warn" | "block"
    run_dir: Path
    package_dir: Path | None
    package_manifest_path: Path | None
    summary_path: Path
    generated_at: str
    issues: tuple[OperatorReviewIssue, ...]
    safe_count: int | None
    review_count: int | None
    rejected_count: int | None
    duplicate_count: int | None
    hard_fail_count: int | None
    smtp_coverage_rate: float | None
    smtp_valid_count: int | None
    smtp_inconclusive_count: int | None
    catch_all_risk_count: int | None
    high_risk_domain_count: int | None
    cold_start_count: int | None
    approved_original_present: bool

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-friendly dict (paths as strings, no dataclasses)."""
        return {
            "report_version": _REPORT_VERSION,
            "generated_at": self.generated_at,
            "ready_for_client": self.ready_for_client,
            "status": self.status,
            "run_dir": str(self.run_dir),
            "package_dir": str(self.package_dir) if self.package_dir else None,
            "package_manifest_path": (
                str(self.package_manifest_path)
                if self.package_manifest_path
                else None
            ),
            "summary_path": str(self.summary_path),
            "issues": [
                {"severity": i.severity, "code": i.code, "message": i.message}
                for i in self.issues
            ],
            "safe_count": self.safe_count,
            "review_count": self.review_count,
            "rejected_count": self.rejected_count,
            "duplicate_count": self.duplicate_count,
            "hard_fail_count": self.hard_fail_count,
            "smtp_coverage_rate": self.smtp_coverage_rate,
            "smtp_valid_count": self.smtp_valid_count,
            "smtp_inconclusive_count": self.smtp_inconclusive_count,
            "catch_all_risk_count": self.catch_all_risk_count,
            "high_risk_domain_count": self.high_risk_domain_count,
            "cold_start_count": self.cold_start_count,
            "approved_original_present": self.approved_original_present,
        }


# --------------------------------------------------------------------------- #
# JSON helpers (defensive)
# --------------------------------------------------------------------------- #


def _read_json(path: Path) -> dict[str, Any] | None:
    """Return ``None`` for missing/unreadable JSON, never raises."""
    try:
        text = path.read_text(encoding="utf-8")
        return json.loads(text)
    except Exception:
        return None


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _v2_metric(
    data: dict[str, Any],
    flat_key: str,
    *nested_path: str,
) -> int | None:
    """Read a V2 metric, preferring flat top-level then nested path.

    The V2.8 ``v2_deliverability_summary.json`` shape produced by
    :class:`app.v2_reporting.V2DeliverabilityReport` is nested
    (``catch_all_summary.catch_all_risk_count``,
    ``domain_intelligence_summary.high_risk_domain_count``, etc.).
    The original V2.9.7 spec describes flat top-level keys, so we
    accept either: flat first (matches the spec / simple test
    fixtures), then walk the nested path for real V2.8 reports.
    """
    val: Any = data.get(flat_key) if isinstance(data, dict) else None
    if val is None and nested_path:
        cur: Any = data
        for key in nested_path:
            if not isinstance(cur, dict):
                cur = None
                break
            cur = cur.get(key)
        val = cur
    return _coerce_int(val)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# --------------------------------------------------------------------------- #
# Gate
# --------------------------------------------------------------------------- #


def run_operator_review_gate(
    run_dir: str | Path,
    *,
    package_dir: str | Path | None = None,
) -> OperatorReviewResult:
    """Evaluate the operator review gate for ``run_dir``.

    Parameters
    ----------
    run_dir:
        Directory containing the pipeline run artifacts. The
        ``operator_review_summary.json`` file is written here.
    package_dir:
        Optional override for the client package directory. Defaults
        to ``<run_dir>/client_delivery_package``.

    Returns
    -------
    OperatorReviewResult
        Structured decision. Always returns; does not raise on
        per-file failures (those become block/warn issues).
    """
    run_dir_path = Path(run_dir).resolve()
    if package_dir is None:
        pkg_dir = (run_dir_path / _DEFAULT_PACKAGE_SUBDIR).resolve()
    else:
        pkg_dir = Path(package_dir).resolve()

    issues: list[OperatorReviewIssue] = []

    # Initial metric values; populated as we read files.
    safe_count: int | None = None
    review_count: int | None = None
    rejected_count: int | None = None
    duplicate_count: int | None = None
    hard_fail_count: int | None = None
    smtp_coverage_rate: float | None = None
    smtp_valid_count: int | None = None
    smtp_inconclusive_count: int | None = None
    catch_all_risk_count: int | None = None
    high_risk_domain_count: int | None = None
    cold_start_count: int | None = None
    approved_original_present = False
    package_manifest: dict[str, Any] | None = None
    package_manifest_path: Path | None = None

    # ---- Rule 1 / 2 / 3: client package presence and integrity ----------- #
    package_dir_exists = pkg_dir.is_dir()
    if not package_dir_exists:
        issues.append(
            OperatorReviewIssue(
                severity=SEVERITY_BLOCK,
                code="client_package_missing",
                message=f"Client package directory not found at {pkg_dir}",
            )
        )
    else:
        manifest_path = pkg_dir / _PACKAGE_MANIFEST_FILENAME
        if not manifest_path.is_file():
            issues.append(
                OperatorReviewIssue(
                    severity=SEVERITY_BLOCK,
                    code="client_package_manifest_missing",
                    message=f"Package manifest not found at {manifest_path}",
                )
            )
        else:
            package_manifest = _read_json(manifest_path)
            if package_manifest is None:
                issues.append(
                    OperatorReviewIssue(
                        severity=SEVERITY_BLOCK,
                        code="client_package_manifest_missing",
                        message=(
                            f"Package manifest at {manifest_path} is unreadable."
                        ),
                    )
                )
            else:
                package_manifest_path = manifest_path

        # Rule 3: contents must all be client_safe (manifest is allowed).
        leaked: list[str] = []
        for entry in sorted(pkg_dir.iterdir(), key=lambda p: p.name):
            if not entry.is_file():
                continue
            if entry.name == _PACKAGE_MANIFEST_FILENAME:
                continue
            if not is_client_safe_artifact(entry.name):
                leaked.append(entry.name)
        if leaked:
            issues.append(
                OperatorReviewIssue(
                    severity=SEVERITY_BLOCK,
                    code="client_package_contains_non_client_safe",
                    message=(
                        "Client package contains non-client-safe files: "
                        f"{leaked}"
                    ),
                )
            )

    # Read manifest counts/warnings/included keys (if available).
    if package_manifest is not None:
        safe_count = _coerce_int(package_manifest.get("safe_count"))
        review_count = _coerce_int(package_manifest.get("review_count"))
        rejected_count = _coerce_int(package_manifest.get("rejected_count"))

        included = package_manifest.get("files_included") or []
        approved_original_present = any(
            isinstance(entry, dict)
            and entry.get("key") == "approved_original_format"
            for entry in included
        )

        # Rule 10: propagate package manifest warnings.
        for warn in package_manifest.get("warnings") or []:
            if not isinstance(warn, dict):
                continue
            code = str(warn.get("code") or "")
            message = str(warn.get("message") or "")
            if code == "approved_original_format_absent":
                issues.append(
                    OperatorReviewIssue(
                        severity=SEVERITY_WARN,
                        code="approved_original_absent",
                        message=(
                            message
                            or "approved_original_format.xlsx is absent."
                        ),
                    )
                )
            else:
                issues.append(
                    OperatorReviewIssue(
                        severity=SEVERITY_WARN,
                        code="package_manifest_warning",
                        message=f"{code}: {message}".strip(": "),
                    )
                )

    # Rule 5 (defensive): if package built but approved original absent and
    # the manifest didn't already flag it (e.g. stale build), warn.
    if (
        package_manifest is not None
        and not approved_original_present
        and not any(i.code == "approved_original_absent" for i in issues)
    ):
        issues.append(
            OperatorReviewIssue(
                severity=SEVERITY_WARN,
                code="approved_original_absent",
                message=(
                    "approved_original_format is not in the client package."
                ),
            )
        )

    # ---- Rule 4: artifact consistency ------------------------------------ #
    consistency_path = run_dir_path / _ARTIFACT_CONSISTENCY_FILENAME
    if consistency_path.is_file():
        consistency = _read_json(consistency_path) or {}
        mutated = bool(
            consistency.get("materialized_outputs_mutated_after_reports", False)
        )
        post_pass_enabled = bool(
            consistency.get("post_pass_mutation_enabled", False)
        )
        regenerated = bool(
            consistency.get("artifacts_regenerated_after_post_passes", False)
        )
        if mutated:
            issues.append(
                OperatorReviewIssue(
                    severity=SEVERITY_BLOCK,
                    code="artifact_consistency_failed",
                    message=(
                        "materialized_outputs_mutated_after_reports=true; "
                        "the materialized CSVs no longer match the reports."
                    ),
                )
            )
        if post_pass_enabled and not regenerated:
            issues.append(
                OperatorReviewIssue(
                    severity=SEVERITY_BLOCK,
                    code="post_pass_mutation_detected",
                    message=(
                        "post_pass_mutation_enabled=true but "
                        "artifacts_regenerated_after_post_passes=false."
                    ),
                )
            )
    else:
        issues.append(
            OperatorReviewIssue(
                severity=SEVERITY_WARN,
                code="artifact_consistency_missing",
                message=(
                    f"{_ARTIFACT_CONSISTENCY_FILENAME} not found at "
                    f"{consistency_path}"
                ),
            )
        )

    # ---- Rule 6: safe_count == 0 ---------------------------------------- #
    if safe_count == 0:
        issues.append(
            OperatorReviewIssue(
                severity=SEVERITY_WARN,
                code="safe_count_zero",
                message="No client-safe email rows in this run.",
            )
        )

    # ---- Rule 7: review_count > 0 --------------------------------------- #
    if review_count is not None and review_count > 0:
        issues.append(
            OperatorReviewIssue(
                severity=SEVERITY_WARN,
                code="review_rows_present",
                message=f"{review_count} rows require human review.",
            )
        )

    # ---- Rule 8: SMTP coverage ------------------------------------------ #
    smtp_path = run_dir_path / _SMTP_RUNTIME_FILENAME
    if smtp_path.is_file():
        smtp = _read_json(smtp_path) or {}
        seen = _coerce_int(smtp.get("smtp_candidates_seen"))
        attempted = _coerce_int(smtp.get("smtp_candidates_attempted"))
        smtp_valid_count = _coerce_int(smtp.get("smtp_valid_count"))
        smtp_inconclusive_count = _coerce_int(
            smtp.get("smtp_inconclusive_count")
        )
        if seen is not None and seen > 0 and attempted is not None:
            smtp_coverage_rate = float(attempted) / float(seen)
            if smtp_coverage_rate < SMTP_COVERAGE_THRESHOLD:
                issues.append(
                    OperatorReviewIssue(
                        severity=SEVERITY_WARN,
                        code="smtp_coverage_low",
                        message=(
                            f"SMTP coverage {smtp_coverage_rate:.2%} is below "
                            f"threshold {SMTP_COVERAGE_THRESHOLD:.2%} "
                            f"({attempted}/{seen})."
                        ),
                    )
                )
    else:
        issues.append(
            OperatorReviewIssue(
                severity=SEVERITY_WARN,
                code="smtp_runtime_summary_missing",
                message=(
                    f"{_SMTP_RUNTIME_FILENAME} not found at {smtp_path}"
                ),
            )
        )

    # ---- Rule 9: V2 deliverability risk metrics ------------------------- #
    v2_path = run_dir_path / _V2_SUMMARY_FILENAME
    if v2_path.is_file():
        v2 = _read_json(v2_path) or {}
        catch_all_risk_count = _v2_metric(
            v2, "catch_all_risk_count",
            "catch_all_summary", "catch_all_risk_count",
        )
        high_risk_domain_count = _v2_metric(
            v2, "high_risk_domain_count",
            "domain_intelligence_summary", "high_risk_domain_count",
        )
        cold_start_count = _v2_metric(
            v2, "cold_start_count",
            "domain_intelligence_summary", "cold_start_count",
        )
        duplicate_count = _coerce_int(v2.get("duplicate_count"))
        hard_fail_count = _coerce_int(v2.get("hard_fail_count"))

        if catch_all_risk_count is not None and catch_all_risk_count > 0:
            issues.append(
                OperatorReviewIssue(
                    severity=SEVERITY_WARN,
                    code="catch_all_risk_present",
                    message=(
                        f"{catch_all_risk_count} catch-all risk rows present."
                    ),
                )
            )
        if high_risk_domain_count is not None and high_risk_domain_count > 0:
            issues.append(
                OperatorReviewIssue(
                    severity=SEVERITY_WARN,
                    code="high_risk_domains_present",
                    message=(
                        f"{high_risk_domain_count} high-risk domain rows present."
                    ),
                )
            )
        if cold_start_count is not None and cold_start_count > 0:
            issues.append(
                OperatorReviewIssue(
                    severity=SEVERITY_WARN,
                    code="cold_start_domains_present",
                    message=(
                        f"{cold_start_count} cold-start domain rows present."
                    ),
                )
            )
    else:
        issues.append(
            OperatorReviewIssue(
                severity=SEVERITY_WARN,
                code="v2_summary_missing",
                message=f"{_V2_SUMMARY_FILENAME} not found at {v2_path}",
            )
        )

    # ---- Final status / readiness --------------------------------------- #
    has_block = any(i.severity == SEVERITY_BLOCK for i in issues)
    has_warn = any(i.severity == SEVERITY_WARN for i in issues)
    if has_block:
        status = STATUS_BLOCK
        ready = False
    elif has_warn:
        status = STATUS_WARN
        ready = False
    else:
        status = STATUS_READY
        ready = True

    summary_path = run_dir_path / _OPERATOR_SUMMARY_FILENAME
    generated_at = _utc_now_iso()

    result = OperatorReviewResult(
        ready_for_client=ready,
        status=status,
        run_dir=run_dir_path,
        package_dir=pkg_dir if package_dir_exists else None,
        package_manifest_path=package_manifest_path,
        summary_path=summary_path,
        generated_at=generated_at,
        issues=tuple(issues),
        safe_count=safe_count,
        review_count=review_count,
        rejected_count=rejected_count,
        duplicate_count=duplicate_count,
        hard_fail_count=hard_fail_count,
        smtp_coverage_rate=smtp_coverage_rate,
        smtp_valid_count=smtp_valid_count,
        smtp_inconclusive_count=smtp_inconclusive_count,
        catch_all_risk_count=catch_all_risk_count,
        high_risk_domain_count=high_risk_domain_count,
        cold_start_count=cold_start_count,
        approved_original_present=approved_original_present,
    )

    # Best-effort summary write — does not change the in-memory result on
    # failure. The decision in ``result`` is the source of truth.
    try:
        summary_path.write_text(
            json.dumps(result.to_dict(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    except Exception:  # pragma: no cover - defensive
        pass

    return result


__all__ = [
    "OperatorReviewIssue",
    "OperatorReviewResult",
    "SMTP_COVERAGE_THRESHOLD",
    "STATUS_BLOCK",
    "STATUS_READY",
    "STATUS_WARN",
    "run_operator_review_gate",
]
