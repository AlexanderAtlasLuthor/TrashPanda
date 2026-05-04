"""V2.9.2 preflight safety checks for real-list rollout runs.

The preflight is intentionally local-file-only. It does not run the
pipeline, open sockets, call SMTP, or mutate outputs. Its job is to
surface unsafe rollout conditions before an operator starts a run.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

from app.config import AppConfig


PREFLIGHT_STATUS_PASS = "pass"
PREFLIGHT_STATUS_WARN = "warn"
PREFLIGHT_STATUS_BLOCK = "block"
PREFLIGHT_STATUSES: frozenset[str] = frozenset({
    PREFLIGHT_STATUS_PASS,
    PREFLIGHT_STATUS_WARN,
    PREFLIGHT_STATUS_BLOCK,
})

PREFLIGHT_SEVERITY_WARN = "warn"
PREFLIGHT_SEVERITY_BLOCK = "block"
PREFLIGHT_SEVERITIES: frozenset[str] = frozenset({
    PREFLIGHT_SEVERITY_WARN,
    PREFLIGHT_SEVERITY_BLOCK,
})

ISSUE_INPUT_MISSING = "input_missing"
ISSUE_INPUT_UNREADABLE = "input_unreadable"
ISSUE_LARGE_FILE_REQUIRES_CONFIRMATION = "large_file_requires_confirmation"
ISSUE_UNCAPPED_LIVE_SMTP_BLOCKED = "uncapped_live_smtp_blocked"
ISSUE_SMTP_LIVE_WITHOUT_CAP_WARNING = "smtp_live_without_cap_warning"
ISSUE_SMTP_PORT_NOT_VERIFIED = "smtp_port_not_verified"
ISSUE_APPROVED_ORIGINAL_MAY_BE_ABSENT = "approved_original_may_be_absent"
ISSUE_OUTPUT_DIR_MISSING = "output_dir_missing"
ISSUE_ROW_COUNT_ESTIMATE_UNAVAILABLE = "row_count_estimate_unavailable"


@dataclass(frozen=True)
class PreflightIssue:
    severity: str
    code: str
    message: str


@dataclass(frozen=True)
class PreflightResult:
    status: str
    profile: str
    row_count_estimate: int | None
    file_size_bytes: int | None
    issues: tuple[PreflightIssue, ...]

    def to_dict(self) -> dict:
        """Return a JSON-friendly representation for API/boundary callers."""
        return {
            "status": self.status,
            "profile": self.profile,
            "row_count_estimate": self.row_count_estimate,
            "file_size_bytes": self.file_size_bytes,
            "issues": [asdict(issue) for issue in self.issues],
        }


def run_preflight_check(
    input_path: str | Path,
    *,
    config: AppConfig,
    output_dir: str | Path | None = None,
    operator_confirmed_large_run: bool = False,
    smtp_port_verified: bool = False,
) -> PreflightResult:
    """Evaluate whether a rollout run is safe to start.

    The check is deterministic and local-only. It estimates CSV row
    counts by streaming bytes, inspects rollout/SMTP config, and returns
    a structured result. It does not enforce the result in the main run
    path; callers decide whether to stop on ``status == "block"``.
    """
    issues: list[PreflightIssue] = []
    path = Path(input_path)
    row_count: int | None = None
    file_size: int | None = None

    if not path.exists():
        issues.append(_block(ISSUE_INPUT_MISSING, f"Input path does not exist: {path}"))
    else:
        file_size = _file_size(path, issues)
        if _is_readable_input(path, issues):
            row_count = _estimate_row_count(path, issues)

    if output_dir is not None:
        out = Path(output_dir)
        if not out.exists():
            issues.append(
                _warn(
                    ISSUE_OUTPUT_DIR_MISSING,
                    f"Output directory does not exist yet: {out}",
                )
            )

    rollout_cfg = config.rollout
    smtp_cfg = config.smtp_probe
    threshold = int(rollout_cfg.max_rows_without_confirmation)
    is_large_run = row_count is not None and row_count > threshold
    smtp_live = bool(smtp_cfg.enabled) and not bool(smtp_cfg.dry_run)
    smtp_uncapped = smtp_cfg.max_candidates_per_run is None

    if is_large_run and not operator_confirmed_large_run:
        issues.append(
            _block(
                ISSUE_LARGE_FILE_REQUIRES_CONFIRMATION,
                "Estimated row count exceeds rollout confirmation threshold "
                f"({row_count} > {threshold}).",
            )
        )

    if (
        bool(rollout_cfg.block_uncapped_live_smtp)
        and smtp_live
        and smtp_uncapped
        and is_large_run
    ):
        issues.append(
            _block(
                ISSUE_UNCAPPED_LIVE_SMTP_BLOCKED,
                "Live SMTP is enabled without max_candidates_per_run on a "
                "large run.",
            )
        )

    if smtp_live and smtp_uncapped and row_count is not None and row_count <= threshold:
        issues.append(
            _warn(
                ISSUE_SMTP_LIVE_WITHOUT_CAP_WARNING,
                "Live SMTP is enabled without max_candidates_per_run; this is "
                "only acceptable for a small confirmed run.",
            )
        )

    if smtp_live and not smtp_port_verified:
        issues.append(
            _warn(
                ISSUE_SMTP_PORT_NOT_VERIFIED,
                "Live SMTP is enabled but outbound port 25 has not been "
                "explicitly verified.",
            )
        )

    if not bool(smtp_cfg.enabled) or bool(smtp_cfg.dry_run):
        issues.append(
            _warn(
                ISSUE_APPROVED_ORIGINAL_MAY_BE_ABSENT,
                "SMTP is disabled or dry-run; approved_original_format.xlsx "
                "may be absent if no rows receive a valid SMTP signal.",
            )
        )

    return PreflightResult(
        status=_status_from_issues(issues),
        profile=str(rollout_cfg.profile),
        row_count_estimate=row_count,
        file_size_bytes=file_size,
        issues=tuple(issues),
    )


def _warn(code: str, message: str) -> PreflightIssue:
    return PreflightIssue(PREFLIGHT_SEVERITY_WARN, code, message)


def _block(code: str, message: str) -> PreflightIssue:
    return PreflightIssue(PREFLIGHT_SEVERITY_BLOCK, code, message)


def _status_from_issues(issues: Iterable[PreflightIssue]) -> str:
    issue_tuple = tuple(issues)
    if any(issue.severity == PREFLIGHT_SEVERITY_BLOCK for issue in issue_tuple):
        return PREFLIGHT_STATUS_BLOCK
    if issue_tuple:
        return PREFLIGHT_STATUS_WARN
    return PREFLIGHT_STATUS_PASS


def _file_size(path: Path, issues: list[PreflightIssue]) -> int | None:
    if path.is_dir():
        return None
    try:
        return int(path.stat().st_size)
    except OSError as exc:
        issues.append(
            _block(ISSUE_INPUT_UNREADABLE, f"Cannot stat input path {path}: {exc}")
        )
        return None


def _is_readable_input(path: Path, issues: list[PreflightIssue]) -> bool:
    if path.is_dir():
        return True
    if not path.is_file():
        issues.append(
            _block(ISSUE_INPUT_UNREADABLE, f"Input path is not a file: {path}")
        )
        return False
    try:
        with path.open("rb") as handle:
            handle.read(1)
        return True
    except OSError as exc:
        issues.append(
            _block(ISSUE_INPUT_UNREADABLE, f"Cannot read input path {path}: {exc}")
        )
        return False


def _estimate_row_count(path: Path, issues: list[PreflightIssue]) -> int | None:
    if path.is_dir():
        return _estimate_directory_row_count(path, issues)

    suffix = path.suffix.lower()
    if suffix == ".csv":
        return _estimate_csv_rows(path, issues)
    if suffix == ".xlsx":
        issues.append(
            _warn(
                ISSUE_ROW_COUNT_ESTIMATE_UNAVAILABLE,
                "XLSX row count estimation is not implemented in preflight yet.",
            )
        )
        return None

    issues.append(
        _warn(
            ISSUE_ROW_COUNT_ESTIMATE_UNAVAILABLE,
            f"Row count estimation is not supported for {suffix or 'this input type'}.",
        )
    )
    return None


def _estimate_directory_row_count(path: Path, issues: list[PreflightIssue]) -> int | None:
    try:
        files = [p for p in sorted(path.iterdir()) if p.is_file()]
    except OSError as exc:
        issues.append(
            _block(ISSUE_INPUT_UNREADABLE, f"Cannot read input directory {path}: {exc}")
        )
        return None

    supported = [p for p in files if p.suffix.lower() in {".csv", ".xlsx"}]
    if not supported:
        issues.append(
            _block(
                ISSUE_INPUT_UNREADABLE,
                f"Input directory contains no supported CSV/XLSX files: {path}",
            )
        )
        return None

    total = 0
    saw_unknown = False
    for file_path in supported:
        estimate = _estimate_row_count(file_path, issues)
        if estimate is None:
            saw_unknown = True
            continue
        total += estimate
    return None if saw_unknown and total == 0 else total


def _estimate_csv_rows(path: Path, issues: list[PreflightIssue]) -> int | None:
    try:
        line_count = _count_physical_lines(path)
    except OSError as exc:
        issues.append(
            _block(ISSUE_INPUT_UNREADABLE, f"Cannot count CSV rows for {path}: {exc}")
        )
        return None
    return max(0, line_count - 1)


def _count_physical_lines(path: Path) -> int:
    line_count = 0
    saw_bytes = False
    last_byte = b""
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            saw_bytes = True
            line_count += chunk.count(b"\n")
            last_byte = chunk[-1:]
    if saw_bytes and last_byte != b"\n":
        line_count += 1
    return line_count


__all__ = [
    "ISSUE_APPROVED_ORIGINAL_MAY_BE_ABSENT",
    "ISSUE_INPUT_MISSING",
    "ISSUE_INPUT_UNREADABLE",
    "ISSUE_LARGE_FILE_REQUIRES_CONFIRMATION",
    "ISSUE_OUTPUT_DIR_MISSING",
    "ISSUE_ROW_COUNT_ESTIMATE_UNAVAILABLE",
    "ISSUE_SMTP_LIVE_WITHOUT_CAP_WARNING",
    "ISSUE_SMTP_PORT_NOT_VERIFIED",
    "ISSUE_UNCAPPED_LIVE_SMTP_BLOCKED",
    "PREFLIGHT_SEVERITIES",
    "PREFLIGHT_SEVERITY_BLOCK",
    "PREFLIGHT_SEVERITY_WARN",
    "PREFLIGHT_STATUSES",
    "PREFLIGHT_STATUS_BLOCK",
    "PREFLIGHT_STATUS_PASS",
    "PREFLIGHT_STATUS_WARN",
    "PreflightIssue",
    "PreflightResult",
    "run_preflight_check",
]
