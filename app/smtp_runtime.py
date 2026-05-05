"""V2.9.3 SMTP runtime guardrail summary.

This module is intentionally local-file/config only. It records what the
in-chunk SMTP verification stage did during a run and writes an
operator-facing JSON summary after materialization.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


SMTP_RUNTIME_SUMMARY_REPORT_VERSION = "v2.9.3"
SMTP_RUNTIME_SUMMARY_FILENAME = "smtp_runtime_summary.json"
SMTP_RUNTIME_SUMMARY_EXTRAS_KEY = "smtp_runtime_summary"

# Live retry execution. Retries are only meaningful once per-command
# timeouts and cancellation are honoured by ``smtp_probe.py`` (see the
# hardening comment there). With those in place we re-enable retries
# behind exponential backoff so transient 4xx responses get a second
# chance without stampeding a failing MX.
SMTP_RETRY_EXECUTION_ENABLED = True

# Backoff envelope for retry attempts. Capped at 8s so a single retry
# never dwarfs the per-probe budget.
SMTP_RETRY_BASE_BACKOFF_SECONDS: float = 0.5
SMTP_RETRY_MAX_BACKOFF_SECONDS: float = 8.0


def compute_retry_backoff_seconds(attempt: int) -> float:
    """Exponential backoff: 0.5s, 1s, 2s, 4s, 8s (cap).

    ``attempt`` is 1-indexed. ``compute_retry_backoff_seconds(1)``
    returns the wait *before* the first retry (after the initial try).
    """

    if attempt < 1:
        return 0.0
    delay = SMTP_RETRY_BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
    return min(SMTP_RETRY_MAX_BACKOFF_SECONDS, delay)


SMTP_RUNTIME_STATUS_VALID = "valid"
SMTP_RUNTIME_STATUS_INVALID = "invalid"
SMTP_RUNTIME_STATUS_BLOCKED = "blocked"
SMTP_RUNTIME_STATUS_TIMEOUT = "timeout"
SMTP_RUNTIME_STATUS_TEMP_FAIL = "temp_fail"
SMTP_RUNTIME_STATUS_CATCH_ALL_POSSIBLE = "catch_all_possible"
SMTP_RUNTIME_STATUS_NOT_TESTED = "not_tested"
SMTP_RUNTIME_STATUS_ERROR = "error"


SMTP_RUNTIME_INCONCLUSIVE_STATUSES = frozenset({
    SMTP_RUNTIME_STATUS_BLOCKED,
    SMTP_RUNTIME_STATUS_TIMEOUT,
    SMTP_RUNTIME_STATUS_TEMP_FAIL,
    SMTP_RUNTIME_STATUS_ERROR,
    SMTP_RUNTIME_STATUS_CATCH_ALL_POSSIBLE,
})


@dataclass(slots=True)
class SMTPRuntimeSummary:
    """Operator-facing SMTP runtime counters for one pipeline run."""

    report_version: str = SMTP_RUNTIME_SUMMARY_REPORT_VERSION
    smtp_enabled: bool = True
    smtp_dry_run: bool = True
    smtp_candidate_cap: int | None = None
    smtp_timeout_seconds: float = 4.0
    smtp_rate_limit_per_second: float = 0.0
    smtp_retry_temp_failures_configured: bool = False
    smtp_max_retries_configured: int = 0
    smtp_retry_execution_enabled: bool = SMTP_RETRY_EXECUTION_ENABLED
    smtp_retries_executed: int = 0
    smtp_candidates_seen: int = 0
    smtp_candidates_attempted: int = 0
    smtp_candidates_skipped_by_cap: int = 0
    smtp_not_tested_count: int = 0
    smtp_valid_count: int = 0
    smtp_invalid_count: int = 0
    smtp_inconclusive_count: int = 0
    smtp_error_count: int = 0
    smtp_timeout_count: int = 0
    smtp_blocked_count: int = 0
    smtp_temp_fail_count: int = 0
    smtp_catch_all_possible_count: int = 0

    @classmethod
    def from_config(cls, config: Any | None) -> "SMTPRuntimeSummary":
        """Build summary config fields from ``AppConfig.smtp_probe``."""

        smtp_cfg = getattr(config, "smtp_probe", None) if config is not None else None
        if smtp_cfg is None:
            return cls()

        return cls(
            smtp_enabled=bool(getattr(smtp_cfg, "enabled", True)),
            smtp_dry_run=bool(getattr(smtp_cfg, "dry_run", True)),
            smtp_candidate_cap=_coerce_positive_int_or_none(
                getattr(smtp_cfg, "max_candidates_per_run", None)
            ),
            smtp_timeout_seconds=_coerce_float(
                getattr(smtp_cfg, "timeout_seconds", 4.0), default=4.0
            ),
            smtp_rate_limit_per_second=_coerce_float(
                getattr(smtp_cfg, "rate_limit_per_second", 0.0), default=0.0
            ),
            smtp_retry_temp_failures_configured=bool(
                getattr(smtp_cfg, "retry_temp_failures", False)
            ),
            smtp_max_retries_configured=_coerce_int(
                getattr(smtp_cfg, "max_retries", 0), default=0
            ),
            smtp_retry_execution_enabled=SMTP_RETRY_EXECUTION_ENABLED,
            smtp_retries_executed=0,
        )

    def record_candidate_seen(self) -> None:
        self.smtp_candidates_seen += 1

    def record_probe_attempt(self, status: str) -> None:
        self.smtp_candidates_attempted += 1
        self.record_status(status)

    def record_status(self, status: str) -> None:
        if status == SMTP_RUNTIME_STATUS_VALID:
            self.smtp_valid_count += 1
        elif status == SMTP_RUNTIME_STATUS_INVALID:
            self.smtp_invalid_count += 1
        elif status == SMTP_RUNTIME_STATUS_BLOCKED:
            self.smtp_blocked_count += 1
        elif status == SMTP_RUNTIME_STATUS_TIMEOUT:
            self.smtp_timeout_count += 1
        elif status == SMTP_RUNTIME_STATUS_TEMP_FAIL:
            self.smtp_temp_fail_count += 1
        elif status == SMTP_RUNTIME_STATUS_CATCH_ALL_POSSIBLE:
            self.smtp_catch_all_possible_count += 1
        elif status == SMTP_RUNTIME_STATUS_NOT_TESTED:
            self.smtp_not_tested_count += 1
        else:
            self.smtp_error_count += 1

        if status in SMTP_RUNTIME_INCONCLUSIVE_STATUSES:
            self.smtp_inconclusive_count += 1

    def record_not_tested(self, *, skipped_by_cap: bool = False) -> None:
        if skipped_by_cap:
            self.smtp_candidates_skipped_by_cap += 1
        self.record_status(SMTP_RUNTIME_STATUS_NOT_TESTED)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def get_or_create_smtp_runtime_summary(context: Any) -> SMTPRuntimeSummary:
    """Return the run summary stored on ``context.extras``."""

    summary = context.extras.get(SMTP_RUNTIME_SUMMARY_EXTRAS_KEY)
    if isinstance(summary, SMTPRuntimeSummary):
        return summary

    summary = SMTPRuntimeSummary.from_config(getattr(context, "config", None))
    context.extras[SMTP_RUNTIME_SUMMARY_EXTRAS_KEY] = summary
    return summary


def write_smtp_runtime_summary(
    run_dir: str | Path,
    summary: SMTPRuntimeSummary,
) -> Path:
    """Write ``smtp_runtime_summary.json`` and return its path."""

    output_path = Path(run_dir) / SMTP_RUNTIME_SUMMARY_FILENAME
    output_path.write_text(
        json.dumps(summary.to_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return output_path


def _coerce_positive_int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        n = int(value)
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


def _coerce_int(value: Any, *, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_float(value: Any, *, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


__all__ = [
    "SMTP_RETRY_BASE_BACKOFF_SECONDS",
    "SMTP_RETRY_EXECUTION_ENABLED",
    "SMTP_RETRY_MAX_BACKOFF_SECONDS",
    "SMTP_RUNTIME_SUMMARY_EXTRAS_KEY",
    "SMTP_RUNTIME_SUMMARY_FILENAME",
    "SMTP_RUNTIME_SUMMARY_REPORT_VERSION",
    "SMTPRuntimeSummary",
    "compute_retry_backoff_seconds",
    "get_or_create_smtp_runtime_summary",
    "write_smtp_runtime_summary",
]
