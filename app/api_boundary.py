"""Phase 5 API boundary.

A thin, stable façade around :class:`app.pipeline.EmailCleaningPipeline`
so that external callers (e.g. a Next.js web app via a future HTTP API)
can invoke the email-cleaning pipeline as a *job* with:

* an explicit request/result contract (dataclasses in this module),
* explicit job status values,
* serialisable error objects (no raw tracebacks returned),
* a predictable mapping of artifacts on disk, and
* JSON-friendly serialisation helpers.

This module intentionally does **not**:

* spin up FastAPI/Flask/Django,
* start background workers or queues,
* implement authentication, uploads, or a job database,
* change pipeline/scoring/validation/calibration logic.

It delegates all real work to the existing pipeline. The contract is
designed to be wrappable by a future web API without further changes.
"""

from __future__ import annotations

import json
import logging
import traceback
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from .config import load_config, resolve_project_paths
from .io_utils import build_run_context
from .logger import setup_run_logger
from .models import PipelineResult
from .pipeline import EmailCleaningPipeline


# --------------------------------------------------------------------------- #
# Status constants
# --------------------------------------------------------------------------- #


class JobStatus:
    """String constants for job status.

    Plain strings (rather than ``enum.Enum``) are used so the values
    round-trip cleanly through JSON / HTTP without extra handling.
    """

    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


# --------------------------------------------------------------------------- #
# Error types
# --------------------------------------------------------------------------- #


class JobErrorType:
    """Canonical, serialisable error categories surfaced to callers."""

    FILE_NOT_FOUND = "file_not_found"
    INVALID_INPUT_FORMAT = "invalid_input_format"
    MISSING_REQUIRED_COLUMNS = "missing_required_columns"
    PIPELINE_EXECUTION_ERROR = "pipeline_execution_error"
    CONFIG_ERROR = "config_error"


@dataclass(slots=True)
class JobError:
    """Serialisable error payload returned in ``JobResult.error``."""

    error_type: str
    message: str
    details: dict[str, Any] | None = None


# --------------------------------------------------------------------------- #
# Contracts
# --------------------------------------------------------------------------- #


@dataclass(slots=True)
class JobRequest:
    """Normalised input describing what to run."""

    job_id: str
    input_path: Path
    output_root: Path
    config_path: Path | None = None


@dataclass(slots=True)
class TechnicalCsvs:
    """Technical CSV deliverables produced by the pipeline.

    V2.5 — ``removed_duplicates`` and ``removed_hard_fail`` are
    separated subsets of ``removed_invalid``. The legacy
    ``removed_invalid`` file is unchanged and still contains every
    removed row; the new files exist so V2-aware exports can address
    the duplicate and hard-fail cohorts directly.
    """

    clean_high_confidence: Path | None = None
    review_medium_confidence: Path | None = None
    removed_invalid: Path | None = None
    # V2.5 — V2 semantic separations.
    removed_duplicates: Path | None = None
    removed_hard_fail: Path | None = None


@dataclass(slots=True)
class ClientOutputs:
    """Client-facing XLSX deliverables (may be absent on failure).

    V2.5 — ``duplicate_emails`` and ``hard_fail_emails`` are
    supplementary workbooks that split the ``invalid_or_bounce_risk``
    cohort into V2-semantic buckets. The legacy
    ``invalid_or_bounce_risk`` workbook still contains every removed
    row for backward compatibility.
    """

    valid_emails: Path | None = None
    review_emails: Path | None = None
    invalid_or_bounce_risk: Path | None = None
    summary_report: Path | None = None
    approved_original_format: Path | None = None
    # V2.5 — V2 semantic separations.
    duplicate_emails: Path | None = None
    hard_fail_emails: Path | None = None


@dataclass(slots=True)
class ReportFiles:
    """Structured reporting files written alongside the CSV outputs.

    V2.8 — adds four new V2 deliverability reports
    (``v2_deliverability_summary``, ``v2_reason_breakdown``,
    ``v2_domain_risk_summary``, ``v2_probability_distribution``).
    Legacy fields are unchanged.
    """

    processing_report_json: Path | None = None
    processing_report_csv: Path | None = None
    domain_summary: Path | None = None
    typo_corrections: Path | None = None
    duplicate_summary: Path | None = None
    # V2.8 — V2 deliverability reports.
    v2_deliverability_summary: Path | None = None
    v2_reason_breakdown: Path | None = None
    v2_domain_risk_summary: Path | None = None
    v2_probability_distribution: Path | None = None
    # V2.9.3 - operator-only SMTP runtime guardrail report.
    smtp_runtime_summary: Path | None = None
    # V2.9.4 - operator-only artifact consistency metadata.
    artifact_consistency: Path | None = None
    # V2.9.7 - operator review gate decision summary.
    operator_review_summary: Path | None = None
    # V2.9.8 - operator-only feedback bridge readiness preview.
    feedback_domain_intel_preview: Path | None = None


@dataclass(slots=True)
class JobArtifacts:
    """All on-disk outputs for a completed run.

    Only paths that actually exist on disk are populated. Missing files
    stay as ``None`` so callers can render them as ``null`` in JSON.
    """

    run_dir: Path
    technical_csvs: TechnicalCsvs = field(default_factory=TechnicalCsvs)
    client_outputs: ClientOutputs = field(default_factory=ClientOutputs)
    reports: ReportFiles = field(default_factory=ReportFiles)


@dataclass(slots=True)
class JobSummary:
    """Run-level counts consumed by the web UI.

    All fields default to ``0`` (or ``None`` where the underlying report
    does not provide a value) so the shape is stable for serialisation.
    Values are read from existing report files (``summary_report.xlsx``
    preferred, ``processing_report.json`` as fallback) — nothing is
    recomputed here.
    """

    total_input_rows: int = 0
    total_valid: int = 0
    total_review: int = 0
    total_invalid_or_bounce_risk: int = 0
    duplicates_removed: int = 0
    typo_corrections: int = 0
    disposable_emails: int = 0
    placeholder_or_fake_emails: int = 0
    role_based_emails: int = 0


@dataclass(slots=True)
class JobResult:
    """Final return payload for :func:`run_cleaning_job`."""

    job_id: str
    status: str
    input_filename: str
    run_dir: Path | None
    summary: JobSummary | None
    artifacts: JobArtifacts | None
    error: JobError | None
    started_at: datetime
    finished_at: datetime | None


# --------------------------------------------------------------------------- #
# Artifact discovery
# --------------------------------------------------------------------------- #


# Canonical filenames emitted by the existing pipeline/reporting/client
# output code. Kept in one place so this file is the single source of
# truth for the contract.
_TECHNICAL_CSV_NAMES: dict[str, str] = {
    "clean_high_confidence": "clean_high_confidence.csv",
    "review_medium_confidence": "review_medium_confidence.csv",
    "removed_invalid": "removed_invalid.csv",
    # V2.5 — V2 semantic separations of ``removed_invalid.csv``.
    "removed_duplicates": "removed_duplicates.csv",
    "removed_hard_fail": "removed_hard_fail.csv",
}

_CLIENT_OUTPUT_NAMES: dict[str, str] = {
    "valid_emails": "valid_emails.xlsx",
    "review_emails": "review_emails.xlsx",
    "invalid_or_bounce_risk": "invalid_or_bounce_risk.xlsx",
    "summary_report": "summary_report.xlsx",
    "approved_original_format": "approved_original_format.xlsx",
    # V2.5 — V2 semantic supplementary workbooks.
    "duplicate_emails": "duplicate_emails.xlsx",
    "hard_fail_emails": "hard_fail_emails.xlsx",
}

_REPORT_NAMES: dict[str, str] = {
    "processing_report_json": "processing_report.json",
    "processing_report_csv": "processing_report.csv",
    "domain_summary": "domain_summary.csv",
    "typo_corrections": "typo_corrections.csv",
    "duplicate_summary": "duplicate_summary.csv",
    # V2.8 — V2 deliverability reports.
    "v2_deliverability_summary": "v2_deliverability_summary.json",
    "v2_reason_breakdown": "v2_reason_breakdown.csv",
    "v2_domain_risk_summary": "v2_domain_risk_summary.csv",
    "v2_probability_distribution": "v2_probability_distribution.csv",
    # V2.9.3 - operator-only SMTP runtime guardrail report.
    "smtp_runtime_summary": "smtp_runtime_summary.json",
    # V2.9.4 - operator-only artifact consistency metadata.
    "artifact_consistency": "artifact_consistency.json",
    # V2.9.7 - operator review gate decision summary.
    "operator_review_summary": "operator_review_summary.json",
    # V2.9.8 - operator-only feedback bridge readiness preview.
    "feedback_domain_intel_preview": "feedback_domain_intel_preview.json",
}


def _path_if_exists(run_dir: Path, name: str) -> Path | None:
    candidate = run_dir / name
    return candidate if candidate.is_file() else None


def collect_job_artifacts(run_dir: Path) -> JobArtifacts:
    """Scan ``run_dir`` and return a :class:`JobArtifacts` view.

    Only paths for files that actually exist are populated; every other
    field is ``None``.
    """

    run_dir = Path(run_dir)

    technical = TechnicalCsvs(
        **{
            key: _path_if_exists(run_dir, name)
            for key, name in _TECHNICAL_CSV_NAMES.items()
        }
    )
    client = ClientOutputs(
        **{
            key: _path_if_exists(run_dir, name)
            for key, name in _CLIENT_OUTPUT_NAMES.items()
        }
    )
    reports = ReportFiles(
        **{
            key: _path_if_exists(run_dir, name)
            for key, name in _REPORT_NAMES.items()
        }
    )

    return JobArtifacts(
        run_dir=run_dir,
        technical_csvs=technical,
        client_outputs=client,
        reports=reports,
    )


# --------------------------------------------------------------------------- #
# Summary loading
# --------------------------------------------------------------------------- #


# Mapping from the ``metric`` column in ``summary_report.xlsx`` to
# attributes of :class:`JobSummary`. Names match exactly what
# ``app.client_output._write_summary_report`` writes.
_SUMMARY_METRIC_MAP: dict[str, str] = {
    "total_input_rows": "total_input_rows",
    "total_valid": "total_valid",
    "total_review": "total_review",
    "total_invalid_or_bounce_risk": "total_invalid_or_bounce_risk",
    "duplicates_removed": "duplicates_removed",
    "typo_corrections": "typo_corrections",
    "disposable_emails": "disposable_emails",
    "placeholder_or_fake_emails": "placeholder_or_fake_emails",
    "role_based_emails": "role_based_emails",
}


def _load_summary_from_xlsx(path: Path) -> JobSummary | None:
    try:
        df = pd.read_excel(path, sheet_name="totals")
    except Exception:  # pragma: no cover - defensive I/O guard
        return None

    if "metric" not in df.columns or "value" not in df.columns:
        return None

    summary = JobSummary()
    for _, row in df.iterrows():
        metric = str(row["metric"]).strip()
        attr = _SUMMARY_METRIC_MAP.get(metric)
        if attr is None:
            continue
        try:
            setattr(summary, attr, int(row["value"]))
        except (TypeError, ValueError):
            continue
    return summary


def _load_summary_from_json(path: Path) -> JobSummary | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # pragma: no cover - defensive I/O guard
        return None

    def _get_int(*keys: str) -> int:
        for k in keys:
            if k in data and data[k] is not None:
                try:
                    return int(data[k])
                except (TypeError, ValueError):
                    continue
        return 0

    # Only the subset of fields that ``processing_report.json`` actually
    # carries is populated here; the rest remain 0 (documented as the
    # "fall-back" behaviour when the richer xlsx summary is missing).
    return JobSummary(
        total_input_rows=_get_int("total_rows_processed", "total_rows"),
        total_valid=_get_int("total_clean_high_confidence", "total_output_clean"),
        total_review=_get_int("total_review", "total_output_review"),
        total_invalid_or_bounce_risk=_get_int(
            "total_removed_invalid", "total_output_removed"
        ),
        duplicates_removed=_get_int(
            "total_duplicates_removed", "total_duplicate_rows"
        ),
        typo_corrections=_get_int("total_typo_corrections"),
        # These three are only tracked in the client xlsx summary; leave
        # them at 0 when only the JSON report is available.
        disposable_emails=0,
        placeholder_or_fake_emails=0,
        role_based_emails=0,
    )


def load_job_summary(run_dir: Path) -> JobSummary | None:
    """Load a :class:`JobSummary` from existing report files in ``run_dir``.

    Resolution order:

    1. ``summary_report.xlsx`` (richest; includes client-facing counts
       like ``disposable_emails``).
    2. ``processing_report.json`` (JSON fallback; the three reason-based
       counts default to ``0``).

    Returns ``None`` if no suitable report file is present.
    """

    run_dir = Path(run_dir)

    xlsx = run_dir / _CLIENT_OUTPUT_NAMES["summary_report"]
    if xlsx.is_file():
        summary = _load_summary_from_xlsx(xlsx)
        if summary is not None:
            return summary

    json_path = run_dir / _REPORT_NAMES["processing_report_json"]
    if json_path.is_file():
        return _load_summary_from_json(json_path)

    return None


# --------------------------------------------------------------------------- #
# Main entry point
# --------------------------------------------------------------------------- #


_LOGGER = logging.getLogger(__name__)


def _now_utc() -> datetime:
    """Return a timezone-aware UTC ``datetime`` for lifecycle timestamps.

    All runtime-generated job lifecycle timestamps (``started_at``,
    ``finished_at``, etc.) MUST go through this helper so we never mix
    naive local-time values with the UTC values produced by the DB write
    path (see :func:`app.db.write_path._utc_now`).
    """
    return datetime.now(timezone.utc)


def _new_job_id() -> str:
    # The job id is a *human-readable identifier*, not a stored timestamp,
    # so it stays on local time for operator convenience. Real lifecycle
    # timestamps go through :func:`_now_utc`.
    return datetime.now().strftime("job_%Y%m%d_%H%M%S_") + uuid.uuid4().hex[:8]


def _classify_exception(exc: BaseException) -> JobError:
    """Map raw pipeline exceptions onto the public ``JobError`` contract."""

    message = str(exc) or exc.__class__.__name__
    details: dict[str, Any] = {"exception_class": exc.__class__.__name__}

    if isinstance(exc, FileNotFoundError):
        return JobError(
            error_type=JobErrorType.FILE_NOT_FOUND,
            message=message,
            details=details,
        )

    if isinstance(exc, ValueError):
        lowered = message.lower()
        if "column" in lowered and (
            "missing" in lowered or "required" in lowered or "email" in lowered
        ):
            return JobError(
                error_type=JobErrorType.MISSING_REQUIRED_COLUMNS,
                message=message,
                details=details,
            )
        if (
            "unsupported" in lowered
            or "extension" in lowered
            or "encoding" in lowered
            or "format" in lowered
        ):
            return JobError(
                error_type=JobErrorType.INVALID_INPUT_FORMAT,
                message=message,
                details=details,
            )
        # Remaining ValueErrors are config/validation problems.
        return JobError(
            error_type=JobErrorType.CONFIG_ERROR,
            message=message,
            details=details,
        )

    return JobError(
        error_type=JobErrorType.PIPELINE_EXECUTION_ERROR,
        message=message,
        details=details,
    )


def _build_request(
    input_path: str | Path,
    output_root: str | Path,
    config_path: str | Path | None,
    job_id: str | None,
) -> JobRequest:
    return JobRequest(
        job_id=job_id or _new_job_id(),
        input_path=Path(input_path),
        output_root=Path(output_root),
        config_path=Path(config_path) if config_path else None,
    )


def run_cleaning_job(
    input_path: str | Path,
    output_root: str | Path,
    config_path: str | Path | None = None,
    job_id: str | None = None,
) -> JobResult:
    """Run the email-cleaning pipeline as a single synchronous job.

    Parameters
    ----------
    input_path:
        Either a single CSV/XLSX file **or** a directory containing
        supported inputs.
    output_root:
        Parent directory under which a per-job run directory is
        created (``<output_root>/<run_id>``).
    config_path:
        Optional YAML config path; defaults to ``configs/default.yaml``.
    job_id:
        Optional caller-supplied job identifier; auto-generated if not
        provided.

    Returns
    -------
    JobResult
        Structured result with status ``completed`` or ``failed``. This
        function never raises on pipeline errors; failures are wrapped
        in :class:`JobError` inside the returned :class:`JobResult`.
    """

    started_at = _now_utc()
    request = _build_request(input_path, output_root, config_path, job_id)
    input_filename = request.input_path.name

    # --- Phase 1: input validation (cheap, before building run_dir). --- #
    if not request.input_path.exists():
        return JobResult(
            job_id=request.job_id,
            status=JobStatus.FAILED,
            input_filename=input_filename,
            run_dir=None,
            summary=None,
            artifacts=None,
            error=JobError(
                error_type=JobErrorType.FILE_NOT_FOUND,
                message=f"Input path does not exist: {request.input_path}",
                details={"input_path": str(request.input_path)},
            ),
            started_at=started_at,
            finished_at=_now_utc(),
        )

    # --- Phase 2: config + run directory. --- #
    try:
        project_paths = resolve_project_paths()
        config = load_config(
            config_path=request.config_path,
            base_dir=project_paths.project_root,
        )
        request.output_root.mkdir(parents=True, exist_ok=True)
        run_id = datetime.now().strftime("run_%Y%m%d_%H%M%S")
        # Suffix with a short token so parallel jobs don't collide on
        # second-level granularity.
        run_dir = (request.output_root / f"{run_id}_{uuid.uuid4().hex[:6]}").resolve()
        run_context = build_run_context(config, output_dir=run_dir)
        logger = setup_run_logger(run_context.logs_dir, log_level=config.log_level)
    except Exception as exc:  # pragma: no cover - exercised via tests
        _LOGGER.exception("Failed to prepare run context for job %s", request.job_id)
        return JobResult(
            job_id=request.job_id,
            status=JobStatus.FAILED,
            input_filename=input_filename,
            run_dir=None,
            summary=None,
            artifacts=None,
            error=_classify_exception(exc),
            started_at=started_at,
            finished_at=_now_utc(),
        )

    # --- Phase 3: execute pipeline. --- #
    input_kwargs: dict[str, Any]
    if request.input_path.is_dir():
        input_kwargs = {"input_dir": str(request.input_path)}
    else:
        input_kwargs = {"input_file": str(request.input_path)}

    try:
        pipeline = EmailCleaningPipeline(config=config, logger=logger)
        result: PipelineResult = pipeline.run(
            output_dir=run_context.run_dir,
            run_context=run_context,
            **input_kwargs,
        )
    except Exception as exc:
        logger.exception("Pipeline execution failed for job %s", request.job_id)
        # Still collect whatever partial artifacts exist on disk so
        # callers can inspect what was written before the failure.
        artifacts = collect_job_artifacts(run_context.run_dir)
        error = _classify_exception(exc)
        # Attach traceback to details for internal debugging; NOT
        # exposed as the primary message.
        if error.details is None:
            error.details = {}
        error.details["traceback_tail"] = traceback.format_exc().splitlines()[-5:]
        return JobResult(
            job_id=request.job_id,
            status=JobStatus.FAILED,
            input_filename=input_filename,
            run_dir=run_context.run_dir,
            summary=None,
            artifacts=artifacts,
            error=error,
            started_at=started_at,
            finished_at=_now_utc(),
        )

    # --- Phase 4 (V2): safe post-run updates. --- #
    # Additive, guarded: any failure here must not affect the JobResult
    # or deliverable outputs. History store updates are preserved, while
    # any legacy annotation pass that rewrites materialized CSVs is gated
    # by post_passes.mutate_materialized_outputs.
    post_pass_mutation_enabled = _post_pass_materialized_mutation_enabled(config)
    materialized_outputs_mutated_after_reports = _maybe_update_domain_history(
        run_dir=result.run_dir,
        config=config,
        logger=logger,
    )

    # --- Phase 5 (V2.4): selective SMTP probing. --- #
    # Runs AFTER the history update so the CSVs already carry
    # review_subclass / historical_label / confidence_adjustment_applied
    # for candidate selection. Entirely optional, off by default.
    materialized_outputs_mutated_after_reports = (
        _maybe_run_smtp_probing(
            run_dir=result.run_dir,
            config=config,
            logger=logger,
        )
        or materialized_outputs_mutated_after_reports
    )

    # --- Phase 6 (V2.5): probabilistic deliverability model. --- #
    # Legacy post-pass enrichment is gated by V2.9.4 because it rewrites
    # materialized CSVs after pipeline-generated reports/client outputs.
    materialized_outputs_mutated_after_reports = (
        _maybe_run_probability_model(
            run_dir=result.run_dir,
            config=config,
            logger=logger,
        )
        or materialized_outputs_mutated_after_reports
    )

    # --- Phase 7 (V2.6): Decision Engine (automated actions layer). --- #
    # Legacy post-pass enrichment is gated by V2.9.4 for the same reason:
    # it can rewrite final_action / decision_reason on already-generated
    # deliverable CSVs.
    materialized_outputs_mutated_after_reports = (
        _maybe_run_decision_engine(
            run_dir=result.run_dir,
            config=config,
            logger=logger,
        )
        or materialized_outputs_mutated_after_reports
    )

    _maybe_write_artifact_consistency_report(
        run_dir=result.run_dir,
        post_pass_mutation_enabled=post_pass_mutation_enabled,
        materialized_outputs_mutated_after_reports=(
            materialized_outputs_mutated_after_reports
        ),
        artifacts_regenerated_after_post_passes=False,
        logger=logger,
    )

    # --- Phase 8: post-run discovery. --- #
    # Collect after safe post-run metadata so JobResult points at one
    # consistent artifact state.
    artifacts = collect_job_artifacts(result.run_dir)
    summary = load_job_summary(result.run_dir)

    return JobResult(
        job_id=request.job_id,
        status=JobStatus.COMPLETED,
        input_filename=input_filename,
        run_dir=result.run_dir,
        summary=summary,
        artifacts=artifacts,
        error=None,
        started_at=started_at,
        finished_at=_now_utc(),
    )


def _post_pass_materialized_mutation_enabled(config: Any) -> bool:
    post_passes_cfg = getattr(config, "post_passes", None)
    if post_passes_cfg is None:
        return False
    return bool(getattr(post_passes_cfg, "mutate_materialized_outputs", False))


def _maybe_write_artifact_consistency_report(
    *,
    run_dir: Path,
    post_pass_mutation_enabled: bool,
    materialized_outputs_mutated_after_reports: bool,
    artifacts_regenerated_after_post_passes: bool,
    logger: logging.Logger,
) -> Path | None:
    """Write V2.9.4 consistency metadata. Never blocks job completion."""

    payload = {
        "report_version": "v2.9.4",
        "materialized_outputs_mutated_after_reports": bool(
            materialized_outputs_mutated_after_reports
        ),
        "post_pass_mutation_enabled": bool(post_pass_mutation_enabled),
        "artifacts_regenerated_after_post_passes": bool(
            artifacts_regenerated_after_post_passes
        ),
    }
    try:
        path = Path(run_dir) / _REPORT_NAMES["artifact_consistency"]
        path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return path
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("artifact consistency report failed: %s", exc)
        return None


def _maybe_update_domain_history(
    run_dir: Path,
    config: Any,
    logger: logging.Logger,
) -> bool:
    """Invoke the V2 history layer. Returns True if it rewrote CSVs."""
    history_cfg = getattr(config, "history", None)
    if history_cfg is None or not getattr(history_cfg, "enabled", False):
        return False
    try:
        # Local import keeps V1 boot paths free of V2 dependencies.
        from .validation_v2 import (
            AdjustmentConfig,
            DomainHistoryStore,
            update_history_from_run,
        )

        sqlite_path = Path(history_cfg.sqlite_path)
        if not sqlite_path.is_absolute():
            project_root = resolve_project_paths().project_root
            sqlite_path = (project_root / sqlite_path).resolve()

        allow_materialized_mutation = _post_pass_materialized_mutation_enabled(config)
        apply_history_adjustment = bool(
            history_cfg.apply_light_confidence_adjustment
        ) and allow_materialized_mutation
        if (
            bool(history_cfg.apply_light_confidence_adjustment)
            and not allow_materialized_mutation
        ):
            logger.info(
                "post_passes: skipping history CSV adjustment because "
                "post_passes.mutate_materialized_outputs=false"
            )

        # Phase 2: build the adjustment config from top-level thresholds
        # plus the history knobs. V2.9.4 gates its CSV-rewrite mode.
        adjustment_config = AdjustmentConfig(
            apply=apply_history_adjustment,
            max_positive_adjustment=int(history_cfg.max_positive_adjustment),
            max_negative_adjustment=int(history_cfg.max_negative_adjustment),
            min_observations_for_adjustment=int(history_cfg.min_observations_for_adjustment),
            allow_bucket_flip_from_history=bool(history_cfg.allow_bucket_flip_from_history),
            high_confidence_threshold=int(config.high_confidence_threshold),
            review_threshold=int(config.review_threshold),
        )

        store = DomainHistoryStore(sqlite_path)
        try:
            result = update_history_from_run(
                run_dir=run_dir,
                store=store,
                write_summary_report=bool(history_cfg.write_summary_report),
                max_positive_adjustment=int(history_cfg.max_positive_adjustment),
                max_negative_adjustment=int(history_cfg.max_negative_adjustment),
                adjustment_config=adjustment_config,
                write_adjustment_report=(
                    bool(history_cfg.write_adjustment_report)
                    and apply_history_adjustment
                ),
                logger=logger,
            )
            return getattr(result, "adjustment_stats", None) is not None
        finally:
            store.close()
    except Exception as exc:  # pragma: no cover - defensive guard
        logger.warning("history layer update failed: %s", exc)
        return False


def _maybe_run_smtp_probing(
    run_dir: Path,
    config: Any,
    logger: logging.Logger,
) -> bool:
    """Invoke the legacy V2.4 SMTP post-pass. Returns True if it rewrote CSVs."""
    smtp_cfg = getattr(config, "smtp_probe", None)
    if smtp_cfg is None or not getattr(smtp_cfg, "enabled", False):
        return False
    if not _post_pass_materialized_mutation_enabled(config):
        logger.info(
            "post_passes: skipping SMTP post-pass because "
            "post_passes.mutate_materialized_outputs=false"
        )
        return False
    try:
        from .validation_v2 import SMTPProbeConfig, run_smtp_probing_pass

        runtime_cfg = SMTPProbeConfig(
            enabled=bool(smtp_cfg.enabled),
            dry_run=bool(smtp_cfg.dry_run),
            sample_size=int(smtp_cfg.sample_size),
            max_per_domain=int(smtp_cfg.max_per_domain),
            timeout_seconds=float(smtp_cfg.timeout_seconds),
            rate_limit_per_second=float(smtp_cfg.rate_limit_per_second),
            retries=int(smtp_cfg.retries),
            negative_adjustment_trigger_threshold=int(
                smtp_cfg.negative_adjustment_trigger_threshold
            ),
            sender_address=str(smtp_cfg.sender_address),
        )
        result = run_smtp_probing_pass(
            run_dir=run_dir,
            config=runtime_cfg,
            logger=logger,
        )
        return bool(result is not None and result.candidates_selected > 0)
    except Exception as exc:  # pragma: no cover - defensive guard
        logger.warning("smtp probing pass failed: %s", exc)
        return False


def _maybe_run_probability_model(
    run_dir: Path,
    config: Any,
    logger: logging.Logger,
) -> bool:
    """Invoke the legacy V2.5 probability post-pass. Returns True if it rewrote CSVs."""
    pc = getattr(config, "probability", None)
    if pc is None or not getattr(pc, "enabled", False):
        return False
    if not _post_pass_materialized_mutation_enabled(config):
        logger.info(
            "post_passes: skipping probability post-pass because "
            "post_passes.mutate_materialized_outputs=false"
        )
        return False
    try:
        from .validation_v2 import ProbabilityConfig, run_probability_pass

        runtime_cfg = ProbabilityConfig(
            enabled=bool(pc.enabled),
            high_threshold=float(pc.high_threshold),
            medium_threshold=float(pc.medium_threshold),
            write_summary_report=bool(pc.write_summary_report),
        )
        result = run_probability_pass(
            run_dir=run_dir,
            config=runtime_cfg,
            logger=logger,
        )
        return bool(result is not None and result.rows_processed > 0)
    except Exception as exc:  # pragma: no cover - defensive guard
        logger.warning("probability pass failed: %s", exc)
        return False


def _maybe_run_decision_engine(
    run_dir: Path,
    config: Any,
    logger: logging.Logger,
) -> bool:
    """Invoke the legacy V2.6 decision post-pass. Returns True if it rewrote CSVs."""
    dc = getattr(config, "decision", None)
    if dc is None or not getattr(dc, "enabled", False):
        return False
    if not _post_pass_materialized_mutation_enabled(config):
        logger.info(
            "post_passes: skipping decision post-pass because "
            "post_passes.mutate_materialized_outputs=false"
        )
        return False
    try:
        from .validation_v2 import DecisionConfig, run_decision_pass

        runtime_cfg = DecisionConfig(
            enabled=bool(dc.enabled),
            approve_threshold=float(dc.approve_threshold),
            review_threshold=float(dc.review_threshold),
            enable_bucket_override=bool(dc.enable_bucket_override),
            write_summary_report=bool(dc.write_summary_report),
        )
        result = run_decision_pass(
            run_dir=run_dir,
            config=runtime_cfg,
            logger=logger,
        )
        return bool(result is not None and result.rows_processed > 0)
    except Exception as exc:  # pragma: no cover - defensive guard
        logger.warning("decision engine pass failed: %s", exc)
        return False


# --------------------------------------------------------------------------- #
# JSON serialisation
# --------------------------------------------------------------------------- #


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serialisable")


def job_result_to_dict(result: JobResult) -> dict[str, Any]:
    """Serialise a :class:`JobResult` to a plain JSON-friendly dict.

    * ``Path`` values become strings.
    * ``datetime`` values become ISO-8601 strings.
    * Status / error-type values are already strings by construction.
    * Nested dataclasses are expanded via :func:`dataclasses.asdict`.
    """

    raw = asdict(result)
    # Round-trip through ``json`` to coerce ``Path`` / ``datetime``
    # instances inside nested dicts and lists in one place.
    return json.loads(json.dumps(raw, default=_json_default))


def build_client_package_for_job(
    run_dir: str | Path,
    output_dir: str | Path | None = None,
) -> dict[str, Any]:
    """V2.9.6 — boundary entry point for the client delivery package.

    Wraps :func:`app.client_package_builder.build_client_delivery_package`
    and returns a JSON-friendly dict (paths as strings, no dataclasses).

    Filtering is performed strictly through the V2.9.5 artifact
    classification contract; no operator-only, technical-debug, or
    internal artifact is included even if exposed by legacy routes.
    """
    from .client_package_builder import build_client_delivery_package

    result = build_client_delivery_package(run_dir, output_dir=output_dir)
    return result.to_dict()


def run_operator_review_for_job(
    run_dir: str | Path,
    package_dir: str | Path | None = None,
) -> dict[str, Any]:
    """V2.9.7 — boundary entry point for the operator review gate.

    Wraps :func:`app.operator_review_gate.run_operator_review_gate` and
    returns a JSON-friendly dict (paths as strings, no dataclasses).

    The gate evaluates the V2.9.6 client package plus surrounding
    consistency / SMTP / V2 metadata and writes
    ``operator_review_summary.json`` into ``run_dir``.
    """
    from .operator_review_gate import run_operator_review_gate

    result = run_operator_review_gate(run_dir, package_dir=package_dir)
    return result.to_dict()


def build_feedback_domain_intel_preview_for_job(
    feedback_store_path: str | Path,
    output_dir: str | Path | None = None,
) -> dict[str, Any]:
    """V2.9.8 — boundary entry point for the feedback bridge preview.

    Wraps :func:`app.feedback_domain_intel_preview.build_feedback_domain_intel_preview`
    and returns a JSON-friendly dict (paths as strings, no dataclasses).

    The preview reads V2.7 ``BounceOutcomeStore`` aggregates and runs
    each through :func:`bounce_aggregate_to_domain_intel`. It does not
    mutate the store, the pipeline, or any V2 runtime state.
    """
    from .feedback_domain_intel_preview import (
        build_feedback_domain_intel_preview,
    )

    result = build_feedback_domain_intel_preview(
        feedback_store_path,
        output_dir=output_dir,
    )
    return result.to_dict()


def run_rollout_preflight(
    input_path: str | Path,
    *,
    output_dir: str | Path | None = None,
    config_path: str | Path | None = None,
    operator_confirmed_large_run: bool = False,
    smtp_port_verified: bool = False,
) -> dict[str, Any]:
    """Run V2.9.2 rollout preflight and return a JSON-friendly dict.

    This helper is deliberately not wired into :func:`run_cleaning_job`.
    It gives operators and future HTTP callers an explicit pre-run check
    without changing existing pipeline execution behavior.
    """
    from .rollout.preflight import run_preflight_check

    project_paths = resolve_project_paths()
    config = load_config(
        config_path=Path(config_path) if config_path else None,
        base_dir=project_paths.project_root,
    )
    result = run_preflight_check(
        input_path,
        config=config,
        output_dir=output_dir,
        operator_confirmed_large_run=operator_confirmed_large_run,
        smtp_port_verified=smtp_port_verified,
    )
    return result.to_dict()


def ingest_bounce_feedback(
    feedback_csv_path: str | Path,
    *,
    config_path: str | Path | None = None,
) -> dict[str, Any]:
    """V2.7 — boundary entry point for bounce outcome ingestion.

    Out-of-band operator job. Loads config (so ``bounce_ingestion``
    settings are honoured), opens the dedicated SQLite store at the
    configured path, ingests the CSV, and returns a dict-form
    :class:`~app.validation_v2.feedback.IngestionSummary`.

    The function never raises on per-row failures — invalid emails,
    unrecognised outcomes, missing fields are counted in the summary.
    Ingestion-level failures (file not found, sqlite open error)
    populate ``error`` instead of raising. The cleaning pipeline is
    completely unaffected: this function is only invoked when an
    operator triggers a feedback ingestion.

    Parameters
    ----------
    feedback_csv_path:
        Path to the bounce-outcome CSV. Required columns: ``email``
        (or ``Email``) and ``outcome`` (or ``status``). Optional:
        ``bounce_type``, ``smtp_code``, ``reason``, ``campaign_id``,
        ``timestamp``, ``provider``.
    config_path:
        Optional YAML config override. Defaults to
        ``configs/default.yaml``.

    Returns
    -------
    dict
        ``IngestionSummary.__dict__`` (counts + error). Always a
        plain dict, ready to serialise to JSON.
    """
    from .validation_v2.feedback import (
        BounceOutcomeStore,
        ingest_bounce_outcomes,
    )

    started_at = _now_utc()
    try:
        project_paths = resolve_project_paths()
        cfg = load_config(
            config_path=Path(config_path) if config_path else None,
            base_dir=project_paths.project_root,
        )
    except Exception as exc:  # pragma: no cover - defensive
        return {
            "error": f"config_error:{type(exc).__name__}:{exc}",
            "started_at": started_at.isoformat(),
            "finished_at": _now_utc().isoformat(),
        }

    bounce_cfg = getattr(cfg, "bounce_ingestion", None)
    if bounce_cfg is None or not getattr(bounce_cfg, "enabled", True):
        return {
            "error": "bounce_ingestion_disabled",
            "started_at": started_at.isoformat(),
            "finished_at": _now_utc().isoformat(),
        }

    # Resolve relative store paths against the project root so the
    # default config works without a fully-qualified path.
    raw_store_path = Path(bounce_cfg.store_path)
    if not raw_store_path.is_absolute():
        store_path = (project_paths.project_root / raw_store_path).resolve()
    else:
        store_path = raw_store_path

    try:
        store = BounceOutcomeStore(store_path)
    except Exception as exc:
        return {
            "error": f"store_open_error:{type(exc).__name__}:{exc}",
            "store_path": str(store_path),
            "started_at": started_at.isoformat(),
            "finished_at": _now_utc().isoformat(),
        }

    try:
        summary = ingest_bounce_outcomes(
            feedback_csv_path,
            history_store=store,
            config=bounce_cfg,
        )
    finally:
        store.close()

    payload = dict(asdict(summary))
    payload["store_path"] = str(store_path)
    payload["started_at"] = started_at.isoformat()
    payload["finished_at"] = _now_utc().isoformat()
    return payload


__all__ = [
    "ClientOutputs",
    "JobArtifacts",
    "JobError",
    "JobErrorType",
    "JobRequest",
    "JobResult",
    "JobStatus",
    "JobSummary",
    "ReportFiles",
    "TechnicalCsvs",
    "build_client_package_for_job",
    "build_feedback_domain_intel_preview_for_job",
    "collect_job_artifacts",
    "ingest_bounce_feedback",
    "job_result_to_dict",
    "load_job_summary",
    "run_operator_review_for_job",
    "run_rollout_preflight",
    "run_cleaning_job",
]
