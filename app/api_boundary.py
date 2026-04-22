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
from datetime import datetime
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
    """Technical CSV deliverables produced by the pipeline."""

    clean_high_confidence: Path | None = None
    review_medium_confidence: Path | None = None
    removed_invalid: Path | None = None


@dataclass(slots=True)
class ClientOutputs:
    """Client-facing XLSX deliverables (may be absent on failure)."""

    valid_emails: Path | None = None
    review_emails: Path | None = None
    invalid_or_bounce_risk: Path | None = None
    summary_report: Path | None = None
    approved_original_format: Path | None = None


@dataclass(slots=True)
class ReportFiles:
    """Structured reporting files written alongside the CSV outputs."""

    processing_report_json: Path | None = None
    processing_report_csv: Path | None = None
    domain_summary: Path | None = None
    typo_corrections: Path | None = None
    duplicate_summary: Path | None = None


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
}

_CLIENT_OUTPUT_NAMES: dict[str, str] = {
    "valid_emails": "valid_emails.xlsx",
    "review_emails": "review_emails.xlsx",
    "invalid_or_bounce_risk": "invalid_or_bounce_risk.xlsx",
    "summary_report": "summary_report.xlsx",
    "approved_original_format": "approved_original_format.xlsx",
}

_REPORT_NAMES: dict[str, str] = {
    "processing_report_json": "processing_report.json",
    "processing_report_csv": "processing_report.csv",
    "domain_summary": "domain_summary.csv",
    "typo_corrections": "typo_corrections.csv",
    "duplicate_summary": "duplicate_summary.csv",
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


def _new_job_id() -> str:
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

    started_at = datetime.now()
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
            finished_at=datetime.now(),
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
            finished_at=datetime.now(),
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
            finished_at=datetime.now(),
        )

    # --- Phase 4: post-run discovery. --- #
    artifacts = collect_job_artifacts(result.run_dir)
    summary = load_job_summary(result.run_dir)

    # --- Phase 5 (V2): domain history update. --- #
    # Additive, guarded: any failure here must not affect the JobResult
    # or V1 outputs. Runs only when config.history.enabled.
    _maybe_update_domain_history(
        run_dir=result.run_dir,
        config=config,
        logger=logger,
    )

    # --- Phase 6 (V2.4): selective SMTP probing. --- #
    # Runs AFTER the history update so the CSVs already carry
    # review_subclass / historical_label / confidence_adjustment_applied
    # for candidate selection. Entirely optional, off by default.
    _maybe_run_smtp_probing(
        run_dir=result.run_dir,
        config=config,
        logger=logger,
    )

    # --- Phase 7 (V2.5): probabilistic deliverability model. --- #
    # Runs last so every V2 signal available on the CSV contributes.
    _maybe_run_probability_model(
        run_dir=result.run_dir,
        config=config,
        logger=logger,
    )

    # --- Phase 8 (V2.6): Decision Engine (automated actions layer). --- #
    # Consumes the Phase-5 probability and produces final_action +
    # decision_reason. Only reads/writes columns; never moves rows.
    _maybe_run_decision_engine(
        run_dir=result.run_dir,
        config=config,
        logger=logger,
    )

    return JobResult(
        job_id=request.job_id,
        status=JobStatus.COMPLETED,
        input_filename=input_filename,
        run_dir=result.run_dir,
        summary=summary,
        artifacts=artifacts,
        error=None,
        started_at=started_at,
        finished_at=datetime.now(),
    )


def _maybe_update_domain_history(
    run_dir: Path,
    config: Any,
    logger: logging.Logger,
) -> None:
    """Invoke the V2 history layer. Never raises; logs and returns on failure."""
    history_cfg = getattr(config, "history", None)
    if history_cfg is None or not getattr(history_cfg, "enabled", False):
        return
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

        # Phase 2: build the adjustment config from top-level thresholds
        # plus the history knobs.
        adjustment_config = AdjustmentConfig(
            apply=bool(history_cfg.apply_light_confidence_adjustment),
            max_positive_adjustment=int(history_cfg.max_positive_adjustment),
            max_negative_adjustment=int(history_cfg.max_negative_adjustment),
            min_observations_for_adjustment=int(history_cfg.min_observations_for_adjustment),
            allow_bucket_flip_from_history=bool(history_cfg.allow_bucket_flip_from_history),
            high_confidence_threshold=int(config.high_confidence_threshold),
            review_threshold=int(config.review_threshold),
        )

        store = DomainHistoryStore(sqlite_path)
        try:
            update_history_from_run(
                run_dir=run_dir,
                store=store,
                write_summary_report=bool(history_cfg.write_summary_report),
                max_positive_adjustment=int(history_cfg.max_positive_adjustment),
                max_negative_adjustment=int(history_cfg.max_negative_adjustment),
                adjustment_config=adjustment_config,
                write_adjustment_report=bool(history_cfg.write_adjustment_report),
                logger=logger,
            )
        finally:
            store.close()
    except Exception as exc:  # pragma: no cover - defensive guard
        logger.warning("history layer update failed: %s", exc)


def _maybe_run_smtp_probing(
    run_dir: Path,
    config: Any,
    logger: logging.Logger,
) -> None:
    """Invoke the V2.4 SMTP probing pass. Never raises."""
    smtp_cfg = getattr(config, "smtp_probe", None)
    if smtp_cfg is None or not getattr(smtp_cfg, "enabled", False):
        return
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
        run_smtp_probing_pass(run_dir=run_dir, config=runtime_cfg, logger=logger)
    except Exception as exc:  # pragma: no cover - defensive guard
        logger.warning("smtp probing pass failed: %s", exc)


def _maybe_run_probability_model(
    run_dir: Path,
    config: Any,
    logger: logging.Logger,
) -> None:
    """Invoke the V2.5 deliverability probability pass. Never raises."""
    pc = getattr(config, "probability", None)
    if pc is None or not getattr(pc, "enabled", False):
        return
    try:
        from .validation_v2 import ProbabilityConfig, run_probability_pass

        runtime_cfg = ProbabilityConfig(
            enabled=bool(pc.enabled),
            high_threshold=float(pc.high_threshold),
            medium_threshold=float(pc.medium_threshold),
            write_summary_report=bool(pc.write_summary_report),
        )
        run_probability_pass(run_dir=run_dir, config=runtime_cfg, logger=logger)
    except Exception as exc:  # pragma: no cover - defensive guard
        logger.warning("probability pass failed: %s", exc)


def _maybe_run_decision_engine(
    run_dir: Path,
    config: Any,
    logger: logging.Logger,
) -> None:
    """Invoke the V2.6 decision engine pass. Never raises."""
    dc = getattr(config, "decision", None)
    if dc is None or not getattr(dc, "enabled", False):
        return
    try:
        from .validation_v2 import DecisionConfig, run_decision_pass

        runtime_cfg = DecisionConfig(
            enabled=bool(dc.enabled),
            approve_threshold=float(dc.approve_threshold),
            review_threshold=float(dc.review_threshold),
            enable_bucket_override=bool(dc.enable_bucket_override),
            write_summary_report=bool(dc.write_summary_report),
        )
        run_decision_pass(run_dir=run_dir, config=runtime_cfg, logger=logger)
    except Exception as exc:  # pragma: no cover - defensive guard
        logger.warning("decision engine pass failed: %s", exc)


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
    "collect_job_artifacts",
    "job_result_to_dict",
    "load_job_summary",
    "run_cleaning_job",
]
