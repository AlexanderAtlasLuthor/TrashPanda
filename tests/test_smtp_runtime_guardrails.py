"""V2.9.3 SMTP runtime guardrail tests.

All probes are injected mocks. These tests validate observability only;
they do not exercise live SMTP or change V2 routing policy.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from app.api_boundary import collect_job_artifacts
from app.config import AppConfig, SMTPProbeConfig
from app.engine import ChunkPayload, PipelineContext
from app.engine.stages import DecisionStage, SMTPVerificationStage
from app.engine.stages.catch_all_detection import CATCH_ALL_STATUS_NOT_TESTED
from app.engine.stages.smtp_verification import (
    SMTP_STATUS_BLOCKED,
    SMTP_STATUS_CATCH_ALL_POSSIBLE,
    SMTP_STATUS_ERROR,
    SMTP_STATUS_INVALID,
    SMTP_STATUS_NOT_TESTED,
    SMTP_STATUS_TEMP_FAIL,
    SMTP_STATUS_TIMEOUT,
    SMTP_STATUS_VALID,
)
from app.models import RunContext
from app.pipeline import EmailCleaningPipeline
from app.smtp_runtime import (
    SMTP_RUNTIME_SUMMARY_EXTRAS_KEY,
    SMTP_RUNTIME_SUMMARY_FILENAME,
    SMTP_RETRY_EXECUTION_ENABLED,
    SMTPRuntimeSummary,
    write_smtp_runtime_summary,
)
from app.validation_v2.smtp_probe import SMTPResult


def _config(**smtp_overrides: Any) -> AppConfig:
    values = {
        "enabled": True,
        "dry_run": False,
        "max_candidates_per_run": None,
        "timeout_seconds": 10.0,
        "rate_limit_per_second": 2.0,
        "retry_temp_failures": True,
        "max_retries": 1,
    }
    values.update(smtp_overrides)
    return AppConfig(smtp_probe=SMTPProbeConfig(**values))


def _candidate_frame(count: int = 1) -> pd.DataFrame:
    emails = [f"user{i}@example.com" for i in range(count)]
    return pd.DataFrame(
        {
            "email": emails,
            "domain": ["example.com"] * count,
            "corrected_domain": ["example.com"] * count,
            "syntax_valid": pd.array([True] * count, dtype="boolean"),
            "domain_matches_input_column": pd.array([True] * count, dtype="boolean"),
            "typo_detected": pd.array([False] * count, dtype="boolean"),
            "typo_corrected": pd.array([False] * count, dtype="boolean"),
            "has_mx_record": pd.array([True] * count, dtype="boolean"),
            "has_a_record": pd.array([False] * count, dtype="boolean"),
            "domain_exists": pd.array([True] * count, dtype="boolean"),
            "dns_error": [None] * count,
            "hard_fail": pd.array([False] * count, dtype="boolean"),
            "score": [90] * count,
            "preliminary_bucket": ["high_confidence"] * count,
            "bucket_v2": ["high_confidence"] * count,
            "hard_stop_v2": pd.array([False] * count, dtype="bool"),
        }
    )


def _result_for_status(status: str) -> SMTPResult:
    if status == SMTP_STATUS_VALID:
        return SMTPResult(True, 250, "ok", False, False)
    if status == SMTP_STATUS_INVALID:
        return SMTPResult(False, 550, "no such user", False, False)
    if status == SMTP_STATUS_BLOCKED:
        return SMTPResult(False, 550, "blocked by policy", False, True)
    if status == SMTP_STATUS_TIMEOUT:
        return SMTPResult(False, None, "connection timed out", False, True)
    if status == SMTP_STATUS_TEMP_FAIL:
        return SMTPResult(False, 451, "try later", False, True)
    if status == SMTP_STATUS_CATCH_ALL_POSSIBLE:
        return SMTPResult(True, 250, "ok", True, False)
    if status == SMTP_STATUS_ERROR:
        return SMTPResult(False, None, "unexpected failure", False, True)
    raise AssertionError(f"unsupported status fixture: {status}")


def _static_probe(status: str):
    calls: list[str] = []

    def probe(email: str, **_kwargs: object) -> SMTPResult:
        calls.append(email)
        return _result_for_status(status)

    probe.calls = calls  # type: ignore[attr-defined]
    return probe


def _sequence_probe(statuses: list[str]):
    remaining = list(statuses)
    calls: list[str] = []

    def probe(email: str, **_kwargs: object) -> SMTPResult:
        calls.append(email)
        return _result_for_status(remaining.pop(0))

    probe.calls = calls  # type: ignore[attr-defined]
    return probe


def _run_smtp_stage(
    frame: pd.DataFrame,
    *,
    config: AppConfig | None = None,
    probe_status: str = SMTP_STATUS_VALID,
    probe=None,
) -> tuple[pd.DataFrame, PipelineContext, SMTPRuntimeSummary]:
    ctx = PipelineContext(config=config or _config(), extras={})
    stage = SMTPVerificationStage(
        probe_fn=probe or _static_probe(probe_status),
        sleep_fn=lambda _seconds: None,
        clock_fn=lambda: 0.0,
    )
    out = stage.run(ChunkPayload(frame=frame), ctx).frame
    summary = ctx.extras[SMTP_RUNTIME_SUMMARY_EXTRAS_KEY]
    assert isinstance(summary, SMTPRuntimeSummary)
    return out, ctx, summary


def _add_decision_dependencies(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["catch_all_status"] = [CATCH_ALL_STATUS_NOT_TESTED] * len(out)
    out["catch_all_flag"] = pd.array([False] * len(out), dtype="boolean")
    out["domain_risk_level"] = ["low"] * len(out)
    out["domain_cold_start"] = pd.array([False] * len(out), dtype="boolean")
    return out


def test_summary_object_defaults_include_config_and_zero_counters() -> None:
    cfg = _config(
        max_candidates_per_run=250,
        timeout_seconds=7.5,
        rate_limit_per_second=3.0,
        retry_temp_failures=True,
        max_retries=2,
    )

    summary = SMTPRuntimeSummary.from_config(cfg)

    assert summary.smtp_enabled is True
    assert summary.smtp_dry_run is False
    assert summary.smtp_candidate_cap == 250
    assert summary.smtp_timeout_seconds == 7.5
    assert summary.smtp_rate_limit_per_second == 3.0
    assert summary.smtp_retry_temp_failures_configured is True
    assert summary.smtp_max_retries_configured == 2
    assert summary.smtp_retry_execution_enabled is SMTP_RETRY_EXECUTION_ENABLED
    assert summary.smtp_retries_executed == 0
    assert summary.smtp_candidates_seen == 0
    assert summary.smtp_candidates_attempted == 0
    assert summary.smtp_not_tested_count == 0
    assert summary.smtp_inconclusive_count == 0


def test_candidate_seen_attempted_and_valid_counts() -> None:
    _out, _ctx, summary = _run_smtp_stage(_candidate_frame(3))

    assert summary.smtp_candidates_seen == 3
    assert summary.smtp_candidates_attempted == 3
    assert summary.smtp_candidates_skipped_by_cap == 0
    assert summary.smtp_valid_count == 3


def test_cap_respected_counted_and_skipped_rows_do_not_auto_approve() -> None:
    frame, _ctx, summary = _run_smtp_stage(
        _candidate_frame(3),
        config=_config(max_candidates_per_run=1),
    )

    assert summary.smtp_candidates_seen == 3
    assert summary.smtp_candidates_attempted == 1
    assert summary.smtp_candidates_skipped_by_cap == 2
    assert summary.smtp_not_tested_count == 2
    assert frame["smtp_status"].tolist() == [
        SMTP_STATUS_VALID,
        SMTP_STATUS_NOT_TESTED,
        SMTP_STATUS_NOT_TESTED,
    ]
    assert frame["smtp_was_candidate"].tolist() == [True, True, True]

    decision_frame = _add_decision_dependencies(frame)
    decision_out = DecisionStage().run(
        ChunkPayload(frame=decision_frame),
        PipelineContext(config=_config()),
    ).frame

    skipped_actions = decision_out.iloc[1:]["final_action"].tolist()
    assert all(action != "auto_approve" for action in skipped_actions)


def test_result_statuses_are_counted_with_inconclusive_rollup() -> None:
    statuses = [
        SMTP_STATUS_VALID,
        SMTP_STATUS_INVALID,
        SMTP_STATUS_BLOCKED,
        SMTP_STATUS_TIMEOUT,
        SMTP_STATUS_TEMP_FAIL,
        SMTP_STATUS_CATCH_ALL_POSSIBLE,
        SMTP_STATUS_ERROR,
    ]
    _out, _ctx, summary = _run_smtp_stage(
        _candidate_frame(len(statuses)),
        probe=_sequence_probe(statuses),
    )

    assert summary.smtp_valid_count == 1
    assert summary.smtp_invalid_count == 1
    assert summary.smtp_blocked_count == 1
    assert summary.smtp_timeout_count == 1
    assert summary.smtp_temp_fail_count == 1
    assert summary.smtp_catch_all_possible_count == 1
    assert summary.smtp_error_count == 1
    assert summary.smtp_inconclusive_count == 5


def test_retry_config_is_reported_without_claiming_execution() -> None:
    cfg = _config(retry_temp_failures=True, max_retries=1)
    _out, _ctx, summary = _run_smtp_stage(_candidate_frame(1), config=cfg)

    assert summary.smtp_retry_temp_failures_configured is True
    assert summary.smtp_max_retries_configured == 1
    assert summary.smtp_retry_execution_enabled is False
    assert summary.smtp_retries_executed == 0


def test_summary_is_stored_in_context_and_json_serializable() -> None:
    _out, ctx, summary = _run_smtp_stage(_candidate_frame(1))

    assert ctx.extras[SMTP_RUNTIME_SUMMARY_EXTRAS_KEY] is summary
    payload = summary.to_dict()
    encoded = json.dumps(payload)
    assert "smtp_candidates_seen" in encoded


def test_summary_file_is_written(tmp_path: Path) -> None:
    summary = SMTPRuntimeSummary.from_config(_config(max_candidates_per_run=5))
    summary.record_candidate_seen()
    summary.record_probe_attempt(SMTP_STATUS_VALID)

    path = write_smtp_runtime_summary(tmp_path, summary)
    data = json.loads(path.read_text(encoding="utf-8"))

    assert path == tmp_path / SMTP_RUNTIME_SUMMARY_FILENAME
    assert data["report_version"] == "v2.9.3"
    assert data["smtp_candidate_cap"] == 5
    assert data["smtp_candidates_seen"] == 1
    assert data["smtp_valid_count"] == 1


def test_pipeline_writes_smtp_runtime_summary_file(tmp_path: Path, monkeypatch) -> None:
    from app.engine.stages import enrichment as enrichment_mod

    def fake_dns_enrichment(frame: pd.DataFrame, **_kwargs: object) -> pd.DataFrame:
        out = frame.copy()
        out["dns_check_performed"] = True
        out["domain_exists"] = True
        out["has_mx_record"] = True
        out["has_a_record"] = False
        out["dns_error"] = None
        return out

    monkeypatch.setattr(
        enrichment_mod,
        "apply_dns_enrichment_column",
        fake_dns_enrichment,
    )

    input_path = tmp_path / "input.csv"
    input_path.write_text("email\nalice.smith@gmail.com\n", encoding="utf-8")
    run_dir = tmp_path / "run"
    logs_dir = tmp_path / "logs"
    temp_dir = tmp_path / "temp"
    for path in (run_dir, logs_dir, temp_dir):
        path.mkdir(parents=True, exist_ok=True)

    cfg = _config(dry_run=True, max_candidates_per_run=10)
    logger = logging.getLogger("smtp_runtime_guardrails")
    logger.addHandler(logging.NullHandler())
    run_context = RunContext(
        run_id="run_smtp_runtime_guardrails",
        run_dir=run_dir,
        logs_dir=logs_dir,
        temp_dir=temp_dir,
        staging_db_path=run_dir / "staging.sqlite3",
        started_at=datetime.now(),
    )

    EmailCleaningPipeline(config=cfg, logger=logger).run(
        input_file=input_path,
        run_context=run_context,
    )

    report_path = run_dir / SMTP_RUNTIME_SUMMARY_FILENAME
    data = json.loads(report_path.read_text(encoding="utf-8"))
    assert data["smtp_enabled"] is True
    assert data["smtp_dry_run"] is True
    assert data["smtp_candidates_seen"] == 1
    assert data["smtp_candidates_attempted"] == 1


def test_artifact_discovery_finds_smtp_runtime_summary(tmp_path: Path) -> None:
    (tmp_path / SMTP_RUNTIME_SUMMARY_FILENAME).write_text(
        json.dumps(SMTPRuntimeSummary().to_dict()),
        encoding="utf-8",
    )

    artifacts = collect_job_artifacts(tmp_path)

    assert artifacts.reports.smtp_runtime_summary == (
        tmp_path / SMTP_RUNTIME_SUMMARY_FILENAME
    )


def test_runtime_summary_module_imports_no_network_modules() -> None:
    source = Path("app/smtp_runtime.py").read_text(encoding="utf-8")

    assert "import socket" not in source
    assert "import smtplib" not in source
