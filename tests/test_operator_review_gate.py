"""V2.9.7 — operator review gate tests."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd
import pytest

from app import operator_review_gate
from app.api_boundary import (
    collect_job_artifacts,
    run_operator_review_for_job,
)
from app.client_package_builder import build_client_delivery_package
from app.operator_review_gate import (
    OperatorReviewIssue,
    OperatorReviewResult,
    SMTP_COVERAGE_THRESHOLD,
    STATUS_BLOCK,
    STATUS_READY,
    STATUS_WARN,
    run_operator_review_gate,
)


# --------------------------------------------------------------------------- #
# Fixture helpers
# --------------------------------------------------------------------------- #


def _xlsx(path: Path, n: int) -> None:
    pd.DataFrame({"email": [f"u{i}@x.com" for i in range(n)]}).to_excel(
        path, sheet_name="emails", index=False
    )


def _summary_xlsx(path: Path) -> None:
    pd.DataFrame({"metric": ["x"], "value": [1]}).to_excel(
        path, sheet_name="totals", index=False
    )


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _make_clean_run_dir(
    run_dir: Path,
    *,
    valid_rows: int = 5,
    review_rows: int = 0,
    rejected_rows: int = 0,
    include_approved_original: bool = True,
    include_artifact_consistency: bool = True,
    include_smtp_summary: bool = True,
    include_v2_summary: bool = True,
    smtp_seen: int = 10,
    smtp_attempted: int = 10,
    smtp_valid: int = 10,
    smtp_inconclusive: int = 0,
    catch_all_risk: int = 0,
    high_risk_domains: int = 0,
    cold_start: int = 0,
    duplicates: int = 0,
    hard_fails: int = 0,
    consistency_overrides: dict | None = None,
) -> None:
    """Populate ``run_dir`` with a complete-ish set of run artifacts.

    Then build the client package so the gate has something to inspect.
    """
    run_dir.mkdir(parents=True, exist_ok=True)
    _xlsx(run_dir / "valid_emails.xlsx", valid_rows)
    _xlsx(run_dir / "review_emails.xlsx", review_rows)
    _xlsx(run_dir / "invalid_or_bounce_risk.xlsx", rejected_rows)
    _xlsx(run_dir / "duplicate_emails.xlsx", 0)
    _xlsx(run_dir / "hard_fail_emails.xlsx", 0)
    _summary_xlsx(run_dir / "summary_report.xlsx")
    if include_approved_original:
        _xlsx(run_dir / "approved_original_format.xlsx", max(valid_rows, 1))

    if include_artifact_consistency:
        consistency_payload = {
            "report_version": "v2.9.4",
            "materialized_outputs_mutated_after_reports": False,
            "post_pass_mutation_enabled": False,
            "artifacts_regenerated_after_post_passes": False,
        }
        if consistency_overrides:
            consistency_payload.update(consistency_overrides)
        _write_json(run_dir / "artifact_consistency.json", consistency_payload)

    if include_smtp_summary:
        _write_json(
            run_dir / "smtp_runtime_summary.json",
            {
                "smtp_candidates_seen": smtp_seen,
                "smtp_candidates_attempted": smtp_attempted,
                "smtp_valid_count": smtp_valid,
                "smtp_inconclusive_count": smtp_inconclusive,
            },
        )

    if include_v2_summary:
        _write_json(
            run_dir / "v2_deliverability_summary.json",
            {
                "catch_all_risk_count": catch_all_risk,
                "high_risk_domain_count": high_risk_domains,
                "cold_start_count": cold_start,
                "duplicate_count": duplicates,
                "hard_fail_count": hard_fails,
            },
        )

    # Build the client package so the gate has a manifest to read.
    build_client_delivery_package(run_dir)


def _codes(result: OperatorReviewResult) -> set[str]:
    return {i.code for i in result.issues}


def _block_codes(result: OperatorReviewResult) -> set[str]:
    return {i.code for i in result.issues if i.severity == "block"}


# --------------------------------------------------------------------------- #
# Test 1 — Ready package passes
# --------------------------------------------------------------------------- #


def test_ready_package_passes(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _make_clean_run_dir(run_dir)

    result = run_operator_review_gate(run_dir)

    assert result.status == STATUS_READY, result.issues
    assert result.ready_for_client is True
    assert result.issues == ()
    assert result.safe_count == 5
    assert result.review_count == 0
    assert result.smtp_coverage_rate == 1.0
    assert result.approved_original_present is True


# --------------------------------------------------------------------------- #
# Test 2 — Missing package blocks
# --------------------------------------------------------------------------- #


def test_missing_package_blocks(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    # No package built.

    result = run_operator_review_gate(run_dir)

    assert result.status == STATUS_BLOCK
    assert result.ready_for_client is False
    assert "client_package_missing" in _block_codes(result)
    assert result.package_dir is None


# --------------------------------------------------------------------------- #
# Test 3 — Missing manifest blocks
# --------------------------------------------------------------------------- #


def test_missing_manifest_blocks(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    pkg_dir = run_dir / "client_delivery_package"
    pkg_dir.mkdir()
    # Manifest not written.

    result = run_operator_review_gate(run_dir)

    assert result.status == STATUS_BLOCK
    assert "client_package_manifest_missing" in _block_codes(result)


# --------------------------------------------------------------------------- #
# Test 4 — Non-client-safe file in package blocks
# --------------------------------------------------------------------------- #


def test_non_client_safe_file_in_package_blocks(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _make_clean_run_dir(run_dir)
    # Pollute the built package with an operator_only file.
    pkg = run_dir / "client_delivery_package"
    (pkg / "v2_deliverability_summary.json").write_text("{}", encoding="utf-8")

    result = run_operator_review_gate(run_dir)

    assert result.status == STATUS_BLOCK
    assert "client_package_contains_non_client_safe" in _block_codes(result)


# --------------------------------------------------------------------------- #
# Test 5 — Artifact consistency mutation blocks
# --------------------------------------------------------------------------- #


def test_artifact_consistency_mutation_blocks(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _make_clean_run_dir(
        run_dir,
        consistency_overrides={"materialized_outputs_mutated_after_reports": True},
    )

    result = run_operator_review_gate(run_dir)

    assert result.status == STATUS_BLOCK
    assert "artifact_consistency_failed" in _block_codes(result)


def test_post_pass_mutation_detected_blocks(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _make_clean_run_dir(
        run_dir,
        consistency_overrides={
            "post_pass_mutation_enabled": True,
            "artifacts_regenerated_after_post_passes": False,
        },
    )

    result = run_operator_review_gate(run_dir)

    assert result.status == STATUS_BLOCK
    assert "post_pass_mutation_detected" in _block_codes(result)


# --------------------------------------------------------------------------- #
# Test 6 — Approved original absent warns
# --------------------------------------------------------------------------- #


def test_approved_original_absent_warns(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _make_clean_run_dir(run_dir, include_approved_original=False)

    result = run_operator_review_gate(run_dir)

    codes = _codes(result)
    assert "approved_original_absent" in codes
    assert result.approved_original_present is False
    # Warn-only: no block issues.
    assert _block_codes(result) == set()
    assert result.status == STATUS_WARN


# --------------------------------------------------------------------------- #
# Test 7 — Safe count zero warns
# --------------------------------------------------------------------------- #


def test_safe_count_zero_warns(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _make_clean_run_dir(run_dir, valid_rows=0)

    result = run_operator_review_gate(run_dir)

    assert "safe_count_zero" in _codes(result)
    assert result.safe_count == 0
    assert result.status == STATUS_WARN


# --------------------------------------------------------------------------- #
# Test 8 — Review rows present warns
# --------------------------------------------------------------------------- #


def test_review_rows_present_warns(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _make_clean_run_dir(run_dir, review_rows=3)

    result = run_operator_review_gate(run_dir)

    assert "review_rows_present" in _codes(result)
    assert result.review_count == 3


# --------------------------------------------------------------------------- #
# Test 9 — Low SMTP coverage warns
# --------------------------------------------------------------------------- #


def test_low_smtp_coverage_warns(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _make_clean_run_dir(run_dir, smtp_seen=10, smtp_attempted=4)

    result = run_operator_review_gate(run_dir)

    assert "smtp_coverage_low" in _codes(result)
    assert result.smtp_coverage_rate is not None
    assert result.smtp_coverage_rate < SMTP_COVERAGE_THRESHOLD


# --------------------------------------------------------------------------- #
# Test 10 — Missing optional reports warn but do not block
# --------------------------------------------------------------------------- #


def test_missing_optional_reports_warn_not_block(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _make_clean_run_dir(
        run_dir,
        include_smtp_summary=False,
        include_v2_summary=False,
        include_artifact_consistency=False,
    )

    result = run_operator_review_gate(run_dir)

    codes = _codes(result)
    assert "smtp_runtime_summary_missing" in codes
    assert "v2_summary_missing" in codes
    assert "artifact_consistency_missing" in codes
    # None of these escalate to block.
    assert _block_codes(result) == set()
    assert result.status == STATUS_WARN


# --------------------------------------------------------------------------- #
# Test 11 — Risk metrics warn
# --------------------------------------------------------------------------- #


def test_risk_metrics_warn_from_v2_8_nested_shape(tmp_path: Path) -> None:
    """The real V2DeliverabilityReport.to_dict() shape is nested.

    Replicates ``catch_all_summary.catch_all_risk_count`` and
    ``domain_intelligence_summary.{high_risk_domain_count, cold_start_count}``
    so the gate is locked to the actual on-disk schema, not just flat
    fixture keys.
    """
    run_dir = tmp_path / "run"
    _make_clean_run_dir(run_dir, include_v2_summary=False)
    # Overwrite v2 summary with the nested V2.8 shape.
    _write_json(
        run_dir / "v2_deliverability_summary.json",
        {
            "report_version": "v2.8",
            "classification_summary": {"total_rows": 5, "by_final_action": {}},
            "catch_all_summary": {"catch_all_risk_count": 3},
            "domain_intelligence_summary": {
                "high_risk_domain_count": 2,
                "cold_start_count": 5,
            },
        },
    )

    result = run_operator_review_gate(run_dir)

    codes = _codes(result)
    assert "catch_all_risk_present" in codes
    assert "high_risk_domains_present" in codes
    assert "cold_start_domains_present" in codes
    assert result.catch_all_risk_count == 3
    assert result.high_risk_domain_count == 2
    assert result.cold_start_count == 5


def test_risk_metrics_warn(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _make_clean_run_dir(
        run_dir,
        catch_all_risk=3,
        high_risk_domains=2,
        cold_start=1,
    )

    result = run_operator_review_gate(run_dir)

    codes = _codes(result)
    assert "catch_all_risk_present" in codes
    assert "high_risk_domains_present" in codes
    assert "cold_start_domains_present" in codes
    assert result.catch_all_risk_count == 3
    assert result.high_risk_domain_count == 2
    assert result.cold_start_count == 1


# --------------------------------------------------------------------------- #
# Test 12 — Boundary callable JSON-friendly
# --------------------------------------------------------------------------- #


def test_boundary_callable_json_friendly(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _make_clean_run_dir(run_dir)

    payload = run_operator_review_for_job(run_dir)

    assert isinstance(payload, dict)
    json.dumps(payload)  # round-trip
    assert payload["status"] == STATUS_READY
    assert payload["ready_for_client"] is True
    assert isinstance(payload["issues"], list)


# --------------------------------------------------------------------------- #
# Test 13 — Operator review summary written
# --------------------------------------------------------------------------- #


def test_operator_review_summary_is_written(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _make_clean_run_dir(run_dir)

    result = run_operator_review_gate(run_dir)

    assert result.summary_path.is_file()
    payload = json.loads(result.summary_path.read_text(encoding="utf-8"))
    required = {
        "report_version",
        "generated_at",
        "ready_for_client",
        "status",
        "run_dir",
        "package_dir",
        "package_manifest_path",
        "summary_path",
        "issues",
        "safe_count",
        "review_count",
        "rejected_count",
        "duplicate_count",
        "hard_fail_count",
        "smtp_coverage_rate",
        "smtp_valid_count",
        "smtp_inconclusive_count",
        "catch_all_risk_count",
        "high_risk_domain_count",
        "cold_start_count",
        "approved_original_present",
        # V2.10.8.1 — partial readiness contract.
        "ready_for_client_partial",
        "partial_delivery_mode",
        "partial_delivery_requires_override",
        "partial_delivery_allowed_count",
        "partial_delivery_excluded_count",
        "partial_delivery_reason",
    }
    missing = required - set(payload.keys())
    assert not missing, f"missing summary fields: {missing}"
    assert payload["report_version"] == "v2.9.7"


# --------------------------------------------------------------------------- #
# Test 14 — Artifact discovery includes operator review summary
# --------------------------------------------------------------------------- #


def test_artifact_discovery_includes_operator_review_summary(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _make_clean_run_dir(run_dir)
    run_operator_review_gate(run_dir)

    artifacts = collect_job_artifacts(run_dir)

    expected = run_dir / "operator_review_summary.json"
    assert artifacts.reports.operator_review_summary == expected
    assert expected.is_file()


# --------------------------------------------------------------------------- #
# Test 15 — Client package builder excludes review summary
# --------------------------------------------------------------------------- #


def test_client_package_excludes_operator_review_summary(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    # Minimal client-safe set so the package builder has something to copy.
    _xlsx(run_dir / "valid_emails.xlsx", 1)
    _xlsx(run_dir / "review_emails.xlsx", 0)
    _xlsx(run_dir / "invalid_or_bounce_risk.xlsx", 0)
    # Review summary written BEFORE package build (simulates re-package).
    (run_dir / "operator_review_summary.json").write_text(
        json.dumps({"status": "warn"}), encoding="utf-8"
    )

    pkg_result = build_client_delivery_package(run_dir)

    assert not (pkg_result.package_dir / "operator_review_summary.json").exists()
    excluded_names = {x["filename"] for x in pkg_result.files_excluded}
    assert "operator_review_summary.json" in excluded_names
    excluded_entry = next(
        x for x in pkg_result.files_excluded
        if x["filename"] == "operator_review_summary.json"
    )
    assert excluded_entry["audience"] == "operator_only"


# --------------------------------------------------------------------------- #
# Test 16 — No live network
# --------------------------------------------------------------------------- #


def test_no_network_imports_in_gate() -> None:
    src = Path(operator_review_gate.__file__).read_text(encoding="utf-8")
    forbidden_tokens = (
        "import socket",
        "from socket",
        "import smtplib",
        "from smtplib",
        "import urllib.request",
        "from urllib.request",
        "import requests",
        "from requests",
        "import http.client",
        "from http.client",
    )
    for token in forbidden_tokens:
        assert token not in src, f"gate must not contain {token!r}"


# --------------------------------------------------------------------------- #
# Status semantics tightening
# --------------------------------------------------------------------------- #


def test_warn_is_not_ready(tmp_path: Path) -> None:
    """Even a single warn keeps ready_for_client=False."""
    run_dir = tmp_path / "run"
    _make_clean_run_dir(run_dir, review_rows=1)

    result = run_operator_review_gate(run_dir)

    assert result.status == STATUS_WARN
    assert result.ready_for_client is False


def test_block_overrides_warn(tmp_path: Path) -> None:
    """Multiple severities → block wins."""
    run_dir = tmp_path / "run"
    _make_clean_run_dir(
        run_dir,
        review_rows=1,
        consistency_overrides={"materialized_outputs_mutated_after_reports": True},
    )

    result = run_operator_review_gate(run_dir)

    assert result.status == STATUS_BLOCK
    # Both the block AND the warn are still surfaced.
    assert "artifact_consistency_failed" in _block_codes(result)
    assert "review_rows_present" in _codes(result)


# --------------------------------------------------------------------------- #
# Result-object sanity
# --------------------------------------------------------------------------- #


def test_result_dataclass_shape(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _make_clean_run_dir(run_dir)

    result = run_operator_review_gate(run_dir)

    assert isinstance(result, OperatorReviewResult)
    assert all(isinstance(i, OperatorReviewIssue) for i in result.issues)
    assert isinstance(result.issues, tuple)


# --------------------------------------------------------------------------- #
# V2.9.9 — atomic summary write + logger on write failure
# --------------------------------------------------------------------------- #


def test_summary_atomic_write_leaves_no_temp_file(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _make_clean_run_dir(run_dir)

    result = run_operator_review_gate(run_dir)

    assert result.summary_path.is_file()
    # Atomic write helper writes to .<name>.tmp then os.replace; nothing
    # must be left behind on success.
    leftover = run_dir / f".{result.summary_path.name}.tmp"
    assert not leftover.exists(), f"temp file leaked: {leftover}"
    # JSON is valid.
    json.loads(result.summary_path.read_text(encoding="utf-8"))


def test_summary_write_failure_logs_warning_and_returns_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    run_dir = tmp_path / "run"
    _make_clean_run_dir(run_dir)

    def _boom(*_args, **_kwargs) -> None:
        raise OSError("disk full")

    # Monkey-patch the helper used by the gate so the write fails.
    monkeypatch.setattr(operator_review_gate, "atomic_write_json", _boom)

    with caplog.at_level(logging.WARNING, logger="app.operator_review_gate"):
        result = run_operator_review_gate(run_dir)

    # Gate still returns a valid result — write is best-effort.
    assert isinstance(result, OperatorReviewResult)
    assert result.status in {STATUS_READY, STATUS_WARN, STATUS_BLOCK}
    # Warning was logged with the failure context.
    matching = [
        r for r in caplog.records
        if r.levelno == logging.WARNING
        and "operator review summary" in r.getMessage().lower()
    ]
    assert matching, f"expected a warning log; got: {[r.getMessage() for r in caplog.records]}"


# --------------------------------------------------------------------------- #
# V2.10.8.1 — Backend partial-readiness contract
# --------------------------------------------------------------------------- #


def _assert_partial_unavailable(result: OperatorReviewResult) -> None:
    assert result.ready_for_client_partial is False
    assert result.partial_delivery_mode is None
    assert result.partial_delivery_requires_override is False
    assert result.partial_delivery_allowed_count is None
    assert result.partial_delivery_excluded_count is None
    assert result.partial_delivery_reason is None


def test_partial_safe_count_zero_is_unavailable(tmp_path: Path) -> None:
    """safe_count=0 must keep partial unavailable even with warn-only issues."""
    run_dir = tmp_path / "run"
    _make_clean_run_dir(run_dir, valid_rows=0, review_rows=2)

    result = run_operator_review_gate(run_dir)

    assert result.ready_for_client is False
    assert result.status == STATUS_WARN
    assert result.safe_count == 0
    _assert_partial_unavailable(result)


def test_partial_warn_only_with_safe_rows_is_available(tmp_path: Path) -> None:
    """WY-100 shape: warn-only + safe rows + approved original => partial."""
    run_dir = tmp_path / "run"
    _make_clean_run_dir(
        run_dir,
        valid_rows=7,
        review_rows=59,
        rejected_rows=34,
        include_approved_original=True,
    )

    result = run_operator_review_gate(run_dir)

    # Full gate stays strict.
    assert result.ready_for_client is False
    assert result.status == STATUS_WARN
    assert _block_codes(result) == set()
    assert result.safe_count == 7
    assert result.review_count == 59
    assert result.rejected_count == 34
    assert result.approved_original_present is True

    # Partial contract is exposed.
    assert result.ready_for_client_partial is True
    assert result.partial_delivery_mode == "safe_only"
    assert result.partial_delivery_requires_override is True
    assert result.partial_delivery_allowed_count == 7
    assert result.partial_delivery_excluded_count == 93
    assert result.partial_delivery_reason is not None
    assert "7 safe rows" in result.partial_delivery_reason


def test_partial_block_issue_is_unavailable(tmp_path: Path) -> None:
    """A block issue must make partial unavailable, even with safe rows."""
    run_dir = tmp_path / "run"
    _make_clean_run_dir(
        run_dir,
        valid_rows=7,
        review_rows=59,
        rejected_rows=34,
        include_approved_original=True,
        consistency_overrides={
            "materialized_outputs_mutated_after_reports": True,
        },
    )

    result = run_operator_review_gate(run_dir)

    assert result.ready_for_client is False
    assert result.status == STATUS_BLOCK
    assert "artifact_consistency_failed" in _block_codes(result)
    assert result.safe_count == 7
    _assert_partial_unavailable(result)


def test_partial_approved_original_absent_is_unavailable(tmp_path: Path) -> None:
    """Without the approved-original anchor, partial is not safe to expose."""
    run_dir = tmp_path / "run"
    _make_clean_run_dir(
        run_dir,
        valid_rows=7,
        review_rows=59,
        rejected_rows=34,
        include_approved_original=False,
    )

    result = run_operator_review_gate(run_dir)

    assert result.ready_for_client is False
    assert result.status == STATUS_WARN
    assert _block_codes(result) == set()
    assert result.approved_original_present is False
    _assert_partial_unavailable(result)


def test_partial_full_ready_run_is_unavailable(tmp_path: Path) -> None:
    """A fully-ready run does not expose partial — full delivery is the path."""
    run_dir = tmp_path / "run"
    _make_clean_run_dir(run_dir)

    result = run_operator_review_gate(run_dir)

    assert result.ready_for_client is True
    assert result.status == STATUS_READY
    _assert_partial_unavailable(result)


def test_partial_warn_only_run_keeps_full_ready_false(tmp_path: Path) -> None:
    """Regression guard: V2.10.8.1 must not weaken the strict full gate."""
    run_dir = tmp_path / "run"
    _make_clean_run_dir(
        run_dir,
        valid_rows=7,
        review_rows=59,
        rejected_rows=34,
    )

    result = run_operator_review_gate(run_dir)

    # Strict full readiness remains false on warn.
    assert result.ready_for_client is False
    assert result.status == STATUS_WARN
    # And the new partial channel doesn't bleed into the full flag.
    assert result.ready_for_client_partial is True
    assert result.ready_for_client is not result.ready_for_client_partial


def test_partial_fields_present_in_to_dict_when_available(tmp_path: Path) -> None:
    """to_dict must surface the partial-contract keys with populated values."""
    run_dir = tmp_path / "run"
    _make_clean_run_dir(
        run_dir,
        valid_rows=7,
        review_rows=59,
        rejected_rows=34,
    )

    result = run_operator_review_gate(run_dir)
    payload = result.to_dict()

    expected_keys = {
        "ready_for_client_partial",
        "partial_delivery_mode",
        "partial_delivery_requires_override",
        "partial_delivery_allowed_count",
        "partial_delivery_excluded_count",
        "partial_delivery_reason",
    }
    assert expected_keys <= set(payload.keys())
    assert payload["ready_for_client_partial"] is True
    assert payload["partial_delivery_mode"] == "safe_only"
    assert payload["partial_delivery_requires_override"] is True
    assert payload["partial_delivery_allowed_count"] == 7
    assert payload["partial_delivery_excluded_count"] == 93
    assert "7 safe rows" in payload["partial_delivery_reason"]


def test_partial_fields_present_in_to_dict_when_unavailable(tmp_path: Path) -> None:
    """to_dict surfaces the keys with null/false values when partial is off."""
    run_dir = tmp_path / "run"
    _make_clean_run_dir(run_dir)

    result = run_operator_review_gate(run_dir)
    payload = result.to_dict()

    assert payload["ready_for_client_partial"] is False
    assert payload["partial_delivery_mode"] is None
    assert payload["partial_delivery_requires_override"] is False
    assert payload["partial_delivery_allowed_count"] is None
    assert payload["partial_delivery_excluded_count"] is None
    assert payload["partial_delivery_reason"] is None
