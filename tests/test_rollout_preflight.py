from __future__ import annotations

import ast
import inspect
from pathlib import Path

from app.api_boundary import run_rollout_preflight
from app.config import AppConfig, load_config, resolve_project_paths
from app.rollout import preflight
from app.rollout.preflight import (
    ISSUE_APPROVED_ORIGINAL_MAY_BE_ABSENT,
    ISSUE_INPUT_MISSING,
    ISSUE_LARGE_FILE_REQUIRES_CONFIRMATION,
    ISSUE_OUTPUT_DIR_MISSING,
    ISSUE_SMTP_LIVE_WITHOUT_CAP_WARNING,
    ISSUE_SMTP_PORT_NOT_VERIFIED,
    ISSUE_UNCAPPED_LIVE_SMTP_BLOCKED,
    PREFLIGHT_STATUS_BLOCK,
    PREFLIGHT_STATUS_PASS,
    PREFLIGHT_STATUS_WARN,
    run_preflight_check,
)


def _project_root() -> Path:
    return resolve_project_paths().project_root


def _write_csv(path: Path, rows: int) -> Path:
    lines = ["email"] + [f"user{i}@example.com" for i in range(rows)]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _config(
    tmp_path: Path,
    *,
    smtp_enabled: bool = True,
    dry_run: bool = False,
    max_candidates_per_run: int | None = None,
    max_rows_without_confirmation: int = 1000,
) -> AppConfig:
    max_candidates_value = (
        "null" if max_candidates_per_run is None else str(max_candidates_per_run)
    )
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        f"""
rollout:
  profile: pilot_sample
  require_preflight: true
  block_uncapped_live_smtp: true
  max_rows_without_confirmation: {max_rows_without_confirmation}
  package_client_artifacts: true
  require_operator_review: true
smtp_probe:
  enabled: {str(smtp_enabled).lower()}
  dry_run: {str(dry_run).lower()}
  max_candidates_per_run: {max_candidates_value}
""".lstrip(),
        encoding="utf-8",
    )
    return load_config(config_path=config_path, base_dir=_project_root())


def _codes(result) -> set[str]:
    return {issue.code for issue in result.issues}


def test_missing_input_blocks(tmp_path: Path) -> None:
    cfg = _config(tmp_path, smtp_enabled=True, dry_run=False)

    result = run_preflight_check(tmp_path / "missing.csv", config=cfg)

    assert result.status == PREFLIGHT_STATUS_BLOCK
    assert ISSUE_INPUT_MISSING in _codes(result)
    assert result.row_count_estimate is None
    assert result.file_size_bytes is None


def test_small_dry_run_passes_with_approved_original_warning(tmp_path: Path) -> None:
    cfg = _config(tmp_path, smtp_enabled=True, dry_run=True)
    input_path = _write_csv(tmp_path / "small.csv", rows=2)

    result = run_preflight_check(input_path, config=cfg)

    assert result.status == PREFLIGHT_STATUS_WARN
    assert result.row_count_estimate == 2
    assert ISSUE_APPROVED_ORIGINAL_MAY_BE_ABSENT in _codes(result)


def test_large_run_without_confirmation_blocks(tmp_path: Path) -> None:
    cfg = _config(
        tmp_path,
        smtp_enabled=True,
        dry_run=True,
        max_rows_without_confirmation=2,
    )
    input_path = _write_csv(tmp_path / "large.csv", rows=3)

    result = run_preflight_check(input_path, config=cfg)

    assert result.status == PREFLIGHT_STATUS_BLOCK
    assert result.row_count_estimate == 3
    assert ISSUE_LARGE_FILE_REQUIRES_CONFIRMATION in _codes(result)


def test_large_confirmed_run_with_uncapped_live_smtp_blocks(tmp_path: Path) -> None:
    cfg = _config(
        tmp_path,
        smtp_enabled=True,
        dry_run=False,
        max_candidates_per_run=None,
        max_rows_without_confirmation=2,
    )
    input_path = _write_csv(tmp_path / "large.csv", rows=3)

    result = run_preflight_check(
        input_path,
        config=cfg,
        operator_confirmed_large_run=True,
        smtp_port_verified=True,
    )

    assert result.status == PREFLIGHT_STATUS_BLOCK
    assert ISSUE_UNCAPPED_LIVE_SMTP_BLOCKED in _codes(result)
    assert ISSUE_LARGE_FILE_REQUIRES_CONFIRMATION not in _codes(result)


def test_small_live_smtp_uncapped_warns(tmp_path: Path) -> None:
    cfg = _config(
        tmp_path,
        smtp_enabled=True,
        dry_run=False,
        max_candidates_per_run=None,
    )
    input_path = _write_csv(tmp_path / "small.csv", rows=2)

    result = run_preflight_check(
        input_path,
        config=cfg,
        smtp_port_verified=True,
    )

    assert result.status == PREFLIGHT_STATUS_WARN
    assert ISSUE_SMTP_LIVE_WITHOUT_CAP_WARNING in _codes(result)


def test_live_smtp_without_port_verification_warns(tmp_path: Path) -> None:
    cfg = _config(
        tmp_path,
        smtp_enabled=True,
        dry_run=False,
        max_candidates_per_run=10,
    )
    input_path = _write_csv(tmp_path / "small.csv", rows=2)

    result = run_preflight_check(input_path, config=cfg, smtp_port_verified=False)

    assert result.status == PREFLIGHT_STATUS_WARN
    assert ISSUE_SMTP_PORT_NOT_VERIFIED in _codes(result)


def test_live_smtp_with_cap_and_verified_port_passes_small_run(tmp_path: Path) -> None:
    cfg = _config(
        tmp_path,
        smtp_enabled=True,
        dry_run=False,
        max_candidates_per_run=10,
    )
    input_path = _write_csv(tmp_path / "small.csv", rows=2)

    result = run_preflight_check(
        input_path,
        config=cfg,
        smtp_port_verified=True,
    )

    assert result.status == PREFLIGHT_STATUS_PASS
    assert result.issues == ()


def test_output_dir_missing_warns(tmp_path: Path) -> None:
    cfg = _config(
        tmp_path,
        smtp_enabled=True,
        dry_run=False,
        max_candidates_per_run=10,
    )
    input_path = _write_csv(tmp_path / "small.csv", rows=2)

    result = run_preflight_check(
        input_path,
        config=cfg,
        output_dir=tmp_path / "missing-output",
        smtp_port_verified=True,
    )

    assert result.status == PREFLIGHT_STATUS_WARN
    assert ISSUE_OUTPUT_DIR_MISSING in _codes(result)


def test_boundary_preflight_returns_json_friendly_dict(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
smtp_probe:
  enabled: true
  dry_run: false
  max_candidates_per_run: 10
""".lstrip(),
        encoding="utf-8",
    )
    input_path = _write_csv(tmp_path / "small.csv", rows=2)

    result = run_rollout_preflight(
        input_path,
        config_path=config_path,
        smtp_port_verified=True,
    )

    assert isinstance(result, dict)
    assert result["status"] == PREFLIGHT_STATUS_PASS
    assert result["profile"] == "pilot_sample"
    assert result["row_count_estimate"] == 2
    assert result["issues"] == []


def test_preflight_module_imports_no_live_network_modules() -> None:
    tree = ast.parse(inspect.getsource(preflight))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".", 1)[0])

    assert imported.isdisjoint({"socket", "smtplib", "dns", "requests", "httpx"})
    assert "probe_email" not in inspect.getsource(preflight)
