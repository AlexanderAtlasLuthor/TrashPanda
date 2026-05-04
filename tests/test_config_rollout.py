from __future__ import annotations

import pytest

from app.config import (
    ROLLOUT_PROFILE_LOCAL_DEV,
    ROLLOUT_PROFILE_PILOT_SAMPLE,
    ROLLOUT_PROFILE_PRODUCTION_FULL,
    load_config,
    resolve_project_paths,
)


def _project_root():
    return resolve_project_paths().project_root


def test_default_yaml_loads_rollout_config() -> None:
    cfg = load_config(base_dir=_project_root())

    assert cfg.rollout.profile == ROLLOUT_PROFILE_PILOT_SAMPLE
    assert cfg.rollout.require_preflight is True
    assert cfg.rollout.block_uncapped_live_smtp is True
    assert cfg.rollout.max_rows_without_confirmation == 1000
    assert cfg.rollout.package_client_artifacts is True
    assert cfg.rollout.require_operator_review is True


def test_missing_rollout_block_uses_defaults(tmp_path) -> None:
    config_path = tmp_path / "minimal.yaml"
    config_path.write_text("chunk_size: 100\n", encoding="utf-8")

    cfg = load_config(config_path=config_path, base_dir=_project_root())

    assert cfg.rollout.profile == ROLLOUT_PROFILE_PILOT_SAMPLE
    assert cfg.rollout.require_preflight is True
    assert cfg.rollout.block_uncapped_live_smtp is True
    assert cfg.rollout.max_rows_without_confirmation == 1000
    assert cfg.rollout.package_client_artifacts is True
    assert cfg.rollout.require_operator_review is True


def test_custom_rollout_profile_loads(tmp_path) -> None:
    config_path = tmp_path / "local_dev.yaml"
    config_path.write_text(
        """
rollout:
  profile: local_dev
  require_preflight: false
  block_uncapped_live_smtp: true
  max_rows_without_confirmation: 250
  package_client_artifacts: false
  require_operator_review: false
""".lstrip(),
        encoding="utf-8",
    )

    cfg = load_config(config_path=config_path, base_dir=_project_root())

    assert cfg.rollout.profile == ROLLOUT_PROFILE_LOCAL_DEV
    assert cfg.rollout.require_preflight is False
    assert cfg.rollout.block_uncapped_live_smtp is True
    assert cfg.rollout.max_rows_without_confirmation == 250
    assert cfg.rollout.package_client_artifacts is False
    assert cfg.rollout.require_operator_review is False


def test_production_profile_loads_with_default_fields(tmp_path) -> None:
    config_path = tmp_path / "production.yaml"
    config_path.write_text(
        """
rollout:
  profile: production_full
""".lstrip(),
        encoding="utf-8",
    )

    cfg = load_config(config_path=config_path, base_dir=_project_root())

    assert cfg.rollout.profile == ROLLOUT_PROFILE_PRODUCTION_FULL
    assert cfg.rollout.require_preflight is True
    assert cfg.rollout.block_uncapped_live_smtp is True
    assert cfg.rollout.max_rows_without_confirmation == 1000
    assert cfg.rollout.package_client_artifacts is True
    assert cfg.rollout.require_operator_review is True


def test_unknown_rollout_profile_fails_fast(tmp_path) -> None:
    config_path = tmp_path / "unknown.yaml"
    config_path.write_text(
        """
rollout:
  profile: random_mode
""".lstrip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="rollout.profile.*random_mode"):
        load_config(config_path=config_path, base_dir=_project_root())


def test_existing_v2_config_sections_still_load() -> None:
    cfg = load_config(base_dir=_project_root())

    assert cfg.decision.enabled is True
    assert cfg.smtp_probe.enabled is True
    assert cfg.catch_all.enabled is True
    assert cfg.domain_intelligence.enabled is True
    assert cfg.bounce_ingestion.enabled is True
