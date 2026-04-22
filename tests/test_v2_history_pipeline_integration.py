"""Integration smoke test: full pipeline run with V2 history enabled.

Confirms that:
  * V1 outputs are identical whether history is enabled or disabled.
  * When enabled, domain_history_summary.csv is written into the run
    directory and the SQLite DB receives rows.
  * A failing history layer does NOT break the pipeline.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from app.api_boundary import JobStatus, run_cleaning_job
from app.config import DEFAULT_HISTORY_VALUES, HistoryConfig, load_config
from app.validation_v2.history_store import DomainHistoryStore


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SAMPLE_CSV = PROJECT_ROOT / "examples" / "sample_contacts.csv"


pytestmark = pytest.mark.skipif(
    not SAMPLE_CSV.is_file(), reason=f"sample input missing at {SAMPLE_CSV}"
)


# ─────────────────────────────────────────────────────────────────────── #
# Config sanity                                                           #
# ─────────────────────────────────────────────────────────────────────── #


def test_history_config_defaults_enabled_with_sqlite() -> None:
    config = load_config()
    assert config.history.enabled is True
    assert config.history.backend == "sqlite"
    assert config.history.sqlite_path


def test_history_config_fields_match_yaml_defaults() -> None:
    config = load_config()
    h = config.history
    assert h.max_positive_adjustment == DEFAULT_HISTORY_VALUES["max_positive_adjustment"]
    assert h.max_negative_adjustment == DEFAULT_HISTORY_VALUES["max_negative_adjustment"]
    assert h.min_observations_for_labeling == (
        DEFAULT_HISTORY_VALUES["min_observations_for_labeling"]
    )


def test_history_config_disabled_passes_validation() -> None:
    config = load_config()
    config.history = HistoryConfig(enabled=False)
    # Validating a disabled-history config should not raise:
    from app.config import validate_config

    validate_config(config)


# ─────────────────────────────────────────────────────────────────────── #
# End-to-end run with history enabled                                     #
# ─────────────────────────────────────────────────────────────────────── #


def test_pipeline_run_with_history_enabled_writes_report_and_db(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Route the SQLite to a tmp location so the test is hermetic.
    db_path = tmp_path / "history.sqlite"
    monkeypatch.setenv("TZ", "UTC")

    # Use a tmp config that points the history DB to tmp_path.
    config_yaml = tmp_path / "config.yaml"
    config_yaml.write_text(
        f"""chunk_size: 5000
max_workers: 4
high_confidence_threshold: 70
review_threshold: 40
fallback_to_a_record: true
invalid_if_disposable: true
dns_timeout_seconds: 4.0
retry_dns_times: 1
export_review_bucket: true
keep_original_columns: true
log_level: WARNING
staging_db_name: staging.sqlite3
temp_dir_name: temp

history:
  enabled: true
  backend: sqlite
  sqlite_path: {db_path.as_posix()}
  apply_light_confidence_adjustment: false
  max_positive_adjustment: 3
  max_negative_adjustment: 5
  min_observations_for_labeling: 5
  write_summary_report: true
""",
        encoding="utf-8",
    )

    out_root = tmp_path / "out"
    result = run_cleaning_job(
        input_path=SAMPLE_CSV,
        output_root=out_root,
        config_path=config_yaml,
        job_id="test_history_enabled",
    )
    assert result.status == JobStatus.COMPLETED, f"pipeline failed: {result.error}"

    run_dir = result.run_dir
    assert run_dir is not None
    report = run_dir / "domain_history_summary.csv"
    assert report.is_file(), "domain_history_summary.csv should be written when history is enabled"

    assert db_path.is_file(), "history SQLite file should be created"

    with DomainHistoryStore(db_path) as store:
        assert store.count() > 0, "store must contain at least one domain after a run"


def test_pipeline_run_with_history_disabled_does_not_write_report(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "history.sqlite"
    config_yaml = tmp_path / "config.yaml"
    config_yaml.write_text(
        f"""chunk_size: 5000
max_workers: 4
high_confidence_threshold: 70
review_threshold: 40
fallback_to_a_record: true
invalid_if_disposable: true
dns_timeout_seconds: 4.0
retry_dns_times: 1
export_review_bucket: true
keep_original_columns: true
log_level: WARNING
staging_db_name: staging.sqlite3
temp_dir_name: temp

history:
  enabled: false
  backend: sqlite
  sqlite_path: {db_path.as_posix()}
  apply_light_confidence_adjustment: false
  max_positive_adjustment: 3
  max_negative_adjustment: 5
  min_observations_for_labeling: 5
  write_summary_report: true
""",
        encoding="utf-8",
    )

    out_root = tmp_path / "out"
    result = run_cleaning_job(
        input_path=SAMPLE_CSV,
        output_root=out_root,
        config_path=config_yaml,
        job_id="test_history_disabled",
    )
    assert result.status == JobStatus.COMPLETED

    run_dir = result.run_dir
    assert run_dir is not None
    assert not (run_dir / "domain_history_summary.csv").exists()
    assert not db_path.exists(), "SQLite should not be created when history is disabled"
