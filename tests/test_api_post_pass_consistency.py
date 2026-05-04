"""V2.9.4 API/post-pass consistency tests."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from app import api_boundary
from app.config import (
    AppConfig,
    HistoryConfig,
    PostPassesConfig,
    load_config,
)
from app.api_boundary import JobStatus, run_cleaning_job


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SAMPLE_CSV = PROJECT_ROOT / "examples" / "sample_contacts.csv"


def _logger() -> logging.Logger:
    logger = logging.getLogger("api_post_pass_consistency_test")
    logger.addHandler(logging.NullHandler())
    return logger


def _read_csv(path: Path | None) -> pd.DataFrame:
    if path is None or not path.is_file():
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def _value_counts(df: pd.DataFrame, column: str) -> dict[str, int]:
    if df.empty or column not in df.columns:
        return {}
    return {
        str(key): int(value)
        for key, value in df[column].astype(str).value_counts().items()
    }


def test_default_config_disables_materialized_output_mutation() -> None:
    cfg = load_config(base_dir=PROJECT_ROOT)

    assert cfg.post_passes.mutate_materialized_outputs is False


def test_missing_post_passes_block_uses_defaults(tmp_path: Path) -> None:
    config_path = tmp_path / "minimal.yaml"
    config_path.write_text("chunk_size: 100\n", encoding="utf-8")

    cfg = load_config(config_path=config_path, base_dir=PROJECT_ROOT)

    assert cfg.post_passes.mutate_materialized_outputs is False


def test_mutating_post_passes_are_skipped_by_default(monkeypatch) -> None:
    import app.validation_v2 as validation_v2

    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("mutating post-pass should be gated by default")

    monkeypatch.setattr(validation_v2, "run_smtp_probing_pass", fail_if_called)
    monkeypatch.setattr(validation_v2, "run_probability_pass", fail_if_called)
    monkeypatch.setattr(validation_v2, "run_decision_pass", fail_if_called)

    cfg = AppConfig()
    logger = _logger()

    assert api_boundary._maybe_run_smtp_probing(Path("."), cfg, logger) is False
    assert api_boundary._maybe_run_probability_model(Path("."), cfg, logger) is False
    assert api_boundary._maybe_run_decision_engine(Path("."), cfg, logger) is False


def test_history_update_stays_external_only_by_default(monkeypatch, tmp_path: Path) -> None:
    import app.validation_v2 as validation_v2

    captured: dict[str, object] = {}

    class FakeStore:
        def __init__(self, path: Path) -> None:
            captured["store_path"] = path

        def close(self) -> None:
            captured["closed"] = True

    def fake_update_history_from_run(*_args, **kwargs):
        captured["called"] = True
        captured["adjustment_apply"] = kwargs["adjustment_config"].apply
        captured["write_adjustment_report"] = kwargs["write_adjustment_report"]
        return SimpleNamespace(adjustment_stats=None)

    monkeypatch.setattr(validation_v2, "DomainHistoryStore", FakeStore)
    monkeypatch.setattr(
        validation_v2,
        "update_history_from_run",
        fake_update_history_from_run,
    )

    cfg = AppConfig(
        history=HistoryConfig(
            enabled=True,
            sqlite_path=str(tmp_path / "history.sqlite"),
            apply_light_confidence_adjustment=True,
            write_adjustment_report=True,
        ),
        post_passes=PostPassesConfig(mutate_materialized_outputs=False),
    )

    mutated = api_boundary._maybe_update_domain_history(
        run_dir=tmp_path,
        config=cfg,
        logger=_logger(),
    )

    assert mutated is False
    assert captured["called"] is True
    assert captured["adjustment_apply"] is False
    assert captured["write_adjustment_report"] is False
    assert captured["closed"] is True


def test_mutation_enabled_mode_is_explicit(monkeypatch, tmp_path: Path) -> None:
    import app.validation_v2 as validation_v2

    calls: list[Path] = []

    def fake_run_probability_pass(*, run_dir, config, logger):
        calls.append(Path(run_dir))
        return SimpleNamespace(rows_processed=3)

    monkeypatch.setattr(
        validation_v2,
        "run_probability_pass",
        fake_run_probability_pass,
    )

    cfg = AppConfig(
        post_passes=PostPassesConfig(mutate_materialized_outputs=True),
    )

    assert api_boundary._maybe_run_probability_model(
        tmp_path,
        cfg,
        _logger(),
    ) is True
    assert calls == [tmp_path]


def test_artifact_consistency_metadata_and_server_exposure(tmp_path: Path) -> None:
    path = api_boundary._maybe_write_artifact_consistency_report(
        run_dir=tmp_path,
        post_pass_mutation_enabled=False,
        materialized_outputs_mutated_after_reports=False,
        artifacts_regenerated_after_post_passes=False,
        logger=_logger(),
    )

    assert path == tmp_path / "artifact_consistency.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload == {
        "report_version": "v2.9.4",
        "materialized_outputs_mutated_after_reports": False,
        "post_pass_mutation_enabled": False,
        "artifacts_regenerated_after_post_passes": False,
    }

    artifacts = api_boundary.collect_job_artifacts(tmp_path)
    assert artifacts.reports.artifact_consistency == path

    from app import server

    assert "artifact_consistency" in server.ARTIFACT_KEYS
    assert "artifact_consistency" not in server._PUBLIC_REPORT_KEYS
    assert server._artifact_visibility_for_group("reports") == "internal"


def test_api_run_artifacts_describe_one_consistent_state(tmp_path: Path) -> None:
    assert SAMPLE_CSV.is_file()

    result = run_cleaning_job(
        input_path=SAMPLE_CSV,
        output_root=tmp_path / "out",
        job_id="v294_consistency",
    )

    assert result.status == JobStatus.COMPLETED
    assert result.error is None
    assert result.artifacts is not None
    run_dir = result.run_dir
    assert run_dir is not None

    consistency_path = result.artifacts.reports.artifact_consistency
    assert consistency_path is not None
    consistency = json.loads(consistency_path.read_text(encoding="utf-8"))
    assert consistency["post_pass_mutation_enabled"] is False
    assert consistency["materialized_outputs_mutated_after_reports"] is False

    tech = result.artifacts.technical_csvs
    clean = _read_csv(tech.clean_high_confidence)
    review = _read_csv(tech.review_medium_confidence)
    removed = _read_csv(tech.removed_invalid)
    combined = pd.concat([clean, review, removed], ignore_index=True)

    summary_path = result.artifacts.reports.v2_deliverability_summary
    assert summary_path is not None
    v2_summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert (
        v2_summary["classification_summary"]["by_final_action"]
        == _value_counts(combined, "final_action")
    )

    client = result.artifacts.client_outputs
    valid_xlsx = pd.read_excel(client.valid_emails, sheet_name="valid")
    review_xlsx = pd.read_excel(client.review_emails, sheet_name="review")
    invalid_xlsx = pd.read_excel(
        client.invalid_or_bounce_risk,
        sheet_name="invalid_or_bounce_risk",
    )

    assert len(valid_xlsx) == len(clean)
    assert len(review_xlsx) == len(review)
    assert len(invalid_xlsx) == len(removed)
