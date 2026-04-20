"""Configuration loading for the bootstrap stage of the project."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


DEFAULT_CONFIG_VALUES: dict[str, Any] = {
    "chunk_size": 50_000,
    "max_workers": 20,
    "high_confidence_threshold": 70,
    "review_threshold": 40,
    "fallback_to_a_record": True,
    "invalid_if_disposable": True,
    "dns_timeout_seconds": 4.0,
    "retry_dns_times": 1,
    "export_review_bucket": True,
    "keep_original_columns": True,
    "log_level": "INFO",
    "staging_db_name": "staging.sqlite3",
    "temp_dir_name": "temp",
}


@dataclass(slots=True)
class ProjectPaths:
    """Resolved project-level paths used by the bootstrap stage."""

    project_root: Path
    app_dir: Path
    configs_dir: Path
    input_dir: Path
    output_dir: Path
    logs_dir: Path
    default_config_path: Path
    disposable_domains_path: Path
    typo_map_path: Path


@dataclass(slots=True)
class AppConfig:
    """Runtime configuration used by the pipeline bootstrap."""

    chunk_size: int = int(DEFAULT_CONFIG_VALUES["chunk_size"])
    max_workers: int = int(DEFAULT_CONFIG_VALUES["max_workers"])
    high_confidence_threshold: int = int(DEFAULT_CONFIG_VALUES["high_confidence_threshold"])
    review_threshold: int = int(DEFAULT_CONFIG_VALUES["review_threshold"])
    fallback_to_a_record: bool = bool(DEFAULT_CONFIG_VALUES["fallback_to_a_record"])
    invalid_if_disposable: bool = bool(DEFAULT_CONFIG_VALUES["invalid_if_disposable"])
    dns_timeout_seconds: float = float(DEFAULT_CONFIG_VALUES["dns_timeout_seconds"])
    retry_dns_times: int = int(DEFAULT_CONFIG_VALUES["retry_dns_times"])
    export_review_bucket: bool = bool(DEFAULT_CONFIG_VALUES["export_review_bucket"])
    keep_original_columns: bool = bool(DEFAULT_CONFIG_VALUES["keep_original_columns"])
    log_level: str = str(DEFAULT_CONFIG_VALUES["log_level"])
    staging_db_name: str = str(DEFAULT_CONFIG_VALUES["staging_db_name"])
    temp_dir_name: str = str(DEFAULT_CONFIG_VALUES["temp_dir_name"])
    paths: ProjectPaths | None = field(default=None)


def resolve_project_paths(base_dir: str | Path | None = None) -> ProjectPaths:
    """Resolve the standard project directories from the project root."""

    project_root = Path(base_dir or Path(__file__).resolve().parents[1]).resolve()
    return ProjectPaths(
        project_root=project_root,
        app_dir=project_root / "app",
        configs_dir=project_root / "configs",
        input_dir=project_root / "input",
        output_dir=project_root / "output",
        logs_dir=project_root / "logs",
        default_config_path=project_root / "configs" / "default.yaml",
        disposable_domains_path=project_root / "configs" / "disposable_domains.txt",
        typo_map_path=project_root / "configs" / "typo_map.csv",
    )


def load_config(
    config_path: str | Path | None = None,
    base_dir: str | Path | None = None,
    overrides: dict[str, Any] | None = None,
) -> AppConfig:
    """Load YAML configuration, apply defaults and CLI overrides, and validate the result."""

    paths = resolve_project_paths(base_dir)
    source_path = Path(config_path).resolve() if config_path else paths.default_config_path
    raw_values = _read_yaml(source_path) if source_path.exists() else {}
    merged = {**DEFAULT_CONFIG_VALUES, **raw_values}
    if overrides:
        merged.update({key: value for key, value in overrides.items() if value is not None})

    config = AppConfig(
        chunk_size=int(merged["chunk_size"]),
        max_workers=int(merged["max_workers"]),
        high_confidence_threshold=int(merged["high_confidence_threshold"]),
        review_threshold=int(merged["review_threshold"]),
        fallback_to_a_record=bool(merged["fallback_to_a_record"]),
        invalid_if_disposable=bool(merged["invalid_if_disposable"]),
        dns_timeout_seconds=float(merged["dns_timeout_seconds"]),
        retry_dns_times=int(merged["retry_dns_times"]),
        export_review_bucket=bool(merged["export_review_bucket"]),
        keep_original_columns=bool(merged["keep_original_columns"]),
        log_level=str(merged["log_level"]).upper(),
        staging_db_name=str(merged["staging_db_name"]),
        temp_dir_name=str(merged["temp_dir_name"]),
        paths=paths,
    )
    validate_config(config)
    return config


def validate_config(config: AppConfig) -> None:
    """Validate the minimal set of configuration constraints needed in Subphase 1."""

    if config.chunk_size <= 0:
        raise ValueError("chunk_size must be greater than 0.")
    if config.max_workers <= 0:
        raise ValueError("max_workers must be greater than 0.")
    if config.review_threshold < 0 or config.high_confidence_threshold < 0:
        raise ValueError("Thresholds must be non-negative.")
    if config.review_threshold > config.high_confidence_threshold:
        raise ValueError("review_threshold cannot be greater than high_confidence_threshold.")
    if config.dns_timeout_seconds <= 0:
        raise ValueError("dns_timeout_seconds must be greater than 0.")
    if config.retry_dns_times < 0:
        raise ValueError("retry_dns_times cannot be negative.")
    if not config.log_level:
        raise ValueError("log_level cannot be empty.")
    if not config.staging_db_name.strip():
        raise ValueError("staging_db_name cannot be empty.")
    if not config.temp_dir_name.strip():
        raise ValueError("temp_dir_name cannot be empty.")


def _read_yaml(path: Path) -> dict[str, Any]:
    """Read a YAML mapping from disk."""

    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Configuration file must contain a top-level mapping: {path}")
    return payload
