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


DEFAULT_DECISION_VALUES: dict[str, Any] = {
    "enabled": True,
    "approve_threshold": 0.80,
    "review_threshold": 0.50,
    "enable_bucket_override": False,
    "write_summary_report": True,
}


DEFAULT_PROBABILITY_VALUES: dict[str, Any] = {
    "enabled": True,
    "high_threshold": 0.70,
    "medium_threshold": 0.40,
    "write_summary_report": True,
}


DEFAULT_SMTP_PROBE_VALUES: dict[str, Any] = {
    "enabled": False,
    "dry_run": True,
    "sample_size": 50,
    "max_per_domain": 3,
    "timeout_seconds": 4.0,
    "rate_limit_per_second": 2.0,
    "retries": 0,
    "negative_adjustment_trigger_threshold": 3,
    "sender_address": "trashpanda-probe@localhost",
}


DEFAULT_HISTORY_VALUES: dict[str, Any] = {
    "enabled": True,
    "backend": "sqlite",
    "sqlite_path": "runtime/history/domain_history.sqlite",
    "apply_light_confidence_adjustment": True,
    "max_positive_adjustment": 3,
    "max_negative_adjustment": 5,
    "min_observations_for_labeling": 5,
    "min_observations_for_adjustment": 5,
    "allow_bucket_flip_from_history": False,
    "write_summary_report": True,
    "write_adjustment_report": True,
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
class HistoryConfig:
    """V2 Domain Historical Memory layer configuration.

    Additive, non-invasive. When ``enabled`` is False the whole layer
    stays dormant and V1 behaviour is identical.
    """

    enabled: bool = bool(DEFAULT_HISTORY_VALUES["enabled"])
    backend: str = str(DEFAULT_HISTORY_VALUES["backend"])
    sqlite_path: str = str(DEFAULT_HISTORY_VALUES["sqlite_path"])
    apply_light_confidence_adjustment: bool = bool(
        DEFAULT_HISTORY_VALUES["apply_light_confidence_adjustment"]
    )
    max_positive_adjustment: int = int(DEFAULT_HISTORY_VALUES["max_positive_adjustment"])
    max_negative_adjustment: int = int(DEFAULT_HISTORY_VALUES["max_negative_adjustment"])
    min_observations_for_labeling: int = int(
        DEFAULT_HISTORY_VALUES["min_observations_for_labeling"]
    )
    min_observations_for_adjustment: int = int(
        DEFAULT_HISTORY_VALUES["min_observations_for_adjustment"]
    )
    allow_bucket_flip_from_history: bool = bool(
        DEFAULT_HISTORY_VALUES["allow_bucket_flip_from_history"]
    )
    write_summary_report: bool = bool(DEFAULT_HISTORY_VALUES["write_summary_report"])
    write_adjustment_report: bool = bool(DEFAULT_HISTORY_VALUES["write_adjustment_report"])


@dataclass(slots=True)
class DecisionConfig:
    """V2 Phase 6 — Decision Engine (automated actions layer)."""

    enabled: bool = bool(DEFAULT_DECISION_VALUES["enabled"])
    approve_threshold: float = float(DEFAULT_DECISION_VALUES["approve_threshold"])
    review_threshold: float = float(DEFAULT_DECISION_VALUES["review_threshold"])
    enable_bucket_override: bool = bool(DEFAULT_DECISION_VALUES["enable_bucket_override"])
    write_summary_report: bool = bool(DEFAULT_DECISION_VALUES["write_summary_report"])


@dataclass(slots=True)
class ProbabilityConfig:
    """V2 Phase 5 — per-row deliverability probability model."""

    enabled: bool = bool(DEFAULT_PROBABILITY_VALUES["enabled"])
    high_threshold: float = float(DEFAULT_PROBABILITY_VALUES["high_threshold"])
    medium_threshold: float = float(DEFAULT_PROBABILITY_VALUES["medium_threshold"])
    write_summary_report: bool = bool(DEFAULT_PROBABILITY_VALUES["write_summary_report"])


@dataclass(slots=True)
class SMTPProbeConfig:
    """V2 Phase 4 selective SMTP probing. Off by default."""

    enabled: bool = bool(DEFAULT_SMTP_PROBE_VALUES["enabled"])
    dry_run: bool = bool(DEFAULT_SMTP_PROBE_VALUES["dry_run"])
    sample_size: int = int(DEFAULT_SMTP_PROBE_VALUES["sample_size"])
    max_per_domain: int = int(DEFAULT_SMTP_PROBE_VALUES["max_per_domain"])
    timeout_seconds: float = float(DEFAULT_SMTP_PROBE_VALUES["timeout_seconds"])
    rate_limit_per_second: float = float(
        DEFAULT_SMTP_PROBE_VALUES["rate_limit_per_second"]
    )
    retries: int = int(DEFAULT_SMTP_PROBE_VALUES["retries"])
    negative_adjustment_trigger_threshold: int = int(
        DEFAULT_SMTP_PROBE_VALUES["negative_adjustment_trigger_threshold"]
    )
    sender_address: str = str(DEFAULT_SMTP_PROBE_VALUES["sender_address"])


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
    history: HistoryConfig = field(default_factory=HistoryConfig)
    smtp_probe: SMTPProbeConfig = field(default_factory=SMTPProbeConfig)
    probability: ProbabilityConfig = field(default_factory=ProbabilityConfig)
    decision: DecisionConfig = field(default_factory=DecisionConfig)
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
    history_raw = raw_values.pop("history", {}) if isinstance(raw_values, dict) else {}
    smtp_raw = raw_values.pop("smtp_probe", {}) if isinstance(raw_values, dict) else {}
    prob_raw = raw_values.pop("probability", {}) if isinstance(raw_values, dict) else {}
    decision_raw = raw_values.pop("decision", {}) if isinstance(raw_values, dict) else {}
    merged = {**DEFAULT_CONFIG_VALUES, **raw_values}
    if overrides:
        merged.update({key: value for key, value in overrides.items() if value is not None})

    history_merged = {**DEFAULT_HISTORY_VALUES, **(history_raw or {})}
    history_config = HistoryConfig(
        enabled=bool(history_merged["enabled"]),
        backend=str(history_merged["backend"]),
        sqlite_path=str(history_merged["sqlite_path"]),
        apply_light_confidence_adjustment=bool(
            history_merged["apply_light_confidence_adjustment"]
        ),
        max_positive_adjustment=int(history_merged["max_positive_adjustment"]),
        max_negative_adjustment=int(history_merged["max_negative_adjustment"]),
        min_observations_for_labeling=int(history_merged["min_observations_for_labeling"]),
        min_observations_for_adjustment=int(
            history_merged["min_observations_for_adjustment"]
        ),
        allow_bucket_flip_from_history=bool(
            history_merged["allow_bucket_flip_from_history"]
        ),
        write_summary_report=bool(history_merged["write_summary_report"]),
        write_adjustment_report=bool(history_merged["write_adjustment_report"]),
    )

    prob_merged = {**DEFAULT_PROBABILITY_VALUES, **(prob_raw or {})}
    probability_config = ProbabilityConfig(
        enabled=bool(prob_merged["enabled"]),
        high_threshold=float(prob_merged["high_threshold"]),
        medium_threshold=float(prob_merged["medium_threshold"]),
        write_summary_report=bool(prob_merged["write_summary_report"]),
    )

    decision_merged = {**DEFAULT_DECISION_VALUES, **(decision_raw or {})}
    decision_config = DecisionConfig(
        enabled=bool(decision_merged["enabled"]),
        approve_threshold=float(decision_merged["approve_threshold"]),
        review_threshold=float(decision_merged["review_threshold"]),
        enable_bucket_override=bool(decision_merged["enable_bucket_override"]),
        write_summary_report=bool(decision_merged["write_summary_report"]),
    )

    smtp_merged = {**DEFAULT_SMTP_PROBE_VALUES, **(smtp_raw or {})}
    smtp_config = SMTPProbeConfig(
        enabled=bool(smtp_merged["enabled"]),
        dry_run=bool(smtp_merged["dry_run"]),
        sample_size=int(smtp_merged["sample_size"]),
        max_per_domain=int(smtp_merged["max_per_domain"]),
        timeout_seconds=float(smtp_merged["timeout_seconds"]),
        rate_limit_per_second=float(smtp_merged["rate_limit_per_second"]),
        retries=int(smtp_merged["retries"]),
        negative_adjustment_trigger_threshold=int(
            smtp_merged["negative_adjustment_trigger_threshold"]
        ),
        sender_address=str(smtp_merged["sender_address"]),
    )

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
        history=history_config,
        smtp_probe=smtp_config,
        probability=probability_config,
        decision=decision_config,
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

    # History layer (V2): validate only when enabled so disabled configs
    # can leave fields blank without tripping up the loader.
    h = config.history
    if h.enabled:
        if h.backend not in ("sqlite", "memory"):
            raise ValueError("history.backend must be 'sqlite' or 'memory'.")
        if h.backend == "sqlite" and not h.sqlite_path.strip():
            raise ValueError("history.sqlite_path cannot be empty when backend=sqlite.")
        if h.max_positive_adjustment < 0:
            raise ValueError("history.max_positive_adjustment must be non-negative.")
        if h.max_negative_adjustment < 0:
            raise ValueError("history.max_negative_adjustment must be non-negative.")
        if h.min_observations_for_labeling < 1:
            raise ValueError("history.min_observations_for_labeling must be >= 1.")
        if h.min_observations_for_adjustment < 1:
            raise ValueError("history.min_observations_for_adjustment must be >= 1.")

    # SMTP probe (V2.4): validated only when enabled.
    sp = config.smtp_probe
    if sp.enabled:
        if sp.sample_size < 1:
            raise ValueError("smtp_probe.sample_size must be >= 1.")
        if sp.max_per_domain < 1:
            raise ValueError("smtp_probe.max_per_domain must be >= 1.")
        if sp.timeout_seconds <= 0:
            raise ValueError("smtp_probe.timeout_seconds must be > 0.")
        if sp.rate_limit_per_second <= 0:
            raise ValueError("smtp_probe.rate_limit_per_second must be > 0.")
        if sp.retries < 0:
            raise ValueError("smtp_probe.retries must be >= 0.")
        if sp.negative_adjustment_trigger_threshold < 0:
            raise ValueError(
                "smtp_probe.negative_adjustment_trigger_threshold must be >= 0."
            )
        if not sp.sender_address.strip():
            raise ValueError("smtp_probe.sender_address cannot be empty.")

    # Probability model (V2.5): validated only when enabled.
    pc = config.probability
    if pc.enabled:
        if not 0.0 <= pc.medium_threshold < pc.high_threshold <= 1.0:
            raise ValueError(
                "probability thresholds must satisfy 0.0 <= medium < high <= 1.0."
            )

    # Decision engine (V2.6): validated only when enabled.
    dc = config.decision
    if dc.enabled:
        if not 0.0 <= dc.review_threshold < dc.approve_threshold <= 1.0:
            raise ValueError(
                "decision thresholds must satisfy "
                "0.0 <= review_threshold < approve_threshold <= 1.0."
            )


def _read_yaml(path: Path) -> dict[str, Any]:
    """Read a YAML mapping from disk."""

    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Configuration file must contain a top-level mapping: {path}")
    return payload
