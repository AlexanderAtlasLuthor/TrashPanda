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


ROLLOUT_PROFILE_LOCAL_DEV = "local_dev"
ROLLOUT_PROFILE_PILOT_SAMPLE = "pilot_sample"
ROLLOUT_PROFILE_PRODUCTION_FULL = "production_full"
ROLLOUT_PROFILES: frozenset[str] = frozenset({
    ROLLOUT_PROFILE_LOCAL_DEV,
    ROLLOUT_PROFILE_PILOT_SAMPLE,
    ROLLOUT_PROFILE_PRODUCTION_FULL,
})


DEFAULT_ROLLOUT_VALUES: dict[str, Any] = {
    "profile": ROLLOUT_PROFILE_PILOT_SAMPLE,
    "require_preflight": True,
    "block_uncapped_live_smtp": True,
    "max_rows_without_confirmation": 1000,
    "package_client_artifacts": True,
    "require_operator_review": True,
}


DEFAULT_POST_PASSES_VALUES: dict[str, Any] = {
    "mutate_materialized_outputs": False,
}


DEFAULT_DECISION_VALUES: dict[str, Any] = {
    "enabled": True,
    "approve_threshold": 0.80,
    "review_threshold": 0.50,
    # V2.1 — V2 decision is the production routing authority. Defaults
    # match ``configs/default.yaml``. Flip to False only to fall back
    # to legacy V1-only routing for diagnostics.
    "enable_bucket_override": True,
    "write_summary_report": True,
}


DEFAULT_PROBABILITY_VALUES: dict[str, Any] = {
    "enabled": True,
    # Additive model v2: high ≥ 0.80, medium ≥ 0.50, low < 0.50.
    # Aligned with DEFAULT_DECISION_VALUES so label boundaries match
    # the decision engine's approve/review thresholds.
    "high_threshold": 0.80,
    "medium_threshold": 0.50,
    "write_summary_report": True,
}


DEFAULT_SMTP_PROBE_VALUES: dict[str, Any] = {
    # V2.2 — SMTP is now production-on and feeds the in-chunk decision
    # stage. Tests are protected from real network calls by an autouse
    # fixture in ``conftest.py``; deployments that want a no-network
    # run can flip ``dry_run`` back to ``True``.
    "enabled": True,
    "dry_run": False,
    "max_candidates_per_run": None,
    "timeout_seconds": 10.0,
    "rate_limit_per_second": 2.0,
    "retries": 0,
    "retry_temp_failures": True,
    "max_retries": 1,
    "sender_address": "trashpanda-probe@localhost",
    # Legacy knobs read by the post-pass orchestrator
    # (``app.validation_v2.smtp_integration``).
    "sample_size": 50,
    "max_per_domain": 3,
    "negative_adjustment_trigger_threshold": 3,
}


DEFAULT_TYPO_CORRECTION_VALUES: dict[str, Any] = {
    # "suggest_only" (default, safe) never modifies the row; it only
    # populates the new ``typo_detected`` / ``suggested_*`` columns.
    # "auto_apply_safe" is reserved for a future rollout and currently
    # behaves like "suggest_only".
    "mode": "suggest_only",
    "max_edit_distance": 2,
    "whitelist": [
        "gmail.com",
        "yahoo.com",
        "outlook.com",
        "hotmail.com",
        "icloud.com",
    ],
    "require_original_no_mx": True,
}


DEFAULT_BOUNCE_INGESTION_VALUES: dict[str, Any] = {
    # V2.7 — bounce outcome ingestion + domain reputation thresholds.
    # The chunk pipeline does not depend on this block; ingestion is
    # an out-of-band operator-triggered job. Defaults match the V2.7
    # prompt; lower ``min_observations_for_domain_reputation`` to
    # 1-2 in tests if you want every domain classified after one event.
    "enabled": True,
    "store_path": "runtime/feedback/bounce_outcomes.sqlite",
    "min_observations_for_domain_reputation": 5,
    "medium_hard_bounce_rate": 0.08,
    "high_hard_bounce_rate": 0.20,
    "high_blocked_rate": 0.10,
    "complaint_is_high_risk": True,
}


DEFAULT_DOMAIN_INTELLIGENCE_VALUES: dict[str, Any] = {
    # V2.6 — chunk-time domain intelligence + cold-start handling.
    # The stage classifies each row's domain into a small canonical
    # vocabulary using offline heuristics; high-risk and cold-start
    # cases cap approval at the centralized V2 decision policy.
    "enabled": True,
    "min_observations_for_reputation": 3,
    "cold_start_default_risk": "unknown",
    "high_risk_blocks_auto_approve": True,
    "cold_start_requires_smtp_valid": True,
}


DEFAULT_CATCH_ALL_VALUES: dict[str, Any] = {
    # V2.3 — chunk-time catch-all detection block. The stage runs in
    # the chunk pipeline and never opens a network connection itself;
    # it normalizes the SMTP probe's existing random-RCPT signal into
    # canonical row-level fields. ``cache_by_domain`` caches the
    # classification per domain across chunks. ``default_on_unknown``
    # is informational — DecisionStage already routes unknown to
    # review via the SMTP overrides.
    "enabled": True,
    "method": "smtp",
    "cache_by_domain": True,
    "default_on_unknown": "review",
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
class TypoCorrectionConfig:
    """Configuration for the conservative domain-typo *suggestion* engine.

    In ``suggest_only`` mode (the default) the pipeline never rewrites
    the original domain. It only populates the new suggestion columns
    and routes candidate rows to REVIEW. ``auto_apply_safe`` is reserved
    for a future rollout and currently behaves like ``suggest_only``.
    """

    mode: str = str(DEFAULT_TYPO_CORRECTION_VALUES["mode"])
    max_edit_distance: int = int(DEFAULT_TYPO_CORRECTION_VALUES["max_edit_distance"])
    whitelist: tuple[str, ...] = field(
        default_factory=lambda: tuple(DEFAULT_TYPO_CORRECTION_VALUES["whitelist"])
    )
    require_original_no_mx: bool = bool(
        DEFAULT_TYPO_CORRECTION_VALUES["require_original_no_mx"]
    )


@dataclass(slots=True, frozen=True)
class RolloutConfig:
    """V2.9.1 rollout profile and safety-intent settings.

    This block is configuration foundation only. Preflight enforcement,
    operator gates, and package-building behavior are intentionally left
    to later rollout phases.
    """

    profile: str = str(DEFAULT_ROLLOUT_VALUES["profile"])
    require_preflight: bool = bool(DEFAULT_ROLLOUT_VALUES["require_preflight"])
    block_uncapped_live_smtp: bool = bool(
        DEFAULT_ROLLOUT_VALUES["block_uncapped_live_smtp"]
    )
    max_rows_without_confirmation: int = int(
        DEFAULT_ROLLOUT_VALUES["max_rows_without_confirmation"]
    )
    package_client_artifacts: bool = bool(
        DEFAULT_ROLLOUT_VALUES["package_client_artifacts"]
    )
    require_operator_review: bool = bool(
        DEFAULT_ROLLOUT_VALUES["require_operator_review"]
    )


@dataclass(slots=True, frozen=True)
class PostPassesConfig:
    """V2.9.4 safety gate for API post-run annotation passes.

    The legacy API post-passes can rewrite materialized CSV annotations
    after reports and client workbooks are generated. The default keeps
    deliverable artifacts in one consistent materialized state.
    """

    mutate_materialized_outputs: bool = bool(
        DEFAULT_POST_PASSES_VALUES["mutate_materialized_outputs"]
    )


@dataclass(slots=True)
class BounceIngestionConfig:
    """V2.7 — bounce outcome ingestion settings.

    ``store_path`` is the SQLite file the dedicated
    :class:`app.validation_v2.feedback.BounceOutcomeStore` writes to;
    relative paths are resolved against the project root. The four
    rate fields tune :func:`compute_risk_level`. Set ``enabled=false``
    to disable ingestion outright (the boundary helper will refuse).
    """

    enabled: bool = bool(DEFAULT_BOUNCE_INGESTION_VALUES["enabled"])
    store_path: str = str(DEFAULT_BOUNCE_INGESTION_VALUES["store_path"])
    min_observations_for_domain_reputation: int = int(
        DEFAULT_BOUNCE_INGESTION_VALUES["min_observations_for_domain_reputation"]
    )
    medium_hard_bounce_rate: float = float(
        DEFAULT_BOUNCE_INGESTION_VALUES["medium_hard_bounce_rate"]
    )
    high_hard_bounce_rate: float = float(
        DEFAULT_BOUNCE_INGESTION_VALUES["high_hard_bounce_rate"]
    )
    high_blocked_rate: float = float(
        DEFAULT_BOUNCE_INGESTION_VALUES["high_blocked_rate"]
    )
    complaint_is_high_risk: bool = bool(
        DEFAULT_BOUNCE_INGESTION_VALUES["complaint_is_high_risk"]
    )


@dataclass(slots=True)
class DomainIntelligenceConfig:
    """V2.6 — domain intelligence + cold-start handling.

    Heuristic-driven (free-provider whitelist, disposable list,
    suspicious-shape detection); no network calls at chunk time. Set
    ``enabled: false`` to skip the stage entirely — every row will
    then carry ``domain_intel_status=unavailable`` and the V2 decision
    policy treats that the same as ``unknown`` (never positive
    evidence).
    """

    enabled: bool = bool(DEFAULT_DOMAIN_INTELLIGENCE_VALUES["enabled"])
    min_observations_for_reputation: int = int(
        DEFAULT_DOMAIN_INTELLIGENCE_VALUES["min_observations_for_reputation"]
    )
    cold_start_default_risk: str = str(
        DEFAULT_DOMAIN_INTELLIGENCE_VALUES["cold_start_default_risk"]
    )
    high_risk_blocks_auto_approve: bool = bool(
        DEFAULT_DOMAIN_INTELLIGENCE_VALUES["high_risk_blocks_auto_approve"]
    )
    cold_start_requires_smtp_valid: bool = bool(
        DEFAULT_DOMAIN_INTELLIGENCE_VALUES["cold_start_requires_smtp_valid"]
    )


@dataclass(slots=True)
class CatchAllConfig:
    """V2.3 — catch-all detection configuration.

    The stage runs entirely on signals produced by upstream SMTP
    probing (``probe_email_smtplib`` already does the piggyback random
    RCPT trick). Disable with ``enabled: false`` to skip the stage's
    cache + canonical-column normalization; SMTP-derived
    ``catch_all_possible`` still caps approval via the V2.2 SMTP
    overrides in DecisionStage.
    """

    enabled: bool = bool(DEFAULT_CATCH_ALL_VALUES["enabled"])
    method: str = str(DEFAULT_CATCH_ALL_VALUES["method"])
    cache_by_domain: bool = bool(DEFAULT_CATCH_ALL_VALUES["cache_by_domain"])
    default_on_unknown: str = str(DEFAULT_CATCH_ALL_VALUES["default_on_unknown"])


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
    """V2 Phase 4 + V2.2 SMTP probing.

    V2.2 promotes the probe to an in-chunk verification stage. Defaults
    are now ``enabled=True, dry_run=False`` (production-leaning); the
    test suite installs an autouse safeguard so the suite never opens a
    real socket. ``max_candidates_per_run`` (None = unlimited) is the
    in-chunk safety net; the legacy ``sample_size`` / ``max_per_domain``
    fields are still consumed by the post-pass orchestrator.
    """

    enabled: bool = bool(DEFAULT_SMTP_PROBE_VALUES["enabled"])
    dry_run: bool = bool(DEFAULT_SMTP_PROBE_VALUES["dry_run"])
    max_candidates_per_run: int | None = (
        int(DEFAULT_SMTP_PROBE_VALUES["max_candidates_per_run"])
        if DEFAULT_SMTP_PROBE_VALUES["max_candidates_per_run"] is not None
        else None
    )
    sample_size: int = int(DEFAULT_SMTP_PROBE_VALUES["sample_size"])
    max_per_domain: int = int(DEFAULT_SMTP_PROBE_VALUES["max_per_domain"])
    timeout_seconds: float = float(DEFAULT_SMTP_PROBE_VALUES["timeout_seconds"])
    rate_limit_per_second: float = float(
        DEFAULT_SMTP_PROBE_VALUES["rate_limit_per_second"]
    )
    retries: int = int(DEFAULT_SMTP_PROBE_VALUES["retries"])
    retry_temp_failures: bool = bool(DEFAULT_SMTP_PROBE_VALUES["retry_temp_failures"])
    max_retries: int = int(DEFAULT_SMTP_PROBE_VALUES["max_retries"])
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
    catch_all: CatchAllConfig = field(default_factory=CatchAllConfig)
    domain_intelligence: DomainIntelligenceConfig = field(
        default_factory=DomainIntelligenceConfig
    )
    bounce_ingestion: BounceIngestionConfig = field(
        default_factory=BounceIngestionConfig
    )
    rollout: RolloutConfig = field(default_factory=RolloutConfig)
    post_passes: PostPassesConfig = field(default_factory=PostPassesConfig)
    typo_correction: TypoCorrectionConfig = field(default_factory=TypoCorrectionConfig)
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
    rollout_raw = raw_values.pop("rollout", {}) if isinstance(raw_values, dict) else {}
    post_passes_raw = (
        raw_values.pop("post_passes", {}) if isinstance(raw_values, dict) else {}
    )
    catch_all_raw = (
        raw_values.pop("catch_all", {}) if isinstance(raw_values, dict) else {}
    )
    domain_intel_raw = (
        raw_values.pop("domain_intelligence", {})
        if isinstance(raw_values, dict)
        else {}
    )
    bounce_raw = (
        raw_values.pop("bounce_ingestion", {})
        if isinstance(raw_values, dict)
        else {}
    )
    typo_raw = raw_values.pop("typo_correction", {}) if isinstance(raw_values, dict) else {}
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

    rollout_merged = {**DEFAULT_ROLLOUT_VALUES, **(rollout_raw or {})}
    rollout_config = RolloutConfig(
        profile=str(
            rollout_merged.get("profile") or DEFAULT_ROLLOUT_VALUES["profile"]
        ),
        require_preflight=bool(rollout_merged["require_preflight"]),
        block_uncapped_live_smtp=bool(
            rollout_merged["block_uncapped_live_smtp"]
        ),
        max_rows_without_confirmation=int(
            rollout_merged["max_rows_without_confirmation"]
        ),
        package_client_artifacts=bool(rollout_merged["package_client_artifacts"]),
        require_operator_review=bool(rollout_merged["require_operator_review"]),
    )

    post_passes_merged = {
        **DEFAULT_POST_PASSES_VALUES,
        **(post_passes_raw or {}),
    }
    post_passes_config = PostPassesConfig(
        mutate_materialized_outputs=bool(
            post_passes_merged["mutate_materialized_outputs"]
        ),
    )

    catch_all_merged = {**DEFAULT_CATCH_ALL_VALUES, **(catch_all_raw or {})}
    catch_all_config = CatchAllConfig(
        enabled=bool(catch_all_merged["enabled"]),
        method=str(catch_all_merged["method"]),
        cache_by_domain=bool(catch_all_merged["cache_by_domain"]),
        default_on_unknown=str(catch_all_merged["default_on_unknown"]),
    )

    domain_intel_merged = {
        **DEFAULT_DOMAIN_INTELLIGENCE_VALUES,
        **(domain_intel_raw or {}),
    }
    domain_intel_config = DomainIntelligenceConfig(
        enabled=bool(domain_intel_merged["enabled"]),
        min_observations_for_reputation=int(
            domain_intel_merged["min_observations_for_reputation"]
        ),
        cold_start_default_risk=str(
            domain_intel_merged["cold_start_default_risk"]
        ),
        high_risk_blocks_auto_approve=bool(
            domain_intel_merged["high_risk_blocks_auto_approve"]
        ),
        cold_start_requires_smtp_valid=bool(
            domain_intel_merged["cold_start_requires_smtp_valid"]
        ),
    )

    bounce_merged = {**DEFAULT_BOUNCE_INGESTION_VALUES, **(bounce_raw or {})}
    bounce_config = BounceIngestionConfig(
        enabled=bool(bounce_merged["enabled"]),
        store_path=str(bounce_merged["store_path"]),
        min_observations_for_domain_reputation=int(
            bounce_merged["min_observations_for_domain_reputation"]
        ),
        medium_hard_bounce_rate=float(bounce_merged["medium_hard_bounce_rate"]),
        high_hard_bounce_rate=float(bounce_merged["high_hard_bounce_rate"]),
        high_blocked_rate=float(bounce_merged["high_blocked_rate"]),
        complaint_is_high_risk=bool(bounce_merged["complaint_is_high_risk"]),
    )

    typo_merged = {**DEFAULT_TYPO_CORRECTION_VALUES, **(typo_raw or {})}
    typo_whitelist_raw = typo_merged.get("whitelist") or []
    if isinstance(typo_whitelist_raw, str):
        typo_whitelist_raw = [typo_whitelist_raw]
    typo_config = TypoCorrectionConfig(
        mode=str(typo_merged["mode"]),
        max_edit_distance=int(typo_merged["max_edit_distance"]),
        whitelist=tuple(
            str(d).strip().lower()
            for d in typo_whitelist_raw
            if isinstance(d, str) and d.strip()
        ),
        require_original_no_mx=bool(typo_merged["require_original_no_mx"]),
    )

    smtp_merged = {**DEFAULT_SMTP_PROBE_VALUES, **(smtp_raw or {})}
    raw_max_candidates = smtp_merged.get("max_candidates_per_run")
    if raw_max_candidates is None:
        max_candidates_value: int | None = None
    else:
        try:
            n = int(raw_max_candidates)
            max_candidates_value = n if n > 0 else None
        except (TypeError, ValueError):
            max_candidates_value = None
    smtp_config = SMTPProbeConfig(
        enabled=bool(smtp_merged["enabled"]),
        dry_run=bool(smtp_merged["dry_run"]),
        max_candidates_per_run=max_candidates_value,
        sample_size=int(smtp_merged["sample_size"]),
        max_per_domain=int(smtp_merged["max_per_domain"]),
        timeout_seconds=float(smtp_merged["timeout_seconds"]),
        rate_limit_per_second=float(smtp_merged["rate_limit_per_second"]),
        retries=int(smtp_merged["retries"]),
        retry_temp_failures=bool(smtp_merged.get("retry_temp_failures", True)),
        max_retries=int(smtp_merged.get("max_retries", 1)),
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
        catch_all=catch_all_config,
        domain_intelligence=domain_intel_config,
        bounce_ingestion=bounce_config,
        rollout=rollout_config,
        post_passes=post_passes_config,
        typo_correction=typo_config,
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

    rc = config.rollout
    if rc.profile not in ROLLOUT_PROFILES:
        allowed = ", ".join(sorted(ROLLOUT_PROFILES))
        raise ValueError(
            "rollout.profile must be one of "
            f"{allowed}; got {rc.profile!r}."
        )

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
