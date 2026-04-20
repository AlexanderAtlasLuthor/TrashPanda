"""Pipeline orchestration for Subphase 2-6: ingestion, normalization,
email syntax validation, domain enrichment, DNS enrichment, and scoring."""

from __future__ import annotations

import logging
from pathlib import Path

from .config import AppConfig
from .dns_utils import DnsCache, apply_dns_enrichment_column
from .io_utils import build_run_context, discover_input_files, prepare_input_file, read_csv_in_chunks
from .models import FileIngestionMetrics, PipelineResult, RunContext
from .normalizers import (
    add_technical_metadata,
    apply_domain_typo_correction_column,
    compare_domain_with_input_column,
    extract_email_components,
    normalize_headers,
    normalize_values,
)
from .scoring import apply_scoring_column
from .typo_rules import build_typo_map
from .validators import (
    validate_duplicate_columns,
    validate_email_syntax_column,
    validate_required_columns,
    validate_reserved_columns,
)


class EmailCleaningPipeline:
    """Ingestion pipeline through Subphase 6: scoring and preliminary bucket assignment."""

    def __init__(self, config: AppConfig, logger: logging.Logger) -> None:
        self.config = config
        self.logger = logger

    def run(
        self,
        input_dir: str | Path | None = None,
        input_file: str | Path | None = None,
        output_dir: str | Path | None = None,
        run_context: RunContext | None = None,
    ) -> PipelineResult:
        """Discover files, prepare CSV inputs, and process normalized chunks."""

        active_run_context = run_context or build_run_context(self.config, output_dir=output_dir)
        input_mode = "input_dir" if input_dir else "input_file"
        discovered_files, ignored_files = discover_input_files(input_dir=input_dir, input_file=input_file)

        typo_map_path = (
            self.config.paths.typo_map_path
            if self.config.paths is not None
            else Path(__file__).resolve().parent.parent / "configs" / "typo_map.csv"
        )
        typo_map = build_typo_map(typo_map_path)

        dns_cache = DnsCache()

        # Run-level scoring accumulators.
        run_hard_fails = 0
        run_high_confidence = 0
        run_review = 0
        run_invalid = 0

        self.logger.info("Starting email cleaner ingestion run.")
        self.logger.info("Run directory: %s", active_run_context.run_dir)
        self.logger.info("Input mode: %s", input_mode)
        self.logger.info("Discovered supported files: %s", len(discovered_files))
        self.logger.info("Loaded typo map with %s entries.", len(typo_map))
        for ignored_name in ignored_files:
            self.logger.warning("Ignoring unsupported file: %s", ignored_name)

        total_rows = 0
        total_chunks = 0
        file_metrics: list[FileIngestionMetrics] = []

        for discovered_file in discovered_files:
            prepared_file = prepare_input_file(discovered_file, active_run_context)
            metrics = FileIngestionMetrics(
                source_file=discovered_file.original_name,
                source_file_type=discovered_file.file_type,
                converted_from_xlsx=discovered_file.file_type == "xlsx",
                sheet_name=prepared_file.sheet_name,
            )
            if prepared_file.sheet_name:
                self.logger.info(
                    "Processing %s via sheet '%s'",
                    discovered_file.original_name,
                    prepared_file.sheet_name,
                )
            else:
                self.logger.info("Processing %s", discovered_file.original_name)

            first_chunk = True
            for raw_chunk, chunk_context in read_csv_in_chunks(prepared_file.processing_csv_path, self.config.chunk_size):
                normalized_chunk = normalize_headers(raw_chunk)

                if first_chunk:
                    validate_duplicate_columns(normalized_chunk.columns)
                    validate_reserved_columns(normalized_chunk.columns)
                    validate_required_columns(normalized_chunk.columns)
                    metrics.normalized_columns = normalized_chunk.columns.tolist()
                    first_chunk = False

                normalized_chunk = normalize_values(normalized_chunk)
                normalized_chunk = add_technical_metadata(
                    normalized_chunk,
                    input_file=discovered_file,
                    chunk_context=chunk_context,
                )

                # Subphase 3: Email syntax validation
                normalized_chunk = validate_email_syntax_column(normalized_chunk)
                valid_count = int(normalized_chunk["syntax_valid"].eq(True).sum())
                invalid_count = int(normalized_chunk["syntax_valid"].eq(False).sum())

                # Subphase 4: Domain extraction, typo correction, domain comparison
                normalized_chunk = extract_email_components(normalized_chunk)
                normalized_chunk = apply_domain_typo_correction_column(normalized_chunk, typo_map)
                normalized_chunk = compare_domain_with_input_column(normalized_chunk)

                derived_count = int(normalized_chunk["domain_from_email"].notna().sum())
                typo_count = int(normalized_chunk["typo_corrected"].eq(True).sum())
                mismatch_count = int(normalized_chunk["domain_matches_input_column"].eq(False).sum())

                # Subphase 5: DNS/MX enrichment
                queries_before = dns_cache.domains_queried
                hits_before = dns_cache.cache_hits

                normalized_chunk = apply_dns_enrichment_column(
                    normalized_chunk,
                    cache=dns_cache,
                    timeout_seconds=self.config.dns_timeout_seconds,
                    fallback_to_a_record=self.config.fallback_to_a_record,
                    max_workers=self.config.max_workers,
                )

                new_queries = dns_cache.domains_queried - queries_before
                new_hits = dns_cache.cache_hits - hits_before
                mx_found = int(normalized_chunk["has_mx_record"].eq(True).sum())
                a_fallback = int(normalized_chunk["has_a_record"].eq(True).sum())
                dns_failures = int(normalized_chunk["domain_exists"].eq(False).sum())

                # Subphase 6: Scoring and preliminary bucket assignment
                normalized_chunk = apply_scoring_column(
                    normalized_chunk,
                    high_confidence_threshold=self.config.high_confidence_threshold,
                    review_threshold=self.config.review_threshold,
                )

                chunk_hard_fails = int(normalized_chunk["hard_fail"].eq(True).sum())
                chunk_high = int((normalized_chunk["preliminary_bucket"] == "high_confidence").sum())
                chunk_review = int((normalized_chunk["preliminary_bucket"] == "review").sum())
                chunk_invalid = int((normalized_chunk["preliminary_bucket"] == "invalid").sum())

                scored_rows = normalized_chunk[normalized_chunk["hard_fail"].eq(False)]
                chunk_avg_score = (
                    round(scored_rows["score"].mean(), 1)
                    if len(scored_rows) > 0
                    else 0.0
                )

                run_hard_fails += chunk_hard_fails
                run_high_confidence += chunk_high
                run_review += chunk_review
                run_invalid += chunk_invalid

                metrics.rows_processed += chunk_context.row_count
                metrics.chunks_processed += 1
                total_rows += chunk_context.row_count
                total_chunks += 1

                self.logger.info(
                    "Processed chunk %s from %s | rows=%s "
                    "valid_emails=%s invalid_emails=%s "
                    "derived_domains=%s typo_corrections=%s domain_mismatches=%s "
                    "dns_new_queries=%s dns_cache_hits=%s mx_found=%s a_fallback=%s dns_failures=%s "
                    "hard_fails=%s high_confidence=%s review=%s invalid=%s avg_score=%s",
                    chunk_context.chunk_index,
                    discovered_file.original_name,
                    chunk_context.row_count,
                    valid_count,
                    invalid_count,
                    derived_count,
                    typo_count,
                    mismatch_count,
                    new_queries,
                    new_hits,
                    mx_found,
                    a_fallback,
                    dns_failures,
                    chunk_hard_fails,
                    chunk_high,
                    chunk_review,
                    chunk_invalid,
                    chunk_avg_score,
                )

            self.logger.info(
                "Finished %s | chunks=%s rows=%s",
                discovered_file.original_name,
                metrics.chunks_processed,
                metrics.rows_processed,
            )
            file_metrics.append(metrics)

        self.logger.info(
            "Pipeline run complete | files=%s chunks=%s rows=%s "
            "dns_total_queries=%s dns_total_cache_hits=%s "
            "scoring_hard_fails=%s high_confidence=%s review=%s invalid=%s",
            len(discovered_files),
            total_chunks,
            total_rows,
            dns_cache.domains_queried,
            dns_cache.cache_hits,
            run_hard_fails,
            run_high_confidence,
            run_review,
            run_invalid,
        )
        return PipelineResult(
            status="subphase_6_ready",
            input_mode=input_mode,
            run_id=active_run_context.run_id,
            run_dir=active_run_context.run_dir,
            logs_dir=active_run_context.logs_dir,
            temp_dir=active_run_context.temp_dir,
            total_files=len(discovered_files),
            total_rows=total_rows,
            total_chunks=total_chunks,
            processed_files=[item.original_name for item in discovered_files],
            ignored_files=ignored_files,
            file_metrics=file_metrics,
        )
