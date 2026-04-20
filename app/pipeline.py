"""Pipeline orchestration for Subphase 2-4: ingestion, normalization, email syntax validation, and domain enrichment."""

from __future__ import annotations

import logging
from pathlib import Path

from .config import AppConfig
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
from .typo_rules import build_typo_map
from .validators import (
    validate_duplicate_columns,
    validate_email_syntax_column,
    validate_required_columns,
    validate_reserved_columns,
)



class EmailCleaningPipeline:
    """Ingestion pipeline through Subphase 4: domain extraction, typo correction, and domain comparison."""

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

        # Load typo map once for all chunks
        typo_map_path = (
            self.config.paths.typo_map_path
            if self.config.paths is not None
            else Path(__file__).resolve().parent.parent / "configs" / "typo_map.csv"
        )
        typo_map = build_typo_map(typo_map_path)

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
                valid_count = int((normalized_chunk["syntax_valid"] == True).sum())
                invalid_count = int((normalized_chunk["syntax_valid"] == False).sum())

                # Subphase 4: Domain extraction, typo correction, domain comparison
                normalized_chunk = extract_email_components(normalized_chunk)
                normalized_chunk = apply_domain_typo_correction_column(normalized_chunk, typo_map)
                normalized_chunk = compare_domain_with_input_column(normalized_chunk)

                derived_count = int(normalized_chunk["domain_from_email"].notna().sum())
                typo_count = int((normalized_chunk["typo_corrected"] == True).sum())
                mismatch_count = int((normalized_chunk["domain_matches_input_column"] == False).sum())

                metrics.rows_processed += chunk_context.row_count
                metrics.chunks_processed += 1
                total_rows += chunk_context.row_count
                total_chunks += 1

                self.logger.info(
                    "Processed chunk %s from %s | rows=%s valid_emails=%s invalid_emails=%s "
                    "derived_domains=%s typo_corrections=%s domain_mismatches=%s",
                    chunk_context.chunk_index,
                    discovered_file.original_name,
                    chunk_context.row_count,
                    valid_count,
                    invalid_count,
                    derived_count,
                    typo_count,
                    mismatch_count,
                )

            self.logger.info(
                "Finished %s | chunks=%s rows=%s",
                discovered_file.original_name,
                metrics.chunks_processed,
                metrics.rows_processed,
            )
            file_metrics.append(metrics)

        self.logger.info(
            "Ingestion, email syntax validation, and domain enrichment finished | files=%s chunks=%s rows=%s",
            len(discovered_files),
            total_chunks,
            total_rows,
        )
        return PipelineResult(
            status="subphase_4_ready",
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
