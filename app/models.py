"""Core dataclasses used by the project runtime."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path


@dataclass(slots=True)
class RunContext:
    """Resolved paths and metadata for one CLI execution."""

    run_id: str
    run_dir: Path
    logs_dir: Path
    temp_dir: Path
    staging_db_path: Path
    started_at: datetime


@dataclass(slots=True)
class InputFile:
    """A supported input file discovered by the pipeline."""

    absolute_path: Path
    original_name: str
    file_type: str


@dataclass(slots=True)
class PreparedInputFile:
    """A discovered input file after preparation for internal CSV processing."""

    source: InputFile
    processing_csv_path: Path
    sheet_name: str | None = None


@dataclass(slots=True)
class ChunkContext:
    """Technical context for one processed chunk."""

    chunk_index: int
    row_count: int
    start_row_number: int


@dataclass(slots=True)
class FileIngestionMetrics:
    """Per-file ingestion summary for Subphase 2."""

    source_file: str
    source_file_type: str
    rows_processed: int = 0
    chunks_processed: int = 0
    normalized_columns: list[str] = field(default_factory=list)
    converted_from_xlsx: bool = False
    sheet_name: str | None = None


@dataclass(slots=True)
class EmailSyntaxMetrics:
    """Per-chunk email syntax validation summary for Subphase 3."""

    valid_count: int = 0
    invalid_count: int = 0
    empty_count: int = 0


@dataclass(slots=True)
class DomainEnrichmentMetrics:
    """Per-chunk domain enrichment summary for Subphase 4."""

    derived_domain_count: int = 0
    typo_correction_count: int = 0
    domain_mismatch_count: int = 0


@dataclass(slots=True)
class DnsEnrichmentMetrics:
    """Per-chunk DNS enrichment summary for Subphase 5."""

    new_queries: int = 0
    cache_hits: int = 0
    mx_found: int = 0
    a_fallback_found: int = 0
    dns_failures: int = 0


@dataclass(slots=True)
class ScoringMetrics:
    """Per-chunk scoring summary for Subphase 6."""

    hard_fail_count: int = 0
    high_confidence_count: int = 0
    review_count: int = 0
    invalid_count: int = 0
    total_score: int = 0
    rows_scored: int = 0


@dataclass(slots=True)
class PipelineResult:
    """Result returned by the ingestion pipeline."""

    status: str
    input_mode: str
    run_id: str
    run_dir: Path
    logs_dir: Path
    temp_dir: Path
    total_files: int
    total_rows: int
    total_chunks: int
    processed_files: list[str] = field(default_factory=list)
    ignored_files: list[str] = field(default_factory=list)
    file_metrics: list[FileIngestionMetrics] = field(default_factory=list)
