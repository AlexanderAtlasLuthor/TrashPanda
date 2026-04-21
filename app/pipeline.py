"""Pipeline orchestration for Subphase 2-8: ingestion, normalization,
email syntax validation, domain enrichment, DNS enrichment, scoring,
global deduplication, and materialization."""

from __future__ import annotations

import csv
import logging
from pathlib import Path

from .config import AppConfig
from .dedupe import DedupeIndex
from .dns_utils import DnsCache
from .engine import ChunkPayload, PipelineContext, PipelineEngine
from .engine.stages import (
    CompletenessStage,
    DNSEnrichmentStage,
    DedupeStage,
    DomainComparisonStage,
    DomainExtractionStage,
    EmailNormalizationStage,
    EmailSyntaxValidationStage,
    HeaderNormalizationStage,
    ScoringStage,
    ScoringV2Stage,
    StagingPersistenceStage,
    StructuralValidationStage,
    TechnicalMetadataStage,
    TypoCorrectionStage,
    ValueNormalizationStage,
)
from .io_utils import build_run_context, discover_input_files, prepare_input_file, read_csv_in_chunks
from .models import FileIngestionMetrics, MaterializationMetrics, PipelineResult, RunContext
from .reporting import ReportingStats, generate_reports
from .storage import StagingDB
from .typo_rules import build_typo_map


class EmailCleaningPipeline:
    """Ingestion pipeline through Subphase 8: scoring, deduplication, and output materialization."""

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
        dedupe_index = DedupeIndex()
        staging = StagingDB(active_run_context.staging_db_path)

        # Engine-driven portion of the chunk flow. After Subphase 5 of the
        # engine refactor, ALL chunk-level business logic runs through the
        # PipelineEngine: preprocessing, email processing, DNS enrichment,
        # scoring, completeness, email normalization, dedupe, and staging
        # persistence. The only remaining inline work in ``run`` is
        # metric aggregation and logging. The second-pass materialization
        # (``_materialize``) is explicitly out of scope for this refactor
        # and continues to operate directly on the staging DB.
        pipeline_context = PipelineContext(
            config=self.config,
            logger=self.logger,
            run_context=active_run_context,
            typo_map=typo_map,
            dns_cache=dns_cache,
            dedupe_index=dedupe_index,
            staging=staging,
        )
        # V2 scoring runs immediately after V1 ``ScoringStage`` and before
        # ``CompletenessStage``: at that point every upstream column the
        # V2 evaluators consume is already populated (syntax,
        # corrected_domain, typo/domain_match flags, DNS outcomes), and
        # placing it before completeness keeps the V1-driven downstream
        # path (completeness → dedupe → staging → materialize) totally
        # unaffected. V2 only appends ``*_v2`` columns; no existing V1
        # column is touched.
        chunk_engine = PipelineEngine(
            stages=[
                HeaderNormalizationStage(),
                StructuralValidationStage(),
                ValueNormalizationStage(),
                TechnicalMetadataStage(),
                EmailSyntaxValidationStage(),
                DomainExtractionStage(),
                TypoCorrectionStage(),
                DomainComparisonStage(),
                DNSEnrichmentStage(),
                ScoringStage(),
                ScoringV2Stage(),
                CompletenessStage(),
                EmailNormalizationStage(),
                DedupeStage(),
                StagingPersistenceStage(),
            ],
            logger=self.logger,
        )

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
                # All chunk-level business logic runs inside chunk_engine:
                # preprocessing → email processing → DNS enrichment →
                # scoring → completeness → email_normalized → dedupe →
                # staging persist. The code below only consumes the
                # resulting frame and shared-state counters to emit
                # per-chunk aggregate metrics.
                payload = ChunkPayload(
                    frame=raw_chunk,
                    chunk_index=chunk_context.chunk_index,
                    source_file=discovered_file.original_name,
                    metadata={
                        "is_first_chunk": first_chunk,
                        "file_metrics": metrics,
                        "input_file": discovered_file,
                        "chunk_context": chunk_context,
                    },
                )

                # Snapshot shared-state counters BEFORE engine.run so the
                # per-chunk deltas reflect only the work this engine pass
                # did (DNS cache + dedupe index are mutated by stages).
                queries_before = dns_cache.domains_queried
                hits_before = dns_cache.cache_hits
                canonicals_before = dedupe_index.new_canonicals
                dupes_before = dedupe_index.duplicates_detected
                replaced_before = dedupe_index.replaced_canonicals

                payload = chunk_engine.run(payload, pipeline_context)
                normalized_chunk = payload.frame
                first_chunk = False

                # Per-chunk aggregate counts derived from columns produced
                # by the engine stages above. Identical to the values the
                # previous inline code computed.
                valid_count = int(normalized_chunk["syntax_valid"].eq(True).sum())
                invalid_count = int(normalized_chunk["syntax_valid"].eq(False).sum())
                derived_count = int(normalized_chunk["domain_from_email"].notna().sum())
                typo_count = int(normalized_chunk["typo_corrected"].eq(True).sum())
                mismatch_count = int(normalized_chunk["domain_matches_input_column"].eq(False).sum())

                # DNS delta metrics (DNSEnrichmentStage ran via the engine).
                new_queries = dns_cache.domains_queried - queries_before
                new_hits = dns_cache.cache_hits - hits_before
                mx_found = int(normalized_chunk["has_mx_record"].eq(True).sum())
                a_fallback = int(normalized_chunk["has_a_record"].eq(True).sum())
                dns_failures = int(normalized_chunk["domain_exists"].eq(False).sum())

                # Scoring aggregate metrics (ScoringStage ran via the engine).
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

                # Dedupe delta metrics (DedupeStage ran via the engine).
                chunk_new_canonicals = dedupe_index.new_canonicals - canonicals_before
                chunk_duplicates = dedupe_index.duplicates_detected - dupes_before
                chunk_replaced = dedupe_index.replaced_canonicals - replaced_before

                metrics.rows_processed += chunk_context.row_count
                metrics.chunks_processed += 1
                total_rows += chunk_context.row_count
                total_chunks += 1

                self.logger.info(
                    "Processed chunk %s from %s | rows=%s "
                    "valid_emails=%s invalid_emails=%s "
                    "derived_domains=%s typo_corrections=%s domain_mismatches=%s "
                    "dns_new_queries=%s dns_cache_hits=%s mx_found=%s a_fallback=%s dns_failures=%s "
                    "hard_fails=%s high_confidence=%s review=%s invalid=%s avg_score=%s "
                    "dedupe_new_canonicals=%s dedupe_duplicates=%s dedupe_replaced=%s dedupe_index_size=%s",
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
                    chunk_new_canonicals,
                    chunk_duplicates,
                    chunk_replaced,
                    dedupe_index.index_size,
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
            "scoring_hard_fails=%s high_confidence=%s review=%s invalid=%s "
            "dedupe_total_canonicals=%s dedupe_total_duplicates=%s "
            "dedupe_total_replaced=%s dedupe_index_size=%s",
            len(discovered_files),
            total_chunks,
            total_rows,
            dns_cache.domains_queried,
            dns_cache.cache_hits,
            run_hard_fails,
            run_high_confidence,
            run_review,
            run_invalid,
            dedupe_index.new_canonicals,
            dedupe_index.duplicates_detected,
            dedupe_index.replaced_canonicals,
            dedupe_index.index_size,
        )

        # Subphase 8 second pass: materialize final output
        mat_metrics = self._materialize(staging, dedupe_index, active_run_context)
        staging.close()

        return PipelineResult(
            status="subphase_8_ready",
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
            clean_high_confidence_path=active_run_context.run_dir / "clean_high_confidence.csv",
            review_path=active_run_context.run_dir / "review_medium_confidence.csv",
            removed_path=active_run_context.run_dir / "removed_invalid.csv",
            processing_report_json_path=active_run_context.run_dir / "processing_report.json",
            total_canonical_rows=mat_metrics.total_canonical_rows,
            total_duplicate_rows=mat_metrics.total_duplicate_rows,
            total_output_clean=mat_metrics.total_output_clean,
            total_output_review=mat_metrics.total_output_review,
            total_output_removed=mat_metrics.total_output_removed,
        )

    def _materialize(
        self,
        staging: StagingDB,
        dedupe_index: DedupeIndex,
        run_context: RunContext,
    ) -> MaterializationMetrics:
        """Second pass: reconcile stale canonical flags and write final output CSVs."""
        clean_path = run_context.run_dir / "clean_high_confidence.csv"
        review_path = run_context.run_dir / "review_medium_confidence.csv"
        removed_path = run_context.run_dir / "removed_invalid.csv"

        stats = ReportingStats(run_id=run_context.run_id)
        stats.total_unique_emails = dedupe_index.index_size

        fieldnames: list[str] | None = None

        with (
            clean_path.open("w", newline="", encoding="utf-8") as clean_fh,
            review_path.open("w", newline="", encoding="utf-8") as review_fh,
            removed_path.open("w", newline="", encoding="utf-8") as removed_fh,
        ):
            clean_writer: csv.DictWriter | None = None
            review_writer: csv.DictWriter | None = None
            removed_writer: csv.DictWriter | None = None

            for batch in staging.iter_all_rows():
                for row_dict in batch:
                    if fieldnames is None:
                        fieldnames = [k for k in row_dict if k != "final_output_reason"]
                        fieldnames.append("final_output_reason")
                        clean_writer = csv.DictWriter(
                            clean_fh, fieldnames=fieldnames, extrasaction="ignore", restval=""
                        )
                        review_writer = csv.DictWriter(
                            review_fh, fieldnames=fieldnames, extrasaction="ignore", restval=""
                        )
                        removed_writer = csv.DictWriter(
                            removed_fh, fieldnames=fieldnames, extrasaction="ignore", restval=""
                        )
                        clean_writer.writeheader()
                        review_writer.writeheader()
                        removed_writer.writeheader()

                    email_norm = row_dict.get("email_normalized")
                    source_file = str(row_dict.get("source_file") or "")
                    source_row_number = int(row_dict.get("source_row_number") or 0)
                    hard_fail = bool(row_dict.get("hard_fail", False))
                    preliminary_bucket = str(row_dict.get("preliminary_bucket") or "")
                    was_canonical = bool(row_dict.get("is_canonical", False))

                    is_canonical_final = dedupe_index.is_final_canonical(
                        email_norm, source_file, source_row_number
                    )

                    if was_canonical and not is_canonical_final:
                        stats.replaced_canonical_corrections += 1

                    if not is_canonical_final:
                        reason = "removed_duplicate"
                    elif hard_fail:
                        reason = "removed_hard_fail"
                    elif preliminary_bucket == "high_confidence":
                        reason = "kept_high_confidence"
                    elif preliminary_bucket == "review":
                        reason = "kept_review"
                    else:
                        reason = "removed_low_score"

                    row_dict["final_output_reason"] = reason

                    stats.total_rows += 1
                    if is_canonical_final:
                        stats.total_canonical_rows += 1
                    else:
                        stats.total_duplicate_rows += 1

                    if hard_fail:
                        stats.total_hard_fail += 1
                    else:
                        try:
                            stats.score_sum += int(row_dict.get("score") or 0)
                            stats.score_count += 1
                        except (TypeError, ValueError):
                            pass

                    dns_err = row_dict.get("dns_error")
                    if dns_err is not None and str(dns_err).strip():
                        stats.total_dns_errors += 1

                    # Effective domain: prefer corrected_domain (the value DNS was
                    # queried on), falling back to domain_from_email, then the
                    # input domain column.
                    domain = str(
                        row_dict.get("corrected_domain")
                        or row_dict.get("domain_from_email")
                        or row_dict.get("domain")
                        or ""
                    )
                    info: dict | None = None
                    if domain:
                        info = stats.domain_info.get(domain)
                        if info is None:
                            info = {
                                "total_rows": 0,
                                "corrected_count": 0,
                                "domain_exists": False,
                                "has_mx_record": False,
                                "has_a_record": False,
                                "kept_rows": 0,
                                "removed_rows": 0,
                            }
                            stats.domain_info[domain] = info
                        info["total_rows"] += 1
                        if bool(row_dict.get("typo_corrected")):
                            info["corrected_count"] += 1
                        for dns_key in ("domain_exists", "has_mx_record", "has_a_record"):
                            if bool(row_dict.get(dns_key)):
                                info[dns_key] = True

                    if reason == "kept_high_confidence":
                        stats.total_output_clean += 1
                        if info is not None:
                            info["kept_rows"] += 1
                        assert clean_writer is not None
                        clean_writer.writerow(row_dict)
                    elif reason == "kept_review":
                        stats.total_output_review += 1
                        if info is not None:
                            info["kept_rows"] += 1
                        assert review_writer is not None
                        review_writer.writerow(row_dict)
                    else:
                        stats.total_output_removed += 1
                        if info is not None:
                            info["removed_rows"] += 1
                        assert removed_writer is not None
                        removed_writer.writerow(row_dict)

                    if bool(row_dict.get("typo_corrected")):
                        stats.typo_corrections.append({
                            "source_file": source_file,
                            "source_row_number": source_row_number,
                            "email": row_dict.get("email") or "",
                            "typo_original_domain": (
                                row_dict.get("typo_original_domain")
                                or row_dict.get("domain_from_email")
                                or ""
                            ),
                            "corrected_domain": row_dict.get("corrected_domain") or "",
                        })

                    if email_norm and not is_canonical_final:
                        if email_norm not in stats.duplicate_groups:
                            canonical = dedupe_index.get_final_canonical(email_norm)
                            stats.duplicate_groups[email_norm] = {
                                "duplicate_count": 0,
                                "duplicates_removed_count": 0,
                                "canonical_source_file": canonical.source_file if canonical else "",
                                "canonical_source_row_number": canonical.source_row_number if canonical else 0,
                                "winner_score": canonical.score if canonical else 0,
                                "winner_completeness": canonical.completeness_score if canonical else 0,
                                "duplicate_reasons": [],
                            }
                        group = stats.duplicate_groups[email_norm]
                        group["duplicate_count"] += 1
                        group["duplicates_removed_count"] += 1
                        dup_reason = row_dict.get("duplicate_reason")
                        if dup_reason and dup_reason not in group["duplicate_reasons"]:
                            group["duplicate_reasons"].append(dup_reason)

        generate_reports(stats, run_context.run_dir)

        self.logger.info(
            "Materialization complete | total=%s canonical=%s duplicates=%s "
            "clean=%s review=%s removed=%s stale_corrections=%s",
            stats.total_rows,
            stats.total_canonical_rows,
            stats.total_duplicate_rows,
            stats.total_output_clean,
            stats.total_output_review,
            stats.total_output_removed,
            stats.replaced_canonical_corrections,
        )

        return MaterializationMetrics(
            total_canonical_rows=stats.total_canonical_rows,
            total_duplicate_rows=stats.total_duplicate_rows,
            total_output_clean=stats.total_output_clean,
            total_output_review=stats.total_output_review,
            total_output_removed=stats.total_output_removed,
            replaced_canonical_corrections=stats.replaced_canonical_corrections,
        )
