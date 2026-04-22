"""Reporting for Subphase 8: final run statistics and output summaries."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass
class ReportingStats:
    """Accumulated statistics from the Subphase 8 second pass.

    Field naming follows the spec where possible; legacy names are kept as
    top-level attributes so the surrounding pipeline code remains stable.
    """

    run_id: str
    generated_at: str = field(default_factory=lambda: datetime.now().isoformat())
    # Row-level totals (legacy names; map to spec in the emitted report)
    total_rows: int = 0
    total_canonical_rows: int = 0
    total_duplicate_rows: int = 0
    total_output_clean: int = 0
    total_output_review: int = 0
    total_output_removed: int = 0
    replaced_canonical_corrections: int = 0
    # Spec-only totals
    total_hard_fail: int = 0
    total_unique_emails: int = 0
    total_dns_errors: int = 0
    # Score accumulators (for average_score over non-hard-fail rows)
    score_sum: int = 0
    score_count: int = 0
    # Nested data structures
    domain_info: dict[str, dict[str, Any]] = field(default_factory=dict)
    typo_corrections: list[dict[str, Any]] = field(default_factory=list)
    duplicate_groups: dict[str, dict[str, Any]] = field(default_factory=dict)


def _count_domains_with_mx(domain_info: dict[str, dict[str, Any]]) -> int:
    return sum(1 for info in domain_info.values() if info.get("has_mx_record"))


def _count_domains_with_a_only(domain_info: dict[str, dict[str, Any]]) -> int:
    return sum(
        1
        for info in domain_info.values()
        if info.get("has_a_record") and not info.get("has_mx_record")
    )


def _average_score(stats: ReportingStats) -> float:
    if stats.score_count <= 0:
        return 0.0
    return round(stats.score_sum / stats.score_count, 2)


def generate_reports(stats: ReportingStats, output_dir: Path) -> dict[str, Path]:
    """Write 5 report files to output_dir and return their paths."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    paths: dict[str, Path] = {}

    total_domains_with_mx = _count_domains_with_mx(stats.domain_info)
    total_domains_with_a_only = _count_domains_with_a_only(stats.domain_info)
    average_score = _average_score(stats)

    # 1. processing_report.json
    # Spec-aligned fields are the primary keys; legacy aliases are kept for
    # backward compatibility with callers that already consume the report.
    report_data: dict[str, Any] = {
        "run_id": stats.run_id,
        "generated_at": stats.generated_at,
        # Spec-required fields
        "total_rows_processed": stats.total_rows,
        "total_unique_emails": stats.total_unique_emails,
        "total_duplicates_removed": stats.total_duplicate_rows,
        "total_clean_high_confidence": stats.total_output_clean,
        "total_review": stats.total_output_review,
        "total_removed_invalid": stats.total_output_removed,
        "total_hard_fail": stats.total_hard_fail,
        "total_typo_corrections": len(stats.typo_corrections),
        "total_domains_with_mx": total_domains_with_mx,
        "total_domains_with_a_only": total_domains_with_a_only,
        "total_dns_errors": stats.total_dns_errors,
        "average_score": average_score,
        # Legacy aliases (backward compatibility)
        "total_rows": stats.total_rows,
        "total_canonical_rows": stats.total_canonical_rows,
        "total_duplicate_rows": stats.total_duplicate_rows,
        "total_output_clean": stats.total_output_clean,
        "total_output_review": stats.total_output_review,
        "total_output_removed": stats.total_output_removed,
        "replaced_canonical_corrections": stats.replaced_canonical_corrections,
    }
    json_path = output_dir / "processing_report.json"
    json_path.write_text(json.dumps(report_data, indent=2), encoding="utf-8")
    paths["processing_report_json"] = json_path

    # 2. processing_report.csv — tabular projection of the JSON
    csv_path = output_dir / "processing_report.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["metric", "value"])
        for key, val in report_data.items():
            writer.writerow([key, val])
    paths["processing_report_csv"] = csv_path

    # 3. domain_summary.csv
    domain_path = output_dir / "domain_summary.csv"
    with domain_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow([
            "domain",
            "total_rows",
            "corrected_count",
            "domain_exists",
            "has_mx_record",
            "has_a_record",
            "kept_rows",
            "removed_rows",
        ])
        for domain, info in sorted(stats.domain_info.items()):
            writer.writerow([
                domain,
                info.get("total_rows", 0),
                info.get("corrected_count", 0),
                bool(info.get("domain_exists", False)),
                bool(info.get("has_mx_record", False)),
                bool(info.get("has_a_record", False)),
                info.get("kept_rows", 0),
                info.get("removed_rows", 0),
            ])
    paths["domain_summary"] = domain_path

    # 4. typo_corrections.csv — conservative, non-destructive audit trail.
    # Each row records a *suggestion* the pipeline produced for an email
    # whose original domain looked like a typo. The email was never
    # rewritten by the pipeline; downstream reviewers decide what to do
    # with it. Legacy columns (``typo_original_domain``, ``corrected_domain``)
    # are kept so older consumers keep parsing the file.
    typo_path = output_dir / "typo_corrections.csv"
    with typo_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow([
            "source_file",
            "source_row_number",
            "email_original",
            "suggested_email",
            "original_domain",
            "suggested_domain",
            "typo_type",
            "confidence",
            # Legacy (backward-compat) columns:
            "email",
            "typo_original_domain",
            "corrected_domain",
        ])
        for correction in stats.typo_corrections:
            writer.writerow([
                correction.get("source_file", ""),
                correction.get("source_row_number", ""),
                correction.get("email_original", correction.get("email", "")),
                correction.get("suggested_email", ""),
                correction.get("original_domain", ""),
                correction.get("suggested_domain", ""),
                correction.get("typo_type", ""),
                correction.get("confidence", ""),
                # Legacy mirrors
                correction.get("email", ""),
                correction.get("typo_original_domain", ""),
                correction.get("corrected_domain", ""),
            ])
    paths["typo_corrections"] = typo_path

    # 5. duplicate_summary.csv
    dup_path = output_dir / "duplicate_summary.csv"
    with dup_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow([
            "email_normalized",
            "canonical_source_file",
            "canonical_source_row_number",
            "duplicates_removed_count",
            "winner_score",
            "winner_completeness",
            "duplicate_reasons",
        ])
        for email_norm, group in sorted(stats.duplicate_groups.items()):
            reasons = group.get("duplicate_reasons") or []
            writer.writerow([
                email_norm,
                group.get("canonical_source_file", ""),
                group.get("canonical_source_row_number", ""),
                group.get("duplicates_removed_count", group.get("duplicate_count", 0)),
                group.get("winner_score", 0),
                group.get("winner_completeness", 0),
                "|".join(str(r) for r in reasons),
            ])
    paths["duplicate_summary"] = dup_path

    return paths
