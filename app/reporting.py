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
    """Accumulated statistics from the Subphase 8 second pass."""

    run_id: str
    generated_at: str = field(default_factory=lambda: datetime.now().isoformat())
    total_rows: int = 0
    total_canonical_rows: int = 0
    total_duplicate_rows: int = 0
    total_output_clean: int = 0
    total_output_review: int = 0
    total_output_removed: int = 0
    replaced_canonical_corrections: int = 0
    domain_counts: dict[str, dict[str, int]] = field(default_factory=dict)
    typo_corrections: list[dict[str, Any]] = field(default_factory=list)
    duplicate_groups: dict[str, dict[str, Any]] = field(default_factory=dict)


def generate_reports(stats: ReportingStats, output_dir: Path) -> dict[str, Path]:
    """Write 5 report files to output_dir and return their paths."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    paths: dict[str, Path] = {}

    # 1. processing_report.json
    report_data = {
        "run_id": stats.run_id,
        "generated_at": stats.generated_at,
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

    # 2. processing_report.csv
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
        writer.writerow(["domain", "clean_count", "review_count", "removed_count", "total_count"])
        for domain, counts in sorted(stats.domain_counts.items()):
            clean = counts.get("clean", 0)
            review = counts.get("review", 0)
            removed = counts.get("removed", 0)
            writer.writerow([domain, clean, review, removed, clean + review + removed])
    paths["domain_summary"] = domain_path

    # 4. typo_corrections.csv
    typo_path = output_dir / "typo_corrections.csv"
    with typo_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["source_file", "source_row_number", "original_domain", "corrected_to"])
        for correction in stats.typo_corrections:
            writer.writerow([
                correction.get("source_file", ""),
                correction.get("source_row_number", ""),
                correction.get("original_domain", ""),
                correction.get("corrected_to", ""),
            ])
    paths["typo_corrections"] = typo_path

    # 5. duplicate_summary.csv
    dup_path = output_dir / "duplicate_summary.csv"
    with dup_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow([
            "email_normalized",
            "duplicate_count",
            "canonical_source_file",
            "canonical_source_row_number",
        ])
        for email_norm, group in sorted(stats.duplicate_groups.items()):
            writer.writerow([
                email_norm,
                group.get("duplicate_count", 0),
                group.get("canonical_source_file", ""),
                group.get("canonical_source_row_number", ""),
            ])
    paths["duplicate_summary"] = dup_path

    return paths
