"""Domain typo correction rules for Subphase 4.

Provides a closed, explicit, auditable lookup-based correction for known
common domain typos. No fuzzy matching, no Levenshtein, no heuristics.
Only domains present in the typo map are corrected; everything else passes
through unchanged.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class DomainTypoCorrectionResult:
    """Result of applying typo correction to a single domain."""

    typo_corrected: bool
    typo_original_domain: str | None
    corrected_domain: str | None


def build_typo_map(typo_map_path: Path) -> dict[str, str]:
    """Load the closed typo map CSV into a lookup dict.

    Expects columns: typo_domain, correct_domain.
    Both values are lowercased and stripped before storing.
    Returns an empty dict if the file does not exist.
    """

    typo_map: dict[str, str] = {}
    if not typo_map_path.exists():
        return typo_map

    with typo_map_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            typo = (row.get("typo_domain") or "").strip().lower()
            correct = (row.get("correct_domain") or "").strip().lower()
            if typo and correct:
                typo_map[typo] = correct

    return typo_map


def apply_domain_typo_correction(
    domain: str | None,
    typo_map: dict[str, str],
) -> DomainTypoCorrectionResult:
    """Apply a single typo correction lookup to a domain string.

    Rules:
    - If domain is None, all result fields are None.
    - If domain is in typo_map, mark as corrected and return the mapped value.
    - Otherwise, typo_corrected=False and corrected_domain equals the original.

    The local part of the email is never touched by this function.
    """

    if domain is None:
        return DomainTypoCorrectionResult(
            typo_corrected=False,
            typo_original_domain=None,
            corrected_domain=None,
        )

    corrected = typo_map.get(domain)
    if corrected is not None:
        return DomainTypoCorrectionResult(
            typo_corrected=True,
            typo_original_domain=domain,
            corrected_domain=corrected,
        )

    return DomainTypoCorrectionResult(
        typo_corrected=False,
        typo_original_domain=domain,
        corrected_domain=domain,
    )
