"""V2 Phase 4 — selective SMTP-probe orchestration.

Reads the technical CSVs (already enriched by Phase 2+3), picks a
small subset of "hard" review cases, runs :func:`probe_email_*` against
them with rate-limiting, and appends six SMTP columns to every row so
the output schema stays uniform.

The hook in :func:`app.api_boundary.run_cleaning_job` wraps this whole
pass in a try/except: any failure leaves V1/V2.1/V2.2/V2.3 outputs
intact.

Key design choices
------------------
* Candidates are capped by ``max_per_domain`` before ``sample_size`` so
  one noisy domain never monopolises the probe budget.
* Rate-limiting uses a simple "minimum interval" sleep — no background
  threads, no token buckets. Easy to reason about under mocked time.
* ``probe_fn`` is injectable so tests don't touch the network. The
  default resolves to :func:`probe_email_dry_run` when ``dry_run=True``
  and :func:`probe_email_smtplib` otherwise.
* Columns use string representations throughout so the CSV stays
  parseable without type hints.
"""

from __future__ import annotations

import csv
import logging
import os
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from .smtp_probe import (
    SMTPResult,
    probe_email_dry_run,
    probe_email_smtplib,
)


_LOGGER = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Column contract                                                             #
# --------------------------------------------------------------------------- #


SMTP_COLUMNS: tuple[str, ...] = (
    "smtp_tested",
    "smtp_result",
    "smtp_confidence",
    "smtp_response_code",
    "smtp_confirmed_valid",
    "smtp_suspicious",
)


# --------------------------------------------------------------------------- #
# Config + stats                                                              #
# --------------------------------------------------------------------------- #


@dataclass(slots=True, frozen=True)
class SMTPProbeConfig:
    """Runtime knobs for the Phase-4 probing pass.

    Two independent safety switches:
      * ``enabled``  — master switch. Defaults to False.
      * ``dry_run``  — even when enabled, no network I/O unless this is
                       explicitly set to False.
    """

    enabled: bool = False
    dry_run: bool = True
    sample_size: int = 50
    max_per_domain: int = 3
    timeout_seconds: float = 4.0
    rate_limit_per_second: float = 2.0
    retries: int = 0
    negative_adjustment_trigger_threshold: int = 3
    sender_address: str = "trashpanda-probe@localhost"


@dataclass(slots=True)
class SMTPProbeStats:
    total_rows_scanned: int = 0
    total_candidates: int = 0
    total_probed: int = 0
    deliverable: int = 0
    undeliverable: int = 0
    catch_all: int = 0
    inconclusive: int = 0
    candidates_by_reason: dict[str, int] = field(default_factory=dict)

    def record_verdict(self, verdict: str) -> None:
        self.total_probed += 1
        if verdict == "deliverable":
            self.deliverable += 1
        elif verdict == "undeliverable":
            self.undeliverable += 1
        elif verdict == "catch_all":
            self.catch_all += 1
        else:
            self.inconclusive += 1

    def record_candidate(self, reason: str) -> None:
        self.total_candidates += 1
        self.candidates_by_reason[reason] = self.candidates_by_reason.get(reason, 0) + 1


@dataclass(slots=True)
class SMTPProbeResult:
    """Payload returned by :func:`run_smtp_probing_pass`."""

    candidates_selected: int
    probed: int
    stats: SMTPProbeStats
    report_path: Path | None


ProbeFn = Callable[..., SMTPResult]


# --------------------------------------------------------------------------- #
# Candidate selection                                                         #
# --------------------------------------------------------------------------- #


_QUALIFYING_REVIEW_SUBCLASSES: frozenset[str] = frozenset(
    {"review_catch_all", "review_timeout"}
)
_QUALIFYING_LABELS: frozenset[str] = frozenset(
    {"historically_unstable", "historically_risky"}
)


_CSV_NAMES_IN_PRIORITY: tuple[str, ...] = (
    "review_medium_confidence.csv",
    "removed_invalid.csv",
    "clean_high_confidence.csv",
)


def _truthy(value: str | None) -> bool:
    if not value:
        return False
    return str(value).strip().lower() in ("1", "true", "t", "yes", "y")


def _qualifies(
    row: dict[str, str], config: SMTPProbeConfig
) -> str | None:
    """Return the short reason code that qualified this row, or None."""
    # Hard excludes.
    if _truthy(row.get("hard_fail")):
        return None
    if (row.get("v2_final_bucket") or "") in ("hard_fail", "duplicate"):
        return None
    if not _truthy(row.get("has_mx_record")):
        return None

    if row.get("review_subclass") in _QUALIFYING_REVIEW_SUBCLASSES:
        return row["review_subclass"]

    try:
        adj = int(row.get("confidence_adjustment_applied") or 0)
    except ValueError:
        adj = 0
    if adj <= -config.negative_adjustment_trigger_threshold:
        return "negative_adjustment"

    if row.get("historical_label") in _QUALIFYING_LABELS:
        return str(row["historical_label"])

    return None


@dataclass(slots=True, frozen=True)
class _Candidate:
    email: str
    domain: str
    reason: str


def select_candidates(
    run_dir: Path,
    config: SMTPProbeConfig,
    stats: SMTPProbeStats | None = None,
) -> list[_Candidate]:
    """Scan all three CSVs, deduplicate by email, cap by domain and total."""
    run_dir = Path(run_dir)
    seen_emails: set[str] = set()
    raw: list[_Candidate] = []

    for csv_name in _CSV_NAMES_IN_PRIORITY:
        path = run_dir / csv_name
        if not path.is_file():
            continue
        with path.open(encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                reason = _qualifies(row, config)
                if reason is None:
                    continue
                email = (row.get("email") or "").strip()
                domain = (
                    (row.get("corrected_domain") or row.get("domain") or "")
                    .strip()
                    .lower()
                )
                if not email or not domain:
                    continue
                if email in seen_emails:
                    continue
                seen_emails.add(email)
                raw.append(_Candidate(email=email, domain=domain, reason=reason))

    # Per-domain cap.
    per_domain: dict[str, int] = {}
    capped: list[_Candidate] = []
    for cand in raw:
        if per_domain.get(cand.domain, 0) >= config.max_per_domain:
            continue
        per_domain[cand.domain] = per_domain.get(cand.domain, 0) + 1
        capped.append(cand)
        if stats is not None:
            stats.record_candidate(cand.reason)
        if len(capped) >= config.sample_size:
            break

    return capped


# --------------------------------------------------------------------------- #
# Rate-limited probing                                                        #
# --------------------------------------------------------------------------- #


def run_probing(
    candidates: list[_Candidate],
    config: SMTPProbeConfig,
    *,
    probe_fn: ProbeFn | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
    clock_fn: Callable[[], float] = time.perf_counter,
) -> dict[str, SMTPResult]:
    """Execute the probes with rate-limiting. Returns {email → result}."""
    if not candidates:
        return {}

    if probe_fn is None:
        probe_fn = probe_email_dry_run if config.dry_run else probe_email_smtplib

    min_interval = (
        1.0 / config.rate_limit_per_second
        if config.rate_limit_per_second > 0
        else 0.0
    )
    results: dict[str, SMTPResult] = {}
    last_t: float | None = None

    for cand in candidates:
        if last_t is not None and min_interval > 0:
            elapsed = clock_fn() - last_t
            if elapsed < min_interval:
                sleep_fn(min_interval - elapsed)

        try:
            result = probe_fn(
                cand.email,
                sender=config.sender_address,
                timeout=config.timeout_seconds,
            )
        except Exception as exc:  # pragma: no cover - defensive guard
            result = SMTPResult(False, None, f"probe exception: {exc}"[:200], False, True)

        results[cand.email] = result
        last_t = clock_fn()

    return results


# --------------------------------------------------------------------------- #
# CSV enrichment                                                              #
# --------------------------------------------------------------------------- #


def _verdict_to_confidence(result: SMTPResult) -> float:
    """Map a verdict to a confidence number for the CSV column."""
    verdict = result.verdict
    if verdict == "deliverable":
        return 0.9
    if verdict == "undeliverable":
        return 0.8
    if verdict == "catch_all":
        return 0.4
    return 0.1  # inconclusive / dry_run


def _probe_columns_for_row(
    result: SMTPResult | None,
    stats: SMTPProbeStats,
) -> dict[str, str]:
    if result is None:
        return {
            "smtp_tested": "False",
            "smtp_result": "not_tested",
            "smtp_confidence": "0.000",
            "smtp_response_code": "",
            "smtp_confirmed_valid": "False",
            "smtp_suspicious": "False",
        }
    verdict = result.verdict
    stats.record_verdict(verdict)
    confidence = _verdict_to_confidence(result)
    return {
        "smtp_tested": "True",
        "smtp_result": verdict,
        "smtp_confidence": f"{confidence:.3f}",
        "smtp_response_code": "" if result.response_code is None else str(result.response_code),
        "smtp_confirmed_valid": str(verdict == "deliverable"),
        "smtp_suspicious": str(verdict in ("catch_all", "undeliverable")),
    }


def _enrich_csv_with_smtp(
    csv_path: Path,
    probe_results: dict[str, SMTPResult],
    stats: SMTPProbeStats,
) -> bool:
    """Rewrite one CSV in-place with the six SMTP columns appended."""
    if not csv_path.is_file():
        return False

    fd, tmp_name = tempfile.mkstemp(
        prefix=csv_path.stem + ".smtp.",
        suffix=".tmp.csv",
        dir=str(csv_path.parent),
    )
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as fw, csv_path.open(
            encoding="utf-8", newline=""
        ) as fr:
            reader = csv.DictReader(fr)
            existing = list(reader.fieldnames or [])
            fieldnames = existing + [c for c in SMTP_COLUMNS if c not in existing]
            writer = csv.DictWriter(fw, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for row in reader:
                email = (row.get("email") or "").strip()
                result = probe_results.get(email)
                row.update(_probe_columns_for_row(result, stats))
                stats.total_rows_scanned += 1
                writer.writerow(row)
        os.replace(tmp_path, csv_path)
        return True
    except Exception:
        try:
            tmp_path.unlink()
        except OSError:
            pass
        raise


# --------------------------------------------------------------------------- #
# Summary report                                                              #
# --------------------------------------------------------------------------- #


def write_smtp_summary(run_dir: Path, stats: SMTPProbeStats) -> Path:
    run_dir = Path(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    path = run_dir / "smtp_probe_summary.csv"
    rows: list[tuple[str, object]] = [
        ("total_rows_scanned", stats.total_rows_scanned),
        ("total_candidates", stats.total_candidates),
        ("total_probed", stats.total_probed),
        ("deliverable", stats.deliverable),
        ("undeliverable", stats.undeliverable),
        ("catch_all", stats.catch_all),
        ("inconclusive", stats.inconclusive),
    ]
    for reason, count in sorted(stats.candidates_by_reason.items()):
        rows.append((f"candidates_by_reason:{reason}", count))
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(("metric", "value"))
        for k, v in rows:
            writer.writerow((k, v))
    return path


# --------------------------------------------------------------------------- #
# Top-level orchestrator                                                      #
# --------------------------------------------------------------------------- #


def run_smtp_probing_pass(
    run_dir: Path,
    config: SMTPProbeConfig,
    *,
    probe_fn: ProbeFn | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
    clock_fn: Callable[[], float] = time.perf_counter,
    logger: logging.Logger | None = None,
) -> SMTPProbeResult | None:
    """Selection + probing + CSV enrichment + summary. Returns None when disabled."""
    log = logger or _LOGGER
    if not config.enabled:
        return None

    run_dir = Path(run_dir)
    stats = SMTPProbeStats()
    candidates = select_candidates(run_dir, config, stats=stats)

    if not candidates:
        log.info("smtp_probe: no candidates after selection")
        report = write_smtp_summary(run_dir, stats)
        return SMTPProbeResult(0, 0, stats, report)

    log.info(
        "smtp_probe: probing %d candidates (dry_run=%s rate=%s/s)",
        len(candidates),
        config.dry_run,
        config.rate_limit_per_second,
    )
    probe_results = run_probing(
        candidates, config,
        probe_fn=probe_fn, sleep_fn=sleep_fn, clock_fn=clock_fn,
    )

    for csv_name in (
        "clean_high_confidence.csv",
        "review_medium_confidence.csv",
        "removed_invalid.csv",
    ):
        try:
            _enrich_csv_with_smtp(run_dir / csv_name, probe_results, stats)
        except Exception as exc:  # pragma: no cover - defensive guard
            log.warning("smtp_probe: failed to enrich %s (%s)", csv_name, exc)

    report = write_smtp_summary(run_dir, stats)
    return SMTPProbeResult(
        candidates_selected=len(candidates),
        probed=len(probe_results),
        stats=stats,
        report_path=report,
    )


__all__ = [
    "ProbeFn",
    "SMTPProbeConfig",
    "SMTPProbeResult",
    "SMTPProbeStats",
    "SMTP_COLUMNS",
    "run_smtp_probing_pass",
    "run_probing",
    "select_candidates",
    "write_smtp_summary",
]
