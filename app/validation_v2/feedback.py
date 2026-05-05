"""V2.7 — Feedback Loop / Bounce Outcome Ingestion.

Closes the learning loop: ingest real campaign outcomes (delivered,
hard bounce, soft bounce, blocked, deferred, complaint, unsubscribed,
unknown), normalize the vocabulary, aggregate per-domain counts, and
persist to a dedicated SQLite store so V2.6 domain intelligence and
future subphases can read aggregate reputation.

Why a dedicated store
---------------------

The existing :class:`app.validation_v2.history.DomainHistoryStore`
tracks a richer (TTL, provider key, reputation tiers) schema bound to
the legacy V2 history pipeline. Coupling V2.7 ingestion to that
schema would entangle the feedback contract with unrelated concerns.
The V2.7 store is intentionally narrow: domain → running counts →
risk level. Future subphases can bridge the two stores or fold them
together; V2.7 deliberately keeps them separate so an outage in one
cannot affect the other.

Public API
----------

  * :data:`OUTCOMES` — canonical outcome vocabulary frozenset.
  * :func:`normalize_outcome` — raw string → canonical token.
  * :func:`normalize_outcome_with_type` — raw outcome + bounce_type.
  * :func:`normalize_email` / :func:`extract_domain` — input cleaners.
  * :class:`DomainBounceAggregate` — running per-domain counts.
  * :class:`BounceOutcomeStore` — SQLite-backed aggregate store.
  * :class:`ReputationThresholds` / :func:`compute_risk_level` /
    :func:`compute_reputation_score` — pure-function reputation.
  * :class:`IngestionSummary` — return value of :func:`ingest_bounce_outcomes`.
  * :func:`ingest_bounce_outcomes` — main ingestion entry point.
  * :func:`bounce_aggregate_to_domain_intel` — bridge from a
    :class:`DomainBounceAggregate` to a V2.6
    :class:`DomainIntelligenceClassification` (read-only consumer).
"""

from __future__ import annotations

import csv
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator


# =========================================================================== #
# Canonical outcome vocabulary                                                #
# =========================================================================== #


OUTCOME_DELIVERED = "delivered"
OUTCOME_HARD_BOUNCE = "hard_bounce"
OUTCOME_SOFT_BOUNCE = "soft_bounce"
OUTCOME_BLOCKED = "blocked"
OUTCOME_DEFERRED = "deferred"
OUTCOME_COMPLAINT = "complaint"
OUTCOME_UNSUBSCRIBED = "unsubscribed"
OUTCOME_UNKNOWN = "unknown"


OUTCOMES: frozenset[str] = frozenset({
    OUTCOME_DELIVERED,
    OUTCOME_HARD_BOUNCE,
    OUTCOME_SOFT_BOUNCE,
    OUTCOME_BLOCKED,
    OUTCOME_DEFERRED,
    OUTCOME_COMPLAINT,
    OUTCOME_UNSUBSCRIBED,
    OUTCOME_UNKNOWN,
})


POSITIVE_OUTCOMES: frozenset[str] = frozenset({OUTCOME_DELIVERED})
NEGATIVE_OUTCOMES: frozenset[str] = frozenset({
    OUTCOME_HARD_BOUNCE,
    OUTCOME_BLOCKED,
    OUTCOME_COMPLAINT,
})
TEMPORARY_OUTCOMES: frozenset[str] = frozenset({
    OUTCOME_SOFT_BOUNCE,
    OUTCOME_DEFERRED,
})
SUPPRESSION_OUTCOMES: frozenset[str] = frozenset({OUTCOME_UNSUBSCRIBED})


# Risk-level vocabulary mirrors V2.6
# (``app.engine.stages.domain_intelligence.RISK_LEVEL_*``).
RISK_LEVEL_LOW = "low"
RISK_LEVEL_MEDIUM = "medium"
RISK_LEVEL_HIGH = "high"
RISK_LEVEL_UNKNOWN = "unknown"


# =========================================================================== #
# Normalization                                                               #
# =========================================================================== #


# Direct token mapping. Anything not in this map AND not handled by
# ``normalize_outcome_with_type`` falls through to ``unknown``.
_DIRECT_OUTCOME_MAP: dict[str, str] = {
    "delivered": OUTCOME_DELIVERED,
    "deliver": OUTCOME_DELIVERED,
    "sent": OUTCOME_DELIVERED,
    "ok": OUTCOME_DELIVERED,
    "success": OUTCOME_DELIVERED,
    "hard_bounce": OUTCOME_HARD_BOUNCE,
    "hard-bounce": OUTCOME_HARD_BOUNCE,
    "hardbounce": OUTCOME_HARD_BOUNCE,
    "permanent_failure": OUTCOME_HARD_BOUNCE,
    "permanent-failure": OUTCOME_HARD_BOUNCE,
    "soft_bounce": OUTCOME_SOFT_BOUNCE,
    "soft-bounce": OUTCOME_SOFT_BOUNCE,
    "softbounce": OUTCOME_SOFT_BOUNCE,
    "transient_failure": OUTCOME_SOFT_BOUNCE,
    "blocked": OUTCOME_BLOCKED,
    "block": OUTCOME_BLOCKED,
    "rejected": OUTCOME_BLOCKED,
    "deferred": OUTCOME_DEFERRED,
    "defer": OUTCOME_DEFERRED,
    "delayed": OUTCOME_DEFERRED,
    "complaint": OUTCOME_COMPLAINT,
    "spam": OUTCOME_COMPLAINT,
    "complained": OUTCOME_COMPLAINT,
    "abuse": OUTCOME_COMPLAINT,
    "unsubscribed": OUTCOME_UNSUBSCRIBED,
    "unsubscribe": OUTCOME_UNSUBSCRIBED,
    "opt_out": OUTCOME_UNSUBSCRIBED,
    "optout": OUTCOME_UNSUBSCRIBED,
    "unknown": OUTCOME_UNKNOWN,
}


# Compound outcomes — when ``outcome`` says "bounce[d]" without a type,
# we don't yet know hard vs soft; require a ``bounce_type`` field.
_AMBIGUOUS_BOUNCE: frozenset[str] = frozenset({"bounce", "bounced"})


_BOUNCE_TYPE_MAP: dict[str, str] = {
    "hard": OUTCOME_HARD_BOUNCE,
    "hard_bounce": OUTCOME_HARD_BOUNCE,
    "permanent": OUTCOME_HARD_BOUNCE,
    "soft": OUTCOME_SOFT_BOUNCE,
    "soft_bounce": OUTCOME_SOFT_BOUNCE,
    "transient": OUTCOME_SOFT_BOUNCE,
    "blocked": OUTCOME_BLOCKED,
    "block": OUTCOME_BLOCKED,
    "deferred": OUTCOME_DEFERRED,
    "defer": OUTCOME_DEFERRED,
}


# Conservative email regex — matches ``local@domain.tld``. Mirrors the
# stricter checks in ``app.email_rules`` but is intentionally narrower
# here: ingestion should reject anything that doesn't look like an
# email so a corrupt CSV column doesn't pollute domain reputation.
_EMAIL_RE = re.compile(
    r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$"
)


def normalize_outcome(raw: str | None) -> str:
    """Map raw outcome strings to canonical token.

    ``None`` / empty / unrecognized → ``unknown``. The bare
    ``bounce`` / ``bounced`` token without a ``bounce_type`` falls
    through to ``unknown`` here — callers should prefer
    :func:`normalize_outcome_with_type` so a separate type column can
    refine the result.
    """
    if raw is None:
        return OUTCOME_UNKNOWN
    s = str(raw).strip().lower()
    if not s:
        return OUTCOME_UNKNOWN
    if s in _DIRECT_OUTCOME_MAP:
        return _DIRECT_OUTCOME_MAP[s]
    if s in _AMBIGUOUS_BOUNCE:
        # Conservative: ambiguous bounce without a type → unknown
        # rather than guessing hard. ``normalize_outcome_with_type``
        # is the path that resolves these.
        return OUTCOME_UNKNOWN
    return OUTCOME_UNKNOWN


def normalize_outcome_with_type(
    outcome_raw: str | None,
    bounce_type_raw: str | None,
) -> str:
    """Combine ``outcome`` + ``bounce_type`` into one canonical token.

    Resolution rules:

      1. If ``outcome_raw`` directly maps to a non-bounce canonical
         token (delivered, blocked, complaint, etc.), use it. The
         ``bounce_type`` is informational only.
      2. If ``outcome_raw`` is ambiguous (``bounce`` / ``bounced``),
         use ``bounce_type_raw`` to refine (``hard`` → hard_bounce,
         ``soft`` → soft_bounce, ``deferred`` → deferred, etc.).
      3. If both are missing or unrecognized, fall through to
         ``unknown``.
    """
    if outcome_raw is None and bounce_type_raw is None:
        return OUTCOME_UNKNOWN

    raw = str(outcome_raw or "").strip().lower()
    bt_raw = str(bounce_type_raw or "").strip().lower()

    # 1. Direct, unambiguous outcome wins.
    if raw in _DIRECT_OUTCOME_MAP:
        # Hard-bounce direct match — accept refinement to soft/blocked
        # if bounce_type explicitly disagrees. (Some senders emit
        # ``hard_bounce`` as a default and override via bounce_type.)
        canonical = _DIRECT_OUTCOME_MAP[raw]
        if (
            canonical == OUTCOME_HARD_BOUNCE
            and bt_raw
            and bt_raw in _BOUNCE_TYPE_MAP
            and _BOUNCE_TYPE_MAP[bt_raw] != OUTCOME_HARD_BOUNCE
        ):
            return _BOUNCE_TYPE_MAP[bt_raw]
        return canonical

    # 2. Ambiguous "bounce[d]" + type → resolve via type.
    if raw in _AMBIGUOUS_BOUNCE:
        if bt_raw in _BOUNCE_TYPE_MAP:
            return _BOUNCE_TYPE_MAP[bt_raw]
        # ``bounce`` with no type is intentionally treated as unknown
        # (we'd rather skip than mis-aggregate).
        return OUTCOME_UNKNOWN

    # 3. No outcome at all — try to derive solely from bounce_type.
    if not raw and bt_raw in _BOUNCE_TYPE_MAP:
        return _BOUNCE_TYPE_MAP[bt_raw]

    return OUTCOME_UNKNOWN


def normalize_email(raw: str | None) -> str | None:
    """Lowercase + strip + RFC-adjacent shape check. Returns ``None``
    when the input cannot be considered an email."""
    if raw is None:
        return None
    s = str(raw).strip().lower()
    if not s or not _EMAIL_RE.match(s):
        return None
    return s


def extract_domain(email: str | None) -> str | None:
    """Extract the lowercased domain from an email, or ``None``."""
    if not email or "@" not in email:
        return None
    local, _, domain = email.rpartition("@")
    if not local or not domain:
        return None
    return domain.strip().lower() or None


def is_positive(outcome: str) -> bool:
    return outcome in POSITIVE_OUTCOMES


def is_negative(outcome: str) -> bool:
    return outcome in NEGATIVE_OUTCOMES


def is_temporary(outcome: str) -> bool:
    return outcome in TEMPORARY_OUTCOMES


def is_suppression(outcome: str) -> bool:
    return outcome in SUPPRESSION_OUTCOMES


# =========================================================================== #
# Aggregate model                                                             #
# =========================================================================== #


@dataclass
class DomainBounceAggregate:
    """Running per-domain counts produced by V2.7 ingestion.

    Mutated in place by :meth:`record`. Persisted by
    :class:`BounceOutcomeStore`. Read back as a value object — never
    expose the same instance to multiple writers from different
    ingestions concurrently.
    """

    domain: str
    total_observations: int = 0
    delivered_count: int = 0
    hard_bounce_count: int = 0
    soft_bounce_count: int = 0
    blocked_count: int = 0
    deferred_count: int = 0
    complaint_count: int = 0
    unsubscribed_count: int = 0
    unknown_count: int = 0
    last_seen_at: str | None = None

    def record(self, outcome: str, timestamp: str | None = None) -> None:
        """Increment the counter for ``outcome`` and update last_seen.

        Out-of-vocabulary outcomes increment ``unknown_count`` rather
        than raising — robustness over strictness for ingestion.
        """
        self.total_observations += 1
        if outcome == OUTCOME_DELIVERED:
            self.delivered_count += 1
        elif outcome == OUTCOME_HARD_BOUNCE:
            self.hard_bounce_count += 1
        elif outcome == OUTCOME_SOFT_BOUNCE:
            self.soft_bounce_count += 1
        elif outcome == OUTCOME_BLOCKED:
            self.blocked_count += 1
        elif outcome == OUTCOME_DEFERRED:
            self.deferred_count += 1
        elif outcome == OUTCOME_COMPLAINT:
            self.complaint_count += 1
        elif outcome == OUTCOME_UNSUBSCRIBED:
            self.unsubscribed_count += 1
        else:
            self.unknown_count += 1
        if timestamp:
            self.last_seen_at = timestamp


# =========================================================================== #
# Reputation                                                                  #
# =========================================================================== #


@dataclass(frozen=True)
class ReputationThresholds:
    """Tunables for :func:`compute_risk_level`.

    Defaults match the V2.7 prompt. ``min_observations`` is the floor
    at which a domain stops being treated as cold-start. The two
    bounce-rate thresholds split medium from high; the blocked-rate
    and complaint flags only push toward high.
    """

    min_observations: int = 5
    medium_hard_bounce_rate: float = 0.08
    high_hard_bounce_rate: float = 0.20
    high_blocked_rate: float = 0.10
    complaint_is_high_risk: bool = True


DEFAULT_REPUTATION_THRESHOLDS: ReputationThresholds = ReputationThresholds()


def compute_risk_level(
    aggregate: DomainBounceAggregate,
    thresholds: ReputationThresholds = DEFAULT_REPUTATION_THRESHOLDS,
) -> str:
    """Map an aggregate to ``low | medium | high | unknown``.

    Rules:

      * ``total_observations < min_observations`` → ``unknown``
        (cold-start; no statistical evidence).
      * Any of ``hard_bounce_rate >= high_hard_bounce_rate`` /
        ``blocked_rate >= high_blocked_rate`` /
        ``complaints present`` (when configured) → ``high``.
      * ``hard_bounce_rate >= medium_hard_bounce_rate`` → ``medium``.
      * Otherwise → ``low``.
    """
    if aggregate.total_observations < thresholds.min_observations:
        return RISK_LEVEL_UNKNOWN

    total = aggregate.total_observations
    hard_bounce_rate = aggregate.hard_bounce_count / total
    blocked_rate = aggregate.blocked_count / total
    has_complaints = aggregate.complaint_count > 0

    if (
        hard_bounce_rate >= thresholds.high_hard_bounce_rate
        or blocked_rate >= thresholds.high_blocked_rate
        or (thresholds.complaint_is_high_risk and has_complaints)
    ):
        return RISK_LEVEL_HIGH
    if hard_bounce_rate >= thresholds.medium_hard_bounce_rate:
        return RISK_LEVEL_MEDIUM
    return RISK_LEVEL_LOW


def compute_reputation_score(aggregate: DomainBounceAggregate) -> float:
    """Map an aggregate to a reputation score in ``[0, 1]``.

    Defined as the delivered rate. ``0`` observations → 0.5 (neutral).
    Hard bounces and blocks naturally lower the score because they
    don't increment the delivered count.
    """
    if aggregate.total_observations == 0:
        return 0.5
    return max(
        0.0,
        min(1.0, aggregate.delivered_count / aggregate.total_observations),
    )


# =========================================================================== #
# SQLite-backed store                                                         #
# =========================================================================== #


_SCHEMA = """
CREATE TABLE IF NOT EXISTS bounce_outcomes_aggregate (
    domain                TEXT PRIMARY KEY,
    total_observations    INTEGER NOT NULL DEFAULT 0,
    delivered_count       INTEGER NOT NULL DEFAULT 0,
    hard_bounce_count     INTEGER NOT NULL DEFAULT 0,
    soft_bounce_count     INTEGER NOT NULL DEFAULT 0,
    blocked_count         INTEGER NOT NULL DEFAULT 0,
    deferred_count        INTEGER NOT NULL DEFAULT 0,
    complaint_count       INTEGER NOT NULL DEFAULT 0,
    unsubscribed_count    INTEGER NOT NULL DEFAULT 0,
    unknown_count         INTEGER NOT NULL DEFAULT 0,
    last_seen_at          TEXT
)
"""


_UPSERT_SQL = """
INSERT INTO bounce_outcomes_aggregate (
    domain,
    total_observations,
    delivered_count,
    hard_bounce_count,
    soft_bounce_count,
    blocked_count,
    deferred_count,
    complaint_count,
    unsubscribed_count,
    unknown_count,
    last_seen_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(domain) DO UPDATE SET
    total_observations = excluded.total_observations,
    delivered_count    = excluded.delivered_count,
    hard_bounce_count  = excluded.hard_bounce_count,
    soft_bounce_count  = excluded.soft_bounce_count,
    blocked_count      = excluded.blocked_count,
    deferred_count     = excluded.deferred_count,
    complaint_count    = excluded.complaint_count,
    unsubscribed_count = excluded.unsubscribed_count,
    unknown_count      = excluded.unknown_count,
    last_seen_at       = excluded.last_seen_at
"""


class BounceOutcomeStore:
    """SQLite-backed aggregate store for V2.7 bounce outcomes.

    Schema is one row per domain with running counters. Re-ingesting
    the same domain merges into the existing row (callers compose
    the new aggregate by reading the current row, applying
    :meth:`DomainBounceAggregate.record` for each new event, and
    upserting the result — see :func:`ingest_bounce_outcomes`).

    The store lives in its own SQLite file (default
    ``runtime/feedback/bounce_outcomes.sqlite``) so an outage in the
    legacy ``DomainHistoryStore`` cannot affect feedback ingestion
    and vice-versa. A future subphase can bridge the two stores
    via :func:`bounce_aggregate_to_domain_intel`.
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        # V2.9.9 — open into a local first so we can close the connection
        # before re-raising if PRAGMA / schema init fails (e.g. the file
        # exists but is not a valid SQLite database). Without this, a
        # failed init leaks the OS file handle until GC, which on Windows
        # blocks the corrupt file from being deleted/rewritten.
        conn = sqlite3.connect(str(self._path))
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(_SCHEMA)
            conn.commit()
        except Exception:
            try:
                conn.close()
            except sqlite3.Error:  # pragma: no cover - defensive
                pass
            raise
        self._conn = conn

    # -- read ----------------------------------------------------------- #

    def get(self, domain: str) -> DomainBounceAggregate | None:
        normalized = (domain or "").strip().lower()
        if not normalized:
            return None
        row = self._conn.execute(
            "SELECT domain, total_observations, delivered_count, "
            "hard_bounce_count, soft_bounce_count, blocked_count, "
            "deferred_count, complaint_count, unsubscribed_count, "
            "unknown_count, last_seen_at "
            "FROM bounce_outcomes_aggregate WHERE domain = ?",
            (normalized,),
        ).fetchone()
        if row is None:
            return None
        return self._row_to_aggregate(row)

    def list_all(self) -> list[DomainBounceAggregate]:
        rows = self._conn.execute(
            "SELECT domain, total_observations, delivered_count, "
            "hard_bounce_count, soft_bounce_count, blocked_count, "
            "deferred_count, complaint_count, unsubscribed_count, "
            "unknown_count, last_seen_at "
            "FROM bounce_outcomes_aggregate"
        ).fetchall()
        return [self._row_to_aggregate(r) for r in rows]

    # -- write ---------------------------------------------------------- #

    def upsert_aggregate(self, aggregate: DomainBounceAggregate) -> None:
        """Replace the row for ``aggregate.domain`` with the new totals.

        Callers must have already merged any prior aggregate into
        ``aggregate``; this method does NOT add to existing counts.
        """
        self._conn.execute(
            _UPSERT_SQL,
            (
                aggregate.domain,
                aggregate.total_observations,
                aggregate.delivered_count,
                aggregate.hard_bounce_count,
                aggregate.soft_bounce_count,
                aggregate.blocked_count,
                aggregate.deferred_count,
                aggregate.complaint_count,
                aggregate.unsubscribed_count,
                aggregate.unknown_count,
                aggregate.last_seen_at,
            ),
        )
        self._conn.commit()

    # -- lifecycle ------------------------------------------------------ #

    def close(self) -> None:
        try:
            self._conn.close()
        except sqlite3.Error:  # pragma: no cover - defensive
            pass

    @property
    def path(self) -> Path:
        return self._path

    # -- helpers -------------------------------------------------------- #

    @staticmethod
    def _row_to_aggregate(row: tuple) -> DomainBounceAggregate:
        return DomainBounceAggregate(
            domain=row[0],
            total_observations=int(row[1] or 0),
            delivered_count=int(row[2] or 0),
            hard_bounce_count=int(row[3] or 0),
            soft_bounce_count=int(row[4] or 0),
            blocked_count=int(row[5] or 0),
            deferred_count=int(row[6] or 0),
            complaint_count=int(row[7] or 0),
            unsubscribed_count=int(row[8] or 0),
            unknown_count=int(row[9] or 0),
            last_seen_at=row[10],
        )


# =========================================================================== #
# Ingestion                                                                   #
# =========================================================================== #


@dataclass
class IngestionSummary:
    """Structured return value of :func:`ingest_bounce_outcomes`.

    ``error`` is ``None`` on success and a short token + message
    when ingestion failed early (file not found, sqlite open error,
    etc.). Per-row failures (invalid email, unknown outcome) are
    counted, not raised.
    """

    total_rows: int = 0
    accepted_rows: int = 0
    skipped_rows: int = 0
    invalid_email_rows: int = 0
    unknown_outcome_rows: int = 0
    domains_updated: int = 0
    delivered_count: int = 0
    hard_bounce_count: int = 0
    soft_bounce_count: int = 0
    blocked_count: int = 0
    deferred_count: int = 0
    complaint_count: int = 0
    unsubscribed_count: int = 0
    unknown_count: int = 0
    error: str | None = None

    def _record(self, outcome: str) -> None:
        if outcome == OUTCOME_DELIVERED:
            self.delivered_count += 1
        elif outcome == OUTCOME_HARD_BOUNCE:
            self.hard_bounce_count += 1
        elif outcome == OUTCOME_SOFT_BOUNCE:
            self.soft_bounce_count += 1
        elif outcome == OUTCOME_BLOCKED:
            self.blocked_count += 1
        elif outcome == OUTCOME_DEFERRED:
            self.deferred_count += 1
        elif outcome == OUTCOME_COMPLAINT:
            self.complaint_count += 1
        elif outcome == OUTCOME_UNSUBSCRIBED:
            self.unsubscribed_count += 1
        else:
            self.unknown_count += 1


def _read_field(row: dict[str, Any], *names: str) -> str:
    """Tolerant case-insensitive field reader for CSV dicts."""
    if not row:
        return ""
    # Direct match first (fast path).
    for name in names:
        if name in row and row[name] is not None:
            return str(row[name])
    # Case-insensitive fallback.
    lowered = {k.lower(): v for k, v in row.items()}
    for name in names:
        v = lowered.get(name.lower())
        if v is not None:
            return str(v)
    return ""


def ingest_bounce_outcomes(
    input_path: str | Path,
    *,
    history_store: BounceOutcomeStore,
    config: Any = None,
) -> IngestionSummary:
    """Ingest a bounce-outcome CSV into the aggregate store.

    Parameters
    ----------
    input_path:
        Path to the CSV file. Required columns: ``email`` (or
        ``Email``), ``outcome`` (or ``status``). Optional columns:
        ``bounce_type``, ``smtp_code``, ``reason``, ``campaign_id``,
        ``timestamp``, ``provider``.
    history_store:
        Open :class:`BounceOutcomeStore`. The function reads any
        existing aggregate for each domain before writing back the
        merged result, so re-ingesting an updated CSV preserves
        previous counts.
    config:
        Optional config object — currently unused by ingestion (the
        thresholds it carries are consumed by :func:`compute_risk_level`,
        not by the writer). Reserved for future tuning.

    Returns
    -------
    :class:`IngestionSummary`
        Always returned; failures populate ``error`` rather than
        raising. Per-row failures are counted in ``skipped_rows`` /
        ``invalid_email_rows`` / ``unknown_outcome_rows``.
    """
    summary = IngestionSummary()
    path = Path(input_path)

    if not path.is_file():
        summary.error = f"file_not_found:{path}"
        return summary

    if history_store is None:
        summary.error = "history_store_missing"
        return summary

    # Per-run domain → aggregate map. We pre-load the existing
    # aggregate for any domain we encounter so re-ingestion adds to
    # prior counts rather than replacing them.
    in_memory: dict[str, DomainBounceAggregate] = {}

    try:
        with path.open(encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for raw_row in reader:
                summary.total_rows += 1

                email_raw = _read_field(raw_row, "email", "Email")
                email = normalize_email(email_raw)
                if email is None:
                    summary.skipped_rows += 1
                    summary.invalid_email_rows += 1
                    continue

                domain = extract_domain(email)
                if domain is None:
                    summary.skipped_rows += 1
                    summary.invalid_email_rows += 1
                    continue

                outcome_raw = _read_field(raw_row, "outcome", "status")
                bounce_type_raw = _read_field(raw_row, "bounce_type", "type")
                outcome = normalize_outcome_with_type(outcome_raw, bounce_type_raw)

                # Distinguish "row had no outcome at all" from
                # "row had an unknown outcome string". The first is
                # ingestion noise; the second is recorded so the
                # aggregate counters reflect that the domain was
                # seen even if the outcome was illegible.
                if not outcome_raw and not bounce_type_raw:
                    summary.skipped_rows += 1
                    summary.unknown_outcome_rows += 1
                    continue

                timestamp = _read_field(raw_row, "timestamp", "Timestamp") or None

                if domain not in in_memory:
                    existing = history_store.get(domain)
                    in_memory[domain] = existing or DomainBounceAggregate(domain=domain)
                in_memory[domain].record(outcome, timestamp)

                summary.accepted_rows += 1
                summary._record(outcome)

        # Persist all merged aggregates.
        for agg in in_memory.values():
            history_store.upsert_aggregate(agg)

        summary.domains_updated = len(in_memory)

    except sqlite3.Error as exc:
        summary.error = f"sqlite_error:{exc}"
    except OSError as exc:
        summary.error = f"io_error:{exc}"
    except csv.Error as exc:
        summary.error = f"csv_error:{exc}"
    except Exception as exc:  # pragma: no cover - defensive
        summary.error = f"unexpected:{type(exc).__name__}:{exc}"

    return summary


# =========================================================================== #
# Bridge to V2.6 domain intelligence                                          #
# =========================================================================== #


def bounce_aggregate_to_domain_intel(
    aggregate: DomainBounceAggregate,
    thresholds: ReputationThresholds = DEFAULT_REPUTATION_THRESHOLDS,
) -> dict[str, Any]:
    """Translate a :class:`DomainBounceAggregate` to V2.6-shaped fields.

    Returns a dict with the same keys :class:`DomainIntelligenceClassification`
    accepts (status, reputation_score, risk_level, behavior_class,
    observation_count, cold_start, reason). A future subphase can
    pre-populate ``context.extras["domain_intel_cache"]`` with these
    classifications and the chunk-time
    :class:`app.engine.stages.domain_intelligence.DomainIntelligenceStage`
    will prefer them over the heuristic.
    """
    risk = compute_risk_level(aggregate, thresholds)
    score = compute_reputation_score(aggregate)
    cold_start = aggregate.total_observations < thresholds.min_observations

    if cold_start:
        behavior = "cold_start"
    elif risk == RISK_LEVEL_HIGH:
        behavior = "known_risky"
    elif risk == RISK_LEVEL_LOW:
        behavior = "known_good"
    else:
        behavior = "unknown"

    return {
        "status": "available",
        "reputation_score": score,
        "risk_level": risk,
        "behavior_class": behavior,
        "observation_count": aggregate.total_observations,
        "cold_start": cold_start,
        "reason": (
            "feedback_history"
            if not cold_start
            else f"insufficient_feedback_history:{aggregate.total_observations}"
        ),
    }


# =========================================================================== #
# Public API surface                                                          #
# =========================================================================== #


__all__ = [
    # Outcome vocabulary
    "OUTCOMES",
    "OUTCOME_BLOCKED",
    "OUTCOME_COMPLAINT",
    "OUTCOME_DEFERRED",
    "OUTCOME_DELIVERED",
    "OUTCOME_HARD_BOUNCE",
    "OUTCOME_SOFT_BOUNCE",
    "OUTCOME_UNKNOWN",
    "OUTCOME_UNSUBSCRIBED",
    "POSITIVE_OUTCOMES",
    "NEGATIVE_OUTCOMES",
    "TEMPORARY_OUTCOMES",
    "SUPPRESSION_OUTCOMES",
    # Risk vocabulary
    "RISK_LEVEL_HIGH",
    "RISK_LEVEL_LOW",
    "RISK_LEVEL_MEDIUM",
    "RISK_LEVEL_UNKNOWN",
    # Normalization
    "extract_domain",
    "is_negative",
    "is_positive",
    "is_suppression",
    "is_temporary",
    "normalize_email",
    "normalize_outcome",
    "normalize_outcome_with_type",
    # Aggregate + reputation
    "DEFAULT_REPUTATION_THRESHOLDS",
    "DomainBounceAggregate",
    "ReputationThresholds",
    "compute_reputation_score",
    "compute_risk_level",
    # Store
    "BounceOutcomeStore",
    # Ingestion
    "IngestionSummary",
    "ingest_bounce_outcomes",
    # Bridge to V2.6
    "bounce_aggregate_to_domain_intel",
]
