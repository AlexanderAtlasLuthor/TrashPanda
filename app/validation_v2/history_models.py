"""Data models for the V2 Domain Historical Memory layer.

Design notes
------------
* :class:`DomainObservation` is a single, stateless fact about a domain
  derived from one processed row — produced by the pipeline, consumed by
  the store.
* :class:`DomainHistoryRecord` is the aggregate — what the store persists
  across runs. Counts only grow (monotonic) except when the caller
  explicitly resets them.
* :class:`HistoricalLabel` is a compact, enum-like string category used
  by downstream reports and explanations. Kept as a plain string to
  round-trip cleanly through SQLite/JSON.

No heuristics or pipeline integration live here — this module is pure
data modelling.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Final


# --------------------------------------------------------------------------- #
# Final-decision vocabulary                                                   #
# --------------------------------------------------------------------------- #


class FinalDecision:
    """Canonical final-decision string values used in observations.

    These mirror the pipeline's terminal classification for a row:
      * ``READY``      -> kept_high_confidence (Ready to send)
      * ``REVIEW``     -> kept_review          (Needs attention)
      * ``INVALID``    -> removed_low_score / removed_duplicate (Do not use)
      * ``HARD_FAIL``  -> removed_hard_fail    (Do not use, syntax/disposable)
      * ``UNKNOWN``    -> safe default; treated as neutral for counting.
    """

    READY: Final[str] = "ready"
    REVIEW: Final[str] = "review"
    INVALID: Final[str] = "invalid"
    HARD_FAIL: Final[str] = "hard_fail"
    UNKNOWN: Final[str] = "unknown"

    ALL: Final[frozenset[str]] = frozenset({READY, REVIEW, INVALID, HARD_FAIL, UNKNOWN})


# --------------------------------------------------------------------------- #
# Historical labels (categorical summaries over a record)                     #
# --------------------------------------------------------------------------- #


class HistoricalLabel:
    """Short deterministic classifier output over a history record.

    The exact rules that produce each label live in
    :mod:`app.validation_v2.domain_memory`. This module only defines the
    string constants so tests and consumers can import them.
    """

    RELIABLE: Final[str] = "historically_reliable"
    UNSTABLE: Final[str] = "historically_unstable"
    RISKY: Final[str] = "historically_risky"
    NEUTRAL: Final[str] = "neutral"
    INSUFFICIENT_DATA: Final[str] = "insufficient_data"

    ALL: Final[frozenset[str]] = frozenset(
        {RELIABLE, UNSTABLE, RISKY, NEUTRAL, INSUFFICIENT_DATA}
    )


# --------------------------------------------------------------------------- #
# Observations                                                                #
# --------------------------------------------------------------------------- #


@dataclass(slots=True)
class DomainObservation:
    """One observation of a domain's behaviour in a single run.

    Produced from a materialised output row (or an aggregate of rows
    sharing the same domain). Consumed by the store, which translates
    observations into count increments on the persisted record.
    """

    domain: str
    had_mx: bool = False
    had_a_fallback: bool = False
    had_dns_failure: bool = False
    had_timeout: bool = False
    was_typo_corrected: bool = False
    had_hard_fail: bool = False
    final_decision: str = FinalDecision.UNKNOWN

    def __post_init__(self) -> None:
        self.domain = (self.domain or "").strip().lower()
        if self.final_decision not in FinalDecision.ALL:
            self.final_decision = FinalDecision.UNKNOWN


# --------------------------------------------------------------------------- #
# Persisted history record                                                    #
# --------------------------------------------------------------------------- #


def _safe_rate(numerator: int, denominator: int) -> float:
    """Return ``numerator / denominator`` or 0.0 if denominator is zero.

    Kept as a module-level helper so tests can import and exercise it.
    """
    if denominator <= 0:
        return 0.0
    return float(numerator) / float(denominator)


@dataclass(slots=True)
class DomainHistoryRecord:
    """Aggregate counts for a domain across every TrashPanda run.

    All counts are monotonic integers; the store increments them in
    transactions. Derived rates are expressed as read-only properties so
    callers never store stale values.
    """

    domain: str
    first_seen_at: datetime
    last_seen_at: datetime
    total_seen_count: int = 0
    mx_present_count: int = 0
    a_fallback_count: int = 0
    dns_failure_count: int = 0
    timeout_count: int = 0
    typo_corrected_count: int = 0
    review_count: int = 0
    invalid_count: int = 0
    ready_count: int = 0
    hard_fail_count: int = 0

    def __post_init__(self) -> None:
        self.domain = (self.domain or "").strip().lower()

    # ── Derived rates ───────────────────────────────────────────────── #

    @property
    def mx_rate(self) -> float:
        return _safe_rate(self.mx_present_count, self.total_seen_count)

    @property
    def a_fallback_rate(self) -> float:
        return _safe_rate(self.a_fallback_count, self.total_seen_count)

    @property
    def dns_failure_rate(self) -> float:
        return _safe_rate(self.dns_failure_count, self.total_seen_count)

    @property
    def timeout_rate(self) -> float:
        return _safe_rate(self.timeout_count, self.total_seen_count)

    @property
    def typo_corrected_rate(self) -> float:
        return _safe_rate(self.typo_corrected_count, self.total_seen_count)

    @property
    def ready_rate(self) -> float:
        return _safe_rate(self.ready_count, self.total_seen_count)

    @property
    def review_rate(self) -> float:
        return _safe_rate(self.review_count, self.total_seen_count)

    @property
    def invalid_rate(self) -> float:
        return _safe_rate(self.invalid_count, self.total_seen_count)

    @property
    def hard_fail_rate(self) -> float:
        return _safe_rate(self.hard_fail_count, self.total_seen_count)

    # ── Mutators (used by store.update_from_observation) ────────────── #

    def apply_observation(
        self,
        observation: DomainObservation,
        now: datetime | None = None,
    ) -> None:
        """In-place increment based on a single observation.

        Intended for use inside :class:`DomainHistoryStore` transactions;
        it does not touch persistence itself.
        """
        when = now or datetime.now()
        self.total_seen_count += 1
        if observation.had_mx:
            self.mx_present_count += 1
        if observation.had_a_fallback:
            self.a_fallback_count += 1
        if observation.had_dns_failure:
            self.dns_failure_count += 1
        if observation.had_timeout:
            self.timeout_count += 1
        if observation.was_typo_corrected:
            self.typo_corrected_count += 1
        if observation.had_hard_fail:
            self.hard_fail_count += 1

        decision = observation.final_decision
        if decision == FinalDecision.READY:
            self.ready_count += 1
        elif decision == FinalDecision.REVIEW:
            self.review_count += 1
        elif decision == FinalDecision.INVALID:
            self.invalid_count += 1
        elif decision == FinalDecision.HARD_FAIL:
            # Hard fails already bump hard_fail_count above when the flag
            # is set. Guard against callers that only set the decision.
            if not observation.had_hard_fail:
                self.hard_fail_count += 1

        if when > self.last_seen_at:
            self.last_seen_at = when


__all__ = [
    "DomainHistoryRecord",
    "DomainObservation",
    "FinalDecision",
    "HistoricalLabel",
    "_safe_rate",
]
