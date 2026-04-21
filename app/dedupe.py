"""Deduplication engine for Subphase 7: global email deduplication,
canonical row selection, and duplicate flagging.

Deduplication key: email_normalized (lowercased, stripped email).
Winner selection follows a strict deterministic 4-rule hierarchy:
  1. hard_fail=False beats hard_fail=True
  2. higher score wins
  3. higher completeness_score wins
  4. lower global_ordinal wins (earlier occurrence — stable tiebreak)

No export. No materialization. No final decisions.
is_canonical is provisional: if a canonical row is later replaced by a
better-ranked row in a subsequent chunk, its is_canonical=True flag in its
original chunk is stale. Subphase 8 (materialization) resolves this by
applying the final DedupeIndex state before writing output files.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


# ---------------------------------------------------------------------------
# Business columns used for completeness scoring
# ---------------------------------------------------------------------------

BUSINESS_COLUMNS: frozenset[str] = frozenset({
    "email",
    "domain",
    "fname",
    "lname",
    "state",
    "address",
    "county",
    "city",
    "zip",
    "website",
    "ip",
})


# ---------------------------------------------------------------------------
# Completeness
# ---------------------------------------------------------------------------

def compute_completeness_score(row: pd.Series) -> int:
    """Count non-null, non-empty business columns present in the row.

    Only columns in BUSINESS_COLUMNS that exist in the row index are counted.
    Technical, scoring, DNS, and pipeline metadata columns are excluded.
    """
    total = 0
    for col in BUSINESS_COLUMNS:
        if col not in row.index:
            continue
        val = row[col]
        if val is None:
            continue
        try:
            if pd.isna(val):
                continue
        except (TypeError, ValueError):
            pass
        if isinstance(val, str) and not val.strip():
            continue
        total += 1
    return total


# ---------------------------------------------------------------------------
# Canonical entry stored in the index
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class CanonicalEntry:
    """Minimal row summary stored in the dedupe index for comparison.

    Contains only what is needed to apply the 4-rule hierarchy and to
    identify the row for Subphase 8 retroactive flag correction.
    """

    email_normalized: str
    hard_fail: bool
    score: int
    completeness_score: int
    source_file: str
    source_row_number: int
    global_ordinal: int  # monotonically increasing; lower = earlier = wins on tiebreak


# ---------------------------------------------------------------------------
# Decision result
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class DedupeDecision:
    """Result of comparing two canonical candidates via compare_rows_for_canonical."""

    winner: CanonicalEntry
    loser: CanonicalEntry
    loser_reason: str


# ---------------------------------------------------------------------------
# Pure comparison function (no side effects, fully unit-testable)
# ---------------------------------------------------------------------------

def compare_rows_for_canonical(
    current: CanonicalEntry,
    challenger: CanonicalEntry,
) -> DedupeDecision:
    """Apply the 4-rule hierarchy to select a winner between two entries.

    loser_reason tokens:
      duplicate_hard_fail_loser            — loser had hard_fail=True
      duplicate_lower_score                — loser had a lower score
      duplicate_lower_completeness         — loser had fewer non-null business columns
      duplicate_later_occurrence_tiebreak  — loser arrived later in the stream
    """
    # Rule 1: hard_fail=False beats hard_fail=True
    if not current.hard_fail and challenger.hard_fail:
        return DedupeDecision(winner=current, loser=challenger,
                              loser_reason="duplicate_hard_fail_loser")
    if current.hard_fail and not challenger.hard_fail:
        return DedupeDecision(winner=challenger, loser=current,
                              loser_reason="duplicate_hard_fail_loser")

    # Rule 2: higher score wins
    if current.score > challenger.score:
        return DedupeDecision(winner=current, loser=challenger,
                              loser_reason="duplicate_lower_score")
    if challenger.score > current.score:
        return DedupeDecision(winner=challenger, loser=current,
                              loser_reason="duplicate_lower_score")

    # Rule 3: higher completeness wins
    if current.completeness_score > challenger.completeness_score:
        return DedupeDecision(winner=current, loser=challenger,
                              loser_reason="duplicate_lower_completeness")
    if challenger.completeness_score > current.completeness_score:
        return DedupeDecision(winner=challenger, loser=current,
                              loser_reason="duplicate_lower_completeness")

    # Rule 4: stable tiebreak — lower global_ordinal = earlier = wins
    if current.global_ordinal < challenger.global_ordinal:
        return DedupeDecision(winner=current, loser=challenger,
                              loser_reason="duplicate_later_occurrence_tiebreak")
    return DedupeDecision(winner=challenger, loser=current,
                          loser_reason="duplicate_later_occurrence_tiebreak")


# ---------------------------------------------------------------------------
# Global dedupe index
# ---------------------------------------------------------------------------

class DedupeIndex:
    """In-memory global index: tracks the canonical entry per email_normalized.

    Lives for the duration of one pipeline run, surviving across all chunks
    and all files. Cumulative metrics are readable at any point for logging.
    """

    def __init__(self) -> None:
        self._store: dict[str, CanonicalEntry] = {}
        self._ordinal: int = 0
        self.emails_seen: int = 0
        self.new_canonicals: int = 0
        self.duplicates_detected: int = 0
        self.replaced_canonicals: int = 0

    @property
    def index_size(self) -> int:
        """Number of unique email_normalized values in the index."""
        return len(self._store)

    @property
    def last_ordinal_assigned(self) -> int:
        """The ordinal assigned by the most recent process_row() call."""
        return self._ordinal - 1

    def get_final_canonical(self, email_normalized: str) -> CanonicalEntry | None:
        """Return the final canonical entry for an email, or None if not in index."""
        return self._store.get(email_normalized)

    def is_final_canonical(
        self,
        email_normalized: str | None,
        source_file: str,
        source_row_number: int,
    ) -> bool:
        """Return True if this row is the definitive final canonical for its email group."""
        if not email_normalized:
            return True
        if email_normalized not in self._store:
            return True
        entry = self._store[email_normalized]
        return entry.source_file == source_file and entry.source_row_number == source_row_number

    def _next_ordinal(self) -> int:
        n = self._ordinal
        self._ordinal += 1
        return n

    def process_row(
        self,
        email_normalized: str | None,
        hard_fail: bool,
        score: int,
        completeness_score: int,
        source_file: str,
        source_row_number: int,
    ) -> tuple[bool, bool, str | None]:
        """Evaluate one row against the index and update state.

        Returns:
            (is_canonical, duplicate_flag, duplicate_reason)

        Rows with no valid email_normalized are singletons: they cannot be
        grouped with any other row, so they are always marked canonical.

        When a challenger wins over the current canonical:
          - challenger is returned as canonical (is_canonical=True)
          - the displaced entry's is_canonical flag in its original chunk is
            stale; replaced_canonicals is incremented for Subphase 8 tracking
        """
        ordinal = self._next_ordinal()
        self.emails_seen += 1

        if not email_normalized:
            return True, False, None

        if email_normalized not in self._store:
            self._store[email_normalized] = CanonicalEntry(
                email_normalized=email_normalized,
                hard_fail=hard_fail,
                score=score,
                completeness_score=completeness_score,
                source_file=source_file,
                source_row_number=source_row_number,
                global_ordinal=ordinal,
            )
            self.new_canonicals += 1
            return True, False, None

        current = self._store[email_normalized]
        challenger = CanonicalEntry(
            email_normalized=email_normalized,
            hard_fail=hard_fail,
            score=score,
            completeness_score=completeness_score,
            source_file=source_file,
            source_row_number=source_row_number,
            global_ordinal=ordinal,
        )
        decision = compare_rows_for_canonical(current, challenger)
        self.duplicates_detected += 1

        if decision.winner is challenger:
            self._store[email_normalized] = challenger
            self.replaced_canonicals += 1
            return True, False, None

        return False, True, decision.loser_reason


# ---------------------------------------------------------------------------
# DataFrame-level helpers
# ---------------------------------------------------------------------------

def _safe_bool(val: Any, default: bool = False) -> bool:
    """Coerce a pandas boolean-compatible value to Python bool.

    Handles np.True_/np.False_ from pandas 3.x nullable boolean columns.
    Returns default on None, pd.NA, or coercion failure.
    """
    if val is None:
        return default
    try:
        if pd.isna(val):
            return default
    except (TypeError, ValueError):
        pass
    try:
        return bool(val)
    except (TypeError, ValueError):
        return default


def apply_email_normalized_column(frame: pd.DataFrame) -> pd.DataFrame:
    """Add email_normalized: the explicit, immutable dedupe key for this row.

    email is already lowercased and stripped by normalize_values().
    email_normalized makes the dedupe key a named, auditable column rather
    than an implicit operation on email at dedupe time.
    Rows with null or empty email receive email_normalized=None.
    """
    result = frame.copy()
    if "email" not in result.columns:
        result["email_normalized"] = None
        return result

    def _normalize_one(val: Any) -> str | None:
        if val is None:
            return None
        try:
            if pd.isna(val):
                return None
        except (TypeError, ValueError):
            pass
        s = str(val).strip().lower()
        return s if s else None

    result["email_normalized"] = result["email"].map(_normalize_one)
    return result


def apply_completeness_column(frame: pd.DataFrame) -> pd.DataFrame:
    """Add completeness_score: count of non-null business columns per row."""
    result = frame.copy()
    result["completeness_score"] = result.apply(compute_completeness_score, axis=1)
    return result


def apply_dedupe_columns(
    frame: pd.DataFrame,
    dedupe_index: DedupeIndex,
) -> pd.DataFrame:
    """Add is_canonical, duplicate_flag, and duplicate_reason columns.

    Reads:  email_normalized, hard_fail, score, completeness_score,
            source_file, source_row_number
    Adds:   is_canonical, duplicate_flag, duplicate_reason

    Rows are processed through the DedupeIndex in DataFrame index order.
    The index is mutated in-place (it is the global state for the run).
    Returns a copy; the original frame is never mutated.
    """
    result = frame.copy()

    is_canonical_list: list[bool] = []
    duplicate_flag_list: list[bool] = []
    duplicate_reason_list: list[str | None] = []
    global_ordinal_list: list[int] = []

    for idx in result.index:
        # email_normalized
        email_norm: str | None = None
        if "email_normalized" in result.columns:
            raw = result.loc[idx, "email_normalized"]
            if raw is not None:
                try:
                    if not pd.isna(raw):
                        s = str(raw).strip().lower()
                        email_norm = s if s else None
                except (TypeError, ValueError):
                    s = str(raw).strip().lower()
                    email_norm = s if s else None

        hard_fail = _safe_bool(
            result.loc[idx, "hard_fail"] if "hard_fail" in result.columns else None
        )

        score_int = 0
        if "score" in result.columns:
            try:
                score_int = int(result.loc[idx, "score"])
            except (TypeError, ValueError):
                score_int = 0

        completeness_int = 0
        if "completeness_score" in result.columns:
            try:
                completeness_int = int(result.loc[idx, "completeness_score"])
            except (TypeError, ValueError):
                completeness_int = 0

        source_file = ""
        if "source_file" in result.columns:
            sf = result.loc[idx, "source_file"]
            source_file = str(sf) if sf is not None else ""

        source_row_number = 0
        if "source_row_number" in result.columns:
            try:
                source_row_number = int(result.loc[idx, "source_row_number"])
            except (TypeError, ValueError):
                source_row_number = 0

        is_can, dup_flag, dup_reason = dedupe_index.process_row(
            email_normalized=email_norm,
            hard_fail=hard_fail,
            score=score_int,
            completeness_score=completeness_int,
            source_file=source_file,
            source_row_number=source_row_number,
        )

        is_canonical_list.append(is_can)
        duplicate_flag_list.append(dup_flag)
        duplicate_reason_list.append(dup_reason)
        global_ordinal_list.append(dedupe_index.last_ordinal_assigned)

    result["is_canonical"] = pd.array(is_canonical_list, dtype="boolean")
    result["duplicate_flag"] = pd.array(duplicate_flag_list, dtype="boolean")
    result["duplicate_reason"] = duplicate_reason_list
    result["global_ordinal"] = global_ordinal_list

    return result
