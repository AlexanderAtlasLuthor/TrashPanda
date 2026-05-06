"""V2.10.11 — consensus aggregator for external validator results."""

from __future__ import annotations

from collections.abc import Sequence

from .registry import (
    VERDICT_CATCH_ALL,
    VERDICT_INVALID,
    VERDICT_RISKY,
    VERDICT_UNKNOWN,
    VERDICT_VALID,
    ExternalValidationResult,
)


# Consensus vocabulary surfaced as the ``external_consensus`` row
# column. Decision rule 5f only fires on ``invalid``; the other
# values are informational (the policy never *escalates* a row to
# auto_approve based on external opinions).
EXTERNAL_CONSENSUS_VALID: str = "valid"
EXTERNAL_CONSENSUS_INVALID: str = "invalid"
EXTERNAL_CONSENSUS_DISPUTED: str = "disputed"
EXTERNAL_CONSENSUS_UNCONFIRMED: str = "unconfirmed"
EXTERNAL_CONSENSUS_NOT_RUN: str = "not_run"

EXTERNAL_CONSENSUS_VALUES: tuple[str, ...] = (
    EXTERNAL_CONSENSUS_VALID,
    EXTERNAL_CONSENSUS_INVALID,
    EXTERNAL_CONSENSUS_DISPUTED,
    EXTERNAL_CONSENSUS_UNCONFIRMED,
    EXTERNAL_CONSENSUS_NOT_RUN,
)


def compute_consensus(
    results: Sequence[ExternalValidationResult],
) -> str:
    """Combine N validator results into one consensus label.

    Rules (first match wins):

    1. Empty input → ``not_run`` — no validators registered or every
       adapter erred and was filtered out by the caller.
    2. Any ``invalid`` verdict → ``invalid``. A single confident
       "this address bounces" is enough — the policy will reject.
    3. ``valid`` verdicts AND no ``invalid`` AND no ``risky`` →
       ``valid``. (``catch_all`` and ``unknown`` are tolerated
       because they don't contradict a positive verdict.)
    4. Any ``valid`` AND any ``risky`` (no invalids) → ``disputed``.
       Used to flag rows where vendors disagree non-fatally.
    5. Otherwise → ``unconfirmed``. Includes the all-unknown / all-
       catch-all / all-risky cases that no aggregation can rescue.

    The function is pure and never raises. Each result's ``error``
    field is treated like ``verdict=unknown``; callers can also
    pre-filter erroring results if they want stricter behaviour.
    """
    if not results:
        return EXTERNAL_CONSENSUS_NOT_RUN

    invalid_seen = False
    valid_seen = False
    risky_seen = False
    catch_all_seen = False
    unknown_seen = False

    for r in results:
        verdict = r.verdict if r.error is None else VERDICT_UNKNOWN
        if verdict == VERDICT_INVALID:
            invalid_seen = True
        elif verdict == VERDICT_VALID:
            valid_seen = True
        elif verdict == VERDICT_RISKY:
            risky_seen = True
        elif verdict == VERDICT_CATCH_ALL:
            catch_all_seen = True
        elif verdict == VERDICT_UNKNOWN:
            unknown_seen = True
        # Any unknown verdict string falls through to unknown_seen
        # below — keeps the aggregator forward-compatible if a
        # vendor adds a new verdict label and the adapter forwards
        # it verbatim.
        else:
            unknown_seen = True

    if invalid_seen:
        return EXTERNAL_CONSENSUS_INVALID
    if valid_seen and not risky_seen:
        return EXTERNAL_CONSENSUS_VALID
    if valid_seen and risky_seen:
        return EXTERNAL_CONSENSUS_DISPUTED
    # No invalids, no positive valids — every other combination is
    # unconfirmed (catch_all-only / unknown-only / risky-only / mix
    # of catch_all + unknown / catch_all + risky).
    if catch_all_seen or unknown_seen or risky_seen:
        return EXTERNAL_CONSENSUS_UNCONFIRMED
    return EXTERNAL_CONSENSUS_UNCONFIRMED


__all__ = [
    "EXTERNAL_CONSENSUS_DISPUTED",
    "EXTERNAL_CONSENSUS_INVALID",
    "EXTERNAL_CONSENSUS_NOT_RUN",
    "EXTERNAL_CONSENSUS_UNCONFIRMED",
    "EXTERNAL_CONSENSUS_VALID",
    "EXTERNAL_CONSENSUS_VALUES",
    "compute_consensus",
]
