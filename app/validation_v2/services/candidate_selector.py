"""DefaultValidationCandidateSelector: decide which requests deserve validation.

Candidate selection is the *soft* gate after exclusion. A request
that passes exclusion may still not be worth validating — for
example an already-invalid V2 bucket, or a row with a vanishingly
small V2 score. This service encodes those rules in one place so
the engine can stay a thin orchestrator.

Decision order:

    1. ``bucket_v2`` must be in ``policy.allow_validation_for_buckets``.
       The ``review`` bucket is the primary target; operators may
       opt in to ``high_confidence`` as well.

    2. ``syntax_valid`` must be True (a syntax-invalid address is
       never a candidate, regardless of score).

    3. ``score_v2`` must meet ``policy.strong_candidate_min_score``.

    4. ``confidence_v2`` must meet
       ``policy.strong_candidate_min_confidence``.

First rule that fails short-circuits the decision. The selector
surfaces the reason via :meth:`explain` so the engine can attach
it to the result's metadata.
"""

from __future__ import annotations

from typing import NamedTuple

from ..interfaces import ValidationCandidateSelector
from ..policy import ValidationPolicy
from ..request import ValidationRequest


# Reason codes the selector emits. The engine maps these into
# its own reason-code vocabulary; keeping the selector's codes
# separate means the engine can rename without a ripple here.
REASON_ACCEPTED = "candidate_accepted"
REASON_BUCKET_NOT_ALLOWED = "bucket_not_allowed"
REASON_SYNTAX_INVALID = "syntax_invalid_for_candidacy"
REASON_SCORE_BELOW_THRESHOLD = "score_below_threshold"
REASON_CONFIDENCE_BELOW_THRESHOLD = "confidence_below_threshold"


class CandidateDecision(NamedTuple):
    """Outcome returned by :meth:`DefaultValidationCandidateSelector.explain`.

    Attributes:
        accepted: True iff the request is a validation candidate.
        reason: Machine-readable reason code — one of the
            ``REASON_*`` constants in this module.
    """

    accepted: bool
    reason: str


class DefaultValidationCandidateSelector(ValidationCandidateSelector):
    """Default candidate-selection policy.

    Reads only the policy's thresholds and bucket allow-list;
    never consults external state. Deterministic for any given
    (request, policy) pair.
    """

    def should_validate(
        self,
        request: ValidationRequest,
        policy: ValidationPolicy,
    ) -> bool:
        return self.explain(request, policy).accepted

    # ------------------------------------------------------------------
    # Extended API (not on the ABC)
    # ------------------------------------------------------------------

    def explain(
        self,
        request: ValidationRequest,
        policy: ValidationPolicy,
    ) -> CandidateDecision:
        """Return the full decision with the matching reason code.

        The engine uses this to populate result metadata without
        re-running the decision. Keeping ``should_validate`` and
        ``explain`` in sync is a test concern (see
        ``test_validation_v2_passive.py``).
        """
        if request.bucket_v2 not in policy.allow_validation_for_buckets:
            return CandidateDecision(False, REASON_BUCKET_NOT_ALLOWED)

        if not request.syntax_valid:
            return CandidateDecision(False, REASON_SYNTAX_INVALID)

        if request.score_v2 < policy.strong_candidate_min_score:
            return CandidateDecision(False, REASON_SCORE_BELOW_THRESHOLD)

        if request.confidence_v2 < policy.strong_candidate_min_confidence:
            return CandidateDecision(False, REASON_CONFIDENCE_BELOW_THRESHOLD)

        return CandidateDecision(True, REASON_ACCEPTED)


__all__ = [
    "DefaultValidationCandidateSelector",
    "CandidateDecision",
    "REASON_ACCEPTED",
    "REASON_BUCKET_NOT_ALLOWED",
    "REASON_SYNTAX_INVALID",
    "REASON_SCORE_BELOW_THRESHOLD",
    "REASON_CONFIDENCE_BELOW_THRESHOLD",
]
