"""Calibration tests for ScoringEngineV2.

These tests lock in the 2026-04 calibration pass. Each scenario maps
to a failure mode the evaluation exposed:

  * Normalization against a fixed theoretical max (not the emitted
    positive sum) — weak positive-only rows can no longer reach 1.0.
  * Evidence-strength gating for ``high_confidence`` — A-only rows
    can no longer be promoted.
  * Separate confidence aggregation weights — structural signals no
    longer inflate confidence.
  * Typo+MX promotion path preserved — legitimate corrections remain
    promotable.
  * Timeout rows remain eligible for ``review``.
  * Hard stops still dominate the bucket regardless of score.
"""

from __future__ import annotations

import copy
import json

import pytest

from app.scoring_v2 import (
    DnsSignalEvaluator,
    DomainMatchSignalEvaluator,
    DomainPresenceSignalEvaluator,
    ScoringEngineV2,
    SyntaxSignalEvaluator,
    TypoCorrectionSignalEvaluator,
    build_default_profile,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_engine():
    return ScoringEngineV2(
        evaluators=[
            SyntaxSignalEvaluator(),
            DomainPresenceSignalEvaluator(),
            TypoCorrectionSignalEvaluator(),
            DomainMatchSignalEvaluator(),
            DnsSignalEvaluator(),
        ],
        profile=build_default_profile(),
    )


def _mx_row(**overrides):
    row = {
        "syntax_valid": True,
        "corrected_domain": "gmail.com",
        "typo_corrected": False,
        "domain_matches_input_column": True,
        "has_mx_record": True,
        "has_a_record": False,
        "domain_exists": True,
        "dns_error": None,
    }
    row.update(overrides)
    return row


def _a_only_row(**overrides):
    row = {
        "syntax_valid": True,
        "corrected_domain": "some-site.example",
        "typo_corrected": False,
        "domain_matches_input_column": True,
        "has_mx_record": False,
        "has_a_record": True,
        "domain_exists": True,
        "dns_error": None,
    }
    row.update(overrides)
    return row


def _timeout_row(**overrides):
    row = {
        "syntax_valid": True,
        "corrected_domain": "slow.example",
        "typo_corrected": False,
        "domain_matches_input_column": True,
        "has_mx_record": None,
        "has_a_record": None,
        "domain_exists": None,
        "dns_error": "timeout",
    }
    row.update(overrides)
    return row


# ---------------------------------------------------------------------------
# Normalization — fixed denominator, not emitted-positive sum
# ---------------------------------------------------------------------------


class TestNormalization:
    def test_mx_row_scores_higher_than_a_only_row(self):
        engine = _make_engine()
        mx = engine.evaluate_row(_mx_row())
        a_only = engine.evaluate_row(_a_only_row())
        assert mx.final_score > a_only.final_score

    def test_a_only_row_does_not_reach_one(self):
        """The bug we are fixing: weak positive-only rows used to
        score 1.0 because the denominator was the *emitted* positive
        sum. With a fixed denominator that includes mx_present (50),
        an A-only row cannot reach 1.0."""
        engine = _make_engine()
        out = engine.evaluate_row(_a_only_row())
        # syntax_valid (25) + domain_present (10) + a_fallback (20)
        # + domain_match (5) = 60. Denominator = 90. -> 60/90.
        assert out.final_score == pytest.approx(60.0 / 90.0, rel=1e-6)
        assert out.final_score < 1.0

    def test_mx_row_reaches_final_score_of_one(self):
        """Full positive evidence (syntax, domain, MX, match) = 90.
        Denominator = 90. final_score = 1.0 exactly."""
        engine = _make_engine()
        out = engine.evaluate_row(_mx_row())
        assert out.final_score == pytest.approx(1.0, rel=1e-6)

    def test_final_score_is_clamped_to_unit_interval(self):
        """Negative-heavy rows clamp to 0.0, never negative."""
        engine = _make_engine()
        out = engine.evaluate_row(
            {
                "syntax_valid": True,
                "corrected_domain": "bad.example",
                "typo_corrected": True,
                "domain_matches_input_column": False,
                "has_mx_record": False,
                "has_a_record": False,
                "domain_exists": False,
                "dns_error": "no_mx_no_a",
            }
        )
        assert 0.0 <= out.final_score <= 1.0

    def test_denominator_is_read_from_profile_weights_not_literals(self):
        """Change ``signal_weights['mx_present']`` and the
        denominator follows — proving the engine does not hard-code
        the theoretical max."""
        profile = build_default_profile()
        profile.signal_weights["mx_present"] = 100.0
        engine = ScoringEngineV2(
            evaluators=[
                SyntaxSignalEvaluator(),
                DomainPresenceSignalEvaluator(),
                TypoCorrectionSignalEvaluator(),
                DomainMatchSignalEvaluator(),
                DnsSignalEvaluator(),
            ],
            profile=profile,
        )
        # New denominator: 25 + 10 + 100 + 5 = 140.
        assert profile.derived_max_positive_possible() == 140.0
        out = engine.evaluate_row(_mx_row())
        # Full row at new weights: 25 + 10 + 100 + 5 = 140. -> 1.0.
        assert out.final_score == pytest.approx(1.0, rel=1e-6)


# ---------------------------------------------------------------------------
# Evidence-strength gating — high_confidence requires MX
# ---------------------------------------------------------------------------


class TestHighConfidenceGate:
    def test_mx_row_reaches_high_confidence(self):
        engine = _make_engine()
        out = engine.evaluate_row(_mx_row())
        assert out.bucket == "high_confidence"

    def test_a_only_row_is_not_high_confidence(self):
        """Regression guard: the previous V2 promoted A-only rows.
        With the strong-evidence gate this must no longer happen."""
        engine = _make_engine()
        out = engine.evaluate_row(_a_only_row())
        assert out.bucket != "high_confidence"
        # A-only rows (60/90 = 0.67) land in review, not invalid.
        assert out.bucket == "review"

    def test_high_score_without_strong_evidence_cannot_be_high_confidence(self):
        """Even an A-only row that clears the 0.75 score threshold
        (hypothetically) must still be blocked from high_confidence
        because the strong-evidence reason code is missing."""
        profile = build_default_profile()
        # Crank the score threshold down so the A-only row clears it
        # purely on score.
        profile.high_confidence_threshold = 0.5
        engine = ScoringEngineV2(
            evaluators=[
                SyntaxSignalEvaluator(),
                DomainPresenceSignalEvaluator(),
                TypoCorrectionSignalEvaluator(),
                DomainMatchSignalEvaluator(),
                DnsSignalEvaluator(),
            ],
            profile=profile,
        )
        out = engine.evaluate_row(_a_only_row())
        assert out.final_score >= 0.5
        # Still gated by the strong-evidence rule.
        assert out.bucket != "high_confidence"

    def test_review_bucket_is_not_gated_on_strong_evidence(self):
        """The gate only restricts ``high_confidence`` — A-only and
        timeout rows must still be reviewable."""
        engine = _make_engine()
        a_only = engine.evaluate_row(_a_only_row())
        timeout = engine.evaluate_row(_timeout_row())
        assert a_only.bucket == "review"
        assert timeout.bucket == "review"


# ---------------------------------------------------------------------------
# Confidence aggregation — structural signals down-weighted
# ---------------------------------------------------------------------------


class TestConfidenceAggregation:
    def test_mx_row_confidence_above_eighty(self):
        engine = _make_engine()
        out = engine.evaluate_row(_mx_row())
        assert out.confidence >= 0.80

    def test_a_only_confidence_lower_than_mx(self):
        engine = _make_engine()
        mx = engine.evaluate_row(_mx_row())
        a_only = engine.evaluate_row(_a_only_row())
        assert a_only.confidence < mx.confidence

    def test_timeout_confidence_lower_than_mx(self):
        engine = _make_engine()
        mx = engine.evaluate_row(_mx_row())
        timeout = engine.evaluate_row(_timeout_row())
        assert timeout.confidence < mx.confidence

    def test_trivial_structural_only_does_not_inflate_confidence(self):
        """A row with only structural positives (syntax + domain) and
        no DNS evidence must not have its confidence above 0.80.
        Under the old score-weight-as-confidence-weight aggregation
        those two signals' 1.0 confidences dominated and pushed the
        weighted average up."""
        engine = _make_engine()
        out = engine.evaluate_row(
            {
                "syntax_valid": True,
                "corrected_domain": "x.example",
                "typo_corrected": False,
                # domain_matches_input_column: silent on None
                "domain_matches_input_column": None,
                # No DNS signals at all.
                "has_mx_record": None,
                "has_a_record": None,
                "domain_exists": None,
                "dns_error": None,
            }
        )
        assert out.confidence < 0.80


# ---------------------------------------------------------------------------
# Typo-corrected + MX — V2's promotion behavior preserved
# ---------------------------------------------------------------------------


class TestTypoCorrectedMxPromotion:
    def test_typo_corrected_with_mx_stays_strong(self):
        """A legitimate typo correction (e.g. gnail.com → gmail.com)
        should not kill the row. With the mismatch reduction applied
        the row still lands in a strong bucket."""
        engine = _make_engine()
        row = _mx_row(
            typo_corrected=True,
            domain_matches_input_column=False,
        )
        out = engine.evaluate_row(row)
        # Evidence is strong: MX present, syntax valid, domain present.
        # Typo+mismatch are not allowed to pull the row into invalid.
        assert out.bucket in {"high_confidence", "review"}
        assert out.final_score > 0.80

    def test_mismatch_reduction_only_fires_when_typo_corrected(self):
        """A row with mismatch but no typo correction gets the full
        penalty. A row with both gets the reduced penalty."""
        engine = _make_engine()
        plain_mismatch = engine.evaluate_row(
            _mx_row(domain_matches_input_column=False)
        )
        typo_mismatch = engine.evaluate_row(
            _mx_row(
                typo_corrected=True,
                domain_matches_input_column=False,
            )
        )
        # The typo-corrected variant pays a mismatch penalty, plus
        # the typo_corrected penalty (3), but minus the mismatch
        # reduction. Net: typo_mismatch.final_score should be
        # *close to* plain_mismatch but not dramatically lower.
        # More importantly, the reduction must be applied:
        assert (
            typo_mismatch.breakdown_dict["mismatch_adjustment"] > 0
        )
        assert (
            plain_mismatch.breakdown_dict["mismatch_adjustment"] == 0
        )

    def test_domain_mismatch_weight_was_increased(self):
        """The profile was tuned to penalize ``domain_mismatch`` more
        conservatively — bumped from 5 → 8. A mismatch without a
        typo correction should feel the full, larger penalty."""
        profile = build_default_profile()
        assert profile.signal_weights["domain_mismatch"] == 8.0


# ---------------------------------------------------------------------------
# Timeout rows — stay reviewable with lower confidence
# ---------------------------------------------------------------------------


class TestTimeoutRowBehavior:
    def test_timeout_row_lands_in_review(self):
        engine = _make_engine()
        out = engine.evaluate_row(_timeout_row())
        assert out.bucket == "review"
        assert not out.hard_stop

    def test_timeout_row_confidence_is_noticeably_lower(self):
        engine = _make_engine()
        mx = engine.evaluate_row(_mx_row())
        timeout = engine.evaluate_row(_timeout_row())
        # The DNS timeout signal's low confidence (0.30) drags the
        # weighted average below the MX row.
        assert timeout.confidence < 0.80
        assert timeout.confidence < mx.confidence - 0.05


# ---------------------------------------------------------------------------
# Hard stops — always dominate
# ---------------------------------------------------------------------------


class TestHardStops:
    def test_syntax_invalid_is_hard_stop(self):
        engine = _make_engine()
        out = engine.evaluate_row({"syntax_valid": False})
        assert out.hard_stop is True
        assert out.hard_stop_reason == "syntax_invalid"
        assert out.bucket == "invalid"
        assert out.final_score == 0.0

    def test_no_domain_is_hard_stop(self):
        engine = _make_engine()
        out = engine.evaluate_row(
            {"syntax_valid": True, "corrected_domain": ""}
        )
        assert out.hard_stop is True
        assert out.hard_stop_reason == "no_domain"
        assert out.bucket == "invalid"

    def test_nxdomain_is_hard_stop(self):
        engine = _make_engine()
        out = engine.evaluate_row(
            {
                "syntax_valid": True,
                "corrected_domain": "does-not-exist.example",
                "domain_exists": False,
                "dns_error": "nxdomain",
            }
        )
        assert out.hard_stop is True
        assert out.hard_stop_reason == "nxdomain"
        assert out.bucket == "invalid"

    def test_hard_stopped_row_still_has_totals_and_breakdown(self):
        """Hard stops zero out the final score but must preserve the
        audit trail — downstream consumers rely on totals / signals /
        explanation being populated regardless of the bucket."""
        engine = _make_engine()
        out = engine.evaluate_row(
            {
                "syntax_valid": True,
                "corrected_domain": "does-not-exist.example",
                "domain_exists": False,
                "dns_error": "nxdomain",
            }
        )
        assert out.positive_total > 0  # syntax + domain_present fired
        assert out.negative_total > 0  # nxdomain fired
        assert out.confidence > 0
        assert out.reason_codes  # non-empty
        assert out.breakdown_dict  # non-empty
        assert out.explanation  # non-empty


# ---------------------------------------------------------------------------
# Regression protection — deterministic, serializable output
# ---------------------------------------------------------------------------


class TestRegressionProtection:
    def test_breakdown_dict_is_json_serializable(self):
        engine = _make_engine()
        rows = [_mx_row(), _a_only_row(), _timeout_row()]
        for row in rows:
            out = engine.evaluate_row(row)
            encoded = json.dumps(out.breakdown_dict)
            assert json.loads(encoded) == json.loads(encoded)

    def test_to_dict_is_json_serializable(self):
        engine = _make_engine()
        out = engine.evaluate_row(_mx_row())
        encoded = json.dumps(out.to_dict())
        roundtrip = json.loads(encoded)
        # Public surface fields all present.
        for key in (
            "positive_total",
            "negative_total",
            "raw_score",
            "final_score",
            "confidence",
            "hard_stop",
            "hard_stop_reason",
            "bucket",
            "reason_codes",
            "explanation",
            "signals",
        ):
            assert key in roundtrip

    def test_explanation_is_deterministic(self):
        """Same input row → identical explanation string across runs."""
        engine = _make_engine()
        row = _mx_row()
        a = engine.evaluate_row(row).explanation
        b = engine.evaluate_row(copy.deepcopy(row)).explanation
        assert a == b
        # Also stable across separate engine instances.
        engine2 = _make_engine()
        c = engine2.evaluate_row(copy.deepcopy(row)).explanation
        assert a == c

    def test_engine_does_not_mutate_input_row(self):
        engine = _make_engine()
        row = _mx_row(typo_corrected=True, domain_matches_input_column=False)
        snapshot = copy.deepcopy(row)
        engine.evaluate_row(row)
        assert row == snapshot


# ---------------------------------------------------------------------------
# Focused comparison — the specific pre-calibration failure mode
# ---------------------------------------------------------------------------


class TestPreCalibrationFailureModeFixed:
    def test_a_only_row_no_longer_promoted(self):
        """The specific pattern the evaluation flagged: an A-only
        fallback row with ``final_score = 1.0`` (because
        normalization divided by the emitted positive sum) being
        promoted to high_confidence.

        Post-calibration this row:
          * has ``final_score`` well below 1.0 (fixed denominator)
          * is blocked from high_confidence by the strong-evidence
            gate even if the score threshold is met
          * lands in ``review`` — which is the correct bucket for
            weak-but-not-invalid evidence.
        """
        engine = _make_engine()
        out = engine.evaluate_row(_a_only_row())
        assert out.final_score < 1.0
        assert out.bucket != "high_confidence"
        assert out.bucket == "review"

    def test_mx_row_still_strong_after_calibration(self):
        """Regression guard: the calibration must not have weakened
        the legitimate MX-backed path."""
        engine = _make_engine()
        out = engine.evaluate_row(_mx_row())
        assert out.bucket == "high_confidence"
        assert out.final_score >= 0.75
        assert out.confidence >= 0.80
