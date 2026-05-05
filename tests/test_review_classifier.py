"""V2.10.10.b — unit tests for ``app.review_classifier``.

Pins the action-oriented classification of review rows. The
classifier is pure; these tests run on plain dicts so the contract
stays decoupled from pandas.
"""

from __future__ import annotations

import pytest

from app.review_classifier import (
    CONSUMER_CATCH_ALL_PROVIDERS,
    LOW_RISK_PROBABILITY_THRESHOLD,
    REVIEW_ACTION_CATCH_ALL_CONSUMER,
    REVIEW_ACTION_DO_NOT_SEND,
    REVIEW_ACTION_HIGH_RISK,
    REVIEW_ACTION_LOW_RISK,
    REVIEW_ACTION_TIMEOUT_RETRY,
    SECOND_PASS_CANDIDATE_ACTIONS,
    classify_review_row,
    is_second_pass_candidate,
)


def _row(**kwargs) -> dict:
    """Minimal review row with conservative defaults.

    Defaults represent a "blank" row that lands in ``high_risk`` (no
    catch-all signal, no operational SMTP failure, low probability).
    Each test overrides only the fields it cares about.
    """
    base = {
        "email": "alice@example-corp.com",
        "decision_reason": "medium_probability",
        "client_reason": "Needs manual review",
        "smtp_status": "not_tested",
        "catch_all_status": "not_tested",
        "catch_all_flag": False,
        "deliverability_probability": 0.0,
    }
    base.update(kwargs)
    return base


# ------------------------------------------------------------------ #
# Priority 1 — do_not_send
# ------------------------------------------------------------------ #


class TestDoNotSend:
    def test_domain_high_risk_decision_reason(self):
        result = classify_review_row(
            _row(decision_reason="domain_high_risk"),
        )
        assert result == REVIEW_ACTION_DO_NOT_SEND

    def test_disposable_keyword_in_client_reason(self):
        result = classify_review_row(
            _row(client_reason="Disposable email service detected"),
        )
        assert result == REVIEW_ACTION_DO_NOT_SEND

    def test_suspicious_pattern_keyword(self):
        result = classify_review_row(
            _row(client_reason="suspicious_pattern: hyphen-heavy"),
        )
        assert result == REVIEW_ACTION_DO_NOT_SEND

    def test_fake_keyword(self):
        result = classify_review_row(
            _row(client_reason="Fake placeholder address"),
        )
        assert result == REVIEW_ACTION_DO_NOT_SEND

    def test_domain_high_risk_beats_catch_all(self):
        """Hard domain risk evidence wins over catch-all signals."""
        result = classify_review_row(
            _row(
                decision_reason="domain_high_risk",
                catch_all_flag=True,
            ),
        )
        assert result == REVIEW_ACTION_DO_NOT_SEND


# ------------------------------------------------------------------ #
# Priority 2 — catch_all_consumer
# ------------------------------------------------------------------ #


class TestCatchAllConsumer:
    def test_catch_all_flag_routes_to_consumer(self):
        result = classify_review_row(
            _row(
                email="bob@somecorp.com",
                catch_all_flag=True,
                deliverability_probability=0.9,
            ),
        )
        assert result == REVIEW_ACTION_CATCH_ALL_CONSUMER

    def test_catch_all_status_possible_routes_to_consumer(self):
        result = classify_review_row(
            _row(catch_all_status="possible_catch_all"),
        )
        assert result == REVIEW_ACTION_CATCH_ALL_CONSUMER

    def test_catch_all_status_confirmed_routes_to_consumer(self):
        result = classify_review_row(
            _row(catch_all_status="confirmed_catch_all"),
        )
        assert result == REVIEW_ACTION_CATCH_ALL_CONSUMER

    def test_yahoo_domain_is_consumer_catch_all(self):
        result = classify_review_row(
            _row(
                email="user@yahoo.com",
                deliverability_probability=0.9,
            ),
        )
        assert result == REVIEW_ACTION_CATCH_ALL_CONSUMER

    def test_aol_domain_is_consumer_catch_all(self):
        result = classify_review_row(_row(email="user@aol.com"))
        assert result == REVIEW_ACTION_CATCH_ALL_CONSUMER

    def test_verizon_domain_is_consumer_catch_all(self):
        result = classify_review_row(_row(email="user@verizon.net"))
        assert result == REVIEW_ACTION_CATCH_ALL_CONSUMER

    def test_smtp_status_catch_all_possible(self):
        result = classify_review_row(
            _row(
                email="user@unrelated.com",
                smtp_status="catch_all_possible",
            ),
        )
        assert result == REVIEW_ACTION_CATCH_ALL_CONSUMER

    def test_decision_reason_catch_all(self):
        result = classify_review_row(
            _row(decision_reason="catch_all_possible"),
        )
        assert result == REVIEW_ACTION_CATCH_ALL_CONSUMER


# ------------------------------------------------------------------ #
# Priority 3 — timeout_retry (operational SMTP failures)
# ------------------------------------------------------------------ #


class TestTimeoutRetry:
    @pytest.mark.parametrize(
        "smtp_status",
        ["blocked", "timeout", "temp_fail"],
    )
    def test_operational_smtp_status(self, smtp_status):
        result = classify_review_row(_row(smtp_status=smtp_status))
        assert result == REVIEW_ACTION_TIMEOUT_RETRY

    @pytest.mark.parametrize(
        "decision_reason",
        ["smtp_blocked", "smtp_timeout", "smtp_temp_fail"],
    )
    def test_operational_decision_reason(self, decision_reason):
        result = classify_review_row(_row(decision_reason=decision_reason))
        assert result == REVIEW_ACTION_TIMEOUT_RETRY

    def test_catch_all_consumer_beats_operational_smtp(self):
        """A Yahoo row with smtp_blocked goes to catch_all_consumer,
        not timeout_retry — re-probing Yahoo is unlikely to help."""
        result = classify_review_row(
            _row(
                email="user@yahoo.com",
                smtp_status="blocked",
            ),
        )
        assert result == REVIEW_ACTION_CATCH_ALL_CONSUMER


# ------------------------------------------------------------------ #
# Priority 4 — low_risk
# ------------------------------------------------------------------ #


class TestLowRisk:
    def test_high_probability_with_clean_signals(self):
        """The headline rescue case: cold-start B2B with high
        probability, no catch-all, no operational issue → low_risk."""
        result = classify_review_row(
            _row(
                email="alice@some-b2b.com",
                decision_reason="cold_start_no_smtp_valid",
                deliverability_probability=0.85,
            ),
        )
        assert result == REVIEW_ACTION_LOW_RISK

    def test_threshold_boundary_inclusive(self):
        result = classify_review_row(
            _row(deliverability_probability=LOW_RISK_PROBABILITY_THRESHOLD),
        )
        assert result == REVIEW_ACTION_LOW_RISK

    def test_just_below_threshold_falls_to_high_risk(self):
        result = classify_review_row(
            _row(
                deliverability_probability=
                    LOW_RISK_PROBABILITY_THRESHOLD - 0.01,
            ),
        )
        assert result == REVIEW_ACTION_HIGH_RISK


# ------------------------------------------------------------------ #
# Priority 5 — high_risk fallback
# ------------------------------------------------------------------ #


class TestHighRiskFallback:
    def test_no_signals_low_probability(self):
        result = classify_review_row(
            _row(deliverability_probability=0.3),
        )
        assert result == REVIEW_ACTION_HIGH_RISK

    def test_unparseable_probability(self):
        result = classify_review_row(
            _row(deliverability_probability="not-a-number"),
        )
        assert result == REVIEW_ACTION_HIGH_RISK

    def test_missing_probability(self):
        row = _row()
        row["deliverability_probability"] = None
        result = classify_review_row(row)
        assert result == REVIEW_ACTION_HIGH_RISK


# ------------------------------------------------------------------ #
# Stringified CSV inputs
# ------------------------------------------------------------------ #


class TestCsvStringInputs:
    """The pipeline reads CSVs with everything as strings; classifier
    must coerce stringified booleans and floats correctly."""

    def test_string_true_catch_all_flag(self):
        result = classify_review_row(
            _row(catch_all_flag="True"),
        )
        assert result == REVIEW_ACTION_CATCH_ALL_CONSUMER

    def test_string_false_catch_all_flag(self):
        result = classify_review_row(
            _row(catch_all_flag="False", deliverability_probability="0.9"),
        )
        assert result == REVIEW_ACTION_LOW_RISK

    def test_string_probability(self):
        result = classify_review_row(
            _row(deliverability_probability="0.85"),
        )
        assert result == REVIEW_ACTION_LOW_RISK


# ------------------------------------------------------------------ #
# Second-pass candidate predicate
# ------------------------------------------------------------------ #


class TestSecondPassCandidate:
    def test_low_risk_is_candidate(self):
        assert is_second_pass_candidate(REVIEW_ACTION_LOW_RISK)

    def test_timeout_retry_is_candidate(self):
        assert is_second_pass_candidate(REVIEW_ACTION_TIMEOUT_RETRY)

    def test_catch_all_consumer_is_not_candidate(self):
        """Catch-all consumer cannot be confirmed by retry."""
        assert not is_second_pass_candidate(REVIEW_ACTION_CATCH_ALL_CONSUMER)

    def test_high_risk_is_not_candidate(self):
        assert not is_second_pass_candidate(REVIEW_ACTION_HIGH_RISK)

    def test_do_not_send_is_not_candidate(self):
        assert not is_second_pass_candidate(REVIEW_ACTION_DO_NOT_SEND)

    def test_set_size(self):
        assert len(SECOND_PASS_CANDIDATE_ACTIONS) == 2


# ------------------------------------------------------------------ #
# Provider table sanity
# ------------------------------------------------------------------ #


class TestProviderTable:
    def test_yahoo_in_table(self):
        assert "yahoo.com" in CONSUMER_CATCH_ALL_PROVIDERS

    def test_aol_in_table(self):
        assert "aol.com" in CONSUMER_CATCH_ALL_PROVIDERS

    def test_verizon_in_table(self):
        assert "verizon.net" in CONSUMER_CATCH_ALL_PROVIDERS

    def test_gmail_not_in_table(self):
        """Gmail is not a catch-all provider — must not be in the
        fallback table or every Gmail review row would route here."""
        assert "gmail.com" not in CONSUMER_CATCH_ALL_PROVIDERS
