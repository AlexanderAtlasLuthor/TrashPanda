"""Unit tests for the first-wave scoring_v2 evaluators."""

from __future__ import annotations

import copy

import pytest

from app.scoring_v2 import (
    DnsSignalEvaluator,
    DomainMatchSignalEvaluator,
    DomainPresenceSignalEvaluator,
    ScoringSignal,
    SyntaxSignalEvaluator,
    TypoCorrectionSignalEvaluator,
)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _assert_is_signal_list(out) -> None:
    assert isinstance(out, list)
    for item in out:
        assert isinstance(item, ScoringSignal)


def _run_and_check_no_mutation(evaluator, row: dict):
    snapshot = copy.deepcopy(row)
    out = evaluator.evaluate(row)
    assert row == snapshot, "evaluator must not mutate input row"
    _assert_is_signal_list(out)
    return out


# ---------------------------------------------------------------------------
# SyntaxSignalEvaluator
# ---------------------------------------------------------------------------


class TestSyntaxSignalEvaluator:
    def test_valid_syntax_emits_positive(self):
        out = _run_and_check_no_mutation(
            SyntaxSignalEvaluator(), {"syntax_valid": True}
        )
        assert len(out) == 1
        assert out[0].reason_code == "syntax_valid"
        assert out[0].direction == "positive"
        assert out[0].value == 1.0
        assert out[0].confidence == 1.0

    def test_invalid_syntax_emits_negative(self):
        out = _run_and_check_no_mutation(
            SyntaxSignalEvaluator(), {"syntax_valid": False}
        )
        assert len(out) == 1
        assert out[0].reason_code == "syntax_invalid"
        assert out[0].direction == "negative"

    def test_missing_syntax_emits_negative(self):
        """Missing syntax is treated as a negative signal, not silence."""
        out = _run_and_check_no_mutation(SyntaxSignalEvaluator(), {})
        assert len(out) == 1
        assert out[0].reason_code == "syntax_invalid"
        assert out[0].direction == "negative"

    def test_none_syntax_emits_negative(self):
        out = _run_and_check_no_mutation(
            SyntaxSignalEvaluator(), {"syntax_valid": None}
        )
        assert len(out) == 1
        assert out[0].reason_code == "syntax_invalid"


# ---------------------------------------------------------------------------
# DomainPresenceSignalEvaluator
# ---------------------------------------------------------------------------


class TestDomainPresenceSignalEvaluator:
    def test_non_empty_string_emits_positive(self):
        out = _run_and_check_no_mutation(
            DomainPresenceSignalEvaluator(),
            {"corrected_domain": "gmail.com"},
        )
        assert len(out) == 1
        assert out[0].reason_code == "domain_present"
        assert out[0].direction == "positive"

    def test_empty_string_emits_negative(self):
        out = _run_and_check_no_mutation(
            DomainPresenceSignalEvaluator(),
            {"corrected_domain": ""},
        )
        assert len(out) == 1
        assert out[0].reason_code == "no_domain"
        assert out[0].direction == "negative"

    def test_none_emits_negative(self):
        out = _run_and_check_no_mutation(
            DomainPresenceSignalEvaluator(),
            {"corrected_domain": None},
        )
        assert len(out) == 1
        assert out[0].reason_code == "no_domain"

    def test_missing_key_emits_negative(self):
        out = _run_and_check_no_mutation(DomainPresenceSignalEvaluator(), {})
        assert len(out) == 1
        assert out[0].reason_code == "no_domain"

    def test_non_string_value_emits_negative(self):
        """Defensive: a bad-type value (e.g. an accidental number) is
        treated as absent rather than used as a domain."""
        out = _run_and_check_no_mutation(
            DomainPresenceSignalEvaluator(), {"corrected_domain": 42}
        )
        assert len(out) == 1
        assert out[0].reason_code == "no_domain"


# ---------------------------------------------------------------------------
# TypoCorrectionSignalEvaluator
# ---------------------------------------------------------------------------


class TestTypoCorrectionSignalEvaluator:
    def test_true_emits_negative(self):
        out = _run_and_check_no_mutation(
            TypoCorrectionSignalEvaluator(), {"typo_corrected": True}
        )
        assert len(out) == 1
        assert out[0].reason_code == "typo_corrected"
        assert out[0].direction == "negative"

    def test_false_emits_nothing(self):
        out = _run_and_check_no_mutation(
            TypoCorrectionSignalEvaluator(), {"typo_corrected": False}
        )
        assert out == []

    def test_missing_emits_nothing(self):
        out = _run_and_check_no_mutation(TypoCorrectionSignalEvaluator(), {})
        assert out == []

    def test_none_emits_nothing(self):
        out = _run_and_check_no_mutation(
            TypoCorrectionSignalEvaluator(), {"typo_corrected": None}
        )
        assert out == []


# ---------------------------------------------------------------------------
# DomainMatchSignalEvaluator
# ---------------------------------------------------------------------------


class TestDomainMatchSignalEvaluator:
    def test_true_emits_positive(self):
        out = _run_and_check_no_mutation(
            DomainMatchSignalEvaluator(),
            {"domain_matches_input_column": True},
        )
        assert len(out) == 1
        assert out[0].reason_code == "domain_match"
        assert out[0].direction == "positive"

    def test_false_emits_negative(self):
        out = _run_and_check_no_mutation(
            DomainMatchSignalEvaluator(),
            {"domain_matches_input_column": False},
        )
        assert len(out) == 1
        assert out[0].reason_code == "domain_mismatch"
        assert out[0].direction == "negative"

    def test_missing_is_silent(self):
        out = _run_and_check_no_mutation(DomainMatchSignalEvaluator(), {})
        assert out == []

    def test_none_is_silent(self):
        out = _run_and_check_no_mutation(
            DomainMatchSignalEvaluator(),
            {"domain_matches_input_column": None},
        )
        assert out == []


# ---------------------------------------------------------------------------
# DnsSignalEvaluator
# ---------------------------------------------------------------------------


class TestDnsSignalEvaluator:
    def test_mx_present_emits_mx_positive(self):
        out = _run_and_check_no_mutation(
            DnsSignalEvaluator(),
            {"has_mx_record": True},
        )
        assert [s.reason_code for s in out] == ["mx_present"]
        assert out[0].direction == "positive"

    def test_a_fallback_when_no_mx_but_a(self):
        out = _run_and_check_no_mutation(
            DnsSignalEvaluator(),
            {"has_mx_record": False, "has_a_record": True},
        )
        assert [s.reason_code for s in out] == ["a_fallback"]
        assert out[0].direction == "positive"

    def test_mx_takes_precedence_over_a_fallback(self):
        """MX and A-fallback are mutually exclusive — only one positive
        signal is emitted even if both flags are True."""
        out = _run_and_check_no_mutation(
            DnsSignalEvaluator(),
            {"has_mx_record": True, "has_a_record": True},
        )
        assert [s.reason_code for s in out] == ["mx_present"]

    def test_nxdomain_emits_nxdomain_negative(self):
        out = _run_and_check_no_mutation(
            DnsSignalEvaluator(),
            {"domain_exists": False, "dns_error": "nxdomain"},
        )
        assert [s.reason_code for s in out] == ["nxdomain"]
        assert out[0].direction == "negative"

    def test_timeout_emits_dns_timeout_negative(self):
        out = _run_and_check_no_mutation(
            DnsSignalEvaluator(),
            {"domain_exists": False, "dns_error": "timeout"},
        )
        assert [s.reason_code for s in out] == ["dns_timeout"]

    def test_no_nameservers_emits_specific_negative(self):
        out = _run_and_check_no_mutation(
            DnsSignalEvaluator(),
            {"domain_exists": False, "dns_error": "no_nameservers"},
        )
        assert [s.reason_code for s in out] == ["dns_no_nameservers"]

    def test_no_mx_emits_specific_negative(self):
        out = _run_and_check_no_mutation(
            DnsSignalEvaluator(),
            {"domain_exists": False, "dns_error": "no_mx"},
        )
        assert [s.reason_code for s in out] == ["dns_no_mx"]

    def test_no_mx_no_a_emits_specific_negative(self):
        out = _run_and_check_no_mutation(
            DnsSignalEvaluator(),
            {"domain_exists": False, "dns_error": "no_mx_no_a"},
        )
        assert [s.reason_code for s in out] == ["dns_no_mx_no_a"]

    def test_generic_error_emits_dns_error_negative(self):
        out = _run_and_check_no_mutation(
            DnsSignalEvaluator(),
            {"domain_exists": False, "dns_error": "error"},
        )
        assert [s.reason_code for s in out] == ["dns_error"]

    def test_domain_exists_false_without_specific_error_emits_fallback(self):
        out = _run_and_check_no_mutation(
            DnsSignalEvaluator(),
            {"domain_exists": False, "dns_error": None},
        )
        assert [s.reason_code for s in out] == ["domain_not_resolving"]

    def test_nxdomain_does_not_also_emit_domain_not_resolving(self):
        """Specific DNS negatives short-circuit the generic fallback."""
        out = _run_and_check_no_mutation(
            DnsSignalEvaluator(),
            {"domain_exists": False, "dns_error": "nxdomain"},
        )
        assert [s.reason_code for s in out] == ["nxdomain"]

    def test_no_dns_signals_returns_empty_list(self):
        out = _run_and_check_no_mutation(DnsSignalEvaluator(), {})
        assert out == []

    def test_mx_and_error_can_coexist(self):
        """Rare but legal: both a positive (MX) and a negative (error)
        can be emitted if the row reports both kinds of evidence."""
        out = _run_and_check_no_mutation(
            DnsSignalEvaluator(),
            {
                "has_mx_record": True,
                "dns_error": "timeout",
            },
        )
        codes = [s.reason_code for s in out]
        assert "mx_present" in codes
        assert "dns_timeout" in codes
        assert len(codes) == 2

    def test_unknown_dns_error_token_is_ignored(self):
        """An unknown dns_error token does not emit a specific signal;
        if domain_exists is False the fallback kicks in; otherwise
        the evaluator is silent."""
        out = _run_and_check_no_mutation(
            DnsSignalEvaluator(),
            {"dns_error": "some_unknown_value"},
        )
        assert out == []

    def test_unknown_dns_error_with_domain_exists_false_triggers_fallback(self):
        out = _run_and_check_no_mutation(
            DnsSignalEvaluator(),
            {"domain_exists": False, "dns_error": "some_unknown_value"},
        )
        assert [s.reason_code for s in out] == ["domain_not_resolving"]


# ---------------------------------------------------------------------------
# Shared contract checks across all evaluators
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "evaluator_cls",
    [
        SyntaxSignalEvaluator,
        DomainPresenceSignalEvaluator,
        TypoCorrectionSignalEvaluator,
        DomainMatchSignalEvaluator,
        DnsSignalEvaluator,
    ],
)
class TestEvaluatorContracts:
    def test_output_is_list_of_scoring_signals(self, evaluator_cls):
        evaluator = evaluator_cls()
        out = evaluator.evaluate(
            {
                "syntax_valid": True,
                "corrected_domain": "gmail.com",
                "typo_corrected": False,
                "domain_matches_input_column": True,
                "has_mx_record": True,
                "has_a_record": False,
                "domain_exists": True,
                "dns_error": None,
            }
        )
        _assert_is_signal_list(out)

    def test_does_not_mutate_rich_input(self, evaluator_cls):
        evaluator = evaluator_cls()
        row = {
            "syntax_valid": False,
            "corrected_domain": "bad.example",
            "typo_corrected": True,
            "domain_matches_input_column": False,
            "has_mx_record": False,
            "has_a_record": False,
            "domain_exists": False,
            "dns_error": "timeout",
        }
        snapshot = copy.deepcopy(row)
        evaluator.evaluate(row)
        assert row == snapshot

    def test_accepts_empty_row_without_error(self, evaluator_cls):
        evaluator = evaluator_cls()
        # Should not raise regardless of which evaluator this is.
        out = evaluator.evaluate({})
        _assert_is_signal_list(out)
