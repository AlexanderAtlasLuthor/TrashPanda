"""V2.10.11 — unit tests for ``app.external_validators``."""

from __future__ import annotations

import pytest

from app.external_validators import (
    EXTERNAL_CONSENSUS_DISPUTED,
    EXTERNAL_CONSENSUS_INVALID,
    EXTERNAL_CONSENSUS_NOT_RUN,
    EXTERNAL_CONSENSUS_UNCONFIRMED,
    EXTERNAL_CONSENSUS_VALID,
    ExternalEmailValidator,
    ExternalValidationResult,
    clear_registry,
    compute_consensus,
    register,
    registered_validators,
)
from app.external_validators.registry import (
    VERDICT_CATCH_ALL,
    VERDICT_INVALID,
    VERDICT_RISKY,
    VERDICT_UNKNOWN,
    VERDICT_VALID,
)
from app.v2_decision_policy import (
    REASON_EXTERNAL_VALIDATORS_INVALID,
    REASON_HIGH_PROBABILITY,
    apply_v2_decision_policy,
)
from app.validation_v2.decision.policy import FinalAction


# --------------------------------------------------------------------- #
# Fakes
# --------------------------------------------------------------------- #


class _FakeValidator:
    def __init__(self, name: str, verdict: str, confidence: float = 0.9):
        self.name = name
        self._verdict = verdict
        self._confidence = confidence

    def probe(self, email: str, *, timeout: float):
        return ExternalValidationResult(
            validator_name=self.name,
            verdict=self._verdict,
            confidence=self._confidence,
            raw_response={"echo": email},
        )


class _FailingValidator:
    name = "fails"

    def probe(self, email: str, *, timeout: float):
        return ExternalValidationResult(
            validator_name=self.name,
            verdict=VERDICT_UNKNOWN,
            confidence=0.0,
            raw_response={},
            error="network unreachable",
        )


@pytest.fixture(autouse=True)
def _isolated_registry():
    clear_registry()
    yield
    clear_registry()


# --------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------- #


class TestRegistry:
    def test_register_and_iterate(self):
        v = _FakeValidator("zerobounce_like", VERDICT_VALID)
        register(v)
        assert registered_validators() == (v,)

    def test_register_replaces_same_name(self):
        a = _FakeValidator("dup", VERDICT_VALID)
        b = _FakeValidator("dup", VERDICT_INVALID)
        register(a)
        register(b)
        assert registered_validators() == (b,)

    def test_register_rejects_unnamed_validator(self):
        class _Bad:
            name = ""

            def probe(self, email, *, timeout):
                return ExternalValidationResult(
                    validator_name="", verdict=VERDICT_VALID,
                )

        with pytest.raises(ValueError):
            register(_Bad())

    def test_register_rejects_missing_probe(self):
        class _Bad:
            name = "no_probe"

        with pytest.raises(ValueError):
            register(_Bad())  # type: ignore[arg-type]

    def test_protocol_runtime_check(self):
        v = _FakeValidator("ok", VERDICT_VALID)
        assert isinstance(v, ExternalEmailValidator)


# --------------------------------------------------------------------- #
# Consensus
# --------------------------------------------------------------------- #


def _result(name: str, verdict: str, error: str | None = None):
    return ExternalValidationResult(
        validator_name=name, verdict=verdict, error=error,
    )


class TestConsensus:
    def test_empty_input_is_not_run(self):
        assert compute_consensus([]) == EXTERNAL_CONSENSUS_NOT_RUN

    def test_any_invalid_wins(self):
        assert (
            compute_consensus(
                [
                    _result("a", VERDICT_VALID),
                    _result("b", VERDICT_INVALID),
                    _result("c", VERDICT_VALID),
                ]
            )
            == EXTERNAL_CONSENSUS_INVALID
        )

    def test_all_valid_no_risky_is_valid(self):
        assert (
            compute_consensus(
                [
                    _result("a", VERDICT_VALID),
                    _result("b", VERDICT_VALID),
                ]
            )
            == EXTERNAL_CONSENSUS_VALID
        )

    def test_valid_plus_catch_all_is_valid(self):
        """Catch-all does not dispute a positive valid."""
        assert (
            compute_consensus(
                [
                    _result("a", VERDICT_VALID),
                    _result("b", VERDICT_CATCH_ALL),
                ]
            )
            == EXTERNAL_CONSENSUS_VALID
        )

    def test_valid_plus_risky_is_disputed(self):
        assert (
            compute_consensus(
                [
                    _result("a", VERDICT_VALID),
                    _result("b", VERDICT_RISKY),
                ]
            )
            == EXTERNAL_CONSENSUS_DISPUTED
        )

    def test_all_unknown_is_unconfirmed(self):
        assert (
            compute_consensus(
                [
                    _result("a", VERDICT_UNKNOWN),
                    _result("b", VERDICT_UNKNOWN),
                ]
            )
            == EXTERNAL_CONSENSUS_UNCONFIRMED
        )

    def test_all_catch_all_is_unconfirmed(self):
        assert (
            compute_consensus(
                [
                    _result("a", VERDICT_CATCH_ALL),
                ]
            )
            == EXTERNAL_CONSENSUS_UNCONFIRMED
        )

    def test_errored_result_treated_as_unknown(self):
        assert (
            compute_consensus(
                [
                    _result("a", VERDICT_VALID, error="auth"),
                    _result("b", VERDICT_VALID),
                ]
            )
            == EXTERNAL_CONSENSUS_VALID
        )

    def test_unknown_verdict_string_falls_through(self):
        """If a vendor adds a new verdict label, the aggregator
        treats it as unknown rather than raising."""
        assert (
            compute_consensus(
                [
                    _result("a", "weird_new_label"),
                ]
            )
            == EXTERNAL_CONSENSUS_UNCONFIRMED
        )


# --------------------------------------------------------------------- #
# Decision policy rule 5f
# --------------------------------------------------------------------- #


def _policy_inputs(**overrides):
    base = dict(
        probability=0.95,
        smtp_status="valid",
        smtp_was_candidate=True,
        catch_all_status="not_catch_all",
        catch_all_flag=False,
        hard_fail=False,
        v2_final_bucket="ready",
        domain_risk_level="low",
        domain_cold_start=False,
    )
    base.update(overrides)
    return base


class TestExternalConsensusRule:
    def test_invalid_consensus_rejects_high_probability(self):
        result = apply_v2_decision_policy(
            **_policy_inputs(),
            external_consensus="invalid",
        )
        assert result.final_action == FinalAction.AUTO_REJECT
        assert result.decision_reason == REASON_EXTERNAL_VALIDATORS_INVALID

    def test_valid_consensus_does_not_upgrade_review(self):
        """External 'valid' must NOT escalate a row that probability
        alone wouldn't auto-approve — second opinion, not source of
        truth."""
        result = apply_v2_decision_policy(
            **_policy_inputs(probability=0.6),
            external_consensus="valid",
        )
        assert result.final_action == FinalAction.MANUAL_REVIEW

    def test_not_run_is_pass_through(self):
        result = apply_v2_decision_policy(
            **_policy_inputs(),
            external_consensus="not_run",
        )
        assert result.final_action == FinalAction.AUTO_APPROVE
        assert result.decision_reason == REASON_HIGH_PROBABILITY

    def test_disputed_does_not_reject(self):
        """`disputed` is informational; rule 5f only fires on
        explicit `invalid`."""
        result = apply_v2_decision_policy(
            **_policy_inputs(),
            external_consensus="disputed",
        )
        assert result.final_action == FinalAction.AUTO_APPROVE

    def test_hard_fail_beats_external_invalid(self):
        """Rule 1 (V1 hard_fail) still terminates first — same reason
        path, but the audit token names hard_fail, not external."""
        result = apply_v2_decision_policy(
            **_policy_inputs(hard_fail=True),
            external_consensus="invalid",
        )
        assert result.final_action == FinalAction.AUTO_REJECT
        # Hard-fail wins because rule 1 is checked before rule 3b.
        assert result.decision_reason == "hard_fail"
