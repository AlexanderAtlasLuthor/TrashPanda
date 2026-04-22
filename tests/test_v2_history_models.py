"""Tests for app.validation_v2.history_models — record + observation."""

from __future__ import annotations

from datetime import datetime

import pytest

from app.validation_v2.history_models import (
    DomainHistoryRecord,
    DomainObservation,
    FinalDecision,
    _safe_rate,
)


def _now() -> datetime:
    return datetime(2026, 4, 22, 10, 0, 0)


# ─────────────────────────────────────────────────────────────────────── #
# _safe_rate                                                              #
# ─────────────────────────────────────────────────────────────────────── #


@pytest.mark.parametrize(
    "num,den,expected",
    [
        (0, 0, 0.0),
        (1, 0, 0.0),
        (0, 10, 0.0),
        (5, 10, 0.5),
        (7, 7, 1.0),
    ],
)
def test_safe_rate_handles_zero_and_typical_cases(num: int, den: int, expected: float) -> None:
    assert _safe_rate(num, den) == pytest.approx(expected)


# ─────────────────────────────────────────────────────────────────────── #
# DomainObservation                                                       #
# ─────────────────────────────────────────────────────────────────────── #


def test_observation_normalises_domain_to_lowercase() -> None:
    obs = DomainObservation(domain="  Example.COM  ")
    assert obs.domain == "example.com"


def test_observation_coerces_unknown_decision_to_unknown() -> None:
    obs = DomainObservation(domain="a.com", final_decision="not_a_real_value")
    assert obs.final_decision == FinalDecision.UNKNOWN


def test_observation_preserves_known_decision() -> None:
    obs = DomainObservation(domain="a.com", final_decision=FinalDecision.READY)
    assert obs.final_decision == FinalDecision.READY


# ─────────────────────────────────────────────────────────────────────── #
# DomainHistoryRecord — rate properties                                   #
# ─────────────────────────────────────────────────────────────────────── #


def test_empty_record_reports_zero_rates() -> None:
    rec = DomainHistoryRecord(domain="x.com", first_seen_at=_now(), last_seen_at=_now())
    assert rec.mx_rate == 0.0
    assert rec.ready_rate == 0.0
    assert rec.invalid_rate == 0.0
    assert rec.timeout_rate == 0.0


def test_rate_properties_compute_correctly() -> None:
    rec = DomainHistoryRecord(
        domain="x.com",
        first_seen_at=_now(),
        last_seen_at=_now(),
        total_seen_count=20,
        mx_present_count=16,
        a_fallback_count=2,
        dns_failure_count=1,
        timeout_count=3,
        ready_count=14,
        review_count=4,
        invalid_count=2,
        hard_fail_count=1,
        typo_corrected_count=5,
    )
    assert rec.mx_rate == pytest.approx(0.8)
    assert rec.a_fallback_rate == pytest.approx(0.1)
    assert rec.dns_failure_rate == pytest.approx(0.05)
    assert rec.timeout_rate == pytest.approx(0.15)
    assert rec.ready_rate == pytest.approx(0.7)
    assert rec.review_rate == pytest.approx(0.2)
    assert rec.invalid_rate == pytest.approx(0.1)
    assert rec.hard_fail_rate == pytest.approx(0.05)
    assert rec.typo_corrected_rate == pytest.approx(0.25)


# ─────────────────────────────────────────────────────────────────────── #
# apply_observation incremental behaviour                                 #
# ─────────────────────────────────────────────────────────────────────── #


def test_apply_observation_increments_expected_counters() -> None:
    rec = DomainHistoryRecord(domain="x.com", first_seen_at=_now(), last_seen_at=_now())
    rec.apply_observation(
        DomainObservation(
            domain="x.com",
            had_mx=True,
            was_typo_corrected=True,
            final_decision=FinalDecision.READY,
        )
    )
    assert rec.total_seen_count == 1
    assert rec.mx_present_count == 1
    assert rec.ready_count == 1
    assert rec.typo_corrected_count == 1
    assert rec.review_count == 0
    assert rec.invalid_count == 0


def test_hard_fail_decision_implies_hard_fail_count_even_without_flag() -> None:
    rec = DomainHistoryRecord(domain="x.com", first_seen_at=_now(), last_seen_at=_now())
    rec.apply_observation(
        DomainObservation(
            domain="x.com",
            had_hard_fail=False,
            final_decision=FinalDecision.HARD_FAIL,
        )
    )
    assert rec.hard_fail_count == 1
    assert rec.total_seen_count == 1


def test_apply_observation_advances_last_seen_at() -> None:
    start = datetime(2026, 1, 1)
    later = datetime(2026, 4, 22)
    rec = DomainHistoryRecord(domain="x.com", first_seen_at=start, last_seen_at=start)
    rec.apply_observation(
        DomainObservation(domain="x.com", final_decision=FinalDecision.REVIEW),
        now=later,
    )
    assert rec.last_seen_at == later
    assert rec.first_seen_at == start


def test_apply_observation_does_not_regress_last_seen_at() -> None:
    start = datetime(2026, 4, 22)
    earlier = datetime(2026, 1, 1)
    rec = DomainHistoryRecord(domain="x.com", first_seen_at=start, last_seen_at=start)
    rec.apply_observation(
        DomainObservation(domain="x.com", final_decision=FinalDecision.READY),
        now=earlier,
    )
    assert rec.last_seen_at == start
