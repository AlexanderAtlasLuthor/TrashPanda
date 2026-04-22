"""Tests for app.validation_v2.domain_memory — classification + adjustment."""

from __future__ import annotations

from datetime import datetime

import pytest

from app.validation_v2.domain_memory import (
    DEFAULT_THRESHOLDS,
    classify_domain,
    compute_adjustment,
)
from app.validation_v2.history_models import DomainHistoryRecord, HistoricalLabel


# ─────────────────────────────────────────────────────────────────────── #
# Builders                                                                #
# ─────────────────────────────────────────────────────────────────────── #


def _rec(**kwargs: int) -> DomainHistoryRecord:
    """Build a DomainHistoryRecord with counts; timestamps are filler."""
    when = datetime(2026, 4, 22)
    defaults = {
        "total_seen_count": 100,
        "mx_present_count": 0,
        "a_fallback_count": 0,
        "dns_failure_count": 0,
        "timeout_count": 0,
        "typo_corrected_count": 0,
        "review_count": 0,
        "invalid_count": 0,
        "ready_count": 0,
        "hard_fail_count": 0,
    }
    defaults.update(kwargs)
    return DomainHistoryRecord(
        domain="example.com", first_seen_at=when, last_seen_at=when, **defaults
    )


# ─────────────────────────────────────────────────────────────────────── #
# classify_domain                                                         #
# ─────────────────────────────────────────────────────────────────────── #


def test_none_record_is_insufficient_data() -> None:
    assert classify_domain(None) == HistoricalLabel.INSUFFICIENT_DATA


def test_too_few_observations_is_insufficient_data() -> None:
    rec = _rec(total_seen_count=DEFAULT_THRESHOLDS.min_observations - 1, ready_count=0)
    assert classify_domain(rec) == HistoricalLabel.INSUFFICIENT_DATA


def test_high_timeout_rate_labels_unstable() -> None:
    rec = _rec(total_seen_count=100, timeout_count=40)
    assert classify_domain(rec) == HistoricalLabel.UNSTABLE


def test_high_dns_failure_rate_labels_unstable() -> None:
    rec = _rec(total_seen_count=100, dns_failure_count=35)
    assert classify_domain(rec) == HistoricalLabel.UNSTABLE


def test_high_invalid_rate_labels_risky() -> None:
    rec = _rec(total_seen_count=100, invalid_count=60, mx_present_count=100)
    assert classify_domain(rec) == HistoricalLabel.RISKY


def test_high_hard_fail_rate_labels_risky() -> None:
    rec = _rec(total_seen_count=100, hard_fail_count=40, mx_present_count=100)
    assert classify_domain(rec) == HistoricalLabel.RISKY


def test_strong_positive_signals_label_reliable() -> None:
    rec = _rec(
        total_seen_count=100,
        mx_present_count=95,
        ready_count=85,
        review_count=10,
        invalid_count=5,
    )
    assert classify_domain(rec) == HistoricalLabel.RELIABLE


def test_mixed_signals_fall_through_to_neutral() -> None:
    rec = _rec(
        total_seen_count=100,
        mx_present_count=70,       # below reliable mx threshold
        ready_count=50,            # below reliable ready threshold
        review_count=30,
        invalid_count=20,          # below risky invalid threshold
    )
    assert classify_domain(rec) == HistoricalLabel.NEUTRAL


def test_unstable_beats_risky_when_both_trigger() -> None:
    rec = _rec(
        total_seen_count=100,
        timeout_count=40,  # unstable
        invalid_count=70,  # also risky
    )
    assert classify_domain(rec) == HistoricalLabel.UNSTABLE


# ─────────────────────────────────────────────────────────────────────── #
# compute_adjustment                                                      #
# ─────────────────────────────────────────────────────────────────────── #


def test_adjustment_zero_when_insufficient_data() -> None:
    assert compute_adjustment(None, max_positive=3, max_negative=5) == 0
    rec = _rec(total_seen_count=1, ready_count=1)
    assert compute_adjustment(rec, max_positive=3, max_negative=5) == 0


def test_adjustment_bounded_positive_for_reliable_domain() -> None:
    rec = _rec(
        total_seen_count=100,
        mx_present_count=95,
        ready_count=80,
    )
    result = compute_adjustment(rec, max_positive=3, max_negative=5)
    assert 0 < result <= 3


def test_adjustment_bounded_negative_for_risky_domain() -> None:
    rec = _rec(total_seen_count=100, invalid_count=80, mx_present_count=100)
    result = compute_adjustment(rec, max_positive=3, max_negative=5)
    assert -5 <= result < 0


def test_adjustment_negative_for_unstable_domain() -> None:
    rec = _rec(total_seen_count=100, timeout_count=90)
    result = compute_adjustment(rec, max_positive=3, max_negative=5)
    assert result < 0
    assert result >= -5


def test_adjustment_respects_caller_maxima() -> None:
    rec = _rec(total_seen_count=100, invalid_count=90, mx_present_count=100)
    assert compute_adjustment(rec, max_positive=10, max_negative=2) >= -2


@pytest.mark.parametrize(
    "max_pos,max_neg",
    [(0, 0), (1, 1), (3, 5), (10, 10)],
)
def test_adjustment_never_exceeds_configured_bounds(max_pos: int, max_neg: int) -> None:
    scenarios = [
        _rec(total_seen_count=100, mx_present_count=100, ready_count=95),  # reliable
        _rec(total_seen_count=100, timeout_count=70),                       # unstable
        _rec(total_seen_count=100, invalid_count=95, mx_present_count=100),  # risky
        _rec(total_seen_count=50, mx_present_count=40, ready_count=30),     # neutral
    ]
    for rec in scenarios:
        value = compute_adjustment(rec, max_positive=max_pos, max_negative=max_neg)
        assert -max_neg <= value <= max_pos
