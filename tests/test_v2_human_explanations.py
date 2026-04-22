"""Tests for app.validation_v2.explanation_v2.explain_row_with_history.

Goal: assert that every (final_bucket, historical_label) combination
produces deterministic, non-empty, human-readable text with a coherent
risk grade.
"""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytest

from app.validation_v2.explanation_v2 import (
    explain_domain_history,
    explain_row_with_history,
)
from app.validation_v2.history_models import (
    DomainHistoryRecord,
    HistoricalLabel,
)


BUCKETS = ("ready", "review", "invalid", "hard_fail", "duplicate")

LABELS = (
    HistoricalLabel.RELIABLE,
    HistoricalLabel.UNSTABLE,
    HistoricalLabel.RISKY,
    HistoricalLabel.NEUTRAL,
    HistoricalLabel.INSUFFICIENT_DATA,
)

ALLOWED_RISKS = {
    "Low", "Low-Medium", "Medium", "Medium-High", "High", "Critical", "N/A", "Unknown",
}


def _decision(bucket: str, label: str) -> SimpleNamespace:
    return SimpleNamespace(final_bucket=bucket, historical_label=label)


def _record() -> DomainHistoryRecord:
    return DomainHistoryRecord(
        domain="x.com", first_seen_at=datetime(2026, 4, 22),
        last_seen_at=datetime(2026, 4, 22), total_seen_count=100,
        mx_present_count=90, ready_count=80,
    )


# ──────────────────────────────────────────────────────────────────────── #
# Exhaustive coverage of (bucket × label)                                  #
# ──────────────────────────────────────────────────────────────────────── #


@pytest.mark.parametrize("bucket", BUCKETS)
@pytest.mark.parametrize("label", LABELS)
def test_every_combination_returns_three_non_empty_fields(
    bucket: str, label: str,
) -> None:
    result = explain_row_with_history(_decision(bucket, label), _record())
    assert set(result.keys()) == {"human_reason", "human_risk", "human_recommendation"}
    for key, value in result.items():
        assert value, f"{bucket}/{label} produced empty {key}"
        assert isinstance(value, str)
    assert result["human_risk"] in ALLOWED_RISKS


# ──────────────────────────────────────────────────────────────────────── #
# Specific narrative assertions                                            #
# ──────────────────────────────────────────────────────────────────────── #


def test_hard_fail_is_critical_regardless_of_history() -> None:
    for label in LABELS:
        result = explain_row_with_history(_decision("hard_fail", label), _record())
        assert result["human_risk"] == "Critical"
        assert "Do not use" in result["human_recommendation"]


def test_duplicate_recommendation_points_to_canonical_record() -> None:
    result = explain_row_with_history(
        _decision("duplicate", HistoricalLabel.RELIABLE), _record()
    )
    assert "canonical" in result["human_recommendation"].lower()


def test_ready_plus_reliable_is_low_risk_and_safe() -> None:
    result = explain_row_with_history(
        _decision("ready", HistoricalLabel.RELIABLE), _record()
    )
    assert result["human_risk"] == "Low"
    assert "safe" in result["human_recommendation"].lower()


def test_ready_plus_risky_flags_caution() -> None:
    result = explain_row_with_history(
        _decision("ready", HistoricalLabel.RISKY), _record()
    )
    assert result["human_risk"] == "Medium"
    assert "invalid" in result["human_reason"].lower()


def test_review_plus_unstable_mentions_timeouts() -> None:
    result = explain_row_with_history(
        _decision("review", HistoricalLabel.UNSTABLE), _record()
    )
    assert "timeout" in result["human_reason"].lower() or "dns" in result["human_reason"].lower()
    assert result["human_risk"] in ("Medium", "Medium-High")


def test_review_plus_reliable_downgrades_risk() -> None:
    result = explain_row_with_history(
        _decision("review", HistoricalLabel.RELIABLE), _record()
    )
    assert result["human_risk"] == "Low-Medium"
    assert "safe" in result["human_recommendation"].lower()


def test_invalid_plus_risky_is_high_risk_do_not_use() -> None:
    result = explain_row_with_history(
        _decision("invalid", HistoricalLabel.RISKY), _record()
    )
    assert result["human_risk"] == "High"
    assert "do not use" in result["human_recommendation"].lower()


def test_insufficient_data_text_on_ready_is_neutral_low() -> None:
    result = explain_row_with_history(
        _decision("ready", HistoricalLabel.INSUFFICIENT_DATA), _record()
    )
    assert result["human_risk"] == "Low"
    # "No meaningful historical signal" explicitly describes lack of data.
    assert "no meaningful historical signal" in result["human_reason"].lower() or \
           "historical signal" in result["human_reason"].lower()


def test_unknown_bucket_falls_back_to_unknown_risk() -> None:
    result = explain_row_with_history(_decision("something_weird", "whatever"), None)
    assert result["human_risk"] == "Unknown"


# ──────────────────────────────────────────────────────────────────────── #
# Phase-1 explanation still works                                          #
# ──────────────────────────────────────────────────────────────────────── #


def test_explain_domain_history_unchanged_for_phase_1() -> None:
    assert explain_domain_history(None).startswith("No prior history")
    rec = _record()
    text = explain_domain_history(rec)
    assert "100" in text  # observation count appears verbatim
