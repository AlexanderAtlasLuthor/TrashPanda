"""V2.10.12 — pilot candidate selector tests."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from app.pilot_send.selector import select_candidates


def _write_xlsx(path: Path, rows: list[dict]) -> None:
    df = pd.DataFrame(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="rows", index=False)


def _row(email: str, **overrides) -> dict:
    base = {
        "email": email,
        "source_row_number": "1",
        "provider_family": "corporate_unknown",
        "deliverability_probability": "0.7",
    }
    base.update(overrides)
    return base


# --------------------------------------------------------------------- #
# Empty / missing files
# --------------------------------------------------------------------- #


class TestEmptyInputs:
    def test_no_files_returns_empty(self, tmp_path: Path):
        assert select_candidates(tmp_path, batch_size=10) == []

    def test_zero_batch_size_returns_empty(self, tmp_path: Path):
        _write_xlsx(
            tmp_path / "review_ready_probable.xlsx",
            [_row("a@x.com")],
        )
        assert select_candidates(tmp_path, batch_size=0) == []


# --------------------------------------------------------------------- #
# Priority + dedupe
# --------------------------------------------------------------------- #


class TestPriorityOrder:
    def test_ready_probable_before_low_risk(self, tmp_path: Path):
        _write_xlsx(
            tmp_path / "review_ready_probable.xlsx",
            [_row("a@x.com")],
        )
        _write_xlsx(
            tmp_path / "review_low_risk.xlsx",
            [_row("b@x.com")],
        )
        candidates = select_candidates(tmp_path, batch_size=10)
        assert [c.email for c in candidates] == ["a@x.com", "b@x.com"]
        assert candidates[0].action == "ready_probable"
        assert candidates[1].action == "low_risk"

    def test_full_priority_chain(self, tmp_path: Path):
        _write_xlsx(
            tmp_path / "review_ready_probable.xlsx",
            [_row("a@x.com")],
        )
        _write_xlsx(
            tmp_path / "review_low_risk.xlsx",
            [_row("b@x.com")],
        )
        _write_xlsx(
            tmp_path / "review_timeout_retry.xlsx",
            [_row("c@x.com")],
        )
        _write_xlsx(
            tmp_path / "review_catch_all_consumer.xlsx",
            [_row("d@x.com")],
        )
        candidates = select_candidates(tmp_path, batch_size=10)
        actions = [c.action for c in candidates]
        assert actions == [
            "ready_probable",
            "low_risk",
            "timeout_retry",
            "catch_all_consumer",
        ]


class TestDedupe:
    def test_dedupes_email_across_files(self, tmp_path: Path):
        _write_xlsx(
            tmp_path / "review_ready_probable.xlsx",
            [_row("dup@x.com")],
        )
        _write_xlsx(
            tmp_path / "review_low_risk.xlsx",
            [_row("dup@x.com")],
        )
        candidates = select_candidates(tmp_path, batch_size=10)
        assert len(candidates) == 1
        # Higher-priority (ready_probable) wins.
        assert candidates[0].action == "ready_probable"

    def test_dedupes_within_a_file(self, tmp_path: Path):
        _write_xlsx(
            tmp_path / "review_low_risk.xlsx",
            [_row("dup@x.com"), _row("dup@x.com")],
        )
        candidates = select_candidates(tmp_path, batch_size=10)
        assert len(candidates) == 1

    def test_normalizes_email_lowercase(self, tmp_path: Path):
        _write_xlsx(
            tmp_path / "review_low_risk.xlsx",
            [_row("MixedCase@X.com")],
        )
        candidates = select_candidates(tmp_path, batch_size=10)
        assert candidates[0].email == "mixedcase@x.com"


# --------------------------------------------------------------------- #
# Batch size cap
# --------------------------------------------------------------------- #


class TestBatchCap:
    def test_caps_at_batch_size(self, tmp_path: Path):
        _write_xlsx(
            tmp_path / "review_ready_probable.xlsx",
            [_row(f"a{i}@x.com") for i in range(20)],
        )
        candidates = select_candidates(tmp_path, batch_size=5)
        assert len(candidates) == 5


# --------------------------------------------------------------------- #
# Forbidden sources
# --------------------------------------------------------------------- #


class TestForbiddenSources:
    def test_never_picks_from_high_risk(self, tmp_path: Path):
        _write_xlsx(
            tmp_path / "review_high_risk.xlsx",
            [_row("a@x.com")],
        )
        _write_xlsx(
            tmp_path / "do_not_send.xlsx",
            [_row("b@x.com")],
        )
        candidates = select_candidates(tmp_path, batch_size=10)
        assert candidates == []


# --------------------------------------------------------------------- #
# Malformed rows
# --------------------------------------------------------------------- #


class TestMalformedRows:
    def test_skips_rows_without_email(self, tmp_path: Path):
        _write_xlsx(
            tmp_path / "review_low_risk.xlsx",
            [
                {"email": "", "source_row_number": "1"},
                _row("good@x.com"),
                {"email": "no-at-sign", "source_row_number": "2"},
            ],
        )
        candidates = select_candidates(tmp_path, batch_size=10)
        assert [c.email for c in candidates] == ["good@x.com"]
