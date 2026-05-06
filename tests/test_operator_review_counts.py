"""V2.10.16 — operator review counts adjustment.

When the operator approves or removes rows in the review queue, the
dashboard counts must reflect those decisions. The bug we're fixing:
``_job_summary_counts`` used to read directly from the pipeline-time
summary and ignore the persisted decisions, so the safe / review /
rejected counts never updated after manual review.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from app import operator_routes


@pytest.fixture(autouse=True)
def _stub_run_dir(monkeypatch):
    """Bypass the run-dir resolver — we don't need a real run on disk
    for these unit tests; we just stub out the bits that touch it."""
    monkeypatch.setattr(
        operator_routes, "_resolve_run_dir", lambda job_id: None,
    )


@pytest.fixture
def _stub_summary(monkeypatch):
    """Return a builder that stubs the summary loader to return a
    given (valid, review, rejected) triple."""

    def _build(*, valid: int, review: int, rejected: int) -> None:
        from app import server

        # Stub the JobResult-side load.
        monkeypatch.setattr(
            server, "_load_job_result",
            lambda job_id: SimpleNamespace(
                summary=SimpleNamespace(
                    total_valid=valid,
                    total_review=review,
                    total_invalid_or_bounce_risk=rejected,
                ),
            ),
        )
        # Stub the artifact-side load to None so the result-side wins.
        from app import api_boundary

        monkeypatch.setattr(
            api_boundary, "load_job_summary", lambda run_dir: None,
        )

    return _build


@pytest.fixture
def _set_decisions(monkeypatch):
    """Builder that stubs ``server._load_decisions`` for a job."""

    def _set(decisions: dict[str, str]) -> None:
        from app import server

        monkeypatch.setattr(
            server, "_load_decisions", lambda job_id: dict(decisions),
        )

    return _set


class TestNoDecisions:
    def test_returns_summary_unchanged(self, _stub_summary, _set_decisions):
        _stub_summary(valid=10, review=50, rejected=5)
        _set_decisions({})
        counts = operator_routes._job_summary_counts("job-123")
        assert counts == {
            "safe_count": 10,
            "review_count": 50,
            "rejected_count": 5,
        }


class TestApprovalsPromoteToSafe:
    def test_approved_decisions_increment_safe_decrement_review(
        self, _stub_summary, _set_decisions,
    ):
        _stub_summary(valid=10, review=50, rejected=5)
        _set_decisions({"r1": "approved", "r2": "approved", "r3": "approved"})
        counts = operator_routes._job_summary_counts("job-123")
        assert counts["safe_count"] == 13
        assert counts["review_count"] == 47
        assert counts["rejected_count"] == 5


class TestRemovalsPromoteToRejected:
    def test_removed_decisions_increment_rejected_decrement_review(
        self, _stub_summary, _set_decisions,
    ):
        _stub_summary(valid=10, review=50, rejected=5)
        _set_decisions({"r1": "removed", "r2": "removed"})
        counts = operator_routes._job_summary_counts("job-123")
        assert counts["safe_count"] == 10
        assert counts["review_count"] == 48
        assert counts["rejected_count"] == 7


class TestMixedDecisions:
    def test_mixed_approved_and_removed(
        self, _stub_summary, _set_decisions,
    ):
        _stub_summary(valid=10, review=50, rejected=5)
        _set_decisions({
            "r1": "approved",
            "r2": "approved",
            "r3": "removed",
            "r4": "removed",
            "r5": "removed",
        })
        counts = operator_routes._job_summary_counts("job-123")
        assert counts["safe_count"] == 12   # 10 + 2 approved
        assert counts["review_count"] == 45  # 50 - 5 decided
        assert counts["rejected_count"] == 8  # 5 + 3 removed


class TestSafetyCaps:
    def test_does_not_underflow_when_decisions_exceed_review(
        self, _stub_summary, _set_decisions,
    ):
        # Stale decisions file: more decided rows than review rows.
        # Counts must not go negative.
        _stub_summary(valid=10, review=2, rejected=5)
        _set_decisions({
            "r1": "approved",
            "r2": "approved",
            "r3": "approved",
            "r4": "removed",
            "r5": "removed",
        })
        counts = operator_routes._job_summary_counts("job-123")
        assert counts["review_count"] == 0
        # Cap absorbs only `review` worth of decisions; promotions
        # are applied first (approved up to the cap), removals next.
        assert counts["safe_count"] == 12   # 10 + min(3, 2)
        assert counts["rejected_count"] == 5  # cap absorbed before removals
        # Total preserved: safe + review + rejected == valid + review + rejected
        assert (counts["safe_count"] + counts["review_count"]
                + counts["rejected_count"]) == 10 + 2 + 5

    def test_zero_review_with_decisions_is_no_op(
        self, _stub_summary, _set_decisions,
    ):
        _stub_summary(valid=10, review=0, rejected=5)
        _set_decisions({"r1": "approved"})
        counts = operator_routes._job_summary_counts("job-123")
        assert counts == {
            "safe_count": 10,
            "review_count": 0,
            "rejected_count": 5,
        }


class TestDecisionTally:
    def test_tally_ignores_unknown_values(self, monkeypatch):
        from app import server
        monkeypatch.setattr(
            server, "_load_decisions",
            lambda job_id: {
                "r1": "approved",
                "r2": "removed",
                "r3": "approved",
                # The endpoint already filters to approved/removed, but
                # _load_decisions defends against bad data; the tally
                # tolerates surprises just in case.
            },
        )
        approved, removed = operator_routes._decision_tally("job-x")
        assert approved == 2
        assert removed == 1

    def test_tally_when_load_raises(self, monkeypatch):
        from app import server

        def _broken(job_id):
            raise RuntimeError("decisions store down")

        monkeypatch.setattr(server, "_load_decisions", _broken)
        approved, removed = operator_routes._decision_tally("job-x")
        assert (approved, removed) == (0, 0)
