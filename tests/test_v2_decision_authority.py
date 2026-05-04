"""Subphase V2.1 — V2 Decision Authority.

Covers the behaviour change introduced in V2.1: V2 ``final_action``
controls ``clean / review / invalid`` placement in materialization.

Three layers of tests:

  * Pure unit tests for :func:`map_v2_decision_to_output_bucket`
    — the routing rule is the single source of truth, so it must be
    pinned independently of the surrounding pipeline.
  * Stage-level tests for :class:`DecisionStage` — verifies the new
    chunk-pipeline stage produces the columns ``_materialize`` reads
    and never mutates V1 columns.
  * End-to-end materialization tests — fabricated staged rows feed
    :meth:`EmailCleaningPipeline._materialize`; the final CSVs are
    inspected to confirm the routing actually changed.

The six prompt acceptance scenarios (V2 accept / review / reject /
missing decision / duplicate / V1 hard fail) are pinned in
:class:`TestV21AcceptanceScenarios`.
"""

from __future__ import annotations

import csv
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from app.config import load_config, resolve_project_paths
from app.dedupe import DedupeIndex
from app.engine import ChunkPayload, PipelineContext
from app.engine.stages import DecisionStage
from app.engine.stages.decision import DECISION_STAGE_OUTPUT_COLUMNS
from app.models import RunContext
from app.pipeline import EmailCleaningPipeline
from app.storage import StagingDB
from app.v2_classification import (
    OUTPUT_CLEAN,
    OUTPUT_DUPLICATE,
    OUTPUT_HARD_FAIL,
    OUTPUT_INVALID,
    OUTPUT_REVIEW,
    map_v2_decision_to_output_bucket,
    output_reason_from_bucket,
)


# ---------------------------------------------------------------------------
# Layer 1 — pure unit tests on the mapping helper
# ---------------------------------------------------------------------------


class TestMapV2DecisionToOutputBucket:
    """Pin the V2.1 routing rule independently of the pipeline."""

    def test_v2_auto_approve_routes_to_clean(self):
        assert map_v2_decision_to_output_bucket(
            is_canonical=True,
            v1_hard_fail=False,
            final_action="auto_approve",
        ) == OUTPUT_CLEAN

    def test_v2_manual_review_routes_to_review(self):
        assert map_v2_decision_to_output_bucket(
            is_canonical=True,
            v1_hard_fail=False,
            final_action="manual_review",
        ) == OUTPUT_REVIEW

    def test_v2_auto_reject_routes_to_invalid(self):
        assert map_v2_decision_to_output_bucket(
            is_canonical=True,
            v1_hard_fail=False,
            final_action="auto_reject",
        ) == OUTPUT_INVALID

    def test_missing_final_action_falls_back_to_review(self):
        """Conservative fallback: missing V2 decision must NOT go clean."""
        assert map_v2_decision_to_output_bucket(
            is_canonical=True,
            v1_hard_fail=False,
            final_action=None,
        ) == OUTPUT_REVIEW

    def test_empty_string_final_action_falls_back_to_review(self):
        assert map_v2_decision_to_output_bucket(
            is_canonical=True,
            v1_hard_fail=False,
            final_action="",
        ) == OUTPUT_REVIEW

    def test_unknown_final_action_falls_back_to_review(self):
        assert map_v2_decision_to_output_bucket(
            is_canonical=True,
            v1_hard_fail=False,
            final_action="totally_unknown_action",
        ) == OUTPUT_REVIEW

    def test_non_canonical_routes_to_duplicate_regardless_of_v2(self):
        for action in ("auto_approve", "manual_review", "auto_reject", None, ""):
            assert map_v2_decision_to_output_bucket(
                is_canonical=False,
                v1_hard_fail=False,
                final_action=action,
            ) == OUTPUT_DUPLICATE

    def test_v1_hard_fail_overrides_v2_approve(self):
        """V2 cannot rescue a V1 structural hard fail in this subphase."""
        assert map_v2_decision_to_output_bucket(
            is_canonical=True,
            v1_hard_fail=True,
            final_action="auto_approve",
        ) == OUTPUT_HARD_FAIL

    def test_duplicate_check_runs_before_hard_fail_check(self):
        """A non-canonical hard-failed row is still routed as a duplicate.

        Dedupe is the outermost filter — duplicates never get a chance
        to claim hard-fail / clean / review status.
        """
        assert map_v2_decision_to_output_bucket(
            is_canonical=False,
            v1_hard_fail=True,
            final_action="auto_approve",
        ) == OUTPUT_DUPLICATE


class TestOutputReasonFromBucket:
    """The legacy ``final_output_reason`` vocabulary must keep working."""

    def test_clean_maps_to_kept_high_confidence(self):
        assert output_reason_from_bucket(OUTPUT_CLEAN) == "kept_high_confidence"

    def test_review_maps_to_kept_review(self):
        assert output_reason_from_bucket(OUTPUT_REVIEW) == "kept_review"

    def test_duplicate_maps_to_removed_duplicate(self):
        assert output_reason_from_bucket(OUTPUT_DUPLICATE) == "removed_duplicate"

    def test_hard_fail_maps_to_removed_hard_fail(self):
        assert output_reason_from_bucket(OUTPUT_HARD_FAIL) == "removed_hard_fail"

    def test_invalid_with_low_probability_carries_v2_reason(self):
        assert (
            output_reason_from_bucket(OUTPUT_INVALID, "low_probability")
            == "removed_v2_low_probability"
        )

    def test_invalid_without_decision_reason_falls_back(self):
        assert output_reason_from_bucket(OUTPUT_INVALID, None) == "removed_low_score"


# ---------------------------------------------------------------------------
# Layer 2 — DecisionStage (chunk-pipeline) behaviour
# ---------------------------------------------------------------------------


def _post_v2_scoring_frame() -> pd.DataFrame:
    """A minimal frame with every column DecisionStage requires."""
    return pd.DataFrame(
        {
            "email": [
                "alice@gmail.com",      # MX → high deliverability
                "bob@example.net",      # A-only → medium / low
                "carol@bad.invalid",    # NXDOMAIN → V1 hard fail
            ],
            "domain": ["gmail.com", "example.net", "bad.invalid"],
            "corrected_domain": ["gmail.com", "example.net", "bad.invalid"],
            "syntax_valid": pd.array([True, True, True], dtype="boolean"),
            "domain_matches_input_column": pd.array(
                [True, True, True], dtype="boolean"
            ),
            "typo_detected": pd.array([False, False, False], dtype="boolean"),
            "typo_corrected": pd.array([False, False, False], dtype="boolean"),
            "has_mx_record": pd.array([True, False, False], dtype="boolean"),
            "has_a_record": pd.array([False, True, False], dtype="boolean"),
            "domain_exists": pd.array([True, True, False], dtype="boolean"),
            "dns_error": [None, None, "nxdomain"],
            # V1 scoring
            "hard_fail": pd.array([False, False, True], dtype="boolean"),
            "score": [75, 45, 0],
            "preliminary_bucket": ["high_confidence", "review", "invalid"],
            # V2 scoring
            "bucket_v2": ["high_confidence", "review", "invalid"],
            "hard_stop_v2": pd.array([False, False, True], dtype="bool"),
        }
    )


class TestDecisionStageColumns:
    def test_adds_all_expected_columns(self):
        out = DecisionStage().run(
            ChunkPayload(frame=_post_v2_scoring_frame()),
            PipelineContext(),
        ).frame
        for col in DECISION_STAGE_OUTPUT_COLUMNS:
            assert col in out.columns, f"missing column: {col}"

    def test_does_not_modify_v1_columns(self):
        df = _post_v2_scoring_frame()
        before = df[
            ["hard_fail", "score", "preliminary_bucket"]
        ].copy()
        out = DecisionStage().run(
            ChunkPayload(frame=df.copy()),
            PipelineContext(),
        ).frame
        after = out[["hard_fail", "score", "preliminary_bucket"]]
        pd.testing.assert_frame_equal(
            before.reset_index(drop=True),
            after.reset_index(drop=True),
        )

    def test_does_not_modify_v2_scoring_columns(self):
        df = _post_v2_scoring_frame()
        before = df[["bucket_v2", "hard_stop_v2"]].copy()
        out = DecisionStage().run(
            ChunkPayload(frame=df.copy()),
            PipelineContext(),
        ).frame
        after = out[["bucket_v2", "hard_stop_v2"]]
        pd.testing.assert_frame_equal(
            before.reset_index(drop=True),
            after.reset_index(drop=True),
        )

    def test_input_frame_is_not_mutated(self):
        df = _post_v2_scoring_frame()
        snapshot_cols = list(df.columns)
        DecisionStage().run(ChunkPayload(frame=df), PipelineContext())
        assert list(df.columns) == snapshot_cols


class TestDecisionStageValues:
    def test_emits_canonical_final_action_vocabulary(self):
        out = DecisionStage().run(
            ChunkPayload(frame=_post_v2_scoring_frame()),
            PipelineContext(),
        ).frame
        for action in out["final_action"]:
            assert action in {"auto_approve", "manual_review", "auto_reject"}

    def test_hard_fail_row_yields_auto_reject_with_hard_fail_reason(self):
        out = DecisionStage().run(
            ChunkPayload(frame=_post_v2_scoring_frame()),
            PipelineContext(),
        ).frame
        # Row 2 is the NXDOMAIN hard-fail.
        row = out.iloc[2]
        assert row["final_action"] == "auto_reject"
        assert row["decision_reason"] == "hard_fail"
        assert row["v2_final_bucket"] == "hard_fail"

    def test_high_mx_row_gets_higher_probability_than_a_only_row(self):
        out = DecisionStage().run(
            ChunkPayload(frame=_post_v2_scoring_frame()),
            PipelineContext(),
        ).frame
        prob_mx = float(out.iloc[0]["deliverability_probability"])
        prob_a = float(out.iloc[1]["deliverability_probability"])
        assert prob_mx > prob_a

    def test_decision_confidence_is_clamped_to_unit_interval(self):
        out = DecisionStage().run(
            ChunkPayload(frame=_post_v2_scoring_frame()),
            PipelineContext(),
        ).frame
        for v in out["decision_confidence"]:
            assert 0.0 <= float(v) <= 1.0


# ---------------------------------------------------------------------------
# Layer 3 — end-to-end materialization with V2 authority
# ---------------------------------------------------------------------------


def _staging_columns() -> list[str]:
    """The minimum columns _materialize needs to read from staging."""
    return [
        "email",
        "email_normalized",
        "source_file",
        "source_row_number",
        "chunk_index",
        "global_ordinal",
        "hard_fail",
        "score",
        "preliminary_bucket",
        "completeness_score",
        "is_canonical",
        "duplicate_flag",
        "duplicate_reason",
        "domain",
        "corrected_domain",
        "domain_from_email",
        "has_mx_record",
        "has_a_record",
        "domain_exists",
        "typo_corrected",
        "final_action",
        "decision_reason",
        "decision_confidence",
        "v2_final_bucket",
    ]


def _staging_frame(rows: list[dict]) -> pd.DataFrame:
    """Build a frame matching the staging insert contract."""
    cols = _staging_columns()
    out: dict[str, list] = {c: [] for c in cols}
    for row in rows:
        for c in cols:
            out[c].append(row.get(c))
    return pd.DataFrame(out)


def _build_run_dir(tmp_path: Path) -> RunContext:
    run_dir = tmp_path / "run"
    logs_dir = tmp_path / "logs"
    temp_dir = tmp_path / "tmp"
    for p in (run_dir, logs_dir, temp_dir):
        p.mkdir(parents=True, exist_ok=True)
    return RunContext(
        run_id="run_v21_test",
        run_dir=run_dir,
        logs_dir=logs_dir,
        temp_dir=temp_dir,
        staging_db_path=run_dir / "staging.sqlite3",
        started_at=datetime.now(),
    )


def _materialize_with(
    tmp_path: Path,
    rows: list[dict],
) -> dict[str, list[dict[str, str]]]:
    """Stage ``rows``, run :meth:`_materialize`, return the parsed CSVs.

    Returns a dict keyed by output filename → list of row dicts read
    back. Empty CSVs return an empty list.
    """
    cfg = load_config(base_dir=resolve_project_paths().project_root)
    logger = logging.getLogger("v21_test")
    logger.addHandler(logging.NullHandler())
    pipeline = EmailCleaningPipeline(config=cfg, logger=logger)

    run_context = _build_run_dir(tmp_path)
    staging = StagingDB(run_context.staging_db_path)
    dedupe = DedupeIndex()

    # Mirror what DedupeIndex would compute over the input rows so
    # ``is_final_canonical`` returns the value baked into each row.
    for row in rows:
        email_norm = row.get("email_normalized")
        if not email_norm or not bool(row.get("is_canonical", False)):
            continue
        dedupe.process_row(
            email_normalized=email_norm,
            hard_fail=bool(row.get("hard_fail", False)),
            score=int(row.get("score") or 0),
            completeness_score=int(row.get("completeness_score") or 0),
            source_file=str(row.get("source_file") or ""),
            source_row_number=int(row.get("source_row_number") or 0),
        )

    staging.append_chunk(_staging_frame(rows))
    pipeline._materialize(staging, dedupe, run_context)
    staging.close()

    out: dict[str, list[dict[str, str]]] = {}
    for name in (
        "clean_high_confidence.csv",
        "review_medium_confidence.csv",
        "removed_invalid.csv",
    ):
        path = run_context.run_dir / name
        if not path.is_file() or path.stat().st_size == 0:
            out[name] = []
            continue
        with path.open(encoding="utf-8", newline="") as fh:
            out[name] = list(csv.DictReader(fh))
    return out


def _row(
    *,
    email: str,
    final_action: str | None,
    is_canonical: bool = True,
    hard_fail: bool = False,
    preliminary_bucket: str = "high_confidence",
    score: int = 75,
    decision_reason: str = "high_probability",
    decision_confidence: float = 0.85,
    source_row_number: int = 2,
    global_ordinal: int = 0,
) -> dict:
    """Build one staged row with sensible V2 defaults."""
    norm = email.lower()
    return {
        "email": email,
        "email_normalized": norm,
        "source_file": "f.csv",
        "source_row_number": source_row_number,
        "chunk_index": 0,
        "global_ordinal": global_ordinal,
        "hard_fail": hard_fail,
        "score": score,
        "preliminary_bucket": preliminary_bucket,
        "completeness_score": 3,
        "is_canonical": is_canonical,
        "duplicate_flag": not is_canonical,
        "duplicate_reason": None if is_canonical else "duplicate_lower_score",
        "domain": email.split("@", 1)[1] if "@" in email else "",
        "corrected_domain": email.split("@", 1)[1] if "@" in email else "",
        "domain_from_email": email.split("@", 1)[1] if "@" in email else "",
        "has_mx_record": True,
        "has_a_record": False,
        "domain_exists": True,
        "typo_corrected": False,
        "final_action": final_action,
        "decision_reason": decision_reason,
        "decision_confidence": decision_confidence,
        "v2_final_bucket": "ready" if final_action == "auto_approve" else "review",
    }


class TestV21AcceptanceScenarios:
    """The six required scenarios from the V2.1 prompt."""

    def test_1_v2_accept_routes_canonical_row_to_clean(self, tmp_path: Path):
        out = _materialize_with(
            tmp_path,
            [_row(email="alice@gmail.com", final_action="auto_approve")],
        )
        clean_emails = [r["email"] for r in out["clean_high_confidence.csv"]]
        review_emails = [r["email"] for r in out["review_medium_confidence.csv"]]
        invalid_emails = [r["email"] for r in out["removed_invalid.csv"]]
        assert clean_emails == ["alice@gmail.com"]
        assert review_emails == []
        assert invalid_emails == []

    def test_2_v2_review_pulls_high_confidence_row_out_of_clean(
        self, tmp_path: Path
    ):
        """V1 says high_confidence; V2 says manual_review → row goes
        to review, NOT clean. This is the headline V2.1 behaviour."""
        out = _materialize_with(
            tmp_path,
            [
                _row(
                    email="bob@gmail.com",
                    final_action="manual_review",
                    preliminary_bucket="high_confidence",
                    decision_reason="medium_probability",
                    decision_confidence=0.60,
                )
            ],
        )
        clean = [r["email"] for r in out["clean_high_confidence.csv"]]
        review = [r["email"] for r in out["review_medium_confidence.csv"]]
        assert clean == []
        assert review == ["bob@gmail.com"]

    def test_3_v2_reject_pulls_high_confidence_row_to_invalid(
        self, tmp_path: Path
    ):
        out = _materialize_with(
            tmp_path,
            [
                _row(
                    email="carol@gmail.com",
                    final_action="auto_reject",
                    preliminary_bucket="high_confidence",
                    decision_reason="low_probability",
                    decision_confidence=0.10,
                )
            ],
        )
        clean = [r["email"] for r in out["clean_high_confidence.csv"]]
        invalid_rows = out["removed_invalid.csv"]
        assert clean == []
        assert [r["email"] for r in invalid_rows] == ["carol@gmail.com"]
        assert invalid_rows[0]["final_output_reason"] == "removed_v2_low_probability"

    def test_4_missing_v2_decision_does_not_route_to_clean(
        self, tmp_path: Path
    ):
        """The conservative fallback. A row with V1=high_confidence
        but no V2 ``final_action`` must land in review, never clean."""
        out = _materialize_with(
            tmp_path,
            [
                _row(
                    email="dave@gmail.com",
                    final_action=None,
                    preliminary_bucket="high_confidence",
                    decision_reason="",
                    decision_confidence=0.0,
                )
            ],
        )
        clean = [r["email"] for r in out["clean_high_confidence.csv"]]
        review = [r["email"] for r in out["review_medium_confidence.csv"]]
        assert clean == []
        assert review == ["dave@gmail.com"]

    def test_5_duplicate_routes_to_removed_invalid_regardless_of_v2(
        self, tmp_path: Path
    ):
        """Non-canonical rows always land in the removed-invalid CSV
        with reason=removed_duplicate, no matter what V2 said.

        ``_materialize`` writes duplicates and hard fails into the same
        ``removed_invalid.csv`` file — the per-bucket distinction is
        carried by ``final_output_reason``.
        """
        out = _materialize_with(
            tmp_path,
            [
                # Canonical winner.
                _row(
                    email="eve@gmail.com",
                    final_action="auto_approve",
                    is_canonical=True,
                    score=80,
                    source_row_number=2,
                    global_ordinal=0,
                ),
                # Non-canonical loser of the same email.
                _row(
                    email="eve@gmail.com",
                    final_action="auto_approve",
                    is_canonical=False,
                    score=70,
                    source_row_number=3,
                    global_ordinal=1,
                ),
            ],
        )
        clean = [r["email"] for r in out["clean_high_confidence.csv"]]
        removed = out["removed_invalid.csv"]
        # Exactly one canonical landed in clean; the duplicate did not.
        assert clean == ["eve@gmail.com"]
        # The duplicate is in removed_invalid.csv with the duplicate reason.
        assert any(
            r["final_output_reason"] == "removed_duplicate" for r in removed
        )

    def test_6_v1_hard_fail_routes_to_removed_invalid_even_if_v2_approves(
        self, tmp_path: Path
    ):
        """V2 cannot rescue a V1 structural hard fail in this subphase."""
        out = _materialize_with(
            tmp_path,
            [
                _row(
                    email="frank@bad.invalid",
                    final_action="auto_approve",
                    hard_fail=True,
                    preliminary_bucket="invalid",
                    score=0,
                )
            ],
        )
        clean = [r["email"] for r in out["clean_high_confidence.csv"]]
        removed = out["removed_invalid.csv"]
        assert clean == []
        assert any(
            r["final_output_reason"] == "removed_hard_fail" for r in removed
        )


class TestV21NoRegressionForV1Observability:
    """V2.1 must not erase V1 columns; observability is preserved."""

    def test_v1_columns_present_in_materialized_csv(self, tmp_path: Path):
        out = _materialize_with(
            tmp_path,
            [_row(email="alice@gmail.com", final_action="auto_approve")],
        )
        clean_rows = out["clean_high_confidence.csv"]
        assert len(clean_rows) == 1
        for col in (
            "score",
            "preliminary_bucket",
            "hard_fail",
            "final_action",
            "decision_reason",
            "decision_confidence",
        ):
            assert col in clean_rows[0], f"missing column in materialized CSV: {col}"


# ---------------------------------------------------------------------------
# Sanity: config defaults wired correctly
# ---------------------------------------------------------------------------


class TestConfigDefaultsBucketOverrideOn(object):
    def test_default_yaml_enables_bucket_override(self):
        cfg = load_config(base_dir=resolve_project_paths().project_root)
        assert getattr(cfg, "decision", None) is not None
        assert cfg.decision.enable_bucket_override is True


# ---------------------------------------------------------------------------
# Optional: skip-if-missing pytest sentinel for tooling that grep's for it.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "action,expected",
    [
        ("auto_approve", OUTPUT_CLEAN),
        ("manual_review", OUTPUT_REVIEW),
        ("auto_reject", OUTPUT_INVALID),
        (None, OUTPUT_REVIEW),
        ("", OUTPUT_REVIEW),
    ],
)
def test_routing_table_parametrized(action, expected):
    """Compact regression table — easy to extend if vocabulary grows."""
    assert map_v2_decision_to_output_bucket(
        is_canonical=True,
        v1_hard_fail=False,
        final_action=action,
    ) == expected
