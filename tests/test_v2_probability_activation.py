"""Subphase V2.4 — Deliverability Probability Model Activation.

Three layers of tests pin the V2.4 contract:

  * Pure-unit tests for :func:`probability_to_final_action` and
    :func:`clamp_probability` — deterministic probability mapping is
    the single source of truth.
  * Policy-level tests for :func:`apply_v2_decision_policy` — the full
    priority chain (terminals → probability → safety caps).
  * Stage-level integration tests — DecisionStage delegates to the
    centralized policy and persists the four required output fields.
  * End-to-end materialization tests — the policy actually drives row
    placement on disk.

V2.1 + V2.2 + V2.3 invariants (duplicate, V1 hard-fail, SMTP-invalid,
catch-all caps) are pinned again at the V2.4 level so a future refactor
of the policy module cannot quietly weaken them.

No live network is used — the autouse ``_block_live_smtp`` fixture in
``conftest.py`` covers all of these tests.
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
from app.engine.stages.catch_all_detection import (
    CATCH_ALL_STATUS_CONFIRMED,
    CATCH_ALL_STATUS_NOT,
    CATCH_ALL_STATUS_NOT_TESTED,
    CATCH_ALL_STATUS_POSSIBLE,
)
from app.engine.stages.smtp_verification import (
    SMTP_STATUS_BLOCKED,
    SMTP_STATUS_CATCH_ALL_POSSIBLE,
    SMTP_STATUS_ERROR,
    SMTP_STATUS_INVALID,
    SMTP_STATUS_NOT_TESTED,
    SMTP_STATUS_TEMP_FAIL,
    SMTP_STATUS_TIMEOUT,
    SMTP_STATUS_VALID,
)
from app.models import RunContext
from app.pipeline import EmailCleaningPipeline
from app.storage import StagingDB
from app.v2_decision_policy import (
    REASON_DUPLICATE,
    REASON_HARD_FAIL,
    REASON_HIGH_PROBABILITY,
    REASON_LOW_PROBABILITY,
    REASON_MEDIUM_PROBABILITY,
    REASON_SMTP_INVALID,
    REASON_SMTP_UNCONFIRMED_FOR_CANDIDATE,
    apply_v2_decision_policy,
    clamp_probability,
    probability_to_final_action,
)
from app.validation_v2.decision.policy import (
    DEFAULT_DECISION_POLICY,
    DecisionPolicy,
    FinalAction,
)


# ---------------------------------------------------------------------------
# Layer 1 — clamp_probability
# ---------------------------------------------------------------------------


class TestClampProbability:
    @pytest.mark.parametrize(
        "value,expected",
        [
            (0.0, 0.0),
            (0.5, 0.5),
            (1.0, 1.0),
            (-0.1, 0.0),
            (1.1, 1.0),
            (None, 0.0),
            ("not a number", 0.0),
            ("0.85", 0.85),
            (float("nan"), 0.0),
            (float("inf"), 0.0),
            (-float("inf"), 0.0),
        ],
    )
    def test_clamp_handles_various_inputs(self, value, expected):
        result = clamp_probability(value)
        assert result == pytest.approx(expected)
        assert 0.0 <= result <= 1.0


# ---------------------------------------------------------------------------
# Layer 2 — probability_to_final_action (pure mapping)
# ---------------------------------------------------------------------------


class TestProbabilityToFinalAction:
    """Pin the deterministic probability → action mapping."""

    def test_threshold_value_itself_is_high(self):
        action, reason = probability_to_final_action(
            0.80, approve_threshold=0.80, review_threshold=0.50
        )
        assert action == FinalAction.AUTO_APPROVE
        assert reason == REASON_HIGH_PROBABILITY

    def test_above_approve_threshold_is_high(self):
        action, reason = probability_to_final_action(
            0.95, approve_threshold=0.80, review_threshold=0.50
        )
        assert action == FinalAction.AUTO_APPROVE
        assert reason == REASON_HIGH_PROBABILITY

    def test_review_threshold_value_itself_is_medium(self):
        action, reason = probability_to_final_action(
            0.50, approve_threshold=0.80, review_threshold=0.50
        )
        assert action == FinalAction.MANUAL_REVIEW
        assert reason == REASON_MEDIUM_PROBABILITY

    def test_between_thresholds_is_medium(self):
        action, reason = probability_to_final_action(
            0.65, approve_threshold=0.80, review_threshold=0.50
        )
        assert action == FinalAction.MANUAL_REVIEW
        assert reason == REASON_MEDIUM_PROBABILITY

    def test_below_review_threshold_is_low(self):
        action, reason = probability_to_final_action(
            0.30, approve_threshold=0.80, review_threshold=0.50
        )
        assert action == FinalAction.AUTO_REJECT
        assert reason == REASON_LOW_PROBABILITY

    def test_zero_is_low(self):
        action, _ = probability_to_final_action(0.0)
        assert action == FinalAction.AUTO_REJECT

    def test_one_is_high(self):
        action, _ = probability_to_final_action(1.0)
        assert action == FinalAction.AUTO_APPROVE

    def test_input_is_clamped_before_mapping(self):
        action, _ = probability_to_final_action(2.5)
        assert action == FinalAction.AUTO_APPROVE
        action2, _ = probability_to_final_action(-1.0)
        assert action2 == FinalAction.AUTO_REJECT

    def test_custom_thresholds_change_routing(self):
        """Config thresholds drive the mapping, not hard-coded constants."""
        action, reason = probability_to_final_action(
            0.85, approve_threshold=0.90, review_threshold=0.70
        )
        assert action == FinalAction.MANUAL_REVIEW
        assert reason == REASON_MEDIUM_PROBABILITY

        action2, reason2 = probability_to_final_action(
            0.65, approve_threshold=0.90, review_threshold=0.70
        )
        assert action2 == FinalAction.AUTO_REJECT
        assert reason2 == REASON_LOW_PROBABILITY


# ---------------------------------------------------------------------------
# Layer 3 — apply_v2_decision_policy
# ---------------------------------------------------------------------------


def _policy_inputs(
    *,
    probability: float = 0.85,
    smtp_status: str = SMTP_STATUS_VALID,
    smtp_was_candidate: bool = True,
    catch_all_status: str = CATCH_ALL_STATUS_NOT,
    catch_all_flag: bool = False,
    hard_fail: bool = False,
    v2_final_bucket: str = "ready",
    policy: DecisionPolicy = DEFAULT_DECISION_POLICY,
) -> dict:
    return dict(
        probability=probability,
        smtp_status=smtp_status,
        smtp_was_candidate=smtp_was_candidate,
        catch_all_status=catch_all_status,
        catch_all_flag=catch_all_flag,
        hard_fail=hard_fail,
        v2_final_bucket=v2_final_bucket,
        policy=policy,
    )


class TestApplyV2DecisionPolicy_Terminals:
    def test_v1_hard_fail_returns_auto_reject(self):
        result = apply_v2_decision_policy(
            **_policy_inputs(hard_fail=True, probability=0.99)
        )
        assert result.final_action == FinalAction.AUTO_REJECT
        assert result.decision_reason == REASON_HARD_FAIL

    def test_v2_hard_fail_bucket_returns_auto_reject(self):
        result = apply_v2_decision_policy(
            **_policy_inputs(v2_final_bucket="hard_fail", probability=0.99)
        )
        assert result.final_action == FinalAction.AUTO_REJECT
        assert result.decision_reason == REASON_HARD_FAIL

    def test_duplicate_returns_auto_reject(self):
        result = apply_v2_decision_policy(
            **_policy_inputs(v2_final_bucket="duplicate", probability=0.99)
        )
        assert result.final_action == FinalAction.AUTO_REJECT
        assert result.decision_reason == REASON_DUPLICATE

    def test_smtp_invalid_returns_auto_reject_at_high_probability(self):
        result = apply_v2_decision_policy(
            **_policy_inputs(smtp_status=SMTP_STATUS_INVALID, probability=0.99)
        )
        assert result.final_action == FinalAction.AUTO_REJECT
        assert result.decision_reason == REASON_SMTP_INVALID


class TestApplyV2DecisionPolicy_ProbabilityMapping:
    def test_high_probability_with_clean_signals_approves(self):
        result = apply_v2_decision_policy(**_policy_inputs(probability=0.85))
        assert result.final_action == FinalAction.AUTO_APPROVE
        assert result.decision_reason == REASON_HIGH_PROBABILITY

    def test_medium_probability_routes_to_review(self):
        result = apply_v2_decision_policy(**_policy_inputs(probability=0.60))
        assert result.final_action == FinalAction.MANUAL_REVIEW
        assert result.decision_reason == REASON_MEDIUM_PROBABILITY

    def test_low_probability_routes_to_reject(self):
        result = apply_v2_decision_policy(**_policy_inputs(probability=0.30))
        assert result.final_action == FinalAction.AUTO_REJECT
        assert result.decision_reason == REASON_LOW_PROBABILITY


class TestApplyV2DecisionPolicy_SafetyCaps:
    @pytest.mark.parametrize(
        "smtp_status",
        [
            SMTP_STATUS_BLOCKED,
            SMTP_STATUS_TIMEOUT,
            SMTP_STATUS_TEMP_FAIL,
            SMTP_STATUS_ERROR,
            SMTP_STATUS_CATCH_ALL_POSSIBLE,
        ],
    )
    def test_inconclusive_smtp_caps_high_probability_to_review(self, smtp_status):
        result = apply_v2_decision_policy(
            **_policy_inputs(probability=0.95, smtp_status=smtp_status)
        )
        assert result.final_action == FinalAction.MANUAL_REVIEW
        assert result.decision_reason == f"smtp_{smtp_status}"

    def test_confirmed_catch_all_caps_high_probability_to_review(self):
        result = apply_v2_decision_policy(
            **_policy_inputs(
                probability=0.95,
                catch_all_status=CATCH_ALL_STATUS_CONFIRMED,
                catch_all_flag=True,
            )
        )
        assert result.final_action == FinalAction.MANUAL_REVIEW
        assert result.decision_reason == "catch_all_confirmed"

    def test_possible_catch_all_caps_high_probability_to_review(self):
        result = apply_v2_decision_policy(
            **_policy_inputs(
                probability=0.95,
                catch_all_status=CATCH_ALL_STATUS_POSSIBLE,
                catch_all_flag=True,
            )
        )
        assert result.final_action == FinalAction.MANUAL_REVIEW
        assert result.decision_reason == "catch_all_possible"

    def test_candidate_with_not_tested_smtp_caps_to_review(self):
        result = apply_v2_decision_policy(
            **_policy_inputs(
                probability=0.95,
                smtp_status=SMTP_STATUS_NOT_TESTED,
                smtp_was_candidate=True,
            )
        )
        assert result.final_action == FinalAction.MANUAL_REVIEW
        assert result.decision_reason == REASON_SMTP_UNCONFIRMED_FOR_CANDIDATE

    def test_caps_only_apply_to_auto_approve(self):
        """Medium-probability rows retain ``medium_probability`` — the
        caps only fire when the would-be action is ``auto_approve``."""
        result = apply_v2_decision_policy(
            **_policy_inputs(probability=0.60, smtp_status=SMTP_STATUS_BLOCKED)
        )
        assert result.final_action == FinalAction.MANUAL_REVIEW
        assert result.decision_reason == REASON_MEDIUM_PROBABILITY

    def test_low_probability_stays_rejected_under_caps(self):
        result = apply_v2_decision_policy(
            **_policy_inputs(probability=0.10, smtp_status=SMTP_STATUS_BLOCKED)
        )
        assert result.final_action == FinalAction.AUTO_REJECT
        assert result.decision_reason == REASON_LOW_PROBABILITY


class TestApplyV2DecisionPolicy_PriorityChain:
    def test_hard_fail_beats_smtp_valid(self):
        result = apply_v2_decision_policy(
            **_policy_inputs(
                hard_fail=True,
                probability=0.99,
                smtp_status=SMTP_STATUS_VALID,
            )
        )
        assert result.final_action == FinalAction.AUTO_REJECT
        assert result.decision_reason == REASON_HARD_FAIL

    def test_duplicate_beats_smtp_valid(self):
        result = apply_v2_decision_policy(
            **_policy_inputs(
                v2_final_bucket="duplicate",
                probability=0.99,
                smtp_status=SMTP_STATUS_VALID,
            )
        )
        assert result.final_action == FinalAction.AUTO_REJECT
        assert result.decision_reason == REASON_DUPLICATE

    def test_smtp_invalid_beats_catch_all(self):
        result = apply_v2_decision_policy(
            **_policy_inputs(
                probability=0.95,
                smtp_status=SMTP_STATUS_INVALID,
                catch_all_status=CATCH_ALL_STATUS_CONFIRMED,
                catch_all_flag=True,
            )
        )
        assert result.final_action == FinalAction.AUTO_REJECT
        assert result.decision_reason == REASON_SMTP_INVALID


class TestApplyV2DecisionPolicy_ConfigDriven:
    def test_custom_thresholds_change_routing(self):
        strict = DecisionPolicy(
            approve_threshold=0.90,
            review_threshold=0.70,
            enable_bucket_override=True,
        )
        # 0.85 → medium under strict thresholds.
        r1 = apply_v2_decision_policy(
            **_policy_inputs(probability=0.85, policy=strict)
        )
        assert r1.final_action == FinalAction.MANUAL_REVIEW
        # 0.65 → low under strict thresholds.
        r2 = apply_v2_decision_policy(
            **_policy_inputs(probability=0.65, policy=strict)
        )
        assert r2.final_action == FinalAction.AUTO_REJECT


class TestApplyV2DecisionPolicy_ProbabilityIsPersisted:
    def test_high_probability_decision_confidence_equals_probability(self):
        result = apply_v2_decision_policy(**_policy_inputs(probability=0.92))
        assert result.decision_confidence == pytest.approx(0.92)

    def test_decision_confidence_is_clamped_to_unit_interval(self):
        result = apply_v2_decision_policy(**_policy_inputs(probability=2.5))
        assert 0.0 <= result.decision_confidence <= 1.0

    def test_terminal_paths_zero_confidence(self):
        r1 = apply_v2_decision_policy(
            **_policy_inputs(hard_fail=True, probability=0.99)
        )
        assert r1.decision_confidence == 0.0
        r2 = apply_v2_decision_policy(
            **_policy_inputs(v2_final_bucket="duplicate", probability=0.99)
        )
        assert r2.decision_confidence == 0.0

    def test_smtp_invalid_preserves_probability_for_audit(self):
        result = apply_v2_decision_policy(
            **_policy_inputs(probability=0.85, smtp_status=SMTP_STATUS_INVALID)
        )
        assert result.decision_confidence == pytest.approx(0.85)


# ---------------------------------------------------------------------------
# Layer 4 — DecisionStage delegates to the centralized policy
# ---------------------------------------------------------------------------


def _post_v2_frame(
    *,
    smtp_status: str = SMTP_STATUS_VALID,
    catch_all_status: str = CATCH_ALL_STATUS_NOT,
    catch_all_flag: bool = False,
    hard_fail: bool = False,
    bucket_v2: str = "high_confidence",
    smtp_was_candidate: bool = True,
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "email": ["alice@gmail.com"],
            "domain": ["gmail.com"],
            "corrected_domain": ["gmail.com"],
            "syntax_valid": pd.array([True], dtype="boolean"),
            "domain_matches_input_column": pd.array([True], dtype="boolean"),
            "typo_detected": pd.array([False], dtype="boolean"),
            "typo_corrected": pd.array([False], dtype="boolean"),
            "has_mx_record": pd.array([True], dtype="boolean"),
            "has_a_record": pd.array([False], dtype="boolean"),
            "domain_exists": pd.array([True], dtype="boolean"),
            "dns_error": [None],
            "hard_fail": pd.array([hard_fail], dtype="boolean"),
            "score": [75],
            "preliminary_bucket": ["high_confidence"],
            "bucket_v2": [bucket_v2],
            "hard_stop_v2": pd.array([hard_fail], dtype="bool"),
            "smtp_status": [smtp_status],
            "smtp_was_candidate": [smtp_was_candidate],
            "smtp_confidence": [
                0.9 if smtp_status == SMTP_STATUS_VALID else 0.2
            ],
            "catch_all_status": [catch_all_status],
            "catch_all_flag": [catch_all_flag],
        }
    )


class TestDecisionStageDelegates:
    def test_required_decision_fields_persist_for_every_row(self):
        out = DecisionStage().run(
            ChunkPayload(frame=_post_v2_frame()),
            PipelineContext(),
        ).frame
        for col in (
            "deliverability_probability",
            "decision_confidence",
            "final_action",
            "decision_reason",
        ):
            assert col in out.columns
            assert out[col].notna().all()

    def test_persisted_probability_is_in_unit_interval(self):
        out = DecisionStage().run(
            ChunkPayload(frame=_post_v2_frame()),
            PipelineContext(),
        ).frame
        for v in out["deliverability_probability"]:
            assert 0.0 <= float(v) <= 1.0

    def test_final_action_vocabulary_is_canonical(self):
        out = DecisionStage().run(
            ChunkPayload(
                frame=pd.concat(
                    [
                        _post_v2_frame(),
                        _post_v2_frame(smtp_status=SMTP_STATUS_INVALID),
                        _post_v2_frame(
                            catch_all_status=CATCH_ALL_STATUS_CONFIRMED,
                            catch_all_flag=True,
                        ),
                        _post_v2_frame(hard_fail=True, bucket_v2="invalid"),
                    ],
                    ignore_index=True,
                )
            ),
            PipelineContext(),
        ).frame
        for action in out["final_action"]:
            assert action in FinalAction.ALL


# ---------------------------------------------------------------------------
# Layer 5 — End-to-end materialization
# ---------------------------------------------------------------------------


def _build_run_context(tmp_path: Path) -> RunContext:
    run_dir = tmp_path / "run"
    logs_dir = tmp_path / "logs"
    temp_dir = tmp_path / "tmp"
    for p in (run_dir, logs_dir, temp_dir):
        p.mkdir(parents=True, exist_ok=True)
    return RunContext(
        run_id="run_v24_test",
        run_dir=run_dir,
        logs_dir=logs_dir,
        temp_dir=temp_dir,
        staging_db_path=run_dir / "staging.sqlite3",
        started_at=datetime.now(),
    )


def _staging_columns() -> list[str]:
    return [
        "email", "email_normalized", "source_file", "source_row_number",
        "chunk_index", "global_ordinal", "hard_fail", "score",
        "preliminary_bucket", "completeness_score", "is_canonical",
        "duplicate_flag", "duplicate_reason", "domain", "corrected_domain",
        "domain_from_email", "has_mx_record", "has_a_record", "domain_exists",
        "typo_corrected",
        "smtp_status", "smtp_was_candidate", "smtp_confirmed_valid",
        "smtp_response_code",
        "catch_all_status", "catch_all_flag", "catch_all_confidence",
        "catch_all_method",
        "deliverability_probability", "decision_confidence",
        "final_action", "decision_reason",
        "v2_final_bucket",
    ]


def _staging_frame(rows: list[dict]) -> pd.DataFrame:
    cols = _staging_columns()
    out: dict[str, list] = {c: [] for c in cols}
    for r in rows:
        for c in cols:
            out[c].append(r.get(c))
    return pd.DataFrame(out)


def _materialize_with(tmp_path: Path, rows: list[dict]) -> dict[str, list[dict]]:
    cfg = load_config(base_dir=resolve_project_paths().project_root)
    logger = logging.getLogger("v24_test")
    logger.addHandler(logging.NullHandler())
    pipeline = EmailCleaningPipeline(config=cfg, logger=logger)

    rc = _build_run_context(tmp_path)
    staging = StagingDB(rc.staging_db_path)
    dedupe = DedupeIndex()

    for row in rows:
        if row.get("email_normalized") and bool(row.get("is_canonical", False)):
            dedupe.process_row(
                email_normalized=row["email_normalized"],
                hard_fail=bool(row.get("hard_fail", False)),
                score=int(row.get("score") or 0),
                completeness_score=int(row.get("completeness_score") or 0),
                source_file=str(row.get("source_file") or ""),
                source_row_number=int(row.get("source_row_number") or 0),
            )

    staging.append_chunk(_staging_frame(rows))
    pipeline._materialize(staging, dedupe, rc)
    staging.close()

    out: dict[str, list[dict]] = {}
    for name in (
        "clean_high_confidence.csv",
        "review_medium_confidence.csv",
        "removed_invalid.csv",
    ):
        path = rc.run_dir / name
        if not path.is_file() or path.stat().st_size == 0:
            out[name] = []
            continue
        with path.open(encoding="utf-8", newline="") as fh:
            out[name] = list(csv.DictReader(fh))
    return out


def _row(
    email: str,
    *,
    final_action: str,
    decision_reason: str,
    deliverability_probability: float = 0.85,
    is_canonical: bool = True,
    hard_fail: bool = False,
    smtp_status: str = SMTP_STATUS_VALID,
    catch_all_status: str = CATCH_ALL_STATUS_NOT,
    catch_all_flag: bool = False,
    bucket_v2: str = "ready",
    source_row_number: int = 2,
    global_ordinal: int = 0,
) -> dict:
    norm = email.lower()
    return {
        "email": email,
        "email_normalized": norm,
        "source_file": "f.csv",
        "source_row_number": source_row_number,
        "chunk_index": 0,
        "global_ordinal": global_ordinal,
        "hard_fail": hard_fail,
        "score": 75,
        "preliminary_bucket": "high_confidence",
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
        "smtp_status": smtp_status,
        "smtp_was_candidate": True,
        "smtp_confirmed_valid": smtp_status == SMTP_STATUS_VALID,
        "smtp_response_code": 250 if smtp_status == SMTP_STATUS_VALID else 550,
        "catch_all_status": catch_all_status,
        "catch_all_flag": catch_all_flag,
        "catch_all_confidence": 0.85 if catch_all_status != CATCH_ALL_STATUS_NOT_TESTED else 0.0,
        "catch_all_method": "smtp_valid_no_random_accept",
        "deliverability_probability": deliverability_probability,
        "decision_confidence": deliverability_probability,
        "final_action": final_action,
        "decision_reason": decision_reason,
        "v2_final_bucket": bucket_v2,
    }


class TestV24EndToEndMaterialization:
    def test_high_probability_clean_signals_routes_to_clean(self, tmp_path: Path):
        out = _materialize_with(
            tmp_path,
            [_row(
                "alice@gmail.com",
                final_action="auto_approve",
                decision_reason=REASON_HIGH_PROBABILITY,
                deliverability_probability=0.85,
            )],
        )
        clean = [r["email"] for r in out["clean_high_confidence.csv"]]
        assert clean == ["alice@gmail.com"]

    def test_medium_probability_routes_to_review(self, tmp_path: Path):
        out = _materialize_with(
            tmp_path,
            [_row(
                "bob@gmail.com",
                final_action="manual_review",
                decision_reason=REASON_MEDIUM_PROBABILITY,
                deliverability_probability=0.60,
                bucket_v2="review",
            )],
        )
        assert out["clean_high_confidence.csv"] == []
        assert [r["email"] for r in out["review_medium_confidence.csv"]] == ["bob@gmail.com"]

    def test_low_probability_routes_to_invalid(self, tmp_path: Path):
        out = _materialize_with(
            tmp_path,
            [_row(
                "carol@gmail.com",
                final_action="auto_reject",
                decision_reason=REASON_LOW_PROBABILITY,
                deliverability_probability=0.20,
                bucket_v2="invalid",
            )],
        )
        assert out["clean_high_confidence.csv"] == []
        invalid_rows = out["removed_invalid.csv"]
        assert [r["email"] for r in invalid_rows] == ["carol@gmail.com"]
        assert invalid_rows[0]["final_output_reason"] == "removed_v2_low_probability"

    def test_smtp_invalid_overrides_high_probability(self, tmp_path: Path):
        out = _materialize_with(
            tmp_path,
            [_row(
                "dave@gmail.com",
                final_action="auto_reject",
                decision_reason=REASON_SMTP_INVALID,
                deliverability_probability=0.95,
                smtp_status=SMTP_STATUS_INVALID,
                bucket_v2="invalid",
            )],
        )
        assert out["clean_high_confidence.csv"] == []
        invalid_rows = out["removed_invalid.csv"]
        assert invalid_rows[0]["final_output_reason"] == "removed_v2_smtp_invalid"

    def test_duplicate_routes_to_removed_duplicate(self, tmp_path: Path):
        out = _materialize_with(
            tmp_path,
            [
                _row(
                    "eve@gmail.com",
                    final_action="auto_approve",
                    decision_reason=REASON_HIGH_PROBABILITY,
                    is_canonical=True,
                    source_row_number=2,
                    global_ordinal=0,
                ),
                _row(
                    "eve@gmail.com",
                    final_action="auto_approve",
                    decision_reason=REASON_HIGH_PROBABILITY,
                    is_canonical=False,
                    source_row_number=3,
                    global_ordinal=1,
                ),
            ],
        )
        clean = [r["email"] for r in out["clean_high_confidence.csv"]]
        assert clean == ["eve@gmail.com"]
        assert any(
            r["final_output_reason"] == "removed_duplicate"
            for r in out["removed_invalid.csv"]
        )

    def test_v1_hard_fail_routes_to_removed_hard_fail(self, tmp_path: Path):
        out = _materialize_with(
            tmp_path,
            [_row(
                "frank@bad.invalid",
                final_action="auto_reject",
                decision_reason=REASON_HARD_FAIL,
                hard_fail=True,
                deliverability_probability=0.0,
                bucket_v2="invalid",
            )],
        )
        assert out["clean_high_confidence.csv"] == []
        assert any(
            r["final_output_reason"] == "removed_hard_fail"
            for r in out["removed_invalid.csv"]
        )


# ---------------------------------------------------------------------------
# Layer 6 — V1 authority audit
# ---------------------------------------------------------------------------


class TestV1AuthorityRemoved:
    def test_pipeline_materialize_does_not_route_on_preliminary_bucket(self):
        """The legacy V1-authoritative branch must be gone. ``preliminary_bucket``
        may still appear for chunk-level log metric counters; that's fine."""
        from app import pipeline as pipeline_mod

        text = Path(pipeline_mod.__file__).read_text(encoding="utf-8")
        assert 'preliminary_bucket == "high_confidence"' not in text
        # And the V2.1+ helper is the routing path.
        assert "map_v2_decision_to_output_bucket" in text

    def test_decision_stage_uses_centralized_policy(self):
        """``DecisionStage`` must delegate to ``apply_v2_decision_policy`` —
        no inline threshold logic in the stage itself."""
        from app.engine.stages import decision as decision_mod

        text = Path(decision_mod.__file__).read_text(encoding="utf-8")
        assert "apply_v2_decision_policy" in text
