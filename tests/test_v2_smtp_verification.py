"""Subphase V2.2 — SMTP Verification Expansion.

Pins the V2.2 contract end-to-end:

  * ``SMTPVerificationStage`` produces the canonical SMTP columns and
    only probes structurally-eligible candidates.
  * ``DecisionStage`` consumes the canonical ``smtp_status`` and applies
    the V2.2 overrides: ``valid`` lets a row reach ``auto_approve``,
    ``invalid`` forces ``auto_reject``, the inconclusive set
    (``blocked / timeout / temp_fail / error / catch_all_possible``)
    caps at ``manual_review``, and a candidate without a ``valid``
    SMTP signal cannot auto-approve.
  * V2.1 invariants (duplicate precedence, V1 hard-fail precedence)
    are unchanged.
  * No live network is opened — the autouse ``_block_live_smtp``
    fixture in ``conftest.py`` guarantees this even with the new
    production-leaning defaults.
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
from app.engine.stages import (
    DecisionStage,
    SMTPVerificationStage,
    SMTP_VERIFICATION_OUTPUT_COLUMNS,
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
    SMTPCache,
    is_smtp_candidate,
    normalize_smtp_status,
    smtp_status_to_model_smtp_result,
)
from app.models import RunContext
from app.pipeline import EmailCleaningPipeline
from app.storage import StagingDB
from app.validation_v2.smtp_probe import SMTPResult


# ---------------------------------------------------------------------------
# Probe builders + frame fixtures
# ---------------------------------------------------------------------------


def _result_valid() -> SMTPResult:
    return SMTPResult(
        success=True,
        response_code=250,
        response_message="2.1.5 Recipient OK",
        is_catch_all_like=False,
        inconclusive=False,
    )


def _result_invalid() -> SMTPResult:
    return SMTPResult(
        success=False,
        response_code=550,
        response_message="5.1.1 User unknown",
        is_catch_all_like=False,
        inconclusive=False,
    )


def _result_blocked() -> SMTPResult:
    return SMTPResult(
        success=False,
        response_code=550,
        response_message="5.7.1 Sender blocked by policy (Spamhaus)",
        is_catch_all_like=False,
        inconclusive=False,
    )


def _result_temp_fail() -> SMTPResult:
    return SMTPResult(
        success=False,
        response_code=421,
        response_message="4.7.0 Try again later (greylisting)",
        is_catch_all_like=False,
        inconclusive=True,
    )


def _result_timeout() -> SMTPResult:
    return SMTPResult(
        success=False,
        response_code=None,
        response_message="connection timed out",
        is_catch_all_like=False,
        inconclusive=True,
    )


def _result_error() -> SMTPResult:
    return SMTPResult(
        success=False,
        response_code=None,
        response_message="something unexpected",
        is_catch_all_like=False,
        inconclusive=True,
    )


def _result_catch_all() -> SMTPResult:
    return SMTPResult(
        success=True,
        response_code=250,
        response_message="2.1.5 Recipient OK",
        is_catch_all_like=True,
        inconclusive=False,
    )


def _candidate_frame(
    *,
    email: str = "alice@gmail.com",
    has_mx: bool = True,
    syntax_valid: bool = True,
    hard_fail: bool = False,
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "email": [email],
            "domain": ["gmail.com"],
            "corrected_domain": ["gmail.com"],
            "syntax_valid": pd.array([syntax_valid], dtype="boolean"),
            "domain_matches_input_column": pd.array([True], dtype="boolean"),
            "typo_detected": pd.array([False], dtype="boolean"),
            "typo_corrected": pd.array([False], dtype="boolean"),
            "has_mx_record": pd.array([has_mx], dtype="boolean"),
            "has_a_record": pd.array([False], dtype="boolean"),
            "domain_exists": pd.array([has_mx], dtype="boolean"),
            "dns_error": [None],
            "hard_fail": pd.array([hard_fail], dtype="boolean"),
            "score": [75],
            "preliminary_bucket": ["high_confidence"],
            "bucket_v2": ["high_confidence"],
            "hard_stop_v2": pd.array([False], dtype="bool"),
        }
    )


def _make_probe(result: SMTPResult):
    """Return a probe function that always returns ``result`` and counts calls."""
    calls: list[str] = []

    def _probe(email: str, **_kwargs: object) -> SMTPResult:
        calls.append(email)
        return result

    _probe.calls = calls  # type: ignore[attr-defined]
    return _probe


# ---------------------------------------------------------------------------
# Layer 1 — pure normalization
# ---------------------------------------------------------------------------


class TestNormalizeSMTPStatus:
    def test_success_with_no_catchall_is_valid(self):
        assert normalize_smtp_status(_result_valid()) == SMTP_STATUS_VALID

    def test_catch_all_wins_over_success(self):
        assert (
            normalize_smtp_status(_result_catch_all())
            == SMTP_STATUS_CATCH_ALL_POSSIBLE
        )

    def test_5xx_with_mailbox_message_is_invalid(self):
        assert normalize_smtp_status(_result_invalid()) == SMTP_STATUS_INVALID

    def test_5xx_with_policy_keyword_is_blocked(self):
        assert normalize_smtp_status(_result_blocked()) == SMTP_STATUS_BLOCKED

    def test_4xx_is_temp_fail(self):
        assert normalize_smtp_status(_result_temp_fail()) == SMTP_STATUS_TEMP_FAIL

    def test_no_code_with_timeout_message_is_timeout(self):
        assert normalize_smtp_status(_result_timeout()) == SMTP_STATUS_TIMEOUT

    def test_no_code_with_unknown_message_is_error(self):
        assert normalize_smtp_status(_result_error()) == SMTP_STATUS_ERROR


class TestStatusToModelMapping:
    """Ensure DecisionStage feeds the existing probability model correctly."""

    @pytest.mark.parametrize(
        "status,expected",
        [
            (SMTP_STATUS_VALID, "deliverable"),
            (SMTP_STATUS_INVALID, "undeliverable"),
            (SMTP_STATUS_CATCH_ALL_POSSIBLE, "catch_all"),
            (SMTP_STATUS_BLOCKED, "inconclusive"),
            (SMTP_STATUS_TIMEOUT, "inconclusive"),
            (SMTP_STATUS_TEMP_FAIL, "inconclusive"),
            (SMTP_STATUS_ERROR, "inconclusive"),
            (SMTP_STATUS_NOT_TESTED, "not_tested"),
        ],
    )
    def test_mapping(self, status, expected):
        assert smtp_status_to_model_smtp_result(status) == expected


# ---------------------------------------------------------------------------
# Layer 2 — candidate selection
# ---------------------------------------------------------------------------


class TestIsSMTPCandidate:
    def test_typical_row_is_a_candidate(self):
        row = {
            "email": "alice@gmail.com",
            "corrected_domain": "gmail.com",
            "syntax_valid": True,
            "has_mx_record": True,
            "hard_fail": False,
        }
        assert is_smtp_candidate(row) is True

    def test_v1_hard_fail_excluded(self):
        row = {
            "email": "x@y.com",
            "corrected_domain": "y.com",
            "syntax_valid": True,
            "has_mx_record": True,
            "hard_fail": True,
        }
        assert is_smtp_candidate(row) is False

    def test_no_mx_excluded(self):
        row = {
            "email": "x@y.com",
            "corrected_domain": "y.com",
            "syntax_valid": True,
            "has_mx_record": False,
            "hard_fail": False,
        }
        assert is_smtp_candidate(row) is False

    def test_invalid_syntax_excluded(self):
        row = {
            "email": "no-at-sign",
            "corrected_domain": "y.com",
            "syntax_valid": False,
            "has_mx_record": True,
            "hard_fail": False,
        }
        assert is_smtp_candidate(row) is False

    def test_missing_email_excluded(self):
        row = {
            "email": "",
            "corrected_domain": "y.com",
            "syntax_valid": True,
            "has_mx_record": True,
            "hard_fail": False,
        }
        assert is_smtp_candidate(row) is False

    def test_missing_domain_excluded(self):
        row = {
            "email": "alice@",
            "corrected_domain": "",
            "syntax_valid": True,
            "has_mx_record": True,
            "hard_fail": False,
        }
        assert is_smtp_candidate(row) is False


# ---------------------------------------------------------------------------
# Layer 3 — SMTPVerificationStage behavior
# ---------------------------------------------------------------------------


class TestSMTPVerificationStage:
    def test_adds_canonical_columns(self):
        probe = _make_probe(_result_valid())
        out = SMTPVerificationStage(probe_fn=probe).run(
            ChunkPayload(frame=_candidate_frame()),
            PipelineContext(extras={}),
        ).frame
        for col in SMTP_VERIFICATION_OUTPUT_COLUMNS:
            assert col in out.columns

    def test_valid_probe_writes_smtp_valid_and_confirmed(self):
        probe = _make_probe(_result_valid())
        out = SMTPVerificationStage(probe_fn=probe).run(
            ChunkPayload(frame=_candidate_frame()),
            PipelineContext(extras={}),
        ).frame
        row = out.iloc[0]
        assert row["smtp_status"] == SMTP_STATUS_VALID
        assert bool(row["smtp_confirmed_valid"]) is True
        assert bool(row["smtp_was_candidate"]) is True
        assert bool(row["smtp_tested"]) is True
        assert int(row["smtp_response_code"]) == 250
        assert row["smtp_response_type"] == "success"

    def test_non_candidate_short_circuits_with_not_tested(self):
        probe = _make_probe(_result_valid())
        # Hard-fail row → not a candidate.
        out = SMTPVerificationStage(probe_fn=probe).run(
            ChunkPayload(frame=_candidate_frame(hard_fail=True)),
            PipelineContext(extras={}),
        ).frame
        row = out.iloc[0]
        assert row["smtp_status"] == SMTP_STATUS_NOT_TESTED
        assert bool(row["smtp_was_candidate"]) is False
        assert bool(row["smtp_tested"]) is False
        # The probe MUST NOT have been called for non-candidates.
        assert probe.calls == []

    def test_no_mx_row_is_not_probed(self):
        probe = _make_probe(_result_valid())
        out = SMTPVerificationStage(probe_fn=probe).run(
            ChunkPayload(frame=_candidate_frame(has_mx=False)),
            PipelineContext(extras={}),
        ).frame
        row = out.iloc[0]
        assert row["smtp_status"] == SMTP_STATUS_NOT_TESTED
        assert probe.calls == []

    def test_per_run_cache_dedupes_repeated_emails(self):
        """Two rows with the same email_normalized share one probe call."""
        df = pd.concat(
            [_candidate_frame(), _candidate_frame()],  # 2 identical rows
            ignore_index=True,
        )
        probe = _make_probe(_result_valid())
        cache = SMTPCache()
        ctx = PipelineContext(extras={"smtp_cache": cache})
        out = SMTPVerificationStage(probe_fn=probe).run(
            ChunkPayload(frame=df), ctx
        ).frame
        # Two rows, one probe call.
        assert len(probe.calls) == 1
        assert cache.probes_executed == 1
        assert cache.cache_hits == 1
        # Both rows show valid.
        assert all(out["smtp_status"] == SMTP_STATUS_VALID)


# ---------------------------------------------------------------------------
# Layer 4 — DecisionStage SMTP-aware behavior
# ---------------------------------------------------------------------------


def _post_smtp_frame(
    *,
    smtp_status: str,
    smtp_was_candidate: bool = True,
    hard_fail: bool = False,
    bucket_v2: str = "high_confidence",
    has_mx: bool = True,
) -> pd.DataFrame:
    """A frame as it looks after SMTPVerificationStage has populated columns."""
    return pd.DataFrame(
        {
            "email": ["alice@gmail.com"],
            "domain": ["gmail.com"],
            "corrected_domain": ["gmail.com"],
            "syntax_valid": pd.array([True], dtype="boolean"),
            "domain_matches_input_column": pd.array([True], dtype="boolean"),
            "typo_detected": pd.array([False], dtype="boolean"),
            "typo_corrected": pd.array([False], dtype="boolean"),
            "has_mx_record": pd.array([has_mx], dtype="boolean"),
            "has_a_record": pd.array([False], dtype="boolean"),
            "domain_exists": pd.array([has_mx], dtype="boolean"),
            "dns_error": [None],
            "hard_fail": pd.array([hard_fail], dtype="boolean"),
            "score": [75],
            "preliminary_bucket": ["high_confidence"],
            "bucket_v2": [bucket_v2],
            "hard_stop_v2": pd.array([hard_fail], dtype="bool"),
            "smtp_status": [smtp_status],
            "smtp_was_candidate": [smtp_was_candidate],
            "smtp_confidence": [0.9 if smtp_status == SMTP_STATUS_VALID else 0.2],
        }
    )


class TestDecisionStageWithSMTP:
    def test_smtp_valid_can_auto_approve(self):
        out = DecisionStage().run(
            ChunkPayload(frame=_post_smtp_frame(smtp_status=SMTP_STATUS_VALID)),
            PipelineContext(),
        ).frame
        row = out.iloc[0]
        assert row["final_action"] == "auto_approve"
        # Probability got the SMTP boost.
        assert float(row["deliverability_probability"]) >= 0.80

    def test_smtp_invalid_forces_auto_reject(self):
        out = DecisionStage().run(
            ChunkPayload(frame=_post_smtp_frame(smtp_status=SMTP_STATUS_INVALID)),
            PipelineContext(),
        ).frame
        row = out.iloc[0]
        assert row["final_action"] == "auto_reject"
        assert row["decision_reason"] == "smtp_invalid"

    @pytest.mark.parametrize(
        "status",
        [
            SMTP_STATUS_BLOCKED,
            SMTP_STATUS_TIMEOUT,
            SMTP_STATUS_TEMP_FAIL,
            SMTP_STATUS_ERROR,
            SMTP_STATUS_CATCH_ALL_POSSIBLE,
        ],
    )
    def test_inconclusive_smtp_caps_at_manual_review(self, status):
        out = DecisionStage().run(
            ChunkPayload(frame=_post_smtp_frame(smtp_status=status)),
            PipelineContext(),
        ).frame
        row = out.iloc[0]
        # Never auto-approve under any inconclusive SMTP status.
        assert row["final_action"] != "auto_approve"

    def test_candidate_with_not_tested_cannot_auto_approve(self):
        out = DecisionStage().run(
            ChunkPayload(
                frame=_post_smtp_frame(
                    smtp_status=SMTP_STATUS_NOT_TESTED,
                    smtp_was_candidate=True,
                )
            ),
            PipelineContext(),
        ).frame
        row = out.iloc[0]
        assert row["final_action"] != "auto_approve"

    def test_v1_hard_fail_still_auto_rejects_even_with_smtp_valid(self):
        """V2.1 invariant — hard_fail wins over SMTP."""
        out = DecisionStage().run(
            ChunkPayload(
                frame=_post_smtp_frame(
                    smtp_status=SMTP_STATUS_VALID,
                    hard_fail=True,
                    bucket_v2="invalid",
                )
            ),
            PipelineContext(),
        ).frame
        row = out.iloc[0]
        assert row["final_action"] == "auto_reject"
        assert row["decision_reason"] == "hard_fail"


# ---------------------------------------------------------------------------
# Layer 5 — End-to-end materialization with SMTP
# ---------------------------------------------------------------------------


def _build_run_context(tmp_path: Path) -> RunContext:
    run_dir = tmp_path / "run"
    logs_dir = tmp_path / "logs"
    temp_dir = tmp_path / "tmp"
    for p in (run_dir, logs_dir, temp_dir):
        p.mkdir(parents=True, exist_ok=True)
    return RunContext(
        run_id="run_v22_test",
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
        "typo_corrected", "smtp_status", "smtp_was_candidate",
        "smtp_confirmed_valid", "smtp_response_code",
        "final_action", "decision_reason", "decision_confidence",
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
    logger = logging.getLogger("v22_test")
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
    smtp_status: str,
    final_action: str,
    decision_reason: str = "",
    is_canonical: bool = True,
    hard_fail: bool = False,
    smtp_was_candidate: bool = True,
    smtp_confirmed_valid: bool | None = None,
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
        "smtp_was_candidate": smtp_was_candidate,
        "smtp_confirmed_valid": (
            smtp_confirmed_valid
            if smtp_confirmed_valid is not None
            else (smtp_status == SMTP_STATUS_VALID)
        ),
        "smtp_response_code": 250 if smtp_status == SMTP_STATUS_VALID else 550,
        "final_action": final_action,
        "decision_reason": decision_reason or "high_probability",
        "decision_confidence": 0.85 if final_action == "auto_approve" else 0.40,
        "v2_final_bucket": bucket_v2,
    }


class TestV22EndToEndMaterialization:
    """The 10 prompt scenarios end-to-end via _materialize."""

    def test_1_smtp_valid_routes_to_clean(self, tmp_path: Path):
        out = _materialize_with(
            tmp_path,
            [_row(
                "alice@gmail.com",
                smtp_status=SMTP_STATUS_VALID,
                final_action="auto_approve",
            )],
        )
        clean_emails = [r["email"] for r in out["clean_high_confidence.csv"]]
        assert clean_emails == ["alice@gmail.com"]
        assert out["review_medium_confidence.csv"] == []
        assert out["removed_invalid.csv"] == []

    def test_2_smtp_invalid_routes_to_invalid(self, tmp_path: Path):
        out = _materialize_with(
            tmp_path,
            [_row(
                "bob@gmail.com",
                smtp_status=SMTP_STATUS_INVALID,
                final_action="auto_reject",
                decision_reason="smtp_invalid",
                bucket_v2="invalid",
            )],
        )
        assert out["clean_high_confidence.csv"] == []
        invalid_rows = out["removed_invalid.csv"]
        assert [r["email"] for r in invalid_rows] == ["bob@gmail.com"]
        assert invalid_rows[0]["final_output_reason"] == "removed_v2_smtp_invalid"

    @pytest.mark.parametrize(
        "status",
        [
            SMTP_STATUS_BLOCKED,
            SMTP_STATUS_TIMEOUT,
            SMTP_STATUS_TEMP_FAIL,
            SMTP_STATUS_ERROR,
            SMTP_STATUS_CATCH_ALL_POSSIBLE,
        ],
    )
    def test_3_inconclusive_statuses_route_to_review(
        self, tmp_path: Path, status: str
    ):
        out = _materialize_with(
            tmp_path,
            [_row(
                f"x_{status}@gmail.com",
                smtp_status=status,
                final_action="manual_review",
                decision_reason=f"smtp_{status}",
                bucket_v2="review",
            )],
        )
        assert out["clean_high_confidence.csv"] == []
        review = [r["email"] for r in out["review_medium_confidence.csv"]]
        assert review == [f"x_{status}@gmail.com"]

    def test_4_missing_smtp_for_candidate_routes_to_review(self, tmp_path: Path):
        out = _materialize_with(
            tmp_path,
            [_row(
                "candidate@gmail.com",
                smtp_status=SMTP_STATUS_NOT_TESTED,
                final_action="manual_review",
                decision_reason="smtp_unconfirmed_for_candidate",
                smtp_was_candidate=True,
                bucket_v2="review",
            )],
        )
        assert out["clean_high_confidence.csv"] == []
        review = [r["email"] for r in out["review_medium_confidence.csv"]]
        assert review == ["candidate@gmail.com"]

    def test_5_duplicate_still_wins_over_smtp_valid(self, tmp_path: Path):
        out = _materialize_with(
            tmp_path,
            [
                _row(
                    "eve@gmail.com",
                    smtp_status=SMTP_STATUS_VALID,
                    final_action="auto_approve",
                    is_canonical=True,
                    source_row_number=2,
                    global_ordinal=0,
                ),
                _row(
                    "eve@gmail.com",
                    smtp_status=SMTP_STATUS_VALID,
                    final_action="auto_approve",
                    is_canonical=False,
                    source_row_number=3,
                    global_ordinal=1,
                ),
            ],
        )
        assert [r["email"] for r in out["clean_high_confidence.csv"]] == [
            "eve@gmail.com"
        ]
        assert any(
            r["final_output_reason"] == "removed_duplicate"
            for r in out["removed_invalid.csv"]
        )

    def test_6_v1_hard_fail_still_wins_over_smtp_valid(self, tmp_path: Path):
        out = _materialize_with(
            tmp_path,
            [_row(
                "frank@bad.invalid",
                smtp_status=SMTP_STATUS_VALID,
                final_action="auto_approve",
                hard_fail=True,
                bucket_v2="invalid",
            )],
        )
        assert out["clean_high_confidence.csv"] == []
        assert any(
            r["final_output_reason"] == "removed_hard_fail"
            for r in out["removed_invalid.csv"]
        )


# ---------------------------------------------------------------------------
# Layer 6 — Pipeline integration: stage order + full chunk pass
# ---------------------------------------------------------------------------


class TestPipelineIntegration:
    def test_smtp_runs_in_chunk_pipeline_before_decision(self):
        from app import pipeline as pipeline_mod

        text = Path(pipeline_mod.__file__).read_text(encoding="utf-8")
        # SMTPVerificationStage must appear before DecisionStage in the
        # constructed stage list.
        smtp_pos = text.find("SMTPVerificationStage()")
        decision_pos = text.find("DecisionStage()")
        assert smtp_pos != -1, "SMTPVerificationStage not wired in pipeline.py"
        assert decision_pos != -1
        assert smtp_pos < decision_pos, (
            "SMTPVerificationStage must run before DecisionStage so the "
            "decision can read SMTP fields."
        )

    def test_full_chunk_engine_populates_smtp_then_decision(self):
        """Run the SMTP stage and DecisionStage in sequence and check both
        sets of columns land on the same frame."""
        probe = _make_probe(_result_valid())
        df = _candidate_frame()
        ctx = PipelineContext(extras={})
        smtp_out = SMTPVerificationStage(probe_fn=probe).run(
            ChunkPayload(frame=df), ctx
        ).frame
        decision_out = DecisionStage().run(
            ChunkPayload(frame=smtp_out), ctx
        ).frame
        # SMTP columns
        for col in SMTP_VERIFICATION_OUTPUT_COLUMNS:
            assert col in decision_out.columns
        # Decision columns
        for col in (
            "final_action",
            "decision_reason",
            "deliverability_probability",
        ):
            assert col in decision_out.columns
        row = decision_out.iloc[0]
        assert row["smtp_status"] == SMTP_STATUS_VALID
        assert row["final_action"] == "auto_approve"


# ---------------------------------------------------------------------------
# Layer 7 — No live network in tests
# ---------------------------------------------------------------------------


class TestLiveNetworkDisabled:
    def test_block_live_smtp_fixture_replaces_live_probe(self):
        """The autouse fixture must monkey-patch both import paths."""
        from app.engine.stages import smtp_verification as v22_mod
        from app.validation_v2 import smtp_probe as canonical_mod

        # Both bound names must point at the offline stub by the time a
        # test runs.
        assert (
            canonical_mod.probe_email_smtplib.__name__ == "_safe_offline_probe"
        )
        assert (
            v22_mod.probe_email_smtplib.__name__ == "_safe_offline_probe"
        )

    def test_offline_stub_returns_inconclusive(self):
        """The stub must NEVER report a valid mailbox by accident.

        If a test ran without explicit mocking, the offline stub would
        return ``inconclusive`` → normalize to ``error`` → DecisionStage
        caps at review. Never auto-approve from the stub.
        """
        from app.validation_v2 import smtp_probe as canonical_mod

        result = canonical_mod.probe_email_smtplib("anyone@example.com")
        assert result.success is False
        assert result.inconclusive is True
        assert result.is_catch_all_like is False
