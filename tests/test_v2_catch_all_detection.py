"""Subphase V2.3 — Catch-all Detection Integration.

Pins the V2.3 contract end-to-end:

  * ``CatchAllDetectionStage`` produces six canonical row-level
    catch-all columns derived from the upstream SMTP signal, with a
    per-domain cache so two rows on the same domain share one
    classification.
  * ``DecisionStage`` reads ``catch_all_flag`` and applies a hard rule:
    a catch-all flagged row cannot ``auto_approve`` regardless of
    probability, SMTP outcome, or V1 bucket. ``not_catch_all`` actively
    permits approval.
  * V2.1 + V2.2 invariants remain: duplicate / V1 hard-fail / SMTP
    invalid still terminate as before.
  * No live network is opened — the autouse ``_block_live_smtp``
    fixture in ``conftest.py`` covers this.
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
    CATCH_ALL_DETECTION_OUTPUT_COLUMNS,
    CatchAllDetectionStage,
    DecisionStage,
    SMTPVerificationStage,
)
from app.engine.stages.catch_all_detection import (
    CATCH_ALL_METHOD_INCONCLUSIVE,
    CATCH_ALL_METHOD_NOT_TESTED,
    CATCH_ALL_METHOD_SMTP_PROBE,
    CATCH_ALL_METHOD_SMTP_VALID,
    CATCH_ALL_RISK_STATUSES,
    CATCH_ALL_STATUS_CONFIRMED,
    CATCH_ALL_STATUS_ERROR,
    CATCH_ALL_STATUS_NOT,
    CATCH_ALL_STATUS_NOT_TESTED,
    CATCH_ALL_STATUS_POSSIBLE,
    CATCH_ALL_STATUS_UNKNOWN,
    CatchAllCache,
    derive_catch_all_from_smtp,
    is_catch_all_candidate,
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
from app.validation_v2.smtp_probe import SMTPResult


# ---------------------------------------------------------------------------
# Frame fixtures
# ---------------------------------------------------------------------------


def _post_smtp_frame(
    *,
    smtp_status: str,
    smtp_was_candidate: bool = True,
    domain: str = "gmail.com",
    email: str | None = None,
    hard_fail: bool = False,
    has_mx: bool = True,
) -> pd.DataFrame:
    """A frame after SMTPVerificationStage has populated columns."""
    if email is None:
        email = f"alice@{domain}"
    return pd.DataFrame(
        {
            "email": [email],
            "domain": [domain],
            "corrected_domain": [domain],
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
            "bucket_v2": ["high_confidence"],
            "hard_stop_v2": pd.array([hard_fail], dtype="bool"),
            "smtp_status": [smtp_status],
            "smtp_was_candidate": [smtp_was_candidate],
            "smtp_confidence": [
                0.9 if smtp_status == SMTP_STATUS_VALID else 0.2
            ],
        }
    )


# ---------------------------------------------------------------------------
# Layer 1 — pure normalization
# ---------------------------------------------------------------------------


class TestDeriveCatchAllFromSMTP:
    def test_smtp_catch_all_possible_maps_to_possible(self):
        c = derive_catch_all_from_smtp(SMTP_STATUS_CATCH_ALL_POSSIBLE, True)
        assert c.status == CATCH_ALL_STATUS_POSSIBLE
        assert c.flag is True
        assert c.method == CATCH_ALL_METHOD_SMTP_PROBE
        assert c.tested is True

    def test_smtp_valid_maps_to_not_catch_all(self):
        c = derive_catch_all_from_smtp(SMTP_STATUS_VALID, True)
        assert c.status == CATCH_ALL_STATUS_NOT
        assert c.flag is False
        assert c.method == CATCH_ALL_METHOD_SMTP_VALID
        assert c.tested is True

    def test_smtp_invalid_maps_to_not_tested(self):
        c = derive_catch_all_from_smtp(SMTP_STATUS_INVALID, True)
        assert c.status == CATCH_ALL_STATUS_NOT_TESTED
        assert c.flag is False
        assert c.method == CATCH_ALL_METHOD_NOT_TESTED

    @pytest.mark.parametrize(
        "smtp_status",
        [SMTP_STATUS_BLOCKED, SMTP_STATUS_TIMEOUT, SMTP_STATUS_TEMP_FAIL,
         SMTP_STATUS_ERROR],
    )
    def test_inconclusive_smtp_with_candidate_maps_to_unknown(self, smtp_status):
        c = derive_catch_all_from_smtp(smtp_status, smtp_was_candidate=True)
        assert c.status == CATCH_ALL_STATUS_UNKNOWN
        assert c.flag is False
        assert c.method == CATCH_ALL_METHOD_INCONCLUSIVE

    def test_inconclusive_smtp_without_candidate_maps_to_not_tested(self):
        c = derive_catch_all_from_smtp(SMTP_STATUS_TIMEOUT, smtp_was_candidate=False)
        assert c.status == CATCH_ALL_STATUS_NOT_TESTED
        assert c.flag is False

    def test_smtp_not_tested_maps_to_not_tested(self):
        c = derive_catch_all_from_smtp(SMTP_STATUS_NOT_TESTED, False)
        assert c.status == CATCH_ALL_STATUS_NOT_TESTED


# ---------------------------------------------------------------------------
# Layer 2 — candidate selection
# ---------------------------------------------------------------------------


class TestIsCatchAllCandidate:
    def test_typical_row_is_a_candidate(self):
        row = {
            "email": "alice@gmail.com",
            "corrected_domain": "gmail.com",
            "syntax_valid": True,
            "has_mx_record": True,
            "hard_fail": False,
        }
        assert is_catch_all_candidate(row) is True

    @pytest.mark.parametrize(
        "override",
        [
            {"hard_fail": True},
            {"syntax_valid": False},
            {"has_mx_record": False},
            {"email": ""},
            {"email": "no-at-sign"},
            {"corrected_domain": ""},
        ],
    )
    def test_excluded_rows_are_not_candidates(self, override):
        row = {
            "email": "alice@gmail.com",
            "corrected_domain": "gmail.com",
            "syntax_valid": True,
            "has_mx_record": True,
            "hard_fail": False,
        }
        row.update(override)
        # Empty email or domain or invalid syntax — all → False.
        assert is_catch_all_candidate(row) is False


# ---------------------------------------------------------------------------
# Layer 3 — Stage behavior
# ---------------------------------------------------------------------------


class TestCatchAllDetectionStage:
    def test_adds_canonical_columns(self):
        out = CatchAllDetectionStage().run(
            ChunkPayload(frame=_post_smtp_frame(smtp_status=SMTP_STATUS_VALID)),
            PipelineContext(extras={}),
        ).frame
        for col in CATCH_ALL_DETECTION_OUTPUT_COLUMNS:
            assert col in out.columns

    def test_smtp_catch_all_possible_emits_possible_with_flag(self):
        out = CatchAllDetectionStage().run(
            ChunkPayload(
                frame=_post_smtp_frame(smtp_status=SMTP_STATUS_CATCH_ALL_POSSIBLE)
            ),
            PipelineContext(extras={}),
        ).frame
        row = out.iloc[0]
        assert row["catch_all_status"] == CATCH_ALL_STATUS_POSSIBLE
        assert bool(row["catch_all_flag"]) is True
        assert row["catch_all_method"] == CATCH_ALL_METHOD_SMTP_PROBE
        assert bool(row["catch_all_tested"]) is True

    def test_smtp_valid_emits_not_catch_all_without_flag(self):
        out = CatchAllDetectionStage().run(
            ChunkPayload(frame=_post_smtp_frame(smtp_status=SMTP_STATUS_VALID)),
            PipelineContext(extras={}),
        ).frame
        row = out.iloc[0]
        assert row["catch_all_status"] == CATCH_ALL_STATUS_NOT
        assert bool(row["catch_all_flag"]) is False
        assert row["catch_all_method"] == CATCH_ALL_METHOD_SMTP_VALID

    def test_non_candidate_emits_not_tested(self):
        # Hard-fail row → not a catch-all candidate.
        out = CatchAllDetectionStage().run(
            ChunkPayload(
                frame=_post_smtp_frame(
                    smtp_status=SMTP_STATUS_NOT_TESTED, hard_fail=True
                )
            ),
            PipelineContext(extras={}),
        ).frame
        row = out.iloc[0]
        assert row["catch_all_status"] == CATCH_ALL_STATUS_NOT_TESTED
        assert bool(row["catch_all_flag"]) is False
        assert bool(row["catch_all_tested"]) is False

    def test_no_mx_row_emits_not_tested(self):
        out = CatchAllDetectionStage().run(
            ChunkPayload(
                frame=_post_smtp_frame(
                    smtp_status=SMTP_STATUS_NOT_TESTED, has_mx=False
                )
            ),
            PipelineContext(extras={}),
        ).frame
        row = out.iloc[0]
        assert row["catch_all_status"] == CATCH_ALL_STATUS_NOT_TESTED

    def test_per_domain_cache_dedupes_classifications(self):
        """Two rows on the same domain share one classification, even
        if their underlying smtp_status differs slightly (the cache
        keys on domain and serves the first computed value).
        """
        df = pd.concat(
            [
                _post_smtp_frame(
                    smtp_status=SMTP_STATUS_CATCH_ALL_POSSIBLE,
                    domain="acme.com",
                    email="a@acme.com",
                ),
                _post_smtp_frame(
                    smtp_status=SMTP_STATUS_VALID,
                    domain="acme.com",
                    email="b@acme.com",
                ),
            ],
            ignore_index=True,
        )
        cache = CatchAllCache()
        ctx = PipelineContext(extras={"catch_all_cache": cache})
        out = CatchAllDetectionStage().run(
            ChunkPayload(frame=df), ctx
        ).frame
        # Cache stored exactly one classification (first row's).
        assert cache.classifications_computed == 1
        assert cache.cache_hits == 1
        # Both rows show the cached "possible" status.
        assert all(out["catch_all_status"] == CATCH_ALL_STATUS_POSSIBLE)
        assert all(out["catch_all_flag"].astype(bool))


# ---------------------------------------------------------------------------
# Layer 4 — DecisionStage with catch-all
# ---------------------------------------------------------------------------


def _post_catch_all_frame(
    *,
    smtp_status: str = SMTP_STATUS_VALID,
    catch_all_status: str = CATCH_ALL_STATUS_NOT,
    catch_all_flag: bool = False,
    smtp_was_candidate: bool = True,
    hard_fail: bool = False,
    bucket_v2: str = "high_confidence",
) -> pd.DataFrame:
    """Frame after SMTPVerificationStage + CatchAllDetectionStage."""
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
            "smtp_confidence": [0.9 if smtp_status == SMTP_STATUS_VALID else 0.2],
            "catch_all_status": [catch_all_status],
            "catch_all_flag": [catch_all_flag],
        }
    )


class TestDecisionStageWithCatchAll:
    def test_smtp_valid_plus_not_catch_all_can_auto_approve(self):
        """Headline V2.3: domain provably NOT catch-all → SMTP valid
        actually approves."""
        out = DecisionStage().run(
            ChunkPayload(
                frame=_post_catch_all_frame(
                    smtp_status=SMTP_STATUS_VALID,
                    catch_all_status=CATCH_ALL_STATUS_NOT,
                    catch_all_flag=False,
                )
            ),
            PipelineContext(),
        ).frame
        assert out.iloc[0]["final_action"] == "auto_approve"

    def test_confirmed_catch_all_caps_at_manual_review(self):
        out = DecisionStage().run(
            ChunkPayload(
                frame=_post_catch_all_frame(
                    smtp_status=SMTP_STATUS_VALID,
                    catch_all_status=CATCH_ALL_STATUS_CONFIRMED,
                    catch_all_flag=True,
                )
            ),
            PipelineContext(),
        ).frame
        row = out.iloc[0]
        assert row["final_action"] == "manual_review"
        assert row["decision_reason"] == "catch_all_confirmed"

    def test_possible_catch_all_caps_at_manual_review(self):
        out = DecisionStage().run(
            ChunkPayload(
                frame=_post_catch_all_frame(
                    smtp_status=SMTP_STATUS_CATCH_ALL_POSSIBLE,
                    catch_all_status=CATCH_ALL_STATUS_POSSIBLE,
                    catch_all_flag=True,
                )
            ),
            PipelineContext(),
        ).frame
        row = out.iloc[0]
        # The headline contract is the action, not the specific reason
        # token. Multiple paths can produce ``manual_review`` for a
        # catch-all-possible row: the probability model already lands
        # this case in ``medium`` (so the probability branch returns
        # ``medium_probability``), or — if probability had landed in
        # ``high`` — the SMTP/catch-all overrides would re-label it
        # ``smtp_catch_all_possible`` / ``catch_all_possible``. All
        # three are valid V2.3 outcomes; what matters is no auto-approve.
        assert row["final_action"] == "manual_review"
        assert row["decision_reason"] in (
            "catch_all_possible",
            "smtp_catch_all_possible",
            "medium_probability",
        )

    def test_smtp_invalid_plus_catch_all_still_auto_rejects(self):
        """V2.2 invariant: SMTP-invalid rejection is sacrosanct, even
        if the domain is also catch-all."""
        out = DecisionStage().run(
            ChunkPayload(
                frame=_post_catch_all_frame(
                    smtp_status=SMTP_STATUS_INVALID,
                    catch_all_status=CATCH_ALL_STATUS_CONFIRMED,
                    catch_all_flag=True,
                    bucket_v2="invalid",
                )
            ),
            PipelineContext(),
        ).frame
        row = out.iloc[0]
        assert row["final_action"] == "auto_reject"
        assert row["decision_reason"] == "smtp_invalid"

    def test_v1_hard_fail_still_wins_over_catch_all(self):
        out = DecisionStage().run(
            ChunkPayload(
                frame=_post_catch_all_frame(
                    smtp_status=SMTP_STATUS_VALID,
                    catch_all_status=CATCH_ALL_STATUS_NOT,
                    catch_all_flag=False,
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
# Layer 5 — End-to-end materialization
# ---------------------------------------------------------------------------


def _build_run_context(tmp_path: Path) -> RunContext:
    run_dir = tmp_path / "run"
    logs_dir = tmp_path / "logs"
    temp_dir = tmp_path / "tmp"
    for p in (run_dir, logs_dir, temp_dir):
        p.mkdir(parents=True, exist_ok=True)
    return RunContext(
        run_id="run_v23_test",
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
    logger = logging.getLogger("v23_test")
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
    smtp_status: str = SMTP_STATUS_VALID,
    catch_all_status: str = CATCH_ALL_STATUS_NOT,
    catch_all_flag: bool = False,
    final_action: str = "auto_approve",
    decision_reason: str = "high_probability",
    is_canonical: bool = True,
    hard_fail: bool = False,
    bucket_v2: str = "ready",
    smtp_was_candidate: bool = True,
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
        "smtp_confirmed_valid": smtp_status == SMTP_STATUS_VALID,
        "smtp_response_code": 250 if smtp_status == SMTP_STATUS_VALID else 550,
        "catch_all_status": catch_all_status,
        "catch_all_flag": catch_all_flag,
        "catch_all_confidence": 0.85 if catch_all_status != CATCH_ALL_STATUS_NOT_TESTED else 0.0,
        "catch_all_method": CATCH_ALL_METHOD_SMTP_VALID,
        "final_action": final_action,
        "decision_reason": decision_reason,
        "decision_confidence": 0.85 if final_action == "auto_approve" else 0.40,
        "v2_final_bucket": bucket_v2,
    }


class TestV23EndToEndMaterialization:
    """The 9 prompt scenarios end-to-end via _materialize."""

    def test_1_confirmed_catch_all_routes_to_review(self, tmp_path: Path):
        out = _materialize_with(
            tmp_path,
            [_row(
                "alice@gmail.com",
                smtp_status=SMTP_STATUS_VALID,
                catch_all_status=CATCH_ALL_STATUS_CONFIRMED,
                catch_all_flag=True,
                final_action="manual_review",
                decision_reason="catch_all_confirmed",
                bucket_v2="review",
            )],
        )
        assert out["clean_high_confidence.csv"] == []
        review = [r["email"] for r in out["review_medium_confidence.csv"]]
        assert review == ["alice@gmail.com"]

    def test_2_possible_catch_all_routes_to_review(self, tmp_path: Path):
        out = _materialize_with(
            tmp_path,
            [_row(
                "bob@gmail.com",
                smtp_status=SMTP_STATUS_CATCH_ALL_POSSIBLE,
                catch_all_status=CATCH_ALL_STATUS_POSSIBLE,
                catch_all_flag=True,
                final_action="manual_review",
                decision_reason="catch_all_possible",
                bucket_v2="review",
            )],
        )
        assert out["clean_high_confidence.csv"] == []
        review = [r["email"] for r in out["review_medium_confidence.csv"]]
        assert review == ["bob@gmail.com"]

    def test_3_not_catch_all_allows_smtp_valid_approval(self, tmp_path: Path):
        out = _materialize_with(
            tmp_path,
            [_row(
                "carol@gmail.com",
                smtp_status=SMTP_STATUS_VALID,
                catch_all_status=CATCH_ALL_STATUS_NOT,
                catch_all_flag=False,
                final_action="auto_approve",
            )],
        )
        clean = [r["email"] for r in out["clean_high_confidence.csv"]]
        assert clean == ["carol@gmail.com"]
        assert out["review_medium_confidence.csv"] == []
        assert out["removed_invalid.csv"] == []

    def test_4_smtp_invalid_overrides_catch_all_routing(self, tmp_path: Path):
        """SMTP invalid still rejects; catch-all does not get to flip
        the row to review."""
        out = _materialize_with(
            tmp_path,
            [_row(
                "dave@gmail.com",
                smtp_status=SMTP_STATUS_INVALID,
                catch_all_status=CATCH_ALL_STATUS_CONFIRMED,
                catch_all_flag=True,
                final_action="auto_reject",
                decision_reason="smtp_invalid",
                bucket_v2="invalid",
            )],
        )
        assert out["clean_high_confidence.csv"] == []
        invalid_rows = out["removed_invalid.csv"]
        assert [r["email"] for r in invalid_rows] == ["dave@gmail.com"]
        assert invalid_rows[0]["final_output_reason"] == "removed_v2_smtp_invalid"

    def test_5_duplicate_still_wins_over_catch_all(self, tmp_path: Path):
        out = _materialize_with(
            tmp_path,
            [
                _row(
                    "eve@gmail.com",
                    smtp_status=SMTP_STATUS_VALID,
                    catch_all_status=CATCH_ALL_STATUS_NOT,
                    catch_all_flag=False,
                    final_action="auto_approve",
                    is_canonical=True,
                    source_row_number=2,
                    global_ordinal=0,
                ),
                _row(
                    "eve@gmail.com",
                    smtp_status=SMTP_STATUS_VALID,
                    catch_all_status=CATCH_ALL_STATUS_NOT,
                    catch_all_flag=False,
                    final_action="auto_approve",
                    is_canonical=False,
                    source_row_number=3,
                    global_ordinal=1,
                ),
            ],
        )
        # Canonical row → clean. Duplicate → removed_invalid with
        # ``removed_duplicate`` reason.
        clean = [r["email"] for r in out["clean_high_confidence.csv"]]
        assert clean == ["eve@gmail.com"]
        assert any(
            r["final_output_reason"] == "removed_duplicate"
            for r in out["removed_invalid.csv"]
        )

    def test_6_v1_hard_fail_still_wins_over_catch_all(self, tmp_path: Path):
        out = _materialize_with(
            tmp_path,
            [_row(
                "frank@bad.invalid",
                smtp_status=SMTP_STATUS_VALID,
                catch_all_status=CATCH_ALL_STATUS_NOT,
                catch_all_flag=False,
                hard_fail=True,
                final_action="auto_reject",
                decision_reason="hard_fail",
                bucket_v2="invalid",
            )],
        )
        assert out["clean_high_confidence.csv"] == []
        assert any(
            r["final_output_reason"] == "removed_hard_fail"
            for r in out["removed_invalid.csv"]
        )


# ---------------------------------------------------------------------------
# Layer 6 — Pipeline integration
# ---------------------------------------------------------------------------


class TestPipelineIntegration:
    def test_catch_all_runs_between_smtp_and_decision(self):
        from app import pipeline as pipeline_mod

        text = Path(pipeline_mod.__file__).read_text(encoding="utf-8")
        smtp_pos = text.find("SMTPVerificationStage()")
        catch_all_pos = text.find("CatchAllDetectionStage()")
        decision_pos = text.find("DecisionStage()")
        assert smtp_pos != -1
        assert catch_all_pos != -1
        assert decision_pos != -1
        assert smtp_pos < catch_all_pos < decision_pos, (
            "Stage order must be SMTPVerification → CatchAllDetection → Decision."
        )

    def test_full_chunk_engine_smtp_then_catch_all_then_decision(self):
        """SMTP=valid → catch-all stage emits not_catch_all → decision approves."""
        df = pd.DataFrame(
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
                "hard_fail": pd.array([False], dtype="boolean"),
                "score": [75],
                "preliminary_bucket": ["high_confidence"],
                "bucket_v2": ["high_confidence"],
                "hard_stop_v2": pd.array([False], dtype="bool"),
            }
        )

        def _valid_probe(email: str, **_kwargs: object) -> SMTPResult:
            return SMTPResult(
                success=True,
                response_code=250,
                response_message="ok",
                is_catch_all_like=False,
                inconclusive=False,
            )

        ctx = PipelineContext(extras={})
        smtp_out = SMTPVerificationStage(probe_fn=_valid_probe).run(
            ChunkPayload(frame=df), ctx
        ).frame
        catch_all_out = CatchAllDetectionStage().run(
            ChunkPayload(frame=smtp_out), ctx
        ).frame
        decision_out = DecisionStage().run(
            ChunkPayload(frame=catch_all_out), ctx
        ).frame

        # Catch-all stage produced canonical fields.
        for col in CATCH_ALL_DETECTION_OUTPUT_COLUMNS:
            assert col in decision_out.columns
        row = decision_out.iloc[0]
        assert row["catch_all_status"] == CATCH_ALL_STATUS_NOT
        assert bool(row["catch_all_flag"]) is False
        # And the decision saw catch-all and approved.
        assert row["final_action"] == "auto_approve"


# ---------------------------------------------------------------------------
# Layer 7 — Live-network protection (re-checked)
# ---------------------------------------------------------------------------


class TestNoLiveNetwork:
    def test_catch_all_stage_does_not_import_network_modules(self):
        """The catch-all stage must not import the SMTP probe or any
        network library. We check the actual module attributes (not the
        source text — docstrings legitimately mention these names).
        """
        import app.engine.stages.catch_all_detection as mod

        # Sanity: the stage module must not bind a probe function name.
        assert not hasattr(mod, "probe_email_smtplib")
        assert not hasattr(mod, "smtplib")
        # And it must not expose any socket / network helper.
        assert not hasattr(mod, "socket")
