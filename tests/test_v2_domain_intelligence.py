"""Subphase V2.6 — Domain Intelligence & Cold-start Handling.

Pins the V2.6 contract end-to-end:

  * ``DomainIntelligenceStage`` produces seven canonical row-level
    domain fields derived from heuristics (free-provider whitelist,
    disposable list, suspicious-shape detection) with a per-domain
    cache.
  * ``apply_v2_decision_policy`` consumes ``domain_risk_level`` and
    ``domain_cold_start`` and applies safety caps:
      - high-risk domain caps approval at ``manual_review`` with reason
        ``domain_high_risk``.
      - cold-start without SMTP valid caps approval at
        ``manual_review`` with reason ``cold_start_no_smtp_valid``.
  * V2.1–V2.5 invariants remain pinned (duplicate, V1 hard-fail,
    SMTP-invalid, catch-all, candidate-without-valid-SMTP).
  * No live network is used (autouse fixture in ``conftest.py``).
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
    DOMAIN_INTELLIGENCE_OUTPUT_COLUMNS,
    DecisionStage,
    DomainIntelligenceStage,
)
from app.engine.stages.catch_all_detection import (
    CATCH_ALL_STATUS_CONFIRMED,
    CATCH_ALL_STATUS_NOT,
    CATCH_ALL_STATUS_NOT_TESTED,
)
from app.engine.stages.domain_intelligence import (
    BEHAVIOR_COLD_START,
    BEHAVIOR_DISPOSABLE,
    BEHAVIOR_FREE_PROVIDER,
    BEHAVIOR_KNOWN_RISKY,
    BEHAVIOR_UNKNOWN,
    INTEL_STATUS_AVAILABLE,
    INTEL_STATUS_NOT_APPLICABLE,
    INTEL_STATUS_UNAVAILABLE,
    RISK_LEVEL_HIGH,
    RISK_LEVEL_LOW,
    RISK_LEVEL_UNKNOWN,
    DomainIntelCache,
    DomainIntelligenceClassification,
    classify_domain_heuristic,
    is_domain_intel_candidate,
)
from app.engine.stages.smtp_verification import (
    SMTP_STATUS_INVALID,
    SMTP_STATUS_NOT_TESTED,
    SMTP_STATUS_VALID,
)
from app.models import RunContext
from app.pipeline import EmailCleaningPipeline
from app.storage import StagingDB
from app.v2_decision_policy import (
    REASON_COLD_START_NO_SMTP_VALID,
    REASON_DOMAIN_HIGH_RISK,
    REASON_DUPLICATE,
    REASON_HARD_FAIL,
    REASON_HIGH_PROBABILITY,
    REASON_LOW_PROBABILITY,
    REASON_MEDIUM_PROBABILITY,
    REASON_SMTP_INVALID,
    apply_v2_decision_policy,
)
from app.validation_v2.decision.policy import DEFAULT_DECISION_POLICY, FinalAction
from app.validation_v2.services.domain_intelligence import (
    SimpleDomainIntelligenceService,
)


# ---------------------------------------------------------------------------
# Layer 1 — Pure heuristic classifier
# ---------------------------------------------------------------------------


@pytest.fixture
def intel_service() -> SimpleDomainIntelligenceService:
    return SimpleDomainIntelligenceService()


@pytest.fixture
def disposable_set() -> frozenset[str]:
    return frozenset({"throwaway.com", "10minutemail.com"})


class TestClassifyDomainHeuristic:
    def test_disposable_domain_is_known_risky(self, intel_service, disposable_set):
        c = classify_domain_heuristic(
            "throwaway.com",
            disposable_domains=disposable_set,
            intel_service=intel_service,
        )
        assert c.status == INTEL_STATUS_AVAILABLE
        assert c.risk_level == RISK_LEVEL_HIGH
        assert c.behavior_class == BEHAVIOR_DISPOSABLE
        assert c.cold_start is False
        assert c.reason == "disposable_domain"

    def test_common_provider_is_free_provider(self, intel_service, disposable_set):
        c = classify_domain_heuristic(
            "gmail.com",
            disposable_domains=disposable_set,
            intel_service=intel_service,
        )
        assert c.risk_level == RISK_LEVEL_LOW
        assert c.behavior_class == BEHAVIOR_FREE_PROVIDER
        assert c.cold_start is False
        assert c.reputation_score >= 0.7

    def test_suspicious_shape_is_known_risky(self, intel_service, disposable_set):
        # Numeric-heavy domain should fire the suspicious-shape rule.
        c = classify_domain_heuristic(
            "mail123456789.xyz",
            disposable_domains=disposable_set,
            intel_service=intel_service,
        )
        assert c.risk_level == RISK_LEVEL_HIGH
        assert c.behavior_class == BEHAVIOR_KNOWN_RISKY
        assert c.cold_start is False
        assert "suspicious_pattern" in c.reason

    def test_unknown_domain_is_cold_start(self, intel_service, disposable_set):
        c = classify_domain_heuristic(
            "example-corp.com",
            disposable_domains=disposable_set,
            intel_service=intel_service,
        )
        assert c.risk_level == RISK_LEVEL_UNKNOWN
        assert c.behavior_class == BEHAVIOR_COLD_START
        assert c.cold_start is True
        assert c.reputation_score == pytest.approx(0.50)

    def test_empty_domain_is_not_applicable(self, intel_service, disposable_set):
        c = classify_domain_heuristic(
            "",
            disposable_domains=disposable_set,
            intel_service=intel_service,
        )
        assert c.status == INTEL_STATUS_NOT_APPLICABLE


# ---------------------------------------------------------------------------
# Layer 2 — Candidate selection
# ---------------------------------------------------------------------------


class TestIsDomainIntelCandidate:
    def test_typical_row_is_a_candidate(self):
        row = {
            "syntax_valid": True,
            "hard_fail": False,
            "corrected_domain": "gmail.com",
            "email": "alice@gmail.com",
        }
        assert is_domain_intel_candidate(row) is True

    @pytest.mark.parametrize(
        "override",
        [
            {"hard_fail": True},
            {"syntax_valid": False},
            {"corrected_domain": ""},
        ],
    )
    def test_excluded_rows_are_not_candidates(self, override):
        row = {
            "syntax_valid": True,
            "hard_fail": False,
            "corrected_domain": "gmail.com",
            "email": "alice@gmail.com",
        }
        row.update(override)
        assert is_domain_intel_candidate(row) is False


# ---------------------------------------------------------------------------
# Layer 3 — Stage behaviour
# ---------------------------------------------------------------------------


def _frame(
    *,
    domain: str = "gmail.com",
    syntax_valid: bool = True,
    has_mx: bool = True,
    hard_fail: bool = False,
    email: str | None = None,
    rows: int = 1,
) -> pd.DataFrame:
    """Build a chunk-frame as it looks after CatchAllDetectionStage."""
    if email is None:
        email = f"alice@{domain}"
    return pd.DataFrame(
        {
            "email": [email] * rows,
            "domain": [domain] * rows,
            "corrected_domain": [domain] * rows,
            "syntax_valid": pd.array([syntax_valid] * rows, dtype="boolean"),
            "has_mx_record": pd.array([has_mx] * rows, dtype="boolean"),
            "hard_fail": pd.array([hard_fail] * rows, dtype="boolean"),
        }
    )


class TestDomainIntelligenceStage:
    def test_emits_canonical_columns(self):
        out = DomainIntelligenceStage().run(
            ChunkPayload(frame=_frame()),
            PipelineContext(extras={"disposable_domains": frozenset()}),
        ).frame
        for col in DOMAIN_INTELLIGENCE_OUTPUT_COLUMNS:
            assert col in out.columns

    def test_common_provider_classification(self):
        out = DomainIntelligenceStage().run(
            ChunkPayload(frame=_frame(domain="gmail.com")),
            PipelineContext(extras={"disposable_domains": frozenset()}),
        ).frame
        row = out.iloc[0]
        assert row["domain_risk_level"] == RISK_LEVEL_LOW
        assert row["domain_behavior_class"] == BEHAVIOR_FREE_PROVIDER
        assert bool(row["domain_cold_start"]) is False

    def test_disposable_classification(self):
        ctx = PipelineContext(
            extras={"disposable_domains": frozenset({"throwaway.com"})}
        )
        out = DomainIntelligenceStage().run(
            ChunkPayload(frame=_frame(domain="throwaway.com")), ctx
        ).frame
        row = out.iloc[0]
        assert row["domain_risk_level"] == RISK_LEVEL_HIGH
        assert row["domain_behavior_class"] == BEHAVIOR_DISPOSABLE

    def test_unknown_domain_is_cold_start(self):
        out = DomainIntelligenceStage().run(
            ChunkPayload(frame=_frame(domain="some-new-corp.com")),
            PipelineContext(extras={"disposable_domains": frozenset()}),
        ).frame
        row = out.iloc[0]
        assert row["domain_risk_level"] == RISK_LEVEL_UNKNOWN
        assert row["domain_behavior_class"] == BEHAVIOR_COLD_START
        assert bool(row["domain_cold_start"]) is True

    def test_non_candidate_emits_not_applicable(self):
        out = DomainIntelligenceStage().run(
            ChunkPayload(frame=_frame(hard_fail=True)),
            PipelineContext(extras={"disposable_domains": frozenset()}),
        ).frame
        row = out.iloc[0]
        assert row["domain_intel_status"] == INTEL_STATUS_NOT_APPLICABLE
        assert row["domain_behavior_class"] == BEHAVIOR_UNKNOWN

    def test_per_domain_cache_dedupes_classifications(self):
        cache = DomainIntelCache()
        ctx = PipelineContext(
            extras={
                "disposable_domains": frozenset(),
                "domain_intel_cache": cache,
            }
        )
        # Two rows on the same domain.
        out = DomainIntelligenceStage().run(
            ChunkPayload(frame=_frame(domain="acme.com", rows=2)), ctx
        ).frame
        # Only one classification computed; second row is a cache hit.
        assert cache.classifications_computed == 1
        assert cache.cache_hits == 1
        assert all(out["domain_risk_level"] == RISK_LEVEL_UNKNOWN)

    def test_cache_does_not_poison_with_non_applicable(self):
        """A non-candidate row on a domain must not poison the cache
        for a later candidate row on the same domain."""
        cache = DomainIntelCache()
        ctx = PipelineContext(
            extras={
                "disposable_domains": frozenset(),
                "domain_intel_cache": cache,
            }
        )
        # First row: hard_fail (non-candidate) on gmail.com.
        out1 = DomainIntelligenceStage().run(
            ChunkPayload(frame=_frame(domain="gmail.com", hard_fail=True)),
            ctx,
        ).frame
        assert out1.iloc[0]["domain_intel_status"] == INTEL_STATUS_NOT_APPLICABLE
        # Cache should NOT have stored an entry for gmail.com.
        # Second row: real candidate on gmail.com.
        out2 = DomainIntelligenceStage().run(
            ChunkPayload(frame=_frame(domain="gmail.com")), ctx
        ).frame
        assert out2.iloc[0]["domain_risk_level"] == RISK_LEVEL_LOW
        assert out2.iloc[0]["domain_behavior_class"] == BEHAVIOR_FREE_PROVIDER

    def test_disabled_emits_unavailable_for_every_row(self):
        """With the config disabled the stage still emits the canonical
        columns (so DecisionStage requirements stay satisfied) but every
        row is ``unavailable`` and ``cold_start=True`` — never positive."""
        from app.config import load_config, resolve_project_paths

        cfg = load_config(base_dir=resolve_project_paths().project_root)
        cfg.domain_intelligence.enabled = False
        ctx = PipelineContext(
            config=cfg,
            extras={"disposable_domains": frozenset()},
        )
        out = DomainIntelligenceStage().run(
            ChunkPayload(frame=_frame()),
            ctx,
        ).frame
        row = out.iloc[0]
        assert row["domain_intel_status"] == INTEL_STATUS_UNAVAILABLE
        assert row["domain_risk_level"] == RISK_LEVEL_UNKNOWN
        assert bool(row["domain_cold_start"]) is True


# ---------------------------------------------------------------------------
# Layer 4 — Centralized policy with V2.6 inputs
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
    domain_risk_level: str = "low",
    domain_cold_start: bool = False,
) -> dict:
    return dict(
        probability=probability,
        smtp_status=smtp_status,
        smtp_was_candidate=smtp_was_candidate,
        catch_all_status=catch_all_status,
        catch_all_flag=catch_all_flag,
        hard_fail=hard_fail,
        v2_final_bucket=v2_final_bucket,
        policy=DEFAULT_DECISION_POLICY,
        domain_risk_level=domain_risk_level,
        domain_cold_start=domain_cold_start,
    )


class TestPolicyDomainCaps:
    def test_known_good_domain_with_smtp_valid_can_approve(self):
        """V2.6 must not over-block: low-risk + valid SMTP + no catch-all
        + high probability → auto_approve."""
        result = apply_v2_decision_policy(
            **_policy_inputs(
                probability=0.95,
                domain_risk_level="low",
                domain_cold_start=False,
            )
        )
        assert result.final_action == FinalAction.AUTO_APPROVE

    def test_high_risk_domain_caps_approval(self):
        result = apply_v2_decision_policy(
            **_policy_inputs(
                probability=0.95,
                domain_risk_level="high",
            )
        )
        assert result.final_action == FinalAction.MANUAL_REVIEW
        assert result.decision_reason == REASON_DOMAIN_HIGH_RISK

    def test_cold_start_with_smtp_valid_can_approve(self):
        """The headline V2.6 rule: cold-start + SMTP valid + no
        catch-all does NOT block approval."""
        result = apply_v2_decision_policy(
            **_policy_inputs(
                probability=0.95,
                smtp_status=SMTP_STATUS_VALID,
                domain_risk_level="unknown",
                domain_cold_start=True,
            )
        )
        assert result.final_action == FinalAction.AUTO_APPROVE

    def test_cold_start_without_smtp_valid_caps_approval(self):
        """Cold-start without confirmed SMTP cannot approve."""
        result = apply_v2_decision_policy(
            **_policy_inputs(
                probability=0.95,
                smtp_status=SMTP_STATUS_NOT_TESTED,
                smtp_was_candidate=False,  # bypass V2.4 rule 5c
                domain_risk_level="unknown",
                domain_cold_start=True,
            )
        )
        assert result.final_action == FinalAction.MANUAL_REVIEW
        assert result.decision_reason == REASON_COLD_START_NO_SMTP_VALID

    def test_cold_start_caps_only_apply_to_auto_approve(self):
        """Medium-probability rows keep their probability reason."""
        result = apply_v2_decision_policy(
            **_policy_inputs(
                probability=0.60,
                smtp_status=SMTP_STATUS_NOT_TESTED,
                domain_cold_start=True,
            )
        )
        assert result.final_action == FinalAction.MANUAL_REVIEW
        # The cap only relabels reasons it actually changed; medium
        # probability keeps its native reason.
        assert result.decision_reason == REASON_MEDIUM_PROBABILITY


class TestPolicyDomainPriorityChain:
    """V2.6 caps must NOT override V1 / dedupe / SMTP-invalid terminals."""

    def test_v1_hard_fail_beats_high_risk_domain(self):
        result = apply_v2_decision_policy(
            **_policy_inputs(
                hard_fail=True,
                probability=0.95,
                domain_risk_level="high",
            )
        )
        assert result.final_action == FinalAction.AUTO_REJECT
        assert result.decision_reason == REASON_HARD_FAIL

    def test_duplicate_beats_known_good_domain(self):
        result = apply_v2_decision_policy(
            **_policy_inputs(
                v2_final_bucket="duplicate",
                probability=0.95,
                domain_risk_level="low",
            )
        )
        assert result.final_action == FinalAction.AUTO_REJECT
        assert result.decision_reason == REASON_DUPLICATE

    def test_smtp_invalid_beats_known_good_domain(self):
        result = apply_v2_decision_policy(
            **_policy_inputs(
                probability=0.95,
                smtp_status=SMTP_STATUS_INVALID,
                domain_risk_level="low",
            )
        )
        assert result.final_action == FinalAction.AUTO_REJECT
        assert result.decision_reason == REASON_SMTP_INVALID

    def test_catch_all_beats_known_good_domain(self):
        result = apply_v2_decision_policy(
            **_policy_inputs(
                probability=0.95,
                catch_all_status=CATCH_ALL_STATUS_CONFIRMED,
                catch_all_flag=True,
                domain_risk_level="low",
            )
        )
        assert result.final_action == FinalAction.MANUAL_REVIEW
        assert result.decision_reason == "catch_all_confirmed"


# ---------------------------------------------------------------------------
# Layer 5 — Stage + decision integration
# ---------------------------------------------------------------------------


def _post_catch_all_frame(
    *,
    domain: str = "gmail.com",
    smtp_status: str = SMTP_STATUS_VALID,
    catch_all_status: str = CATCH_ALL_STATUS_NOT,
    catch_all_flag: bool = False,
    hard_fail: bool = False,
    bucket_v2: str = "high_confidence",
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "email": [f"alice@{domain}"],
            "domain": [domain],
            "corrected_domain": [domain],
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
            "smtp_was_candidate": [True],
            "smtp_confidence": [0.9 if smtp_status == SMTP_STATUS_VALID else 0.2],
            "catch_all_status": [catch_all_status],
            "catch_all_flag": [catch_all_flag],
        }
    )


class TestStageAndDecisionTogether:
    """Run DomainIntelligenceStage → DecisionStage and verify the
    decision actually consumes the V2.6 fields."""

    def test_disposable_domain_caps_approval(self):
        ctx = PipelineContext(
            extras={"disposable_domains": frozenset({"badmail.com"})}
        )
        df = _post_catch_all_frame(domain="badmail.com")
        intel_out = DomainIntelligenceStage().run(
            ChunkPayload(frame=df), ctx
        ).frame
        decision_out = DecisionStage().run(
            ChunkPayload(frame=intel_out), ctx
        ).frame
        row = decision_out.iloc[0]
        assert row["domain_risk_level"] == RISK_LEVEL_HIGH
        assert row["final_action"] == "manual_review"
        assert row["decision_reason"] == REASON_DOMAIN_HIGH_RISK

    def test_known_good_with_smtp_valid_approves(self):
        ctx = PipelineContext(extras={"disposable_domains": frozenset()})
        df = _post_catch_all_frame(domain="gmail.com")
        intel_out = DomainIntelligenceStage().run(
            ChunkPayload(frame=df), ctx
        ).frame
        decision_out = DecisionStage().run(
            ChunkPayload(frame=intel_out), ctx
        ).frame
        row = decision_out.iloc[0]
        assert row["domain_risk_level"] == RISK_LEVEL_LOW
        assert row["final_action"] == "auto_approve"

    def test_cold_start_with_smtp_valid_can_approve(self):
        ctx = PipelineContext(extras={"disposable_domains": frozenset()})
        df = _post_catch_all_frame(domain="some-new-corp.com")
        intel_out = DomainIntelligenceStage().run(
            ChunkPayload(frame=df), ctx
        ).frame
        decision_out = DecisionStage().run(
            ChunkPayload(frame=intel_out), ctx
        ).frame
        row = decision_out.iloc[0]
        assert bool(row["domain_cold_start"]) is True
        # SMTP valid + cold_start + no catch-all → can approve.
        assert row["final_action"] == "auto_approve"


# ---------------------------------------------------------------------------
# Layer 6 — End-to-end materialization preserves V2.5 export contract
# ---------------------------------------------------------------------------


def _build_run_context(tmp_path: Path) -> RunContext:
    run_dir = tmp_path / "run"
    logs_dir = tmp_path / "logs"
    temp_dir = tmp_path / "tmp"
    for p in (run_dir, logs_dir, temp_dir):
        p.mkdir(parents=True, exist_ok=True)
    return RunContext(
        run_id="run_v26_test",
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
        "domain_intel_status", "domain_reputation_score",
        "domain_risk_level", "domain_behavior_class",
        "domain_observation_count", "domain_cold_start",
        "domain_intel_reason",
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


def _materialize_with(tmp_path: Path, rows: list[dict]) -> RunContext:
    cfg = load_config(base_dir=resolve_project_paths().project_root)
    logger = logging.getLogger("v26_test")
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
    return rc


def _row(
    email: str,
    *,
    final_action: str,
    decision_reason: str,
    domain_risk_level: str = "low",
    domain_cold_start: bool = False,
    is_canonical: bool = True,
    smtp_status: str = SMTP_STATUS_VALID,
    bucket_v2: str = "ready",
) -> dict:
    norm = email.lower()
    return {
        "email": email,
        "email_normalized": norm,
        "source_file": "f.csv",
        "source_row_number": 2,
        "chunk_index": 0,
        "global_ordinal": 0,
        "hard_fail": False,
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
        "catch_all_status": CATCH_ALL_STATUS_NOT,
        "catch_all_flag": False,
        "catch_all_confidence": 0.85,
        "catch_all_method": "smtp_valid_no_random_accept",
        "domain_intel_status": INTEL_STATUS_AVAILABLE,
        "domain_reputation_score": 0.85 if domain_risk_level == "low" else 0.10,
        "domain_risk_level": domain_risk_level,
        "domain_behavior_class": (
            BEHAVIOR_FREE_PROVIDER if domain_risk_level == "low"
            else BEHAVIOR_DISPOSABLE if domain_risk_level == "high"
            else BEHAVIOR_COLD_START
        ),
        "domain_observation_count": 0,
        "domain_cold_start": domain_cold_start,
        "domain_intel_reason": "test",
        "deliverability_probability": 0.85,
        "decision_confidence": 0.85,
        "final_action": final_action,
        "decision_reason": decision_reason,
        "v2_final_bucket": bucket_v2,
    }


class TestV26EndToEndExportInvariants:
    """V2.5 export invariants must hold under V2.6 caps."""

    def test_known_good_domain_safe_lands_in_clean_export(self, tmp_path):
        rc = _materialize_with(
            tmp_path,
            [_row(
                "alice@gmail.com",
                final_action="auto_approve",
                decision_reason=REASON_HIGH_PROBABILITY,
                domain_risk_level="low",
            )],
        )
        rows = list(csv.DictReader(
            (rc.run_dir / "clean_high_confidence.csv").open(encoding="utf-8")
        ))
        assert [r["email"] for r in rows] == ["alice@gmail.com"]

    def test_high_risk_domain_does_not_leak_into_safe_export(self, tmp_path):
        """Even with V1 says clean, a high-risk domain must end up in
        review (the centralized policy is what produces final_action;
        we simulate that by setting final_action=manual_review here)."""
        rc = _materialize_with(
            tmp_path,
            [_row(
                "bad@disposable.com",
                final_action="manual_review",
                decision_reason=REASON_DOMAIN_HIGH_RISK,
                domain_risk_level="high",
                bucket_v2="review",
            )],
        )
        clean = list(csv.DictReader(
            (rc.run_dir / "clean_high_confidence.csv").open(encoding="utf-8")
        ))
        review = list(csv.DictReader(
            (rc.run_dir / "review_medium_confidence.csv").open(encoding="utf-8")
        ))
        assert clean == []
        assert [r["email"] for r in review] == ["bad@disposable.com"]


# ---------------------------------------------------------------------------
# Layer 7 — Pipeline integration (stage order + canonical fields)
# ---------------------------------------------------------------------------


class TestPipelineIntegration:
    def test_domain_intelligence_runs_between_catch_all_and_decision(self):
        from app import pipeline as pipeline_mod

        text = Path(pipeline_mod.__file__).read_text(encoding="utf-8")
        ca_pos = text.find("CatchAllDetectionStage()")
        intel_pos = text.find("DomainIntelligenceStage()")
        decision_pos = text.find("DecisionStage()")
        assert ca_pos != -1
        assert intel_pos != -1
        assert decision_pos != -1
        assert ca_pos < intel_pos < decision_pos


# ---------------------------------------------------------------------------
# Layer 8 — No live network
# ---------------------------------------------------------------------------


class TestNoLiveNetwork:
    def test_domain_intelligence_module_imports_no_network(self):
        import app.engine.stages.domain_intelligence as mod

        assert not hasattr(mod, "probe_email_smtplib")
        assert not hasattr(mod, "smtplib")
        assert not hasattr(mod, "socket")
        assert not hasattr(mod, "requests")

    def test_simple_intel_service_is_offline(self):
        """The underlying SimpleDomainIntelligenceService must not open
        any connection — verified by inspecting the class for the
        ``analyze`` method without DNS/SMTP imports."""
        from app.validation_v2.services import domain_intelligence as svc_mod

        # Module shouldn't import network libs even though it deals with domains.
        assert not hasattr(svc_mod, "smtplib")
        assert not hasattr(svc_mod, "socket")
