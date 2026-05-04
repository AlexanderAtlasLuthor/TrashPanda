"""Subphase V2.7 — Feedback Loop / Bounce Outcome Ingestion.

Pins the V2.7 contract end-to-end:

  * Outcome vocabulary normalization (raw → canonical).
  * Email + domain normalization (case, whitespace, syntax).
  * Malformed-row tolerance (skipped, counted, never crashes).
  * Per-domain aggregation across many events.
  * Reputation calculation (low / medium / high / unknown).
  * SQLite-backed persistence + read-back.
  * Boundary callable ``ingest_bounce_feedback``.
  * V2.6 bridge: ``bounce_aggregate_to_domain_intel`` produces the
    shape ``DomainIntelligenceClassification`` accepts.
  * Cleaning pipeline still runs without ingestion.
  * No live network — pure file/SQLite operations on tmp paths.
"""

from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path

import pytest

from app.api_boundary import ingest_bounce_feedback
from app.config import load_config, resolve_project_paths
from app.validation_v2.feedback import (
    BounceOutcomeStore,
    DEFAULT_REPUTATION_THRESHOLDS,
    DomainBounceAggregate,
    IngestionSummary,
    OUTCOMES,
    OUTCOME_BLOCKED,
    OUTCOME_COMPLAINT,
    OUTCOME_DEFERRED,
    OUTCOME_DELIVERED,
    OUTCOME_HARD_BOUNCE,
    OUTCOME_SOFT_BOUNCE,
    OUTCOME_UNKNOWN,
    OUTCOME_UNSUBSCRIBED,
    RISK_LEVEL_HIGH,
    RISK_LEVEL_LOW,
    RISK_LEVEL_MEDIUM,
    RISK_LEVEL_UNKNOWN,
    ReputationThresholds,
    bounce_aggregate_to_domain_intel,
    compute_reputation_score,
    compute_risk_level,
    extract_domain,
    ingest_bounce_outcomes,
    is_negative,
    is_positive,
    is_suppression,
    is_temporary,
    normalize_email,
    normalize_outcome,
    normalize_outcome_with_type,
)


# ---------------------------------------------------------------------------
# Layer 1 — outcome normalization
# ---------------------------------------------------------------------------


class TestNormalizeOutcome:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("delivered", OUTCOME_DELIVERED),
            ("DELIVERED", OUTCOME_DELIVERED),
            ("  delivered  ", OUTCOME_DELIVERED),
            ("sent", OUTCOME_DELIVERED),
            ("ok", OUTCOME_DELIVERED),
            ("hard_bounce", OUTCOME_HARD_BOUNCE),
            ("hard-bounce", OUTCOME_HARD_BOUNCE),
            ("hardbounce", OUTCOME_HARD_BOUNCE),
            ("soft_bounce", OUTCOME_SOFT_BOUNCE),
            ("soft-bounce", OUTCOME_SOFT_BOUNCE),
            ("softbounce", OUTCOME_SOFT_BOUNCE),
            ("blocked", OUTCOME_BLOCKED),
            ("rejected", OUTCOME_BLOCKED),
            ("deferred", OUTCOME_DEFERRED),
            ("delayed", OUTCOME_DEFERRED),
            ("complaint", OUTCOME_COMPLAINT),
            ("spam", OUTCOME_COMPLAINT),
            ("unsubscribed", OUTCOME_UNSUBSCRIBED),
            ("opt_out", OUTCOME_UNSUBSCRIBED),
            ("unknown", OUTCOME_UNKNOWN),
            ("", OUTCOME_UNKNOWN),
            (None, OUTCOME_UNKNOWN),
            ("totally-bogus-status", OUTCOME_UNKNOWN),
        ],
    )
    def test_normalize_outcome(self, raw, expected):
        assert normalize_outcome(raw) == expected


class TestNormalizeOutcomeWithType:
    """``bounced`` + ``bounce_type=hard`` → hard_bounce, etc."""

    @pytest.mark.parametrize(
        "outcome_raw,bounce_type_raw,expected",
        [
            ("bounced", "hard", OUTCOME_HARD_BOUNCE),
            ("bounce", "hard", OUTCOME_HARD_BOUNCE),
            ("bounced", "soft", OUTCOME_SOFT_BOUNCE),
            ("bounced", "deferred", OUTCOME_DEFERRED),
            ("bounced", "blocked", OUTCOME_BLOCKED),
            ("bounced", "", OUTCOME_UNKNOWN),    # ambiguous → unknown
            ("bounce", "", OUTCOME_UNKNOWN),
            ("delivered", "anything", OUTCOME_DELIVERED),  # type ignored
            ("hard_bounce", "soft", OUTCOME_SOFT_BOUNCE),  # type refines
            ("", "hard", OUTCOME_HARD_BOUNCE),  # type alone
            ("", "", OUTCOME_UNKNOWN),
            (None, None, OUTCOME_UNKNOWN),
        ],
    )
    def test_resolution_rules(self, outcome_raw, bounce_type_raw, expected):
        assert normalize_outcome_with_type(outcome_raw, bounce_type_raw) == expected


class TestOutcomeCategorization:
    def test_delivered_is_positive(self):
        assert is_positive(OUTCOME_DELIVERED)
        assert not is_negative(OUTCOME_DELIVERED)

    @pytest.mark.parametrize(
        "outcome",
        [OUTCOME_HARD_BOUNCE, OUTCOME_BLOCKED, OUTCOME_COMPLAINT],
    )
    def test_negative_outcomes(self, outcome):
        assert is_negative(outcome)
        assert not is_positive(outcome)

    @pytest.mark.parametrize(
        "outcome", [OUTCOME_SOFT_BOUNCE, OUTCOME_DEFERRED]
    )
    def test_temporary_outcomes(self, outcome):
        assert is_temporary(outcome)

    def test_unsubscribed_is_suppression(self):
        assert is_suppression(OUTCOME_UNSUBSCRIBED)
        assert not is_negative(OUTCOME_UNSUBSCRIBED)


# ---------------------------------------------------------------------------
# Layer 2 — email + domain normalization
# ---------------------------------------------------------------------------


class TestNormalizeEmailAndDomain:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("Alice@Gmail.COM", "alice@gmail.com"),
            ("  bob@yahoo.com  ", "bob@yahoo.com"),
            ("user@sub.example.org", "user@sub.example.org"),
            ("", None),
            (None, None),
            ("not-an-email", None),
            ("@nolocal.com", None),
            ("nodomain@", None),
            ("two@@ats.com", None),
            ("space in@email.com", None),
        ],
    )
    def test_normalize_email(self, raw, expected):
        assert normalize_email(raw) == expected

    @pytest.mark.parametrize(
        "email,expected",
        [
            ("alice@gmail.com", "gmail.com"),
            ("user@SUB.EXAMPLE.ORG", "sub.example.org"),
            (None, None),
            ("no-at-sign", None),
        ],
    )
    def test_extract_domain(self, email, expected):
        # extract_domain expects pre-normalized; normalize first if raw.
        if email is not None and "@" in email:
            email = email.lower()
        assert extract_domain(email) == expected


# ---------------------------------------------------------------------------
# Layer 3 — aggregate model + reputation
# ---------------------------------------------------------------------------


class TestDomainBounceAggregate:
    def test_record_increments_correct_counter(self):
        agg = DomainBounceAggregate(domain="x.com")
        agg.record(OUTCOME_DELIVERED, "2026-05-04T12:00:00Z")
        agg.record(OUTCOME_HARD_BOUNCE, "2026-05-04T12:01:00Z")
        agg.record(OUTCOME_SOFT_BOUNCE)
        assert agg.total_observations == 3
        assert agg.delivered_count == 1
        assert agg.hard_bounce_count == 1
        assert agg.soft_bounce_count == 1
        assert agg.last_seen_at == "2026-05-04T12:01:00Z"

    def test_unknown_outcome_increments_unknown(self):
        agg = DomainBounceAggregate(domain="x.com")
        agg.record("totally-not-a-thing")
        assert agg.unknown_count == 1
        assert agg.total_observations == 1


class TestReputationCalculation:
    def test_insufficient_observations_is_unknown(self):
        # Default threshold is 5; 4 observations → unknown.
        agg = DomainBounceAggregate(domain="x.com")
        for _ in range(4):
            agg.record(OUTCOME_DELIVERED)
        assert compute_risk_level(agg) == RISK_LEVEL_UNKNOWN

    def test_clean_history_is_low_risk(self):
        agg = DomainBounceAggregate(domain="x.com")
        for _ in range(10):
            agg.record(OUTCOME_DELIVERED)
        assert compute_risk_level(agg) == RISK_LEVEL_LOW
        assert compute_reputation_score(agg) == 1.0

    def test_high_hard_bounce_rate_is_high_risk(self):
        agg = DomainBounceAggregate(domain="x.com")
        for _ in range(7):
            agg.record(OUTCOME_DELIVERED)
        for _ in range(3):
            agg.record(OUTCOME_HARD_BOUNCE)
        # 3/10 = 0.30 ≥ 0.20 → high.
        assert compute_risk_level(agg) == RISK_LEVEL_HIGH

    def test_medium_hard_bounce_rate_is_medium_risk(self):
        agg = DomainBounceAggregate(domain="x.com")
        for _ in range(9):
            agg.record(OUTCOME_DELIVERED)
        for _ in range(1):
            agg.record(OUTCOME_HARD_BOUNCE)
        # 1/10 = 0.10 → medium (between 0.08 and 0.20).
        assert compute_risk_level(agg) == RISK_LEVEL_MEDIUM

    def test_blocked_rate_high(self):
        agg = DomainBounceAggregate(domain="x.com")
        for _ in range(8):
            agg.record(OUTCOME_DELIVERED)
        for _ in range(2):
            agg.record(OUTCOME_BLOCKED)
        # 2/10 = 0.20 ≥ 0.10 → high.
        assert compute_risk_level(agg) == RISK_LEVEL_HIGH

    def test_complaint_present_is_high_risk(self):
        agg = DomainBounceAggregate(domain="x.com")
        for _ in range(9):
            agg.record(OUTCOME_DELIVERED)
        agg.record(OUTCOME_COMPLAINT)
        assert compute_risk_level(agg) == RISK_LEVEL_HIGH

    def test_custom_thresholds_change_routing(self):
        strict = ReputationThresholds(
            min_observations=3,
            medium_hard_bounce_rate=0.05,
            high_hard_bounce_rate=0.15,
        )
        agg = DomainBounceAggregate(domain="x.com")
        for _ in range(9):
            agg.record(OUTCOME_DELIVERED)
        agg.record(OUTCOME_HARD_BOUNCE)
        # 1/10 = 0.10 → between strict's 0.05 and 0.15 → medium.
        assert compute_risk_level(agg, strict) == RISK_LEVEL_MEDIUM

    def test_reputation_score_is_delivered_rate(self):
        agg = DomainBounceAggregate(domain="x.com")
        for _ in range(8):
            agg.record(OUTCOME_DELIVERED)
        for _ in range(2):
            agg.record(OUTCOME_HARD_BOUNCE)
        assert compute_reputation_score(agg) == pytest.approx(0.8)


# ---------------------------------------------------------------------------
# Layer 4 — Persistence (BounceOutcomeStore)
# ---------------------------------------------------------------------------


class TestBounceOutcomeStore:
    def test_get_returns_none_for_unknown_domain(self, tmp_path: Path):
        store = BounceOutcomeStore(tmp_path / "bounce.sqlite")
        try:
            assert store.get("absent.com") is None
        finally:
            store.close()

    def test_upsert_and_read_roundtrip(self, tmp_path: Path):
        store = BounceOutcomeStore(tmp_path / "bounce.sqlite")
        try:
            agg = DomainBounceAggregate(domain="x.com")
            agg.record(OUTCOME_DELIVERED, "2026-05-04T10:00:00Z")
            agg.record(OUTCOME_HARD_BOUNCE, "2026-05-04T11:00:00Z")
            store.upsert_aggregate(agg)

            read_back = store.get("x.com")
            assert read_back is not None
            assert read_back.domain == "x.com"
            assert read_back.total_observations == 2
            assert read_back.delivered_count == 1
            assert read_back.hard_bounce_count == 1
            assert read_back.last_seen_at == "2026-05-04T11:00:00Z"
        finally:
            store.close()

    def test_upsert_replaces_row(self, tmp_path: Path):
        store = BounceOutcomeStore(tmp_path / "bounce.sqlite")
        try:
            a = DomainBounceAggregate(domain="x.com", total_observations=1, delivered_count=1)
            store.upsert_aggregate(a)
            b = DomainBounceAggregate(domain="x.com", total_observations=5, delivered_count=5)
            store.upsert_aggregate(b)
            read_back = store.get("x.com")
            assert read_back.total_observations == 5
        finally:
            store.close()

    def test_list_all_returns_every_domain(self, tmp_path: Path):
        store = BounceOutcomeStore(tmp_path / "bounce.sqlite")
        try:
            for d in ("a.com", "b.com", "c.com"):
                store.upsert_aggregate(DomainBounceAggregate(domain=d))
            domains = sorted(a.domain for a in store.list_all())
            assert domains == ["a.com", "b.com", "c.com"]
        finally:
            store.close()


# ---------------------------------------------------------------------------
# Layer 5 — Ingestion
# ---------------------------------------------------------------------------


def _write_csv(path: Path, rows: list[dict[str, str]], headers: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


class TestIngestion:
    def test_basic_ingestion_aggregates_by_domain(self, tmp_path: Path):
        csv_path = tmp_path / "feedback.csv"
        _write_csv(
            csv_path,
            [
                {"email": "alice@gmail.com", "outcome": "delivered"},
                {"email": "bob@gmail.com", "outcome": "delivered"},
                {"email": "carol@gmail.com", "outcome": "hard_bounce"},
                {"email": "dave@yahoo.com", "outcome": "delivered"},
            ],
            ["email", "outcome"],
        )
        store = BounceOutcomeStore(tmp_path / "bounce.sqlite")
        try:
            summary = ingest_bounce_outcomes(csv_path, history_store=store)
            assert summary.error is None
            assert summary.total_rows == 4
            assert summary.accepted_rows == 4
            assert summary.skipped_rows == 0
            assert summary.domains_updated == 2
            assert summary.delivered_count == 3
            assert summary.hard_bounce_count == 1

            gmail_agg = store.get("gmail.com")
            assert gmail_agg.total_observations == 3
            assert gmail_agg.delivered_count == 2
            assert gmail_agg.hard_bounce_count == 1

            yahoo_agg = store.get("yahoo.com")
            assert yahoo_agg.total_observations == 1
            assert yahoo_agg.delivered_count == 1
        finally:
            store.close()

    def test_malformed_rows_are_skipped_safely(self, tmp_path: Path):
        csv_path = tmp_path / "feedback.csv"
        _write_csv(
            csv_path,
            [
                {"email": "good@gmail.com", "outcome": "delivered"},
                {"email": "not-an-email", "outcome": "delivered"},
                {"email": "", "outcome": "delivered"},
                {"email": "another@gmail.com", "outcome": ""},
                {"email": "one@yahoo.com", "outcome": "delivered"},
            ],
            ["email", "outcome"],
        )
        store = BounceOutcomeStore(tmp_path / "bounce.sqlite")
        try:
            summary = ingest_bounce_outcomes(csv_path, history_store=store)
            assert summary.error is None
            assert summary.total_rows == 5
            assert summary.accepted_rows == 2  # only the two good ones
            assert summary.skipped_rows == 3
            assert summary.invalid_email_rows == 2  # not-an-email + empty
            assert summary.unknown_outcome_rows == 1  # empty outcome
        finally:
            store.close()

    def test_outcome_with_bounce_type_resolves(self, tmp_path: Path):
        csv_path = tmp_path / "feedback.csv"
        _write_csv(
            csv_path,
            [
                {"email": "a@x.com", "outcome": "bounced", "bounce_type": "hard"},
                {"email": "b@x.com", "outcome": "bounced", "bounce_type": "soft"},
                {"email": "c@x.com", "outcome": "bounced", "bounce_type": "deferred"},
            ],
            ["email", "outcome", "bounce_type"],
        )
        store = BounceOutcomeStore(tmp_path / "bounce.sqlite")
        try:
            summary = ingest_bounce_outcomes(csv_path, history_store=store)
            assert summary.accepted_rows == 3
            agg = store.get("x.com")
            assert agg.hard_bounce_count == 1
            assert agg.soft_bounce_count == 1
            assert agg.deferred_count == 1
        finally:
            store.close()

    def test_re_ingestion_adds_to_existing_counts(self, tmp_path: Path):
        store = BounceOutcomeStore(tmp_path / "bounce.sqlite")
        try:
            csv_a = tmp_path / "a.csv"
            csv_b = tmp_path / "b.csv"
            _write_csv(csv_a, [{"email": "x@d.com", "outcome": "delivered"}],
                       ["email", "outcome"])
            _write_csv(csv_b, [{"email": "y@d.com", "outcome": "hard_bounce"}],
                       ["email", "outcome"])
            ingest_bounce_outcomes(csv_a, history_store=store)
            ingest_bounce_outcomes(csv_b, history_store=store)
            agg = store.get("d.com")
            assert agg.total_observations == 2
            assert agg.delivered_count == 1
            assert agg.hard_bounce_count == 1
        finally:
            store.close()

    def test_missing_file_returns_error(self, tmp_path: Path):
        store = BounceOutcomeStore(tmp_path / "bounce.sqlite")
        try:
            summary = ingest_bounce_outcomes(
                tmp_path / "nonexistent.csv", history_store=store
            )
            assert summary.error is not None
            assert "file_not_found" in summary.error
            assert summary.accepted_rows == 0
        finally:
            store.close()

    def test_optional_fields_are_tolerated(self, tmp_path: Path):
        csv_path = tmp_path / "feedback.csv"
        _write_csv(
            csv_path,
            [
                {
                    "email": "a@x.com",
                    "outcome": "delivered",
                    "bounce_type": "",
                    "smtp_code": "250",
                    "reason": "ok",
                    "campaign_id": "c1",
                    "timestamp": "2026-05-04T12:00:00Z",
                    "provider": "ses",
                },
            ],
            ["email", "outcome", "bounce_type", "smtp_code",
             "reason", "campaign_id", "timestamp", "provider"],
        )
        store = BounceOutcomeStore(tmp_path / "bounce.sqlite")
        try:
            summary = ingest_bounce_outcomes(csv_path, history_store=store)
            assert summary.accepted_rows == 1
            agg = store.get("x.com")
            assert agg.last_seen_at == "2026-05-04T12:00:00Z"
        finally:
            store.close()

    def test_summary_counts_each_outcome_type(self, tmp_path: Path):
        csv_path = tmp_path / "feedback.csv"
        _write_csv(
            csv_path,
            [
                {"email": "a@x.com", "outcome": "delivered"},
                {"email": "b@x.com", "outcome": "hard_bounce"},
                {"email": "c@x.com", "outcome": "soft_bounce"},
                {"email": "d@x.com", "outcome": "blocked"},
                {"email": "e@x.com", "outcome": "deferred"},
                {"email": "f@x.com", "outcome": "complaint"},
                {"email": "g@x.com", "outcome": "unsubscribed"},
            ],
            ["email", "outcome"],
        )
        store = BounceOutcomeStore(tmp_path / "bounce.sqlite")
        try:
            summary = ingest_bounce_outcomes(csv_path, history_store=store)
            assert summary.delivered_count == 1
            assert summary.hard_bounce_count == 1
            assert summary.soft_bounce_count == 1
            assert summary.blocked_count == 1
            assert summary.deferred_count == 1
            assert summary.complaint_count == 1
            assert summary.unsubscribed_count == 1
        finally:
            store.close()


# ---------------------------------------------------------------------------
# Layer 6 — Boundary callable
# ---------------------------------------------------------------------------


class TestBoundaryCallable:
    def test_ingest_bounce_feedback_returns_summary_dict(self, tmp_path: Path, monkeypatch):
        # Point the config at a tmp store path.
        cfg = load_config(base_dir=resolve_project_paths().project_root)
        cfg.bounce_ingestion.store_path = str(tmp_path / "bounce.sqlite")

        # Monkeypatch ``load_config`` so the boundary helper uses our cfg.
        from app import api_boundary

        monkeypatch.setattr(api_boundary, "load_config", lambda **_: cfg)

        csv_path = tmp_path / "feedback.csv"
        _write_csv(
            csv_path,
            [
                {"email": "a@gmail.com", "outcome": "delivered"},
                {"email": "b@gmail.com", "outcome": "hard_bounce"},
            ],
            ["email", "outcome"],
        )

        result = ingest_bounce_feedback(csv_path)
        assert isinstance(result, dict)
        assert result.get("error") is None
        assert result["total_rows"] == 2
        assert result["accepted_rows"] == 2
        assert result["domains_updated"] == 1
        assert "started_at" in result
        assert "finished_at" in result
        assert "store_path" in result

    def test_ingest_bounce_feedback_disabled_via_config(
        self, tmp_path: Path, monkeypatch
    ):
        cfg = load_config(base_dir=resolve_project_paths().project_root)
        cfg.bounce_ingestion.enabled = False
        from app import api_boundary

        monkeypatch.setattr(api_boundary, "load_config", lambda **_: cfg)
        csv_path = tmp_path / "feedback.csv"
        _write_csv(csv_path, [], ["email", "outcome"])
        result = ingest_bounce_feedback(csv_path)
        assert result["error"] == "bounce_ingestion_disabled"

    def test_ingest_bounce_feedback_missing_file_safe(self, tmp_path: Path, monkeypatch):
        cfg = load_config(base_dir=resolve_project_paths().project_root)
        cfg.bounce_ingestion.store_path = str(tmp_path / "bounce.sqlite")
        from app import api_boundary

        monkeypatch.setattr(api_boundary, "load_config", lambda **_: cfg)

        result = ingest_bounce_feedback(tmp_path / "nope.csv")
        assert isinstance(result, dict)
        assert result.get("error") and "file_not_found" in result["error"]


# ---------------------------------------------------------------------------
# Layer 7 — V2.6 bridge
# ---------------------------------------------------------------------------


class TestV26Bridge:
    def test_low_risk_aggregate_maps_to_known_good(self):
        agg = DomainBounceAggregate(domain="x.com")
        for _ in range(10):
            agg.record(OUTCOME_DELIVERED)
        intel = bounce_aggregate_to_domain_intel(agg)
        assert intel["status"] == "available"
        assert intel["risk_level"] == RISK_LEVEL_LOW
        assert intel["behavior_class"] == "known_good"
        assert intel["cold_start"] is False
        assert intel["reputation_score"] == 1.0

    def test_high_risk_aggregate_maps_to_known_risky(self):
        agg = DomainBounceAggregate(domain="x.com")
        for _ in range(7):
            agg.record(OUTCOME_DELIVERED)
        for _ in range(3):
            agg.record(OUTCOME_HARD_BOUNCE)
        intel = bounce_aggregate_to_domain_intel(agg)
        assert intel["risk_level"] == RISK_LEVEL_HIGH
        assert intel["behavior_class"] == "known_risky"

    def test_insufficient_observations_maps_to_cold_start(self):
        agg = DomainBounceAggregate(domain="x.com")
        for _ in range(2):
            agg.record(OUTCOME_DELIVERED)
        intel = bounce_aggregate_to_domain_intel(agg)
        assert intel["risk_level"] == RISK_LEVEL_UNKNOWN
        assert intel["behavior_class"] == "cold_start"
        assert intel["cold_start"] is True


# ---------------------------------------------------------------------------
# Layer 8 — Cleaning pipeline unaffected
# ---------------------------------------------------------------------------


class TestCleaningPipelineUnaffected:
    def test_load_config_succeeds_with_v27_block(self):
        """Just loading the config must work — the bounce_ingestion
        block is parsed but doesn't gate any pipeline behaviour."""
        cfg = load_config(base_dir=resolve_project_paths().project_root)
        assert hasattr(cfg, "bounce_ingestion")
        assert cfg.bounce_ingestion.enabled is True

    def test_pipeline_module_does_not_import_feedback(self):
        """Sanity: the cleaning pipeline must not import the feedback
        module — feedback is strictly out-of-band."""
        from app import pipeline as pipeline_mod

        text = Path(pipeline_mod.__file__).read_text(encoding="utf-8")
        assert "validation_v2.feedback" not in text
        assert "ingest_bounce_outcomes" not in text


# ---------------------------------------------------------------------------
# Layer 9 — No live network
# ---------------------------------------------------------------------------


class TestNoLiveNetwork:
    def test_feedback_module_imports_no_network(self):
        from app.validation_v2 import feedback as mod

        # Module operates on local files + sqlite only.
        assert not hasattr(mod, "smtplib")
        assert not hasattr(mod, "socket")
        assert not hasattr(mod, "requests")
        assert not hasattr(mod, "urllib")

    def test_store_uses_sqlite_only(self, tmp_path: Path):
        """The SQLite store must use a local file path; no network connect."""
        store = BounceOutcomeStore(tmp_path / "bounce.sqlite")
        try:
            assert store.path == tmp_path / "bounce.sqlite"
            assert store.path.is_file()  # SQLite created the file locally.
        finally:
            store.close()
