"""Integration tests for cross-run dedup via EmailSendHistoryStore.

Pins the contract that ``SMTPVerificationStage`` consults the
persistent send-history store *before* invoking the probe function and
records every live probe outcome *after* the per-run cache write.

The fixtures here reuse the same lightweight stand-ins as
``test_v2_smtp_verification.py`` (a fake probe + fake config) so we can
flip ``email_send_history`` knobs without constructing a full
``AppConfig``.
"""

from __future__ import annotations

import pandas as pd
import pytest

from app.engine import ChunkPayload, PipelineContext
from app.engine.stages import SMTPVerificationStage
from app.engine.stages.smtp_verification import (
    SMTP_STATUS_INVALID,
    SMTP_STATUS_VALID,
)
from app.validation_v2.email_send_history import EmailSendHistoryStore
from app.validation_v2.smtp_probe import SMTPResult


# ---------------------------------------------------------------------------
# Helpers (mirrors test_v2_smtp_verification.py, kept local to this file)
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


def _result_dry_run() -> SMTPResult:
    return SMTPResult(
        success=False,
        response_code=None,
        response_message="dry_run",
        is_catch_all_like=False,
        inconclusive=True,
    )


def _candidate_frame(email: str = "alice@gmail.com") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "email": [email],
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


def _make_probe(result: SMTPResult):
    calls: list[str] = []

    def _probe(email: str, **_kwargs: object) -> SMTPResult:
        calls.append(email)
        return result

    _probe.calls = calls  # type: ignore[attr-defined]
    return _probe


class _FakeEmailHistoryConfig:
    def __init__(
        self,
        *,
        enabled: bool = True,
        sqlite_path: str = ":memory:",
        ttl_days: int = 30,
        force_resend: bool = False,
    ) -> None:
        self.enabled = enabled
        self.sqlite_path = sqlite_path
        self.ttl_days = ttl_days
        self.force_resend = force_resend


class _FakeSmtpProbeConfig:
    def __init__(
        self,
        *,
        enabled: bool = True,
        dry_run: bool = True,
        rate_limit_per_second: float = 0.0,
        max_candidates_per_run: int | None = None,
        retry_temp_failures: bool = False,
        max_retries: int = 0,
        timeout_seconds: float = 4.0,
        sender_address: str = "trashpanda-probe@localhost",
    ) -> None:
        self.enabled = enabled
        self.dry_run = dry_run
        self.rate_limit_per_second = rate_limit_per_second
        self.max_candidates_per_run = max_candidates_per_run
        self.retry_temp_failures = retry_temp_failures
        self.max_retries = max_retries
        self.timeout_seconds = timeout_seconds
        self.sender_address = sender_address


class _FakeAppConfig:
    def __init__(
        self,
        *,
        smtp_probe: _FakeSmtpProbeConfig | None = None,
        email_send_history: _FakeEmailHistoryConfig | None = None,
    ) -> None:
        self.smtp_probe = smtp_probe or _FakeSmtpProbeConfig()
        self.email_send_history = email_send_history or _FakeEmailHistoryConfig()


def _ctx_with_store(store: EmailSendHistoryStore, **history_kwargs) -> PipelineContext:
    """Build a PipelineContext that prefers the in-extras store.

    Tests pass an already-opened :class:`EmailSendHistoryStore` so we can
    inspect it after the run; the resolver in the stage will pick it up
    from ``context.extras`` before consulting the config path.
    """
    cfg = _FakeAppConfig(
        email_send_history=_FakeEmailHistoryConfig(**history_kwargs),
    )
    ctx = PipelineContext(extras={"email_send_history_store": store})
    ctx.config = cfg  # type: ignore[attr-defined]
    return ctx


# ---------------------------------------------------------------------------
# Recording: live probes write to the store
# ---------------------------------------------------------------------------


class TestRecording:
    def test_live_probe_outcome_is_recorded(self) -> None:
        probe = _make_probe(_result_valid())
        with EmailSendHistoryStore(":memory:") as store:
            ctx = _ctx_with_store(store)
            SMTPVerificationStage(probe_fn=probe).run(
                ChunkPayload(frame=_candidate_frame()), ctx
            )
            assert len(probe.calls) == 1
            rec = store.lookup("alice@gmail.com")
            assert rec is not None
            assert rec.last_status == SMTP_STATUS_VALID
            assert rec.last_response_code == 250
            assert rec.send_count == 1

    def test_invalid_outcome_is_recorded_with_correct_fields(self) -> None:
        probe = _make_probe(_result_invalid())
        with EmailSendHistoryStore(":memory:") as store:
            ctx = _ctx_with_store(store)
            SMTPVerificationStage(probe_fn=probe).run(
                ChunkPayload(frame=_candidate_frame()), ctx
            )
            rec = store.lookup("alice@gmail.com")
            assert rec is not None
            assert rec.last_status == SMTP_STATUS_INVALID
            assert rec.last_response_code == 550
            assert rec.last_was_success is False

    def test_dry_run_probe_is_never_recorded(self) -> None:
        """The dry-run sentinel result must not poison the store."""
        probe = _make_probe(_result_dry_run())
        with EmailSendHistoryStore(":memory:") as store:
            ctx = _ctx_with_store(store)
            SMTPVerificationStage(probe_fn=probe).run(
                ChunkPayload(frame=_candidate_frame()), ctx
            )
            assert len(probe.calls) == 1
            assert store.count() == 0


# ---------------------------------------------------------------------------
# Replay: a pre-existing record short-circuits the probe
# ---------------------------------------------------------------------------


class TestReplay:
    def test_fresh_history_hit_skips_probe_and_emits_persisted_status(self) -> None:
        with EmailSendHistoryStore(":memory:") as store:
            store.record(
                email_normalized="alice@gmail.com",
                domain="gmail.com",
                status=SMTP_STATUS_VALID,
                smtp_result="deliverable",
                response_code=250,
                response_message="2.1.5 Recipient OK",
                was_success=True,
                is_catch_all=False,
                inconclusive=False,
            )
            probe = _make_probe(_result_invalid())  # would flip the answer
            ctx = _ctx_with_store(store, ttl_days=30)
            out = SMTPVerificationStage(probe_fn=probe).run(
                ChunkPayload(frame=_candidate_frame()), ctx
            ).frame
            # The probe MUST NOT have been called; the persisted result
            # was replayed verbatim.
            assert probe.calls == []
            row = out.iloc[0]
            assert row["smtp_status"] == SMTP_STATUS_VALID
            assert int(row["smtp_response_code"]) == 250
            assert bool(row["smtp_tested"]) is True
            assert store.history_hits == 1

    def test_stale_history_record_is_ignored_and_probe_runs(self) -> None:
        from datetime import datetime, timedelta

        old = datetime.now() - timedelta(days=120)
        with EmailSendHistoryStore(":memory:") as store:
            store.record(
                now=old,
                email_normalized="alice@gmail.com",
                domain="gmail.com",
                status=SMTP_STATUS_VALID,
                smtp_result="deliverable",
                response_code=250,
                response_message="ok",
                was_success=True,
                is_catch_all=False,
                inconclusive=False,
            )
            probe = _make_probe(_result_invalid())
            ctx = _ctx_with_store(store, ttl_days=30)
            out = SMTPVerificationStage(probe_fn=probe).run(
                ChunkPayload(frame=_candidate_frame()), ctx
            ).frame
            # Stale record → fresh probe ran and the new outcome wins.
            assert len(probe.calls) == 1
            assert out.iloc[0]["smtp_status"] == SMTP_STATUS_INVALID
            # And the new outcome is now persisted with send_count=2.
            rec = store.lookup("alice@gmail.com")
            assert rec is not None
            assert rec.send_count == 2
            assert rec.last_status == SMTP_STATUS_INVALID

    def test_force_resend_bypasses_lookup_but_still_records(self) -> None:
        with EmailSendHistoryStore(":memory:") as store:
            store.record(
                email_normalized="alice@gmail.com",
                domain="gmail.com",
                status=SMTP_STATUS_VALID,
                smtp_result="deliverable",
                response_code=250,
                response_message="ok",
                was_success=True,
                is_catch_all=False,
                inconclusive=False,
            )
            probe = _make_probe(_result_invalid())
            ctx = _ctx_with_store(store, ttl_days=30, force_resend=True)
            out = SMTPVerificationStage(probe_fn=probe).run(
                ChunkPayload(frame=_candidate_frame()), ctx
            ).frame
            assert len(probe.calls) == 1  # probe ran despite fresh hit
            assert out.iloc[0]["smtp_status"] == SMTP_STATUS_INVALID
            # The new outcome overwrote the stored one.
            rec = store.lookup("alice@gmail.com")
            assert rec is not None
            assert rec.send_count == 2
            assert rec.last_status == SMTP_STATUS_INVALID


# ---------------------------------------------------------------------------
# End-to-end: re-running the same dataset twice produces zero re-probes
# ---------------------------------------------------------------------------


class TestRepeatRun:
    def test_second_run_over_same_data_does_not_reprobe(self) -> None:
        """The user's actual scenario: run the same CSV twice.

        On the first pass each candidate is probed once and recorded.
        On the second pass — over a *copy* of the same data — the
        store's hit completely replaces the probe call: zero new
        probes, identical canonical columns.
        """
        with EmailSendHistoryStore(":memory:") as store:
            probe1 = _make_probe(_result_valid())
            ctx1 = _ctx_with_store(store)
            SMTPVerificationStage(probe_fn=probe1).run(
                ChunkPayload(frame=_candidate_frame()), ctx1
            )
            assert len(probe1.calls) == 1

            # Second run, fresh PipelineContext (new per-run SMTP cache),
            # same store. The probe MUST NOT be invoked.
            probe2 = _make_probe(_result_invalid())  # would change the answer
            ctx2 = _ctx_with_store(store)
            out2 = SMTPVerificationStage(probe_fn=probe2).run(
                ChunkPayload(frame=_candidate_frame()), ctx2
            ).frame
            assert probe2.calls == []
            assert out2.iloc[0]["smtp_status"] == SMTP_STATUS_VALID
            assert store.history_hits == 1


# ---------------------------------------------------------------------------
# Disabled / absent config gracefully no-ops
# ---------------------------------------------------------------------------


class TestDisabled:
    def test_disabled_config_skips_lookup_and_record(self) -> None:
        probe = _make_probe(_result_valid())
        with EmailSendHistoryStore(":memory:") as store:
            cfg = _FakeAppConfig(
                email_send_history=_FakeEmailHistoryConfig(enabled=False),
            )
            # ``email_send_history_store`` deliberately NOT placed in
            # extras — with enabled=False the resolver should return
            # None even when a config block is present.
            ctx = PipelineContext(extras={})
            ctx.config = cfg  # type: ignore[attr-defined]
            SMTPVerificationStage(probe_fn=probe).run(
                ChunkPayload(frame=_candidate_frame()), ctx
            )
            assert len(probe.calls) == 1
            assert store.count() == 0  # nothing written

    def test_no_config_block_is_a_no_op(self) -> None:
        """A bare PipelineContext (no config) must not blow up; the
        existing test suite hits this code path for every legacy stage
        test."""
        probe = _make_probe(_result_valid())
        ctx = PipelineContext(extras={})
        out = SMTPVerificationStage(probe_fn=probe).run(
            ChunkPayload(frame=_candidate_frame()), ctx
        ).frame
        assert len(probe.calls) == 1
        assert out.iloc[0]["smtp_status"] == SMTP_STATUS_VALID
