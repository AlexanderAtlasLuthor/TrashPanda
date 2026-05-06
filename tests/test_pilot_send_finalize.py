"""V2.10.12 — finalize tests."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from app.db.pilot_send_tracker import (
    open_for_run as open_tracker,
    VERDICT_BLOCKED,
    VERDICT_DELIVERED,
    VERDICT_HARD_BOUNCE,
    VERDICT_SOFT_BOUNCE,
)
from app.pilot_send.config import (
    PilotMessageTemplate,
    PilotSendConfig,
    write_pilot_config,
)
from app.pilot_send.finalize import (
    DELIVERY_VERIFIED_XLSX,
    PILOT_HARD_BOUNCES_XLSX,
    PILOT_SEND_CANDIDATES_XLSX,
    PILOT_SOFT_BOUNCES_XLSX,
    PILOT_BLOCKED_OR_DEFERRED_XLSX,
    PILOT_SUMMARY_REPORT_XLSX,
    UPDATED_DO_NOT_SEND_XLSX,
    finalize_pilot,
)


def _setup_tracker(
    run_dir: Path,
    *,
    rows: list[tuple[str, str, str | None]],
    sent_at: datetime | None = None,
) -> None:
    """rows = [(verp_token, email, verdict_or_None_for_pending_send)]"""
    sent_at = sent_at or datetime.now(tz=timezone.utc)
    with open_tracker(run_dir) as tracker:
        for i, (token, email, verdict) in enumerate(rows):
            tracker.add_candidate(
                job_id="j1",
                batch_id="b1",
                source_row=i + 1,
                email=email,
                domain=email.rsplit("@", 1)[-1],
                provider_family="corporate_unknown",
                verp_token=token,
                now=sent_at,
            )
            row = tracker.by_token(token)
            assert row is not None
            if verdict is None:
                continue
            tracker.mark_sent(row.id, message_id=f"<{token}@x>", now=sent_at)
            if verdict in {VERDICT_DELIVERED, VERDICT_HARD_BOUNCE,
                           VERDICT_SOFT_BOUNCE, VERDICT_BLOCKED}:
                tracker.record_dsn(token, dsn_status=verdict, now=sent_at)


@pytest.fixture
def run_dir(tmp_path: Path):
    write_pilot_config(
        tmp_path,
        PilotSendConfig(
            template=PilotMessageTemplate(
                subject="x", body_text="y",
                sender_address="s@acme.com",
            ),
            return_path_domain="bounces.acme.com",
            authorization_confirmed=True,
        ),
    )
    return tmp_path


# --------------------------------------------------------------------- #
# No-op cases
# --------------------------------------------------------------------- #


class TestNoTracker:
    def test_no_tracker_returns_zero(self, run_dir: Path):
        result = finalize_pilot(run_dir, feed_bounce_ingestion=False)
        assert result.files_written == {}


# --------------------------------------------------------------------- #
# Happy mix of verdicts
# --------------------------------------------------------------------- #


class TestHappyPath:
    def test_emits_per_verdict_xlsx(self, run_dir: Path):
        _setup_tracker(
            run_dir,
            rows=[
                ("t1", "delivered@x.com", VERDICT_DELIVERED),
                ("t2", "hard@x.com", VERDICT_HARD_BOUNCE),
                ("t3", "soft@x.com", VERDICT_SOFT_BOUNCE),
                ("t4", "blocked@x.com", VERDICT_BLOCKED),
            ],
        )
        result = finalize_pilot(run_dir, feed_bounce_ingestion=False)
        files = result.files_written
        assert "delivery_verified" in files
        assert "pilot_hard_bounces" in files
        assert "pilot_soft_bounces" in files
        assert "pilot_blocked_or_deferred" in files
        assert "pilot_send_candidates" in files
        assert "pilot_summary_report" in files
        assert "updated_do_not_send" in files

        # Verify counts.
        assert result.counts.delivered == 1
        assert result.counts.hard_bounce == 1
        assert result.counts.soft_bounce == 1
        assert result.counts.blocked == 1

    def test_files_have_correct_rows(self, run_dir: Path):
        _setup_tracker(
            run_dir,
            rows=[
                ("t1", "ok@x.com", VERDICT_DELIVERED),
                ("t2", "hard@x.com", VERDICT_HARD_BOUNCE),
            ],
        )
        finalize_pilot(run_dir, feed_bounce_ingestion=False)
        delivered_df = pd.read_excel(run_dir / DELIVERY_VERIFIED_XLSX)
        hard_df = pd.read_excel(run_dir / PILOT_HARD_BOUNCES_XLSX)
        assert "ok@x.com" in delivered_df["email"].tolist()
        assert "hard@x.com" in hard_df["email"].tolist()


# --------------------------------------------------------------------- #
# Wait-window flips silent rows to delivered
# --------------------------------------------------------------------- #


class TestWaitWindow:
    def test_silent_rows_become_delivered(self, run_dir: Path):
        old_ts = datetime.now(tz=timezone.utc) - timedelta(hours=72)
        _setup_tracker(
            run_dir,
            rows=[
                ("silent", "silent@x.com", None),
            ],
            sent_at=old_ts,
        )
        # Mark sent without DSN.
        with open_tracker(run_dir) as tracker:
            row = tracker.by_token("silent")
            tracker.mark_sent(row.id, message_id=None, now=old_ts)
        result = finalize_pilot(run_dir, feed_bounce_ingestion=False)
        # 48h cutoff (default) — silent older than 48h flips to delivered.
        assert result.counts.delivered == 1


# --------------------------------------------------------------------- #
# do_not_send merge
# --------------------------------------------------------------------- #


class TestDoNotSendMerge:
    def test_merges_with_existing_do_not_send(self, run_dir: Path):
        # Pre-existing do_not_send.xlsx with 1 row.
        existing = pd.DataFrame(
            [{"email": "old@x.com", "domain": "x.com"}]
        )
        with pd.ExcelWriter(
            run_dir / "do_not_send.xlsx", engine="openpyxl",
        ) as writer:
            existing.to_excel(writer, sheet_name="do_not_send", index=False)

        _setup_tracker(
            run_dir,
            rows=[("t1", "newhard@x.com", VERDICT_HARD_BOUNCE)],
        )
        finalize_pilot(run_dir, feed_bounce_ingestion=False)

        merged = pd.read_excel(run_dir / UPDATED_DO_NOT_SEND_XLSX)
        emails = set(merged["email"].dropna().astype(str).tolist())
        assert "old@x.com" in emails
        assert "newhard@x.com" in emails

    def test_creates_when_missing(self, run_dir: Path):
        _setup_tracker(
            run_dir,
            rows=[("t1", "hard@x.com", VERDICT_HARD_BOUNCE)],
        )
        finalize_pilot(run_dir, feed_bounce_ingestion=False)
        assert (run_dir / UPDATED_DO_NOT_SEND_XLSX).is_file()


# --------------------------------------------------------------------- #
# Idempotence
# --------------------------------------------------------------------- #


class TestIdempotence:
    def test_re_run_safe(self, run_dir: Path):
        _setup_tracker(
            run_dir,
            rows=[("t1", "hard@x.com", VERDICT_HARD_BOUNCE)],
        )
        finalize_pilot(run_dir, feed_bounce_ingestion=False)
        # Second call should work without raising and produce same files.
        result2 = finalize_pilot(run_dir, feed_bounce_ingestion=False)
        assert result2.counts.hard_bounce == 1
