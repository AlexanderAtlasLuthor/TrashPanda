"""V2.10.14 — sender_reputation tests."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.sender_reputation import (
    DEFAULT_FRESHNESS_HOURS,
    REPUTATION_DB_FILENAME,
    ReputationSnapshot,
    SOURCE_MANUAL,
    SOURCE_RBL,
    SOURCE_SNDS,
    STATUS_GREEN,
    STATUS_RED,
    STATUS_YELLOW,
    import_snds_csv,
    is_safe_to_pilot,
    latest_for_ip,
    latest_per_source,
    open_store,
    record_snapshot,
)


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / REPUTATION_DB_FILENAME


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


class TestStoreBasics:
    def test_record_and_retrieve(self, db_path: Path):
        snap = ReputationSnapshot(
            ip="1.2.3.4",
            source=SOURCE_MANUAL,
            captured_at=_now(),
            status=STATUS_GREEN,
            notes="all good",
        )
        with open_store(db_path) as conn:
            record_snapshot(conn, snap)
            got = latest_for_ip(conn, "1.2.3.4")
        assert got is not None
        assert got.ip == "1.2.3.4"
        assert got.status == STATUS_GREEN
        assert got.notes == "all good"

    def test_unknown_ip_returns_none(self, db_path: Path):
        with open_store(db_path) as conn:
            got = latest_for_ip(conn, "9.9.9.9")
        assert got is None

    def test_latest_per_source_picks_most_recent(self, db_path: Path):
        old = _now() - timedelta(hours=2)
        new = _now()
        with open_store(db_path) as conn:
            record_snapshot(conn, ReputationSnapshot(
                ip="1.1.1.1", source=SOURCE_SNDS,
                captured_at=old, status=STATUS_RED,
            ))
            record_snapshot(conn, ReputationSnapshot(
                ip="1.1.1.1", source=SOURCE_SNDS,
                captured_at=new, status=STATUS_GREEN,
            ))
            record_snapshot(conn, ReputationSnapshot(
                ip="1.1.1.1", source=SOURCE_RBL,
                captured_at=new, status=STATUS_YELLOW,
            ))
            per_source = latest_per_source(conn, "1.1.1.1")

        assert per_source[SOURCE_SNDS].status == STATUS_GREEN
        assert per_source[SOURCE_RBL].status == STATUS_YELLOW

    def test_duplicate_unique_key_is_idempotent(self, db_path: Path):
        when = _now()
        snap = ReputationSnapshot(
            ip="1.1.1.1", source=SOURCE_SNDS,
            captured_at=when, status=STATUS_GREEN,
        )
        with open_store(db_path) as conn:
            record_snapshot(conn, snap)
            record_snapshot(conn, snap)  # second insert silently ignored
            count = conn.execute(
                "SELECT count(*) FROM sender_reputation"
            ).fetchone()[0]
        assert count == 1


class TestGate:
    def test_no_data_is_safe_unknown(self, db_path: Path):
        with open_store(db_path) as conn:
            decision = is_safe_to_pilot(conn, "5.5.5.5")
        assert decision.safe is True
        assert decision.overall_status == "unknown"

    def test_red_blocks(self, db_path: Path):
        with open_store(db_path) as conn:
            record_snapshot(conn, ReputationSnapshot(
                ip="2.2.2.2", source=SOURCE_SNDS,
                captured_at=_now(), status=STATUS_RED,
                complaint_rate=0.012,
            ))
            decision = is_safe_to_pilot(conn, "2.2.2.2")
        assert decision.safe is False
        assert decision.overall_status == STATUS_RED

    def test_yellow_warns_but_safe(self, db_path: Path):
        with open_store(db_path) as conn:
            record_snapshot(conn, ReputationSnapshot(
                ip="2.2.2.2", source=SOURCE_SNDS,
                captured_at=_now(), status=STATUS_YELLOW,
            ))
            decision = is_safe_to_pilot(conn, "2.2.2.2")
        assert decision.safe is True
        assert decision.overall_status == STATUS_YELLOW

    def test_red_in_one_source_dominates_green_in_another(self, db_path: Path):
        with open_store(db_path) as conn:
            record_snapshot(conn, ReputationSnapshot(
                ip="3.3.3.3", source=SOURCE_SNDS,
                captured_at=_now(), status=STATUS_GREEN,
            ))
            record_snapshot(conn, ReputationSnapshot(
                ip="3.3.3.3", source=SOURCE_RBL,
                captured_at=_now(), status=STATUS_RED,
                notes="Spamhaus listing",
            ))
            decision = is_safe_to_pilot(conn, "3.3.3.3")
        assert decision.safe is False
        assert decision.overall_status == STATUS_RED

    def test_stale_snapshots_are_ignored(self, db_path: Path):
        # A red snapshot older than the freshness window should not
        # block — operator hasn't refreshed in days.
        old = _now() - timedelta(hours=DEFAULT_FRESHNESS_HOURS + 24)
        with open_store(db_path) as conn:
            record_snapshot(conn, ReputationSnapshot(
                ip="4.4.4.4", source=SOURCE_SNDS,
                captured_at=old, status=STATUS_RED,
            ))
            decision = is_safe_to_pilot(conn, "4.4.4.4")
        assert decision.safe is True
        assert decision.overall_status == "unknown"


class TestSNDSImport:
    def test_imports_microsoft_csv_format(self, tmp_path: Path):
        csv_text = (
            "IP Address,Activity start (UTC),Filter result,Complaint rate,"
            "Trap message count\n"
            "192.3.105.145,2026-05-06T00:00:00Z,red,0.0072,3\n"
            "8.8.8.8,2026-05-06T00:00:00Z,green,0.0001,0\n"
        )
        path = tmp_path / "snds.csv"
        path.write_text(csv_text, encoding="utf-8")

        with open_store(tmp_path / "rep.sqlite") as conn:
            n = import_snds_csv(conn, path)
            decision_red = is_safe_to_pilot(conn, "192.3.105.145")
            decision_green = is_safe_to_pilot(conn, "8.8.8.8")

        assert n == 2
        assert decision_red.safe is False
        assert decision_red.overall_status == STATUS_RED
        assert decision_green.safe is True
        assert decision_green.overall_status == STATUS_GREEN

    def test_classification_when_microsoft_filter_result_missing(
        self, tmp_path: Path,
    ):
        # Microsoft sometimes ships rows without the "Filter result"
        # column. We compute from complaint_rate / trap_count.
        csv_text = (
            "IP Address,Complaint rate,Trap message count\n"
            "1.1.1.1,0.0040,0\n"   # 0.4% → yellow
            "2.2.2.2,0.0001,0\n"   # green
            "3.3.3.3,0.0000,1\n"   # any trap → red
        )
        path = tmp_path / "snds.csv"
        path.write_text(csv_text, encoding="utf-8")

        with open_store(tmp_path / "rep.sqlite") as conn:
            import_snds_csv(conn, path)
            assert is_safe_to_pilot(conn, "1.1.1.1").overall_status == STATUS_YELLOW
            assert is_safe_to_pilot(conn, "2.2.2.2").overall_status == STATUS_GREEN
            assert is_safe_to_pilot(conn, "3.3.3.3").overall_status == STATUS_RED

    def test_missing_csv_raises(self, tmp_path: Path):
        with open_store(tmp_path / "rep.sqlite") as conn:
            with pytest.raises(FileNotFoundError):
                import_snds_csv(conn, tmp_path / "no-such.csv")
