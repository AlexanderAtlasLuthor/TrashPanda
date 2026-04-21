"""SQLite helper for Validation Engine V2 persistent history storage."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterator


class SQLiteHistoryDB:
    """Small SQLite connection helper with explicit schema creation."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(db_path)
        self.initialize()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.executescript(SCHEMA_SQL)

    def schema_objects(self) -> Iterator[sqlite3.Row]:
        with self.connect() as connection:
            yield from connection.execute(
                """
                SELECT type, name, tbl_name, sql
                FROM sqlite_master
                WHERE type IN ('table', 'index')
                ORDER BY type, name
                """
            )


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS domain_history (
    domain TEXT PRIMARY KEY,
    provider_type TEXT,
    provider_hint TEXT,
    first_seen_at REAL NOT NULL,
    last_seen_at REAL NOT NULL,
    ttl_expires_at REAL,
    total_observations INTEGER NOT NULL,
    smtp_attempt_count INTEGER NOT NULL,
    smtp_valid_count INTEGER NOT NULL,
    smtp_invalid_count INTEGER NOT NULL,
    smtp_uncertain_count INTEGER NOT NULL,
    timeout_count INTEGER NOT NULL,
    retry_count INTEGER NOT NULL,
    catch_all_confirmed_count INTEGER NOT NULL,
    catch_all_likely_count INTEGER NOT NULL,
    catch_all_unlikely_count INTEGER NOT NULL,
    last_smtp_status TEXT,
    last_catch_all_status TEXT,
    last_deliverability_probability REAL,
    last_validation_status TEXT,
    domain_reputation_score REAL,
    domain_reputation_confidence REAL
);

CREATE TABLE IF NOT EXISTS provider_history (
    provider_key TEXT PRIMARY KEY,
    provider_type TEXT,
    first_seen_at REAL NOT NULL,
    last_seen_at REAL NOT NULL,
    ttl_expires_at REAL,
    total_domains_seen INTEGER NOT NULL,
    total_observations INTEGER NOT NULL,
    smtp_valid_count INTEGER NOT NULL,
    smtp_invalid_count INTEGER NOT NULL,
    smtp_uncertain_count INTEGER NOT NULL,
    timeout_count INTEGER NOT NULL,
    catch_all_confirmed_count INTEGER NOT NULL,
    catch_all_likely_count INTEGER NOT NULL,
    catch_all_unlikely_count INTEGER NOT NULL,
    provider_reputation_score REAL,
    provider_reputation_confidence REAL
);

CREATE TABLE IF NOT EXISTS probe_events (
    event_id TEXT PRIMARY KEY,
    timestamp REAL NOT NULL,
    domain TEXT NOT NULL,
    provider_key TEXT,
    smtp_status TEXT,
    smtp_code INTEGER,
    smtp_error_type TEXT,
    catch_all_status TEXT,
    retry_attempted INTEGER NOT NULL,
    retry_outcome TEXT,
    deliverability_probability REAL,
    validation_status TEXT
);

CREATE INDEX IF NOT EXISTS idx_probe_events_domain
    ON probe_events(domain);
CREATE INDEX IF NOT EXISTS idx_probe_events_timestamp
    ON probe_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_provider_history_provider_key
    ON provider_history(provider_key);
"""


__all__ = ["SQLiteHistoryDB", "SCHEMA_SQL"]
