"""SQLite-backed provider history store."""

from __future__ import annotations

import time
from pathlib import Path

from .models import ProviderHistoryRecord
from .sqlite import SQLiteHistoryDB


class ProviderHistoryStore:
    def __init__(self, db: SQLiteHistoryDB | str | Path) -> None:
        self.db = db if isinstance(db, SQLiteHistoryDB) else SQLiteHistoryDB(db)

    def get(self, provider_key: str) -> ProviderHistoryRecord | None:
        with self.db.connect() as connection:
            row = connection.execute(
                "SELECT * FROM provider_history WHERE provider_key = ?",
                (provider_key,),
            ).fetchone()
        if row is None:
            return None
        return ProviderHistoryRecord(**dict(row))

    def upsert(self, record: ProviderHistoryRecord) -> None:
        values = record.to_dict()
        columns = list(values)
        placeholders = ", ".join("?" for _ in columns)
        assignments = ", ".join(
            f"{column} = excluded.{column}"
            for column in columns
            if column != "provider_key"
        )
        sql = f"""
            INSERT INTO provider_history ({", ".join(columns)})
            VALUES ({placeholders})
            ON CONFLICT(provider_key) DO UPDATE SET {assignments}
        """
        with self.db.connect() as connection:
            connection.execute(sql, tuple(values[column] for column in columns))

    def delete_expired(self, now: float | None = None) -> int:
        cutoff = time.time() if now is None else float(now)
        with self.db.connect() as connection:
            cursor = connection.execute(
                """
                DELETE FROM provider_history
                WHERE ttl_expires_at IS NOT NULL
                  AND ttl_expires_at <= ?
                """,
                (cutoff,),
            )
            return int(cursor.rowcount)

    def list_recent(self, limit: int = 100) -> list[ProviderHistoryRecord]:
        safe_limit = max(0, int(limit))
        with self.db.connect() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM provider_history
                ORDER BY last_seen_at DESC, provider_key ASC
                LIMIT ?
                """,
                (safe_limit,),
            ).fetchall()
        return [ProviderHistoryRecord(**dict(row)) for row in rows]


__all__ = ["ProviderHistoryStore"]
