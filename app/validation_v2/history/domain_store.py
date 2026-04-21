"""SQLite-backed domain history store."""

from __future__ import annotations

import time
from pathlib import Path

from .models import DomainHistoryRecord
from .sqlite import SQLiteHistoryDB


class DomainHistoryStore:
    def __init__(self, db: SQLiteHistoryDB | str | Path) -> None:
        self.db = db if isinstance(db, SQLiteHistoryDB) else SQLiteHistoryDB(db)

    def get(self, domain: str) -> DomainHistoryRecord | None:
        with self.db.connect() as connection:
            row = connection.execute(
                "SELECT * FROM domain_history WHERE domain = ?",
                (domain,),
            ).fetchone()
        if row is None:
            return None
        return DomainHistoryRecord(**dict(row))

    def upsert(self, record: DomainHistoryRecord) -> None:
        values = record.to_dict()
        columns = list(values)
        placeholders = ", ".join("?" for _ in columns)
        assignments = ", ".join(
            f"{column} = excluded.{column}" for column in columns if column != "domain"
        )
        sql = f"""
            INSERT INTO domain_history ({", ".join(columns)})
            VALUES ({placeholders})
            ON CONFLICT(domain) DO UPDATE SET {assignments}
        """
        with self.db.connect() as connection:
            connection.execute(sql, tuple(values[column] for column in columns))

    def delete_expired(self, now: float | None = None) -> int:
        cutoff = time.time() if now is None else float(now)
        with self.db.connect() as connection:
            cursor = connection.execute(
                """
                DELETE FROM domain_history
                WHERE ttl_expires_at IS NOT NULL
                  AND ttl_expires_at <= ?
                """,
                (cutoff,),
            )
            return int(cursor.rowcount)

    def list_recent(self, limit: int = 100) -> list[DomainHistoryRecord]:
        safe_limit = max(0, int(limit))
        with self.db.connect() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM domain_history
                ORDER BY last_seen_at DESC, domain ASC
                LIMIT ?
                """,
                (safe_limit,),
            ).fetchall()
        return [DomainHistoryRecord(**dict(row)) for row in rows]


__all__ = ["DomainHistoryStore"]
