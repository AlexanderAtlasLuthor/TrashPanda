"""SQLite-backed append-only probe event store."""

from __future__ import annotations

from pathlib import Path

from .models import ProbeEventRecord
from .sqlite import SQLiteHistoryDB


class ProbeEventStore:
    def __init__(self, db: SQLiteHistoryDB | str | Path) -> None:
        self.db = db if isinstance(db, SQLiteHistoryDB) else SQLiteHistoryDB(db)

    def append(self, event: ProbeEventRecord) -> None:
        values = event.to_dict()
        values["retry_attempted"] = 1 if event.retry_attempted else 0
        columns = list(values)
        placeholders = ", ".join("?" for _ in columns)
        with self.db.connect() as connection:
            connection.execute(
                f"""
                INSERT INTO probe_events ({", ".join(columns)})
                VALUES ({placeholders})
                """,
                tuple(values[column] for column in columns),
            )

    def list_by_domain(
        self, domain: str, limit: int = 100
    ) -> list[ProbeEventRecord]:
        safe_limit = max(0, int(limit))
        with self.db.connect() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM probe_events
                WHERE domain = ?
                ORDER BY timestamp DESC, event_id ASC
                LIMIT ?
                """,
                (domain, safe_limit),
            ).fetchall()
        return [_event_from_row(dict(row)) for row in rows]

    def list_recent(self, limit: int = 100) -> list[ProbeEventRecord]:
        safe_limit = max(0, int(limit))
        with self.db.connect() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM probe_events
                ORDER BY timestamp DESC, event_id ASC
                LIMIT ?
                """,
                (safe_limit,),
            ).fetchall()
        return [_event_from_row(dict(row)) for row in rows]

    def delete_older_than(self, cutoff_ts: float) -> int:
        with self.db.connect() as connection:
            cursor = connection.execute(
                "DELETE FROM probe_events WHERE timestamp < ?",
                (float(cutoff_ts),),
            )
            return int(cursor.rowcount)


def _event_from_row(row: dict[str, object]) -> ProbeEventRecord:
    row["retry_attempted"] = bool(row["retry_attempted"])
    return ProbeEventRecord(**row)


__all__ = ["ProbeEventStore"]
