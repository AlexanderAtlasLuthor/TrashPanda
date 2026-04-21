"""SQLite staging for Subphase 8: first-pass persistence of all pipeline rows."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Iterator

import pandas as pd


_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS staged_rows (
    row_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file         TEXT    NOT NULL DEFAULT '',
    source_row_number   INTEGER NOT NULL DEFAULT 0,
    chunk_index         INTEGER NOT NULL DEFAULT 0,
    global_ordinal      INTEGER NOT NULL DEFAULT 0,
    email_normalized    TEXT,
    hard_fail           INTEGER NOT NULL DEFAULT 0,
    score               INTEGER NOT NULL DEFAULT 0,
    preliminary_bucket  TEXT    NOT NULL DEFAULT '',
    completeness_score  INTEGER NOT NULL DEFAULT 0,
    is_canonical        INTEGER NOT NULL DEFAULT 0,
    duplicate_flag      INTEGER NOT NULL DEFAULT 0,
    duplicate_reason    TEXT,
    row_json            TEXT    NOT NULL
)
"""

_INSERT_SQL = """
INSERT INTO staged_rows (
    source_file, source_row_number, chunk_index, global_ordinal,
    email_normalized, hard_fail, score, preliminary_bucket,
    completeness_score, is_canonical, duplicate_flag, duplicate_reason,
    row_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def _to_native(val: Any) -> Any:
    """Convert pandas/numpy scalars to JSON-safe Python native types."""
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(val, "item"):
        return val.item()
    return val


class StagingDB:
    """SQLite-backed staging store for one pipeline run.

    Rows are appended after Subphase 7 and read back in the second pass
    (Subphase 8 materialization) to apply final canonical decisions.
    """

    def __init__(self, db_path: Path) -> None:
        self._path = db_path
        self._conn = sqlite3.connect(str(db_path))
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.execute(_CREATE_TABLE_SQL)
        self._conn.commit()

    def append_chunk(self, chunk: pd.DataFrame) -> None:
        """Persist all rows in *chunk* to staging."""
        rows: list[tuple] = []
        # to_dict("records") is ~10-50x faster than iterrows() because it
        # avoids creating a pandas Series object per row.
        for raw_row in chunk.to_dict("records"):
            row_dict: dict[str, Any] = {col: _to_native(v) for col, v in raw_row.items()}

            rows.append((
                str(row_dict.get("source_file") or ""),
                int(row_dict.get("source_row_number") or 0),
                int(row_dict.get("chunk_index") or 0),
                int(row_dict.get("global_ordinal") or 0),
                row_dict.get("email_normalized"),
                1 if row_dict.get("hard_fail") else 0,
                int(row_dict.get("score") or 0),
                str(row_dict.get("preliminary_bucket") or ""),
                int(row_dict.get("completeness_score") or 0),
                1 if row_dict.get("is_canonical") else 0,
                1 if row_dict.get("duplicate_flag") else 0,
                row_dict.get("duplicate_reason"),
                json.dumps(row_dict, default=str),
            ))

        self._conn.executemany(_INSERT_SQL, rows)
        self._conn.commit()

    def iter_all_rows(self, batch_size: int = 2000) -> Iterator[list[dict[str, Any]]]:
        """Yield batches of rows in insertion order.

        Each row is the deserialized JSON dict with key fields overridden by
        the typed dedicated columns for reliable second-pass evaluation.
        """
        offset = 0
        while True:
            cursor = self._conn.execute(
                "SELECT source_file, source_row_number, email_normalized, "
                "hard_fail, preliminary_bucket, is_canonical, duplicate_flag, "
                "duplicate_reason, global_ordinal, row_json "
                "FROM staged_rows ORDER BY row_id "
                "LIMIT ? OFFSET ?",
                (batch_size, offset),
            )
            db_rows = cursor.fetchall()
            if not db_rows:
                break

            batch: list[dict[str, Any]] = []
            for db_row in db_rows:
                row_dict: dict[str, Any] = json.loads(db_row[9])
                row_dict["source_file"] = db_row[0]
                row_dict["source_row_number"] = db_row[1]
                row_dict["email_normalized"] = db_row[2]
                row_dict["hard_fail"] = bool(db_row[3])
                row_dict["preliminary_bucket"] = db_row[4]
                row_dict["is_canonical"] = bool(db_row[5])
                row_dict["duplicate_flag"] = bool(db_row[6])
                row_dict["duplicate_reason"] = db_row[7]
                row_dict["global_ordinal"] = db_row[8]
                batch.append(row_dict)

            yield batch
            offset += batch_size

    def row_count(self) -> int:
        """Return the total number of rows in staging."""
        cursor = self._conn.execute("SELECT COUNT(*) FROM staged_rows")
        return cursor.fetchone()[0]

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()
