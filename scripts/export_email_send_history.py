"""CLI to export the persistent email send history as a CSV.

Usage::

    python -m scripts.export_email_send_history \\
        --db runtime/history/email_send_history.sqlite \\
        --out runtime/reports/email_send_history.csv

Pass ``--out -`` to stream the CSV to stdout. Without ``--db`` the
default path from ``configs/default.yaml`` (loaded via
:func:`app.config.load_config`) is used so an operator with no flags
still gets the right file.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from app.config import load_config
from app.validation_v2.email_send_history import EmailSendHistoryStore


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="export_email_send_history",
        description=(
            "Export the per-email SMTP send history (cross-run dedup "
            "ledger) to CSV for audit. The store is the same one the "
            "SMTP verification stage consults to skip re-probing "
            "addresses that were already validated in a previous run."
        ),
    )
    parser.add_argument(
        "--db",
        default=None,
        help=(
            "Path to the email_send_history SQLite file. Defaults to "
            "the path declared in configs/default.yaml under "
            "email_send_history.sqlite_path."
        ),
    )
    parser.add_argument(
        "--out",
        default="-",
        help="Output CSV path. Use '-' for stdout (default).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.db:
        db_path = Path(args.db)
    else:
        cfg = load_config()
        db_path = Path(cfg.email_send_history.sqlite_path)

    if not db_path.exists():
        print(
            f"[export_email_send_history] no database at {db_path} — "
            "nothing to export.",
            file=sys.stderr,
        )
        return 1

    with EmailSendHistoryStore(db_path) as store:
        rows = store.export_csv(args.out)

    where = "stdout" if args.out == "-" else args.out
    print(
        f"[export_email_send_history] wrote {rows} rows to {where} "
        f"(source: {db_path}).",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
