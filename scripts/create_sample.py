"""Create a small sample CSV from the WY dataset.

Usage:
    python scripts/create_sample.py --rows 1000
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd


def find_source(project_root: Path) -> Path:
    candidates = [
        project_root / "WY.csv",
        project_root / "data" / "WY.csv",
        project_root / "input" / "WY.csv",
    ]
    for c in candidates:
        if c.is_file():
            return c
    raise FileNotFoundError(
        "Could not locate WY.csv in project root, data/, or input/."
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Create a small WY sample CSV.")
    parser.add_argument("--rows", type=int, default=1000, help="Number of rows to take from the head (default: 1000).")
    parser.add_argument("--source", type=Path, default=None, help="Optional explicit path to source CSV.")
    parser.add_argument("--output", type=Path, default=None, help="Optional explicit output path.")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent
    source = args.source if args.source else find_source(project_root)
    output = args.output if args.output else project_root / "examples" / "WY_small.csv"

    output.parent.mkdir(parents=True, exist_ok=True)

    # Read only the rows we need for efficiency on large files.
    df = pd.read_csv(source, nrows=args.rows, dtype=str, keep_default_na=False, encoding="utf-8")

    df.to_csv(output, index=False, encoding="utf-8")

    # Validation
    check = pd.read_csv(output, dtype=str, keep_default_na=False, encoding="utf-8")
    original_headers = pd.read_csv(source, nrows=0, encoding="utf-8").columns.tolist()

    assert list(check.columns) == original_headers, "Header mismatch between source and sample."
    assert len(check) == args.rows, f"Expected {args.rows} rows, got {len(check)}."

    print(f"Source:  {source}")
    print(f"Output:  {output}")
    print(f"Rows:    {len(check)}")
    print(f"Headers: {len(check.columns)} columns match original")
    return 0


if __name__ == "__main__":
    sys.exit(main())
