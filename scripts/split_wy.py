"""Split WY.csv into N equal chunks (header preserved in each).

Input file is never modified. Output goes to input/wy_chunks/.
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path


def split_csv(src: Path, out_dir: Path, num_chunks: int = 10) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)

    with src.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        rows = list(reader)

    total = len(rows)
    chunk_size = (total + num_chunks - 1) // num_chunks
    paths: list[Path] = []
    for i in range(num_chunks):
        start = i * chunk_size
        end = min(start + chunk_size, total)
        if start >= total:
            break
        part_path = out_dir / f"WY_part_{i+1:02d}.csv"
        with part_path.open("w", encoding="utf-8", newline="") as out:
            writer = csv.writer(out)
            writer.writerow(header)
            writer.writerows(rows[start:end])
        paths.append(part_path)
        print(f"wrote {part_path} ({end - start} rows)")
    print(f"Done. {total} rows split into {len(paths)} chunks.")
    return paths


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    split_csv(root / "WY.csv", root / "input" / "wy_chunks", num_chunks=10)
