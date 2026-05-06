"""Tests for ``scripts/auto_chunked_clean``.

The orchestrator is tested by stubbing out the ``run_chunk`` and
``merge_bundles`` functions so we never spawn a real subprocess and
never run the real pipeline. What we DO exercise:

* row counting + format detection
* CSV splitting with header preservation
* state file shape + transitions
* resume after a crash
* allow-partial vs strict failure modes
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from unittest import mock

import pandas as pd
import pytest

from scripts import auto_chunked_clean as acc
from app.customer_bundle import (
    CLEAN_DELIVERABLE_CSV,
    CUSTOMER_BUNDLE_DIRNAME,
    HIGH_RISK_REMOVED_CSV,
    REVIEW_PROVIDER_LIMITED_CSV,
)


def _write_csv(path: Path, n_rows: int, *, header: str = "email") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        w.writerow([header])
        for i in range(n_rows):
            w.writerow([f"user{i}@example.com"])


def _fake_bundle(
    run_dir: Path,
    *,
    clean: int,
    removed: int,
    review: int,
    chunk_index: int = 0,
) -> None:
    """Mimic what defensive_clean would write so _read_bundle_counts
    sees a valid bundle. ``chunk_index`` namespaces the email values
    so the merger doesn't dedupe across chunks in tests."""
    bundle = run_dir / CUSTOMER_BUNDLE_DIRNAME
    bundle.mkdir(parents=True, exist_ok=True)
    prefix = f"c{chunk_index}_"
    pd.DataFrame({
        "email": [f"{prefix}clean{i}@x.com" for i in range(clean)],
    }).to_csv(bundle / CLEAN_DELIVERABLE_CSV, index=False)
    pd.DataFrame({
        "email": [f"{prefix}removed{i}@x.com" for i in range(removed)],
    }).to_csv(bundle / HIGH_RISK_REMOVED_CSV, index=False)
    pd.DataFrame({
        "email": [f"{prefix}review{i}@x.com" for i in range(review)],
    }).to_csv(bundle / REVIEW_PROVIDER_LIMITED_CSV, index=False)


def _stub_run_chunk(success_per_chunk: dict[int, tuple[int, int, int]]):
    """Build a stub for ``run_chunk`` that writes a fake bundle into
    the run_dir based on a predefined per-chunk-index mapping
    (chunk_index → (clean, removed, review))."""
    call_log: list[tuple[Path, Path]] = []

    def _stub(chunk_input: Path, run_dir: Path, *, extra_env=None):
        call_log.append((chunk_input, run_dir))
        # Find the chunk index from the file name (`*_part_<i>.csv`).
        idx = int(chunk_input.stem.split("_part_")[-1])
        triple = success_per_chunk.get(idx)
        if triple is None:
            return 1, "boom"
        _fake_bundle(
            run_dir, clean=triple[0], removed=triple[1], review=triple[2],
            chunk_index=idx,
        )
        return 0, "ok"

    _stub.calls = call_log  # type: ignore[attr-defined]
    return _stub


# ---------------------------------------------------------------------------
# Row counting
# ---------------------------------------------------------------------------


class TestCountRows:
    def test_counts_csv_excluding_header(self, tmp_path: Path):
        p = tmp_path / "x.csv"
        _write_csv(p, n_rows=42)
        assert acc._count_rows(p, "csv") == 42

    def test_counts_xlsx_excluding_header(self, tmp_path: Path):
        p = tmp_path / "x.xlsx"
        df = pd.DataFrame({"email": [f"u{i}@x.com" for i in range(15)]})
        df.to_excel(p, index=False)
        assert acc._count_rows(p, "xlsx") == 15


# ---------------------------------------------------------------------------
# Splitting
# ---------------------------------------------------------------------------


class TestSplitCsv:
    def test_each_chunk_has_header_plus_chunksize_rows(self, tmp_path: Path):
        src = tmp_path / "input.csv"
        _write_csv(src, n_rows=100)
        out = tmp_path / "chunks"
        chunks = acc._split_csv(src, chunk_size=30, out_dir=out)
        assert [p.name for p in chunks] == [
            "input_part_1.csv", "input_part_2.csv",
            "input_part_3.csv", "input_part_4.csv",
        ]
        # First three chunks have 30 rows, last has 10.
        sizes = []
        for p in chunks:
            with p.open("r") as fh:
                sizes.append(sum(1 for _ in fh) - 1)  # minus header
        assert sizes == [30, 30, 30, 10]
        # Header preserved on every chunk.
        for p in chunks:
            with p.open("r") as fh:
                first = next(csv.reader(fh))
            assert first == ["email"]


# ---------------------------------------------------------------------------
# Orchestrator: below threshold (no splitting)
# ---------------------------------------------------------------------------


class TestBelowThreshold:
    def test_runs_defensive_clean_directly(
        self, tmp_path: Path, monkeypatch,
    ):
        src = tmp_path / "small.csv"
        _write_csv(src, n_rows=100)
        out = tmp_path / "out"
        stub = _stub_run_chunk({1: (80, 15, 5)})

        # The single-run path reuses run_chunk but with chunk_input==src;
        # the index parser would fail on that name, so we patch
        # _read_bundle_counts directly to return canned counts and
        # patch run_chunk to just create the bundle in the out_dir.
        def _direct_run(src_path, run_dir, *, extra_env=None):
            _fake_bundle(run_dir, clean=80, removed=15, review=5)
            return 0, "ok"

        monkeypatch.setattr(acc, "run_chunk", _direct_run)

        opts = acc.OrchestratorOptions(
            input_file=src, output_dir=out,
            chunk_size=25, threshold_rows=200,  # threshold > rows
        )
        state = acc.run(opts)

        assert state.status == acc.BATCH_COMPLETED
        assert state.merged_counts == {
            "clean_deliverable": 80,
            "high_risk_removed": 15,
            "review_provider_limited": 5,
        }
        # State file written.
        assert (out / acc.STATUS_FILE).is_file()


# ---------------------------------------------------------------------------
# Orchestrator: chunked path
# ---------------------------------------------------------------------------


class TestChunkedHappyPath:
    def test_splits_runs_each_then_merges(
        self, tmp_path: Path, monkeypatch,
    ):
        src = tmp_path / "input.csv"
        _write_csv(src, n_rows=100)
        out = tmp_path / "out"
        stub = _stub_run_chunk({
            1: (20, 4, 1),
            2: (22, 2, 1),
            3: (21, 3, 1),
            4: (23, 1, 1),
        })
        monkeypatch.setattr(acc, "run_chunk", stub)

        opts = acc.OrchestratorOptions(
            input_file=src, output_dir=out,
            chunk_size=25, threshold_rows=50,
        )
        state = acc.run(opts)

        assert state.status == acc.BATCH_COMPLETED
        assert len(state.chunks) == 4
        assert all(c.status == acc.CHUNK_COMPLETED for c in state.chunks)
        # Merge totals = sum across chunks.
        assert state.merged_counts["clean_deliverable"] == 86
        assert state.merged_counts["high_risk_removed"] == 10
        assert state.merged_counts["review_provider_limited"] == 4
        # The merged bundle is on disk.
        assert (out / CUSTOMER_BUNDLE_DIRNAME / CLEAN_DELIVERABLE_CSV).is_file()


# ---------------------------------------------------------------------------
# Orchestrator: failure modes
# ---------------------------------------------------------------------------


class TestStrictFailure:
    def test_aborts_on_first_failed_chunk_by_default(
        self, tmp_path: Path, monkeypatch,
    ):
        src = tmp_path / "input.csv"
        _write_csv(src, n_rows=100)
        out = tmp_path / "out"
        # Chunk 2 fails; chunks 3-4 should never run.
        stub = _stub_run_chunk({1: (20, 5, 0), 3: (20, 5, 0), 4: (20, 5, 0)})
        monkeypatch.setattr(acc, "run_chunk", stub)

        opts = acc.OrchestratorOptions(
            input_file=src, output_dir=out,
            chunk_size=25, threshold_rows=50,
            allow_partial=False,
        )
        state = acc.run(opts)

        assert state.status == acc.BATCH_FAILED
        assert state.chunks[0].status == acc.CHUNK_COMPLETED
        assert state.chunks[1].status == acc.CHUNK_FAILED
        # Chunks 3-4 stayed pending.
        assert state.chunks[2].status == acc.CHUNK_PENDING
        assert state.chunks[3].status == acc.CHUNK_PENDING


class TestAllowPartial:
    def test_continues_past_failure_and_merges_completed(
        self, tmp_path: Path, monkeypatch,
    ):
        src = tmp_path / "input.csv"
        _write_csv(src, n_rows=100)
        out = tmp_path / "out"
        # Chunk 3 fails; the other 3 succeed.
        stub = _stub_run_chunk({1: (20, 4, 1), 2: (21, 3, 1), 4: (22, 2, 1)})
        monkeypatch.setattr(acc, "run_chunk", stub)

        opts = acc.OrchestratorOptions(
            input_file=src, output_dir=out,
            chunk_size=25, threshold_rows=50,
            allow_partial=True,
        )
        state = acc.run(opts)

        assert state.status == acc.BATCH_PARTIAL_FAILURE
        assert state.chunks[2].status == acc.CHUNK_FAILED
        # Merge has 3 bundles' worth.
        assert state.merged_counts["clean_deliverable"] == 63


# ---------------------------------------------------------------------------
# Orchestrator: resume
# ---------------------------------------------------------------------------


class TestResume:
    def test_resumes_from_state_file_skipping_completed(
        self, tmp_path: Path, monkeypatch,
    ):
        src = tmp_path / "input.csv"
        _write_csv(src, n_rows=100)
        out = tmp_path / "out"

        # First run: chunks 1-2 succeed, chunk 3 fails (strict abort).
        stub_run1 = _stub_run_chunk({1: (20, 4, 1), 2: (21, 3, 1), 4: (22, 2, 1)})
        monkeypatch.setattr(acc, "run_chunk", stub_run1)
        opts = acc.OrchestratorOptions(
            input_file=src, output_dir=out,
            chunk_size=25, threshold_rows=50, allow_partial=False,
        )
        state1 = acc.run(opts)
        assert state1.status == acc.BATCH_FAILED
        # Two chunks completed at this point.
        completed_indices_1 = {
            c.index for c in state1.chunks
            if c.status == acc.CHUNK_COMPLETED
        }
        assert completed_indices_1 == {1, 2}

        # Second run: now chunks 3 and 4 also succeed. Chunks 1-2
        # should NOT be re-invoked.
        stub_run2 = _stub_run_chunk({3: (19, 5, 1), 4: (22, 2, 1)})
        monkeypatch.setattr(acc, "run_chunk", stub_run2)
        state2 = acc.run(opts)

        assert state2.status == acc.BATCH_COMPLETED
        # run_chunk only got called for chunks 3 and 4 in run 2.
        called_indices = sorted(
            int(call[0].stem.split("_part_")[-1])
            for call in stub_run2.calls  # type: ignore[attr-defined]
        )
        assert called_indices == [3, 4]


# ---------------------------------------------------------------------------
# State file shape (Fase 2 contract)
# ---------------------------------------------------------------------------


class TestStateFileContract:
    def test_state_file_shape_for_fase2_consumer(
        self, tmp_path: Path, monkeypatch,
    ):
        src = tmp_path / "input.csv"
        _write_csv(src, n_rows=50)
        out = tmp_path / "out"
        stub = _stub_run_chunk({1: (20, 4, 1), 2: (21, 3, 1)})
        monkeypatch.setattr(acc, "run_chunk", stub)

        opts = acc.OrchestratorOptions(
            input_file=src, output_dir=out,
            chunk_size=25, threshold_rows=40,
        )
        acc.run(opts)

        raw = json.loads((out / acc.STATUS_FILE).read_text(encoding="utf-8"))
        # Top-level keys.
        assert {
            "started_at", "completed_at", "input_file", "input_format",
            "total_rows", "threshold_rows", "chunk_size", "status",
            "chunks", "merged_at", "merged_counts", "error",
        }.issubset(raw.keys())
        # Per-chunk keys.
        c1 = raw["chunks"][0]
        assert {
            "index", "input_path", "run_dir", "status",
            "started_at", "completed_at", "exit_code", "counts", "error",
        }.issubset(c1.keys())
