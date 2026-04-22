"use client";

import { useEffect, useRef, useState } from "react";
import type { JobResult } from "@/lib/types";

/**
 * ETA / throughput estimator for running pipeline jobs.
 *
 * Why this lives in the frontend:
 *   The backend exposes `started_at` and (post-run) `summary.total_input_rows`,
 *   but during a running job it does NOT publish a live `rows_processed` counter
 *   over HTTP. What it DOES publish is the streaming log, which contains real
 *   per-chunk lines:
 *
 *     "[TIMING] chunk=N rows=K elapsed=Xs ..."
 *
 *   We already poll those logs in ResultsClient, so we parse cumulative rows
 *   from them (real pipeline data — no fake numbers, no hardcoded times) and
 *   smooth the throughput with an EMA. ETA is reported as a live throughput
 *   readout because we don't know the input row count until completion; this is
 *   honest by design — we show what we can prove, not a fabricated countdown.
 *
 * Smoothing:
 *   rows_per_second is updated whenever a new chunk completes:
 *     ema = alpha * sample + (1 - alpha) * ema
 *   with alpha = 0.3 (trades latency for stability, prevents big jumps when
 *   one chunk happens to be slow because of MX cache warmup).
 *
 * Outputs:
 *   - state: "waiting" | "estimating" | "running" | "complete" | "idle"
 *   - label: human-readable string ready to render
 *   - rowsPerSec: smoothed throughput (or null)
 *   - elapsedSec: wall-clock since started_at (or null)
 */

const TIMING_RE = /\[TIMING\]\s+chunk=(\d+)\s+rows=(\d+)/;
const ALPHA = 0.3;

export type EtaState =
  | "idle"
  | "waiting"
  | "estimating"
  | "running"
  | "complete";

export interface EtaInfo {
  state: EtaState;
  label: string;
  rowsPerSec: number | null;
  elapsedSec: number | null;
  rowsProcessed: number | null;
}

function formatDuration(seconds: number): string {
  if (seconds < 60) return `${Math.max(0, Math.round(seconds))}s`;
  const m = Math.floor(seconds / 60);
  const s = Math.round(seconds % 60);
  return s === 0 ? `${m}m` : `${m}m ${s}s`;
}

function formatRate(rps: number): string {
  if (rps >= 100) return `${Math.round(rps).toLocaleString()} rows/s`;
  if (rps >= 10) return `${rps.toFixed(0)} rows/s`;
  return `${rps.toFixed(1)} rows/s`;
}

interface ChunkSample {
  chunkIndex: number;
  cumulativeRows: number;
}

function parseChunks(lines: string[]): ChunkSample[] {
  // Logs may include the same chunk repeated as more lines accumulate; we keep
  // the last cumulative rows seen per chunk index.
  const byIndex = new Map<number, number>();
  let runningTotal = 0;
  let lastIndex = -1;
  for (const raw of lines) {
    const match = raw.match(TIMING_RE);
    if (!match) continue;
    const idx = Number(match[1]);
    const rows = Number(match[2]);
    if (!Number.isFinite(idx) || !Number.isFinite(rows)) continue;
    if (idx > lastIndex) {
      runningTotal += rows;
      byIndex.set(idx, runningTotal);
      lastIndex = idx;
    }
  }
  return [...byIndex.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([chunkIndex, cumulativeRows]) => ({ chunkIndex, cumulativeRows }));
}

export function useEtaEstimator(
  result: JobResult | null,
  logLines: string[],
): EtaInfo {
  const [now, setNow] = useState(() => Date.now());

  // Tick once per second so the elapsed counter stays fresh between polls.
  useEffect(() => {
    if (!result) return;
    if (result.status !== "running" && result.status !== "queued") return;
    const id = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(id);
  }, [result]);

  // Smoothed rows/sec persists across renders.
  const emaRef = useRef<number | null>(null);
  const lastChunkIdxRef = useRef<number>(-1);

  if (!result) {
    return {
      state: "idle",
      label: "",
      rowsPerSec: null,
      elapsedSec: null,
      rowsProcessed: null,
    };
  }

  const startedMs = result.started_at ? Date.parse(result.started_at) : null;
  const finishedMs = result.finished_at ? Date.parse(result.finished_at) : null;

  // ── Completed ─────────────────────────────────────────────────────────────
  if (result.status === "completed") {
    const totalSec =
      startedMs && finishedMs
        ? Math.max(1, Math.round((finishedMs - startedMs) / 1000))
        : null;
    const totalRows = result.summary?.total_input_rows ?? null;
    const rps =
      totalSec && totalRows ? Math.max(0.1, totalRows / totalSec) : null;
    const label = totalSec
      ? rps && totalRows
        ? `Completed in ${formatDuration(totalSec)} · ${totalRows.toLocaleString()} rows · ${formatRate(rps)}`
        : `Completed in ${formatDuration(totalSec)}`
      : "Completed";
    return {
      state: "complete",
      label,
      rowsPerSec: rps,
      elapsedSec: totalSec,
      rowsProcessed: totalRows,
    };
  }

  // ── Failed / unknown ──────────────────────────────────────────────────────
  if (result.status === "failed") {
    return {
      state: "idle",
      label: "",
      rowsPerSec: null,
      elapsedSec: null,
      rowsProcessed: null,
    };
  }

  // ── Queued ────────────────────────────────────────────────────────────────
  if (result.status === "queued") {
    return {
      state: "waiting",
      label: "Waiting to start...",
      rowsPerSec: null,
      elapsedSec: null,
      rowsProcessed: null,
    };
  }

  // ── Running ───────────────────────────────────────────────────────────────
  const elapsedSec = startedMs
    ? Math.max(0, Math.round((now - startedMs) / 1000))
    : null;

  const samples = parseChunks(logLines);

  // Update EMA only when a new chunk completed since the last computation.
  if (samples.length > 0 && elapsedSec && elapsedSec > 0) {
    const last = samples[samples.length - 1];
    if (last.chunkIndex > lastChunkIdxRef.current) {
      const sampleRps = last.cumulativeRows / elapsedSec;
      emaRef.current =
        emaRef.current === null
          ? sampleRps
          : ALPHA * sampleRps + (1 - ALPHA) * emaRef.current;
      lastChunkIdxRef.current = last.chunkIndex;
    }
  }

  const rowsProcessed =
    samples.length > 0 ? samples[samples.length - 1].cumulativeRows : null;
  const rps = emaRef.current;

  // No chunks finished yet → genuine "estimating" state, not a fake number.
  if (rps === null || rowsProcessed === null) {
    return {
      state: "estimating",
      label:
        elapsedSec && elapsedSec >= 1
          ? `Estimating... · ${formatDuration(elapsedSec)} elapsed`
          : "Estimating...",
      rowsPerSec: null,
      elapsedSec,
      rowsProcessed: null,
    };
  }

  return {
    state: "running",
    label: `Processing · ~${formatRate(rps)} · ${rowsProcessed.toLocaleString()} rows · ${formatDuration(elapsedSec ?? 0)} elapsed`,
    rowsPerSec: rps,
    elapsedSec,
    rowsProcessed,
  };
}
