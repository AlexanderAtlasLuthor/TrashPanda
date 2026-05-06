"use client";

/**
 * BatchProgressPanel — live polling view of a batch (auto-chunked job).
 *
 * Polls `/api/batches/{id}` every `pollIntervalMs` (default 3000) and
 * renders:
 *   - the aggregate progress bar
 *   - a per-chunk grid (status, counts, timing)
 *   - a download button when the merged bundle is ready
 *
 * Stops polling once status is terminal (completed | failed |
 * partial_failure).
 */

import { useEffect, useState } from "react";

import { batchBundleDownloadUrl, getBatchStatusDoc } from "@/lib/api";
import type {
  BatchChunkState,
  BatchStatusDoc,
  BatchStatus,
} from "@/lib/types";

import styles from "./BatchProgressPanel.module.css";

const TERMINAL_STATUSES: ReadonlySet<BatchStatus> = new Set([
  "completed",
  "failed",
  "partial_failure",
]);

const STATUS_ICON: Record<BatchChunkState["status"], string> = {
  pending: "⏳",
  running: "🟡",
  completed: "✅",
  failed: "❌",
};

interface BatchProgressPanelProps {
  batchId: string;
  pollIntervalMs?: number;
  /** Optional callback when the batch reaches a terminal state. */
  onComplete?: (doc: BatchStatusDoc) => void;
}

function fmt(n: number | null | undefined): string {
  if (n === null || n === undefined) return "—";
  return n.toLocaleString();
}

function elapsed(start: string | null, end: string | null): string {
  if (!start) return "—";
  const t0 = Date.parse(start);
  const t1 = end ? Date.parse(end) : Date.now();
  if (!Number.isFinite(t0) || !Number.isFinite(t1)) return "—";
  const sec = Math.max(0, Math.round((t1 - t0) / 1000));
  if (sec < 60) return `${sec}s`;
  const m = Math.floor(sec / 60);
  const s = sec % 60;
  return `${m}m ${s}s`;
}

export default function BatchProgressPanel({
  batchId,
  pollIntervalMs = 3000,
  onComplete,
}: BatchProgressPanelProps) {
  const [doc, setDoc] = useState<BatchStatusDoc | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | null = null;
    let firedOnComplete = false;

    async function tick() {
      try {
        const next = await getBatchStatusDoc(batchId);
        if (cancelled) return;
        setDoc(next);
        setError(null);
        if (
          TERMINAL_STATUSES.has(next.status) &&
          !firedOnComplete
        ) {
          firedOnComplete = true;
          onComplete?.(next);
          return; // stop polling
        }
        timer = setTimeout(tick, pollIntervalMs);
      } catch (err) {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : "Polling failed.");
        timer = setTimeout(tick, pollIntervalMs * 2);
      }
    }

    tick();
    return () => {
      cancelled = true;
      if (timer !== null) clearTimeout(timer);
    };
  }, [batchId, pollIntervalMs, onComplete]);

  if (doc === null) {
    return (
      <div className={styles.panel}>
        <div className={styles.title}>Batch {batchId}</div>
        <div className={styles.summaryLine}>
          {error ? `Error: ${error}` : "Loading…"}
        </div>
      </div>
    );
  }

  const totalChunks = doc.chunks.length;
  const nCompleted = doc.chunks.filter(
    (c) => c.status === "completed",
  ).length;
  const nFailed = doc.chunks.filter((c) => c.status === "failed").length;
  const fillPercent =
    totalChunks > 0 ? Math.round((nCompleted / totalChunks) * 100) : 0;
  const isTerminal = TERMINAL_STATUSES.has(doc.status);
  const fillVariant: "completed" | "failed" | "" =
    doc.status === "completed"
      ? "completed"
      : doc.status === "failed"
        ? "failed"
        : "";

  const totalRows = doc.total_rows;
  const cleanRows = doc.merged_counts?.clean_deliverable ?? null;
  const removedRows = doc.merged_counts?.high_risk_removed ?? null;
  const reviewRows = doc.merged_counts?.review_provider_limited ?? null;

  return (
    <div className={styles.panel}>
      <div className={styles.header}>
        <div>
          <div className={styles.title}>Auto-chunked batch</div>
          <div className={styles.batchId}>{doc.input_file}</div>
        </div>
        <span
          className={`${styles.statusBadge} ${styles[doc.status] ?? ""}`}
        >
          {doc.status.replace("_", " ")}
        </span>
      </div>

      <div className={styles.aggregateBar}>
        <div className={styles.barTrack}>
          <div
            className={`${styles.barFill} ${fillVariant ? styles[fillVariant] : ""}`}
            style={{ width: `${fillPercent}%` }}
          />
        </div>
        <div className={styles.summaryLine}>
          {nCompleted} / {totalChunks} chunks complete
          {nFailed > 0 ? ` · ${nFailed} failed` : ""}
        </div>
      </div>

      <div className={styles.summaryLine}>
        {fmt(totalRows)} rows · chunk_size {fmt(doc.chunk_size)} · started{" "}
        {doc.started_at ?? "—"} · elapsed{" "}
        {elapsed(doc.started_at, doc.completed_at)}
      </div>

      {totalChunks > 0 && (
        <table className={styles.table}>
          <thead>
            <tr>
              <th>#</th>
              <th>Status</th>
              <th className={styles.right}>Clean</th>
              <th className={styles.right}>Review</th>
              <th className={styles.right}>Removed</th>
              <th>Time</th>
            </tr>
          </thead>
          <tbody>
            {doc.chunks.map((c) => (
              <tr key={c.index}>
                <td>{c.index}</td>
                <td>
                  <span
                    className={`${styles.chunkStatus} ${styles[c.status] ?? ""}`}
                  >
                    <span className={styles.icon}>
                      {STATUS_ICON[c.status]}
                    </span>
                    {c.status}
                  </span>
                </td>
                <td className={styles.right}>
                  {fmt(c.counts?.clean_deliverable)}
                </td>
                <td className={styles.right}>
                  {fmt(c.counts?.review_provider_limited)}
                </td>
                <td className={styles.right}>
                  {fmt(c.counts?.high_risk_removed)}
                </td>
                <td>{elapsed(c.started_at, c.completed_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {doc.merged_counts && (
        <div className={styles.summaryLine}>
          Merged: {fmt(cleanRows)} clean · {fmt(reviewRows)} review ·{" "}
          {fmt(removedRows)} removed
        </div>
      )}

      {doc.error && <div className={styles.errorBox}>{doc.error}</div>}
      {error && !doc.error && <div className={styles.errorBox}>{error}</div>}

      <div className={styles.actions}>
        <a
          className={`${styles.actionBtn} ${
            isTerminal && doc.merged_counts ? "" : styles.disabled
          }`}
          href={
            isTerminal && doc.merged_counts
              ? batchBundleDownloadUrl(batchId)
              : undefined
          }
          aria-disabled={!isTerminal || !doc.merged_counts}
          onClick={(e) => {
            if (!isTerminal || !doc.merged_counts) e.preventDefault();
          }}
        >
          Download merged bundle
        </a>
      </div>
    </div>
  );
}
