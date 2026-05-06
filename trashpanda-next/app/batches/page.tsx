"use client";

import Link from "next/link";
import { useEffect, useRef, useState } from "react";

import { listBatches, uploadBatch } from "@/lib/api";
import type { BatchProgress } from "@/lib/types";

const POLL_MS = 4000;

function fmt(n: number | null | undefined): string {
  if (n === null || n === undefined) return "—";
  return n.toLocaleString();
}

export default function BatchesIndexPage() {
  const [batches, setBatches] = useState<BatchProgress[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [uploading, setUploading] = useState(false);
  const fileRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | null = null;
    async function tick() {
      try {
        const res = await listBatches();
        if (cancelled) return;
        setBatches(res.batches);
        setError(null);
      } catch (err) {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : "Failed to list.");
      }
      timer = setTimeout(tick, POLL_MS);
    }
    tick();
    return () => {
      cancelled = true;
      if (timer !== null) clearTimeout(timer);
    };
  }, []);

  async function onUpload(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const file = fileRef.current?.files?.[0];
    if (!file) return;
    setUploading(true);
    try {
      const res = await uploadBatch(file);
      // Soft navigation to the new batch's progress page.
      window.location.href = `/batches/${encodeURIComponent(res.batch_id)}`;
    } catch (err) {
      setError(err instanceof Error ? err.message : "Upload failed.");
      setUploading(false);
    }
  }

  return (
    <div style={{ padding: "2rem", maxWidth: 960, margin: "0 auto", color: "#e5e5e7" }}>
      <h1 style={{ marginTop: 0 }}>Batch jobs</h1>
      <p style={{ color: "#a1a1aa", fontSize: "0.9rem" }}>
        Auto-chunked processing for large lists. Inputs ≥ 50,000 rows are
        split into chunks of 25,000 rows and processed as isolated
        subprocesses (OOM-safe). The merged customer bundle is available
        for download once the batch completes.
      </p>

      <form
        onSubmit={onUpload}
        style={{
          display: "flex",
          gap: "0.75rem",
          alignItems: "center",
          padding: "1rem",
          border: "1px solid #2c2c30",
          borderRadius: "8px",
          background: "#18181b",
          marginBottom: "1.5rem",
        }}
      >
        <input
          ref={fileRef}
          type="file"
          accept=".csv,.xlsx"
          required
          disabled={uploading}
          style={{ flex: 1 }}
        />
        <button
          type="submit"
          disabled={uploading}
          style={{
            background: uploading ? "#3f3f46" : "#2563eb",
            color: "white",
            border: "none",
            borderRadius: "6px",
            padding: "0.5rem 1rem",
            fontWeight: 500,
            cursor: uploading ? "not-allowed" : "pointer",
          }}
        >
          {uploading ? "Uploading…" : "Start batch"}
        </button>
      </form>

      {error && (
        <div
          style={{
            background: "#2a0a0a",
            border: "1px solid #7f1d1d",
            color: "#fecaca",
            padding: "0.5rem 0.75rem",
            borderRadius: "6px",
            marginBottom: "1rem",
            fontSize: "0.85rem",
          }}
        >
          {error}
        </div>
      )}

      <h2 style={{ fontSize: "1.05rem", color: "#a1a1aa", marginBottom: "0.5rem" }}>
        Recent batches
      </h2>
      {batches === null ? (
        <div style={{ color: "#71717a" }}>Loading…</div>
      ) : batches.length === 0 ? (
        <div style={{ color: "#71717a" }}>No batches yet.</div>
      ) : (
        <table
          style={{
            width: "100%",
            borderCollapse: "collapse",
            fontSize: "0.85rem",
          }}
        >
          <thead>
            <tr style={{ color: "#a1a1aa", textAlign: "left" }}>
              <th style={{ padding: "0.4rem", borderBottom: "1px solid #27272a" }}>Batch</th>
              <th style={{ padding: "0.4rem", borderBottom: "1px solid #27272a" }}>Status</th>
              <th style={{ padding: "0.4rem", borderBottom: "1px solid #27272a", textAlign: "right" }}>Chunks</th>
              <th style={{ padding: "0.4rem", borderBottom: "1px solid #27272a", textAlign: "right" }}>Clean</th>
              <th style={{ padding: "0.4rem", borderBottom: "1px solid #27272a" }}>Started</th>
            </tr>
          </thead>
          <tbody>
            {[...batches].reverse().map((b) => (
              <tr key={b.batch_id}>
                <td style={{ padding: "0.4rem", borderBottom: "1px solid #27272a" }}>
                  <Link href={`/batches/${encodeURIComponent(b.batch_id)}`}>
                    {b.batch_id}
                  </Link>
                </td>
                <td style={{ padding: "0.4rem", borderBottom: "1px solid #27272a" }}>
                  {b.status}
                </td>
                <td style={{ padding: "0.4rem", borderBottom: "1px solid #27272a", textAlign: "right", fontVariantNumeric: "tabular-nums" }}>
                  {b.n_completed}/{b.n_chunks}
                </td>
                <td style={{ padding: "0.4rem", borderBottom: "1px solid #27272a", textAlign: "right", fontVariantNumeric: "tabular-nums" }}>
                  {fmt(b.merged_counts?.clean_deliverable)}
                </td>
                <td style={{ padding: "0.4rem", borderBottom: "1px solid #27272a", color: "#a1a1aa" }}>
                  {b.started_at ?? "—"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
