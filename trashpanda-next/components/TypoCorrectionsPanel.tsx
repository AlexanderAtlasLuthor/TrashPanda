"use client";

import { useEffect, useState } from "react";
import { getTypoCorrections } from "@/lib/api";
import type { TypoCorrection } from "@/lib/types";

interface Props {
  jobId: string;
}

export function TypoCorrectionsPanel({ jobId }: Props) {
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [loaded, setLoaded] = useState(false);
  const [total, setTotal] = useState<number | null>(null);
  const [rows, setRows] = useState<TypoCorrection[]>([]);

  useEffect(() => {
    let cancelled = false;
    getTypoCorrections(jobId)
      .then((d) => { if (!cancelled) setTotal(d.total); })
      .catch(() => { if (!cancelled) setTotal(0); });
    return () => { cancelled = true; };
  }, [jobId]);

  const toggle = () => {
    setOpen((o) => !o);
    if (!loaded && !loading) {
      setLoading(true);
      getTypoCorrections(jobId)
        .then((d) => { setRows(d.corrections); setTotal(d.total); setLoaded(true); })
        .catch(() => { setLoaded(true); })
        .finally(() => setLoading(false));
    }
  };

  if (total === 0) return null;

  return (
    <div
      style={{
        marginBottom: 28,
        background: "var(--bg-panel)",
        border: "1px solid var(--stroke-steel)",
        borderRadius: 4,
        overflow: "hidden",
      }}
    >
      <button
        type="button"
        onClick={toggle}
        aria-expanded={open}
        style={{
          width: "100%",
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          padding: "14px 20px",
          background: "transparent",
          border: "none",
          cursor: "pointer",
          color: "var(--ink-high)",
          textAlign: "left",
          fontFamily: "var(--font-ui)",
          fontSize: 14,
          fontWeight: 600,
        }}
      >
        <span>
          Typo Corrections {total !== null ? `(${total})` : ""}
          <span
            style={{
              display: "inline-block",
              marginLeft: 10,
              padding: "2px 8px",
              fontFamily: "var(--font-mono)",
              fontSize: 9,
              letterSpacing: 1,
              color: "var(--neon)",
              background: "rgba(142, 255, 58, 0.08)",
              border: "1px solid rgba(142, 255, 58, 0.3)",
              borderRadius: 2,
              textTransform: "uppercase",
            }}
          >
            auto-fixed
          </span>
        </span>
        <span style={{ color: "var(--ink-low)", fontFamily: "var(--font-mono)", fontSize: 12 }}>
          {open ? "▾" : "▸"}
        </span>
      </button>

      {open && (
        <div style={{ padding: "0 20px 18px 20px" }}>
          <div
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: 11,
              color: "var(--ink-low)",
              marginBottom: 12,
              lineHeight: 1.5,
            }}
          >
            We automatically corrected common domain typos to improve deliverability.
          </div>

          {loading ? (
            <div style={{ fontFamily: "var(--font-mono)", fontSize: 12, color: "var(--ink-low)" }}>
              Loading…
            </div>
          ) : rows.length === 0 ? (
            <div style={{ fontFamily: "var(--font-mono)", fontSize: 12, color: "var(--ink-low)" }}>
              No typo corrections in this run.
            </div>
          ) : (
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr>
                    <Th>Original</Th>
                    <Th>Corrected</Th>
                    <Th>Email</Th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map((r, i) => (
                    <tr
                      key={`${r.email}-${i}`}
                      style={{ borderTop: "1px solid rgba(255,255,255,0.04)" }}
                    >
                      <Td color="var(--danger)">{r.original}</Td>
                      <Td color="var(--neon)">{r.corrected}</Td>
                      <Td>{r.email}</Td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function Th({ children }: { children: React.ReactNode }) {
  return (
    <th
      style={{
        textAlign: "left",
        padding: "8px 12px",
        fontFamily: "var(--font-mono)",
        fontSize: 9,
        letterSpacing: 1.5,
        textTransform: "uppercase",
        color: "var(--ink-low)",
        borderBottom: "1px solid var(--stroke-steel)",
      }}
    >
      {children}
    </th>
  );
}

function Td({ children, color }: { children: React.ReactNode; color?: string }) {
  return (
    <td
      style={{
        padding: "9px 12px",
        fontFamily: "var(--font-mono)",
        fontSize: 12,
        color: color ?? "var(--ink-high)",
      }}
    >
      {children}
    </td>
  );
}
