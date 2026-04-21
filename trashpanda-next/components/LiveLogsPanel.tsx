"use client";

import { useEffect, useRef, useState } from "react";
import type { JobStatus } from "@/lib/types";
import styles from "./LiveLogsPanel.module.css";

// ── Log line parsing ──────────────────────────────────────────────────────────

type LineType = "done" | "start" | "metric" | "warn" | "error" | "info";

interface ParsedLine {
  ts: string;
  msg: string;
  type: LineType;
}

/**
 * Log format from logger.py: "YYYY-MM-DD HH:MM:SS | LEVEL | message"
 * Returns {ts: "HH:MM:SS", msg: "...", type: ...}
 */
function parseLine(raw: string): ParsedLine {
  const parts = raw.split(" | ");
  let ts = "";
  let level = "";
  let msg = raw;

  if (parts.length >= 3) {
    // "2024-01-15 14:23:45" → take just the time part
    ts = (parts[0].split(" ")[1] ?? "").slice(0, 8);
    level = parts[1].trim().toUpperCase();
    msg = parts.slice(2).join(" | ").trim();
  }

  const type = classifyLine(msg, level);
  return { ts, msg, type };
}

function classifyLine(msg: string, level: string): LineType {
  if (level === "ERROR" || level === "CRITICAL") return "error";
  if (level === "WARNING" || level === "WARN") return "warn";

  if (msg.startsWith("[TIMING]")) {
    const upper = msg.toUpperCase();
    if (upper.includes("DONE") || upper.includes("COMPLETE")) return "done";
    if (upper.includes("START")) return "start";
    // lines with key=value pairs are metrics
    if (msg.includes("=")) return "metric";
  }

  return "info";
}

// ── Activity summary ──────────────────────────────────────────────────────────

function deriveActivity(lines: string[]): string | null {
  if (lines.length === 0) return null;

  const recent = lines.slice(-6).reverse();
  for (const raw of recent) {
    const msg = raw.split(" | ").slice(2).join(" | ").trim() || raw;

    if (msg.includes("Pipeline DONE")) return "Pipeline complete";
    if (msg.includes("Materialize xlsx DONE")) return "Excel deliverables ready";
    if (msg.includes("Materialize xlsx START")) return "Writing Excel deliverables";
    if (msg.includes("Materialize iter_rows DONE")) return "Finalizing CSV output";
    if (msg.includes("Materialize DONE")) return "Materialization done";
    if (msg.includes("Materialize START")) return "Materializing output";

    const stageMatch = msg.match(/stage=(\w+Stage)/);
    if (stageMatch) {
      const name = stageMatch[1]
        .replace("Stage", "")
        .replace(/([A-Z])/g, " $1")
        .trim();
      return `Running: ${name}`;
    }

    const chunkMatch = msg.match(/chunk=(\d+)\s+rows=(\S+)/);
    if (chunkMatch) {
      const rows = Number(chunkMatch[2].replace(/[^\d]/g, "")).toLocaleString();
      return `Chunk ${chunkMatch[1]} — ${rows} rows processed`;
    }

    if (msg.includes("Pipeline START")) return "Pipeline starting";
    if (msg.includes("Processing ")) {
      const fileMatch = msg.match(/Processing (.+)/);
      if (fileMatch) return `Ingesting ${fileMatch[1]}`;
    }
  }
  return "Pipeline active";
}

// ── Component ─────────────────────────────────────────────────────────────────

interface LiveLogsPanelProps {
  lines: string[];
  status: JobStatus;
  defaultCollapsed?: boolean;
}

export function LiveLogsPanel({
  lines,
  status,
  defaultCollapsed = false,
}: LiveLogsPanelProps) {
  const [collapsed, setCollapsed] = useState(defaultCollapsed);
  const scrollRef = useRef<HTMLDivElement>(null);
  const parsed = lines.map(parseLine);
  const activity = deriveActivity(lines);
  const isDone = status === "completed" || status === "failed";

  // Auto-scroll to bottom when new lines arrive (only when expanded).
  useEffect(() => {
    if (!collapsed && scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [lines, collapsed]);

  return (
    <div className={styles.panel}>
      {/* ── Header (also acts as collapse toggle) ── */}
      <div
        className={styles.header}
        onClick={() => setCollapsed((c) => !c)}
        role="button"
        aria-expanded={!collapsed}
      >
        <div className={styles.titleRow}>
          <span className={styles.title}>LIVE JOB ACTIVITY</span>
        </div>

        <div className={styles.headerRight}>
          {activity && !collapsed && (
            <span className={`${styles.activityBadge}${isDone ? ` ${styles.done}` : ""}`}>
              {activity}
            </span>
          )}
          <button
            className={styles.collapseBtn}
            onClick={(e) => {
              e.stopPropagation();
              setCollapsed((c) => !c);
            }}
            aria-label={collapsed ? "Expand log panel" : "Collapse log panel"}
          >
            {collapsed ? "EXPAND" : "COLLAPSE"}
          </button>
        </div>
      </div>

      {/* ── Body ── */}
      {!collapsed && (
        <div className={styles.body}>
          <div className={styles.logArea} ref={scrollRef}>
            {parsed.length === 0 ? (
              <div className={styles.empty}>
                <span className={styles.emptyDot} />
                Waiting for pipeline activity...
              </div>
            ) : (
              parsed.map((line, i) => (
                <div key={i} className={`${styles.line} ${styles[line.type]}`}>
                  <span className={styles.ts}>{line.ts}</span>
                  <span className={styles.msg} title={line.msg}>
                    {line.msg}
                  </span>
                </div>
              ))
            )}
          </div>
        </div>
      )}
    </div>
  );
}
