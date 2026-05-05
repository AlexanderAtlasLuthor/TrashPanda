"use client";

import { useEffect, useRef, useState } from "react";
import type { JobStatus } from "@/lib/types";
import styles from "./LiveLogsPanel.module.css";

type LineType = "done" | "start" | "metric" | "warn" | "error" | "info";
type ActivityKind = "active" | "done" | "smtp";

interface ParsedLine {
  ts: string;
  msg: string;
  type: LineType;
}

interface ActivitySummary {
  label: string;
  detail?: string;
  kind: ActivityKind;
}

function messageFromRaw(raw: string): string {
  return raw.split(" | ").slice(2).join(" | ").trim() || raw;
}

/**
 * Log format from logger.py: "YYYY-MM-DD HH:MM:SS | LEVEL | message".
 * Returns {ts: "HH:MM:SS", msg: "...", type: ...}.
 */
function parseLine(raw: string): ParsedLine {
  const parts = raw.split(" | ");
  let ts = "";
  let level = "";
  let msg = raw;

  if (parts.length >= 3) {
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
    if (msg.includes("=")) return "metric";
  }

  return "info";
}

const STAGE_LABELS: Record<string, string> = {
  header_normalization: "Header normalization",
  structural_validation: "Structural validation",
  value_normalization: "Value normalization",
  technical_metadata: "Technical metadata",
  email_syntax_validation: "Email syntax validation",
  domain_extraction: "Domain extraction",
  typo_correction: "Typo correction",
  domain_comparison: "Domain comparison",
  dns_enrichment: "DNS enrichment",
  typo_suggestion_validation: "Typo suggestion validation",
  scoring: "Scoring",
  scoring_v2: "Scoring V2",
  scoring_comparison: "Scoring comparison",
  smtp_verification: "SMTP verification",
  catch_all_detection: "Catch-all detection",
  domain_intelligence: "Domain intelligence",
  decision: "Final decisioning",
  completeness: "Completeness check",
  email_normalization: "Email normalization",
  dedupe: "Deduplication",
  staging_persistence: "Persisting rows",
};

const NEXT_STAGE: Record<string, string> = {
  scoring_comparison: "smtp_verification",
  smtp_verification: "catch_all_detection",
  catch_all_detection: "domain_intelligence",
  domain_intelligence: "decision",
  decision: "completeness",
  completeness: "email_normalization",
  email_normalization: "dedupe",
  dedupe: "staging_persistence",
};

function stageFromMessage(msg: string): string | null {
  const match = msg.match(/\bstage=([a-zA-Z0-9_]+)/);
  return match ? match[1] : null;
}

function stageLabel(stage: string): string {
  return (
    STAGE_LABELS[stage] ??
    stage
      .replace(/_/g, " ")
      .replace(/\b\w/g, (ch) => ch.toUpperCase())
  );
}

function latestStage(lines: string[]): string | null {
  for (const raw of lines.slice().reverse()) {
    const stage = stageFromMessage(messageFromRaw(raw));
    if (stage) return stage;
  }
  return null;
}

function smtpActivity(label: string): ActivitySummary {
  return {
    label,
    detail:
      "Live MX checks are now running. For 1,000 rows this commonly takes 10-25 minutes; the activity log can stay quiet while probes are in flight.",
    kind: "smtp",
  };
}

function deriveActivity(
  lines: string[],
  status: JobStatus,
): ActivitySummary | null {
  if (lines.length === 0) return null;

  const lastStage = latestStage(lines);
  if (status === "running" && lastStage === "scoring_comparison") {
    return smtpActivity("Running: SMTP verification");
  }

  if (status === "running" && lastStage && NEXT_STAGE[lastStage]) {
    const next = NEXT_STAGE[lastStage];
    if (next === "smtp_verification") {
      return smtpActivity("Running: SMTP verification");
    }
    return { label: `Running: ${stageLabel(next)}`, kind: "active" };
  }

  const recent = lines.slice(-6).reverse();
  for (const raw of recent) {
    const msg = messageFromRaw(raw);

    if (msg.includes("Pipeline DONE")) {
      return { label: "Pipeline complete", kind: "done" };
    }
    if (msg.includes("Materialize xlsx DONE")) {
      return { label: "Excel deliverables ready", kind: "done" };
    }
    if (msg.includes("Materialize xlsx START")) {
      return { label: "Writing Excel deliverables", kind: "active" };
    }
    if (msg.includes("Materialize iter_rows DONE")) {
      return { label: "Finalizing CSV output", kind: "active" };
    }
    if (msg.includes("Materialize DONE")) {
      return { label: "Materialization done", kind: "done" };
    }
    if (msg.includes("Materialize START")) {
      return { label: "Materializing output", kind: "active" };
    }

    const stage = stageFromMessage(msg);
    if (stage) {
      if (stage === "smtp_verification") {
        return smtpActivity("Running: SMTP verification");
      }
      return { label: `Running: ${stageLabel(stage)}`, kind: "active" };
    }

    const chunkMatch = msg.match(/chunk=(\d+)\s+rows=(\S+)/);
    if (chunkMatch) {
      const rows = Number(chunkMatch[2].replace(/[^\d]/g, "")).toLocaleString();
      return {
        label: `Chunk ${chunkMatch[1]} - ${rows} rows processed`,
        kind: "active",
      };
    }

    if (msg.includes("Pipeline START")) {
      return { label: "Pipeline starting", kind: "active" };
    }
    if (msg.includes("Processing ")) {
      const fileMatch = msg.match(/Processing (.+)/);
      if (fileMatch) {
        return { label: `Ingesting ${fileMatch[1]}`, kind: "active" };
      }
    }
  }

  return { label: "Pipeline active", kind: "active" };
}

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
  const [copyState, setCopyState] = useState<"idle" | "copied" | "error">(
    "idle",
  );
  const scrollRef = useRef<HTMLDivElement>(null);
  const parsed = lines.map(parseLine);
  const activity = deriveActivity(lines, status);
  const isDone = status === "completed" || status === "failed";

  useEffect(() => {
    if (!collapsed && scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [lines, collapsed]);

  const handleCopy = async (e: React.MouseEvent) => {
    e.stopPropagation();
    if (lines.length === 0) return;
    const text = lines.join("\n");
    try {
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(text);
      } else {
        const ta = document.createElement("textarea");
        ta.value = text;
        ta.style.position = "fixed";
        ta.style.opacity = "0";
        document.body.appendChild(ta);
        ta.select();
        document.execCommand("copy");
        document.body.removeChild(ta);
      }
      setCopyState("copied");
    } catch {
      setCopyState("error");
    }
    setTimeout(() => setCopyState("idle"), 1500);
  };

  const copyLabel =
    copyState === "copied"
      ? "COPIED"
      : copyState === "error"
        ? "FAILED"
        : "COPY";

  return (
    <div className={styles.panel}>
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
            <span
              className={[
                styles.activityBadge,
                isDone || activity.kind === "done" ? styles.done : "",
                activity.kind === "smtp" ? styles.smtp : "",
              ]
                .filter(Boolean)
                .join(" ")}
            >
              {activity.label}
            </span>
          )}
          <button
            className={styles.collapseBtn}
            onClick={handleCopy}
            disabled={lines.length === 0}
            aria-label="Copy console output to clipboard"
            title="Copy console output"
          >
            {copyLabel}
          </button>
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

      {!collapsed && (
        <div className={styles.body}>
          {activity?.kind === "smtp" && activity.detail && (
            <div className={styles.activityNotice}>
              <span className={styles.noticeLabel}>SMTP VERIFICATION</span>
              <span className={styles.noticeText}>{activity.detail}</span>
            </div>
          )}
          <div className={styles.logArea} ref={scrollRef}>
            {parsed.length === 0 ? (
              <div className={styles.empty}>
                <span className={styles.emptyDot} />
                Waiting for pipeline activity...
              </div>
            ) : (
              <>
                {parsed.map((line, i) => (
                  <div key={i} className={`${styles.line} ${styles[line.type]}`}>
                    <span className={styles.ts}>{line.ts}</span>
                    <span className={styles.msg} title={line.msg}>
                      {line.msg}
                    </span>
                  </div>
                ))}
                {activity?.kind === "smtp" && (
                  <div className={`${styles.line} ${styles.warn}`}>
                    <span className={styles.ts}>LIVE</span>
                    <span
                      className={styles.msg}
                      title="SMTP verification running - waiting for probe results"
                    >
                      SMTP verification running - waiting for live probe results
                    </span>
                  </div>
                )}
              </>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
