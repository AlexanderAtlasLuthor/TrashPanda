"use client";

import { useCallback, useEffect, useState } from "react";

import { cancelJob, getJobProgress } from "@/lib/api";
import type {
  JobProgress,
  JobResult,
  JobStatus,
  SmtpProgress,
} from "@/lib/types";
import styles from "./JobStatusPanel.module.css";
import { useEtaEstimator } from "./useEtaEstimator";

const STAGES: Array<{ name: string; desc: string }> = [
  { name: "INGEST", desc: "Read CSV/XLSX and parse rows into memory" },
  { name: "NORMALIZE", desc: "Lowercase, trim, strip invisible chars" },
  { name: "VALIDATE", desc: "RFC 5322 structural check" },
  { name: "TYPO FIX", desc: "Fuzzy match common domains" },
  { name: "MX LOOKUP", desc: "Verify domain accepts mail" },
  { name: "SCORE", desc: "Calculate deliverability and policy signals" },
  { name: "SMTP VERIFY", desc: "Live MX checks; slower than structural cleaning" },
  { name: "CLASSIFY & EXPORT", desc: "Tag green/yellow/red and emit xlsx" },
];

const BACKEND_STAGE_ACTIVE_INDEX: Record<string, number> = {
  header_normalization: 1,
  structural_validation: 1,
  value_normalization: 2,
  technical_metadata: 2,
  email_syntax_validation: 3,
  domain_extraction: 3,
  typo_correction: 4,
  domain_comparison: 4,
  dns_enrichment: 5,
  typo_suggestion_validation: 5,
  scoring: 5,
  scoring_v2: 5,
  scoring_comparison: 6,
  smtp_verification: 7,
  catch_all_detection: 7,
  domain_intelligence: 7,
  decision: 7,
  completeness: 7,
  email_normalization: 7,
  dedupe: 7,
  staging_persistence: 7,
};

function latestBackendStage(logLines: string[]): string | null {
  for (const raw of logLines.slice().reverse()) {
    const msg = raw.split(" | ").slice(2).join(" | ").trim() || raw;
    const match = msg.match(/\bstage=([a-zA-Z0-9_]+)/);
    if (match) return match[1];
  }
  return null;
}

function isSmtpQuietWindow(result: JobResult, logLines: string[]): boolean {
  return (
    result.status === "running" &&
    latestBackendStage(logLines) === "scoring_comparison"
  );
}

function estimateStage(result: JobResult, logLines: string[] = []): number {
  if (result.status === "queued") return -1;
  if (result.status === "completed") return STAGES.length;
  if (result.status === "failed") return -1;

  const latestStage = latestBackendStage(logLines);
  if (latestStage && BACKEND_STAGE_ACTIVE_INDEX[latestStage] !== undefined) {
    return BACKEND_STAGE_ACTIVE_INDEX[latestStage];
  }

  const started = result.started_at ? Date.parse(result.started_at) : Date.now();
  const elapsed = Math.max(0, Date.now() - started);
  const idx = Math.floor(elapsed / 1200);
  return Math.min(STAGES.length - 1, idx);
}

function formatTimestamp(iso: string | null | undefined): string {
  if (!iso) return "-";
  try {
    const d = new Date(iso);
    return d.toLocaleTimeString([], {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
  } catch {
    return "-";
  }
}

function statusLabel(status: JobStatus): string {
  switch (status) {
    case "queued":
      return "Upload received - waiting for worker";
    case "running":
      return "Cleaning pipeline in progress";
    case "completed":
      return "Pipeline complete";
    case "failed":
      return "Pipeline failed";
  }
}

interface JobStatusPanelProps {
  result: JobResult;
  /**
   * Streaming log lines from the same poll cycle as `result`. Used by the
   * ETA estimator and stage inference.
   */
  logLines?: string[];
}

function SmtpLiveWarning() {
  return (
    <div className={styles.smtpLiveWarning} role="alert">
      <span className={styles.smtpLiveDot} aria-hidden="true" />
      <span>
        <strong>SMTP LIVE EN CURSO.</strong> This run is opening real
        connections to recipient mail servers. Yahoo / AOL / Verizon-class
        domains are skipped automatically because they cannot be confirmed
        without sending. Use the Cancel button if a probe stalls.
      </span>
    </div>
  );
}

interface SmtpProgressBlockProps {
  smtp: SmtpProgress;
}

function SmtpProgressBlock({ smtp }: SmtpProgressBlockProps) {
  const ratio =
    smtp.ratio !== null && smtp.ratio !== undefined
      ? Math.max(0, Math.min(1, smtp.ratio))
      : smtp.total > 0
        ? smtp.attempted / smtp.total
        : 0;
  const pct = Math.round(ratio * 100);

  return (
    <div className={styles.smtpProgress}>
      <span className={styles.label}>SMTP probes</span>
      <span className={styles.value}>
        {smtp.attempted}
        {smtp.total > 0 ? ` / ${smtp.total}` : ""}
        {smtp.total > 0 ? `  (${pct}%)` : ""}
      </span>
      <span className={styles.label}>Valid - invalid - timeout</span>
      <span className={styles.value}>
        {smtp.valid} - {smtp.invalid} - {smtp.timeout}
      </span>
      {smtp.total > 0 && (
        <div className={styles.smtpProgressBar} aria-hidden="true">
          <div
            className={styles.smtpProgressBarFill}
            style={{ width: `${pct}%` }}
          />
        </div>
      )}
    </div>
  );
}

export function JobStatusPanel({ result, logLines = [] }: JobStatusPanelProps) {
  const activeIdx = estimateStage(result, logLines);
  const eta = useEtaEstimator(result, logLines);
  const fileExt =
    result.input_filename?.toLowerCase().endsWith(".xlsx") ? "XLSX" : "CSV";

  const [progress, setProgress] = useState<JobProgress | null>(null);
  const isPollable = result.status === "queued" || result.status === "running";

  useEffect(() => {
    if (!isPollable) {
      setProgress(null);
      return;
    }
    let cancelled = false;
    const tick = async () => {
      try {
        const p = await getJobProgress(result.job_id);
        if (!cancelled) setProgress(p);
      } catch {
        // Polling failures are non-fatal; the panel still renders the
        // base status from `result`.
      }
    };
    tick();
    const id = setInterval(tick, 2000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [isPollable, result.job_id]);

  const [cancelState, setCancelState] = useState<
    "idle" | "pending" | "cancelled" | "error"
  >("idle");
  const onCancel = useCallback(async () => {
    setCancelState("pending");
    try {
      const res = await cancelJob(result.job_id);
      setCancelState(res.cancelled ? "cancelled" : "cancelled");
    } catch {
      setCancelState("error");
    }
  }, [result.job_id]);

  const smtpLive = Boolean(progress?.smtp?.live);
  const smtp = progress?.smtp ?? null;
  const smtpPhaseInferred = isSmtpQuietWindow(result, logLines);

  return (
    <div className={styles.panel}>
      <div className={styles.header}>
        <div className={styles.title}>CLEANING PIPELINE</div>
        <div
          className={[styles.badge, styles[result.status]]
            .filter(Boolean)
            .join(" ")}
        >
          <span className={styles.badgeDot}></span>
          {result.status.toUpperCase()}
        </div>
      </div>

      <div className={styles.body}>
        <div className={styles.fileRow}>
          <div className={styles.fileIcon}>{fileExt}</div>
          <div className={styles.fileMeta}>
            <div className={styles.fileName}>
              {result.input_filename ?? "unknown file"}
            </div>
            <div className={styles.fileSub}>{statusLabel(result.status)}</div>
          </div>
          <div className={styles.timestamp}>
            <div>STARTED {formatTimestamp(result.started_at)}</div>
            {result.finished_at && (
              <div>FINISHED {formatTimestamp(result.finished_at)}</div>
            )}
          </div>
        </div>

        <div className={styles.stages}>
          {STAGES.map((stage, i) => {
            const done = i < activeIdx;
            const active = i === activeIdx;
            const cls = [
              styles.stage,
              done && styles.done,
              active && styles.active,
            ]
              .filter(Boolean)
              .join(" ");
            return (
              <div key={i} className={cls}>
                <div className={styles.idx}>
                  {(i + 1).toString().padStart(2, "0")}
                </div>
                <div className={styles.info}>
                  <div className={styles.name}>{stage.name}</div>
                  <div className={styles.desc}>{stage.desc}</div>
                </div>
                <div className={styles.status}>
                  {done ? "DONE" : active ? "RUNNING" : "QUEUED"}
                </div>
              </div>
            );
          })}
        </div>

        {(smtpLive || smtpPhaseInferred) && <SmtpLiveWarning />}

        {smtp && (smtp.total > 0 || smtp.attempted > 0) && (
          <SmtpProgressBlock smtp={smtp} />
        )}

        {eta.label && (
          <div
            className={[styles.eta, styles[`eta_${eta.state}`]]
              .filter(Boolean)
              .join(" ")}
          >
            <span className={styles.etaPrefix}>ETA</span>
            <span className={styles.etaValue}>{eta.label}</span>
          </div>
        )}

        {isPollable && (
          <div className={styles.actions}>
            <button
              type="button"
              className={styles.cancelButton}
              onClick={onCancel}
              disabled={
                cancelState === "pending" ||
                cancelState === "cancelled" ||
                progress?.cancelled === true
              }
              aria-label="Cancel this job"
            >
              {cancelState === "pending"
                ? "Cancelling..."
                : cancelState === "cancelled" || progress?.cancelled
                  ? "Cancellation requested"
                  : "Cancel job"}
            </button>
          </div>
        )}
        {cancelState === "error" && (
          <div className={styles.cancelNote}>
            Could not reach the cancel endpoint - try again in a moment.
          </div>
        )}
        {(cancelState === "cancelled" || progress?.cancelled) && (
          <div className={styles.cancelNote}>
            Cancellation flag set. SMTP probes will unwind within seconds.
          </div>
        )}

        {result.status === "queued" && (
          <div className={styles.hint}>
            The worker is spinning up. This usually takes a few seconds. You
            can leave this page and come back; the job keeps running on the
            server.
          </div>
        )}
        {result.status === "running" && (
          <div className={styles.hint}>
            Cleaning in progress. This page polls for updates every 2 seconds
            and will automatically switch to results when ready.
          </div>
        )}
      </div>
    </div>
  );
}
