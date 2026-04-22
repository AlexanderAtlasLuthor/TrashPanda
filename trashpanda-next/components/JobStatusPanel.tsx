import type { JobResult, JobStatus } from "@/lib/types";
import styles from "./JobStatusPanel.module.css";
import { useEtaEstimator } from "./useEtaEstimator";

const STAGES: Array<{ name: string; desc: string }> = [
  { name: "INGEST", desc: "Read CSV/XLSX · parse rows into memory" },
  { name: "NORMALIZE", desc: "Lowercase, trim, strip invisible chars" },
  { name: "VALIDATE", desc: "RFC 5322 structural check" },
  { name: "TYPO FIX", desc: "Fuzzy match common domains · gmial → gmail" },
  { name: "DEDUPLICATE", desc: "Hash compare · keep most complete record" },
  { name: "MX LOOKUP", desc: "Verify domain accepts mail" },
  { name: "CLASSIFY & EXPORT", desc: "Tag green/yellow/red · emit xlsx" },
];

/**
 * Map status + age to an estimated current stage. Backend doesn't yet
 * stream stage-level progress, so we use time elapsed since started_at
 * as a rough progress indicator. When the backend later exposes a
 * current_stage field, wire it here.
 */
function estimateStage(result: JobResult): number {
  if (result.status === "queued") return -1;
  if (result.status === "completed") return STAGES.length;
  if (result.status === "failed") return -1;

  const started = result.started_at ? Date.parse(result.started_at) : Date.now();
  const elapsed = Math.max(0, Date.now() - started);
  // rough heuristic: each stage ~1.2s during mock, gives a believable marquee
  const idx = Math.floor(elapsed / 1200);
  return Math.min(STAGES.length - 1, idx);
}

function formatTimestamp(iso: string | null | undefined): string {
  if (!iso) return "—";
  try {
    const d = new Date(iso);
    return d.toLocaleTimeString([], {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
  } catch {
    return "—";
  }
}

function statusLabel(status: JobStatus): string {
  switch (status) {
    case "queued":
      return "Upload received · waiting for worker";
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
   * ETA estimator to derive a real throughput readout from `[TIMING]` lines.
   * Optional so existing call sites that don't have logs still compile; in
   * that case the ETA row degrades to "Estimating..." while running.
   */
  logLines?: string[];
}

export function JobStatusPanel({ result, logLines = [] }: JobStatusPanelProps) {
  const activeIdx = estimateStage(result);
  const eta = useEtaEstimator(result, logLines);
  const fileExt =
    result.input_filename?.toLowerCase().endsWith(".xlsx") ? "XLSX" : "CSV";

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

        {result.status === "queued" && (
          <div className={styles.hint}>
            The panda is spinning up. This usually takes a few seconds. You
            can leave this page and come back — the job keeps running on the
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
