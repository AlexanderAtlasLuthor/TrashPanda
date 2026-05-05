import type { FeedbackIngestionSummary } from "@/lib/types";
import { StatusBadge } from "./StatusBadge";
import { IssuesList } from "./IssuesList";
import {
  OperatorEmptyState,
  OperatorErrorState,
  OperatorLoadingState,
} from "./OperatorPanelStates";
import styles from "./FeedbackIngestionSummaryPanel.module.css";

interface FeedbackIngestionSummaryPanelProps {
  summary: FeedbackIngestionSummary | null;
  loading?: boolean;
  error?: string | null;
}

interface IssueLike {
  severity?: string | null;
  code?: string | null;
  message?: string | null;
}

interface StatRow {
  label: string;
  value: string;
}

function formatNumber(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  return value.toLocaleString();
}

function pushStat(
  out: StatRow[],
  label: string,
  value: number | null | undefined,
) {
  if (value === null || value === undefined) return;
  out.push({ label, value: formatNumber(value) });
}

export function FeedbackIngestionSummaryPanel({
  summary,
  loading,
  error,
}: FeedbackIngestionSummaryPanelProps) {
  if (loading) {
    return (
      <section className={styles.panel} aria-label="Feedback ingestion">
        <OperatorLoadingState message="Ingesting feedback…" />
      </section>
    );
  }

  if (error) {
    return (
      <section className={styles.panel} aria-label="Feedback ingestion">
        <OperatorErrorState
          title="Feedback ingest failed."
          message={error}
        />
      </section>
    );
  }

  if (!summary) {
    return (
      <section className={styles.panel} aria-label="Feedback ingestion">
        <OperatorEmptyState
          title="No feedback has been ingested yet."
          message="Enter a server-side feedback CSV path to ingest bounce outcomes."
        />
      </section>
    );
  }

  // Backend ingestion payload currently includes outcome counts/store
  // timing fields before the TS contract was aligned; keep this local
  // bridge until a dedicated contract-alignment subphase. Do NOT modify
  // lib/types.ts here.
  const raw = summary as FeedbackIngestionSummary & {
    invalid_email_rows?: number | null;
    unknown_outcome_rows?: number | null;
    domains_updated?: number | null;
    delivered_count?: number | null;
    hard_bounce_count?: number | null;
    soft_bounce_count?: number | null;
    blocked_count?: number | null;
    deferred_count?: number | null;
    complaint_count?: number | null;
    unsubscribed_count?: number | null;
    unknown_count?: number | null;
    error?: string | null;
    store_path?: string | null;
    started_at?: string | null;
    finished_at?: string | null;
  };

  const stats: StatRow[] = [];
  pushStat(stats, "Total rows", summary.total_rows);
  pushStat(stats, "Accepted rows", summary.accepted_rows);
  pushStat(stats, "Skipped rows", summary.skipped_rows);
  pushStat(stats, "Error rows", summary.error_rows);
  pushStat(stats, "Invalid email rows", raw.invalid_email_rows);
  pushStat(stats, "Unknown outcome rows", raw.unknown_outcome_rows);
  pushStat(stats, "Unique emails", summary.unique_emails);
  pushStat(stats, "Unique domains", summary.unique_domains);
  pushStat(stats, "Domains updated", raw.domains_updated);
  pushStat(stats, "Delivered", raw.delivered_count);
  pushStat(stats, "Hard bounces", raw.hard_bounce_count);
  pushStat(stats, "Soft bounces", raw.soft_bounce_count);
  pushStat(stats, "Blocked", raw.blocked_count);
  pushStat(stats, "Deferred", raw.deferred_count);
  pushStat(stats, "Complaints", raw.complaint_count);
  pushStat(stats, "Unsubscribed", raw.unsubscribed_count);
  pushStat(stats, "Unknown outcomes", raw.unknown_count);

  const issues: IssueLike[] = [];
  if (raw.error) {
    issues.push({
      severity: "block",
      code: "ingest_error",
      message: raw.error,
    });
  }
  if (Array.isArray(summary.errors)) {
    for (const err of summary.errors) {
      const code = err.code ?? "ingest_row_error";
      const rowSuffix =
        err.row !== null && err.row !== undefined ? ` (row ${err.row})` : "";
      issues.push({
        severity: "block",
        code,
        message: `${err.message ?? "(no message)"}${rowSuffix}`,
      });
    }
  }

  const status = raw.error ? "error" : summary.status ?? "ok";

  return (
    <section className={styles.panel} aria-label="Feedback ingestion">
      <header className={styles.header}>
        <div className={styles.titleWrap}>
          <div className={styles.eyebrow}>// FEEDBACK INGESTION</div>
          <div className={styles.title}>Feedback ingestion</div>
        </div>
        <div className={styles.headMeta}>
          <StatusBadge status={status} />
        </div>
      </header>

      {(raw.store_path || raw.started_at || raw.finished_at ||
        summary.generated_at) && (
        <div className={styles.metaRow}>
          {raw.store_path && (
            <div className={styles.metaItem}>
              <span className={styles.metaLabel}>Store</span>
              <span className={styles.metaValue}>{raw.store_path}</span>
            </div>
          )}
          {raw.started_at && (
            <div className={styles.metaItem}>
              <span className={styles.metaLabel}>Started</span>
              <span className={styles.metaValue}>{raw.started_at}</span>
            </div>
          )}
          {raw.finished_at && (
            <div className={styles.metaItem}>
              <span className={styles.metaLabel}>Finished</span>
              <span className={styles.metaValue}>{raw.finished_at}</span>
            </div>
          )}
          {summary.generated_at && (
            <div className={styles.metaItem}>
              <span className={styles.metaLabel}>Generated</span>
              <span className={styles.metaValue}>{summary.generated_at}</span>
            </div>
          )}
        </div>
      )}

      {stats.length > 0 && (
        <div className={styles.statsGrid}>
          {stats.map((s) => (
            <div key={s.label} className={styles.stat}>
              <div className={styles.statValue}>{s.value}</div>
              <div className={styles.statLabel}>{s.label}</div>
            </div>
          ))}
        </div>
      )}

      <div className={styles.section}>
        <div className={styles.sectionLabel}>
          Issues ({issues.length})
        </div>
        <IssuesList
          issues={issues}
          emptyLabel="No ingestion errors reported."
        />
      </div>
    </section>
  );
}
