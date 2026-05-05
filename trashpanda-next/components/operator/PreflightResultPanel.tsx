import type { PreflightResult } from "@/lib/types";
import { StatusBadge } from "./StatusBadge";
import { IssuesList } from "./IssuesList";
import {
  OperatorEmptyState,
  OperatorErrorState,
  OperatorLoadingState,
} from "./OperatorPanelStates";
import styles from "./PreflightResultPanel.module.css";

interface PreflightResultPanelProps {
  result: PreflightResult | null;
  loading?: boolean;
  error?: string | null;
}

function formatBytes(bytes: number | null | undefined): string {
  if (bytes === null || bytes === undefined || Number.isNaN(bytes)) return "—";
  if (bytes < 1024) return `${bytes} B`;
  const kb = bytes / 1024;
  if (kb < 1024) return `${kb.toFixed(1)} KB`;
  const mb = kb / 1024;
  if (mb < 1024) return `${mb.toFixed(2)} MB`;
  const gb = mb / 1024;
  return `${gb.toFixed(2)} GB`;
}

function formatBoolean(value: boolean | null | undefined): string {
  if (value === true) return "true";
  if (value === false) return "false";
  return "—";
}

function formatNumber(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  return value.toLocaleString();
}

interface StatRow {
  label: string;
  value: string;
}

export function PreflightResultPanel({
  result,
  loading,
  error,
}: PreflightResultPanelProps) {
  if (loading) {
    return (
      <section className={styles.panel} aria-label="Preflight result">
        <OperatorLoadingState message="Running preflight…" />
      </section>
    );
  }

  if (error) {
    return (
      <section className={styles.panel} aria-label="Preflight result">
        <OperatorErrorState title="Preflight failed." message={error} />
      </section>
    );
  }

  if (!result) {
    return (
      <section className={styles.panel} aria-label="Preflight result">
        <OperatorEmptyState
          title="No preflight has been run yet."
          message="Fill in the form above to run preflight."
        />
      </section>
    );
  }

  // Backend currently emits profile/row_count_estimate/file_size_bytes before
  // the TS contract was aligned; keep this local bridge until a dedicated
  // contract-alignment subphase. Do NOT modify lib/types.ts here.
  const raw = result as PreflightResult & {
    profile?: string | null;
    row_count_estimate?: number | null;
    file_size_bytes?: number | null;
  };

  const stats: StatRow[] = [];
  if (raw.row_count_estimate !== null && raw.row_count_estimate !== undefined) {
    stats.push({
      label: "Row count estimate",
      value: formatNumber(raw.row_count_estimate),
    });
  }
  if (raw.file_size_bytes !== null && raw.file_size_bytes !== undefined) {
    stats.push({
      label: "File size",
      value: formatBytes(raw.file_size_bytes),
    });
  }
  if (result.total_rows !== null && result.total_rows !== undefined) {
    stats.push({
      label: "Total rows",
      value: formatNumber(result.total_rows),
    });
  }
  if (result.total_emails !== null && result.total_emails !== undefined) {
    stats.push({
      label: "Total emails",
      value: formatNumber(result.total_emails),
    });
  }
  if (result.unique_emails !== null && result.unique_emails !== undefined) {
    stats.push({
      label: "Unique emails",
      value: formatNumber(result.unique_emails),
    });
  }
  if (
    result.duplicate_emails !== null &&
    result.duplicate_emails !== undefined
  ) {
    stats.push({
      label: "Duplicate emails",
      value: formatNumber(result.duplicate_emails),
    });
  }
  if (result.invalid_emails !== null && result.invalid_emails !== undefined) {
    stats.push({
      label: "Invalid emails",
      value: formatNumber(result.invalid_emails),
    });
  }
  if (
    result.smtp_port_verified !== null &&
    result.smtp_port_verified !== undefined
  ) {
    stats.push({
      label: "SMTP port verified",
      value: formatBoolean(result.smtp_port_verified),
    });
  }
  if (
    result.operator_confirmed_large_run !== null &&
    result.operator_confirmed_large_run !== undefined
  ) {
    stats.push({
      label: "Operator confirmed large run",
      value: formatBoolean(result.operator_confirmed_large_run),
    });
  }

  return (
    <section className={styles.panel} aria-label="Preflight result">
      <header className={styles.header}>
        <div className={styles.titleWrap}>
          <div className={styles.eyebrow}>// PREFLIGHT RESULT</div>
          <div className={styles.title}>Preflight result</div>
        </div>
        <div className={styles.headMeta}>
          <StatusBadge status={result.status} />
          {raw.profile && (
            <span className={styles.profile}>profile: {raw.profile}</span>
          )}
        </div>
      </header>

      {(result.input_filename || result.generated_at) && (
        <div className={styles.metaRow}>
          {result.input_filename && (
            <div className={styles.metaItem}>
              <span className={styles.metaLabel}>Input</span>
              <span className={styles.metaValue}>{result.input_filename}</span>
            </div>
          )}
          {result.generated_at && (
            <div className={styles.metaItem}>
              <span className={styles.metaLabel}>Generated</span>
              <span className={styles.metaValue}>{result.generated_at}</span>
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
          Issues ({(result.issues ?? []).length})
        </div>
        <IssuesList
          issues={result.issues}
          emptyLabel="No preflight issues reported."
        />
      </div>
    </section>
  );
}
