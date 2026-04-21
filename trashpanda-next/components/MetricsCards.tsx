import type { JobSummary } from "@/lib/types";
import type { Severity } from "@/lib/mockup-theme";
import styles from "./MetricsCards.module.css";

function fmt(n: number | null | undefined): string {
  if (n === null || n === undefined) return "—";
  return n.toLocaleString();
}

interface MetricProps {
  label: string;
  value: number | null | undefined;
  severity?: Severity;
  delta?: string;
  small?: boolean;
}

function Metric({ label, value, severity = "neutral", delta, small }: MetricProps) {
  const severityClass =
    severity === "ok"
      ? styles.ok
      : severity === "warn"
        ? styles.warn
        : severity === "bad"
          ? styles.bad
          : severity === "info"
            ? styles.info
            : "";

  return (
    <div
      className={[
        styles.metric,
        small && styles.metricSmall,
        severityClass,
      ]
        .filter(Boolean)
        .join(" ")}
    >
      <div className={[styles.label, small && styles.labelSmall].filter(Boolean).join(" ")}>
        // {label}
      </div>
      <div
        className={
          value === null || value === undefined
            ? styles.emptyValue
            : [styles.value, small && styles.valueSmall].filter(Boolean).join(" ")
        }
      >
        {fmt(value)}
      </div>
      {delta && <div className={styles.delta}>{delta}</div>}
    </div>
  );
}

interface MetricsCardsProps {
  summary: JobSummary | null | undefined;
}

/**
 * Primary metrics block: 4 top-line stats that every user cares about.
 * Always rendered; empty states render as "—" so the layout is stable
 * across queued/running/completed.
 */
export function MetricsCards({ summary }: MetricsCardsProps) {
  const total = summary?.total_input_rows;
  const valid = summary?.total_valid;
  const review = summary?.total_review;
  const invalid = summary?.total_invalid_or_bounce_risk;

  const validPct =
    total && valid !== null && valid !== undefined
      ? ((valid / total) * 100).toFixed(1) + "% of total"
      : undefined;
  const reviewPct =
    total && review !== null && review !== undefined
      ? ((review / total) * 100).toFixed(1) + "% needs review"
      : undefined;
  const invalidPct =
    total && invalid !== null && invalid !== undefined
      ? ((invalid / total) * 100).toFixed(1) + "% flagged"
      : undefined;

  return (
    <div className={styles.metrics}>
      <Metric label="Total rows" value={total} delta="ingested" />
      <Metric label="Valid / deliverable" value={valid} severity="ok" delta={validPct} />
      <Metric label="Recoverable" value={review} severity="warn" delta={reviewPct} />
      <Metric label="Purged" value={invalid} severity="bad" delta={invalidPct} />
    </div>
  );
}

/**
 * Secondary metrics: pipeline-level counters (deduplication, typo fixes, etc.)
 * These come straight from the backend's JobSummary.
 */
export function SecondaryMetrics({ summary }: MetricsCardsProps) {
  return (
    <div className={styles.metricsSecondary}>
      <Metric small label="Duplicates removed" value={summary?.duplicates_removed} severity="info" />
      <Metric small label="Typo corrections" value={summary?.typo_corrections} severity="ok" />
      <Metric small label="Disposable" value={summary?.disposable_emails} severity="bad" />
      <Metric small label="Placeholder / fake" value={summary?.placeholder_or_fake_emails} severity="bad" />
      <Metric small label="Role-based" value={summary?.role_based_emails} severity="warn" />
    </div>
  );
}
