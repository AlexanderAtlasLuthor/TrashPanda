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
  tooltip?: string;
}

function Metric({ label, value, severity = "neutral", delta, small, tooltip }: MetricProps) {
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
        {tooltip && (
          <span className={styles.tip} data-tip={tooltip} aria-label={tooltip}>
            ⓘ
          </span>
        )}
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
      ? ((valid / total) * 100).toFixed(1) + "% of your list"
      : undefined;
  const reviewPct =
    total && review !== null && review !== undefined
      ? ((review / total) * 100).toFixed(1) + "% of your list"
      : undefined;
  const invalidPct =
    total && invalid !== null && invalid !== undefined
      ? ((invalid / total) * 100).toFixed(1) + "% high-risk"
      : undefined;

  return (
    <div className={styles.metrics}>
      <Metric label="Emails scanned" value={total} />
      <Metric
        label="Ready to send"
        value={valid}
        severity="ok"
        delta={validPct}
        tooltip="Uses strict validation rules to determine if an email is safe to send."
      />
      <Metric
        label="Needs attention"
        value={review}
        severity="warn"
        delta={reviewPct}
        tooltip="May require manual review before sending"
      />
      <Metric
        label="Do not use"
        value={invalid}
        severity="bad"
        delta={invalidPct}
        tooltip="High risk of bounce or invalid address"
      />
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
      <Metric
        small
        label="Duplicates removed"
        value={summary?.duplicates_removed}
        severity="info"
        tooltip="Exact and near-duplicate addresses removed"
      />
      <Metric
        small
        label="Typo corrections"
        value={summary?.typo_corrections}
        severity="ok"
        tooltip="Domain typos auto-corrected (e.g. gmial → gmail)"
      />
      <Metric
        small
        label="Disposable"
        value={summary?.disposable_emails}
        severity="bad"
        tooltip="Temporary or throwaway email addresses"
      />
      <Metric
        small
        label="Fake or placeholder"
        value={summary?.placeholder_or_fake_emails}
        severity="bad"
        tooltip="Addresses like test@test.com, noreply@, etc."
      />
      <Metric
        small
        label="Role-based"
        value={summary?.role_based_emails}
        severity="warn"
        tooltip="Shared inboxes like info@, admin@, support@"
      />
    </div>
  );
}
