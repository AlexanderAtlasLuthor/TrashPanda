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
  hero?: boolean;
  tooltip?: string;
  emptyHint?: string;
  zeroNote?: string;
}

function Metric({
  label,
  value,
  severity = "neutral",
  delta,
  small,
  hero,
  tooltip,
  emptyHint,
  zeroNote,
}: MetricProps) {
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

  // A metric only "alerts" (flares its color) when it has a real non-zero
  // count to draw attention to. Zero or missing values stay neutral so that
  // red/orange is reserved for genuine issues.
  const isAlert =
    severity !== "ok" &&
    severity !== "neutral" &&
    typeof value === "number" &&
    value > 0;

  const isEmpty = value === null || value === undefined;
  const isZero = value === 0;

  return (
    <div
      className={[
        styles.metric,
        small && styles.metricSmall,
        hero && styles.metricHero,
        severityClass,
        isAlert && styles.alert,
      ]
        .filter(Boolean)
        .join(" ")}
    >
      <div
        className={[
          styles.label,
          small && styles.labelSmall,
          hero && styles.labelHero,
        ]
          .filter(Boolean)
          .join(" ")}
      >
        // {label}
        {tooltip && (
          <span className={styles.tip} data-tip={tooltip} aria-label={tooltip}>
            ⓘ
          </span>
        )}
      </div>
      <div
        className={
          isEmpty
            ? styles.emptyValue
            : [
                styles.value,
                small && styles.valueSmall,
                hero && styles.valueHero,
              ]
                .filter(Boolean)
                .join(" ")
        }
      >
        {fmt(value)}
      </div>
      {isEmpty && emptyHint && (
        <div className={styles.emptyHint}>{emptyHint}</div>
      )}
      {!isEmpty && isZero && zeroNote && (
        <div className={styles.zeroNote}>{zeroNote}</div>
      )}
      {!isEmpty && !isZero && delta && (
        <div className={[styles.delta, hero && styles.deltaHero].filter(Boolean).join(" ")}>
          {delta}
        </div>
      )}
    </div>
  );
}

interface MetricsCardsProps {
  summary: JobSummary | null | undefined;
}

/**
 * Primary metrics block. "Ready to send" is the hero — it's the single
 * number a user needs after a run — everything else is a supporting stat.
 */
export function MetricsCards({ summary }: MetricsCardsProps) {
  const total = summary?.total_input_rows;
  const valid = summary?.total_valid;
  const review = summary?.total_review;
  const invalid = summary?.total_invalid_or_bounce_risk;

  const validPct =
    total && valid !== null && valid !== undefined
      ? ((valid / total) * 100).toFixed(1) + "% of your list is ready to send"
      : undefined;
  const reviewPct =
    total && review !== null && review !== undefined && review > 0
      ? ((review / total) * 100).toFixed(1) + "% of your list"
      : undefined;
  const invalidPct =
    total && invalid !== null && invalid !== undefined && invalid > 0
      ? ((invalid / total) * 100).toFixed(1) + "% high-risk"
      : undefined;

  return (
    <div className={styles.metrics}>
      <Metric
        label="Ready to send"
        value={valid}
        severity="ok"
        hero
        delta={validPct}
        tooltip="Emails that passed every strict validation rule — safe to send."
        emptyHint="Waiting for job to finish."
      />
      <Metric
        label="Emails scanned"
        value={total}
        emptyHint="No rows processed yet."
      />
      <Metric
        label="Needs attention"
        value={review}
        severity="warn"
        delta={reviewPct}
        tooltip="May require manual review before sending."
        emptyHint="Waiting for job to finish."
        zeroNote="Nothing to review — all records classified automatically."
      />
      <Metric
        label="Do not use"
        value={invalid}
        severity="bad"
        delta={invalidPct}
        tooltip="High risk of bounce or invalid address."
        emptyHint="Waiting for job to finish."
        zeroNote="No high-risk addresses detected."
      />
    </div>
  );
}

/**
 * Secondary pipeline counters. Kept deliberately quiet — neutral styling
 * by default so they don't compete with the primary metrics row. A counter
 * only lights up (danger/warn) when it has a non-zero value.
 */
export function SecondaryMetrics({ summary }: MetricsCardsProps) {
  const dup = summary?.duplicates_removed;
  const typo = summary?.typo_corrections;
  const disposable = summary?.disposable_emails;
  const placeholder = summary?.placeholder_or_fake_emails;
  const role = summary?.role_based_emails;

  const anySignal =
    [dup, typo, disposable, placeholder, role].some(
      (n) => typeof n === "number" && n > 0,
    );

  return (
    <div>
      <div className={styles.secondaryHeading}>// Pipeline details</div>
      {summary && !anySignal ? (
        <div
          className={styles.metricsSecondary}
          style={{ gridTemplateColumns: "1fr" }}
        >
          <div className={styles.allClear}>
            No duplicates, typos, disposable, placeholder, or role-based
            addresses found in this batch.
          </div>
        </div>
      ) : (
        <div className={styles.metricsSecondary}>
          <Metric
            small
            label="Duplicates removed"
            value={dup}
            severity="info"
            tooltip="Exact and near-duplicate addresses removed."
          />
          <Metric
            small
            label="Typo corrections"
            value={typo}
            severity="ok"
            tooltip="Domain typos auto-corrected (e.g. gmial → gmail)."
          />
          <Metric
            small
            label="Disposable"
            value={disposable}
            severity="bad"
            tooltip="Temporary or throwaway email addresses."
          />
          <Metric
            small
            label="Fake or placeholder"
            value={placeholder}
            severity="bad"
            tooltip="Addresses like test@test.com, noreply@, etc."
          />
          <Metric
            small
            label="Role-based"
            value={role}
            severity="warn"
            tooltip="Shared inboxes like info@, admin@, support@."
          />
        </div>
      )}
    </div>
  );
}
