"use client";

import Link from "next/link";
import type { WorkspaceStats } from "./useWorkspaceStats";
import styles from "./RiskHighlightsPanel.module.css";

/**
 * Risk & reliability snapshot for the most recent completed job that has
 * V2 insights available. If no V2 data is present the whole panel falls
 * back to a short, friendly empty state instead of hiding silently.
 *
 * Styling rules:
 *   - "Top reliable" (ok) is the only row that earns a neon-coloured count.
 *   - warn/bad rows keep their thin left bar but render the count in the
 *     default ink — colour only fires when there's actually something to
 *     flag (count > 0). Zero-value rows stay neutral.
 */

interface Props {
  stats: WorkspaceStats;
}

const EMPTY_HINT =
  "Run a file through the V2 engine to see domain signals here.";

export function RiskHighlightsPanel({ stats }: Props) {
  const ins = stats.latestInsights;
  const jobId = stats.latestCompletedJob?.job_id;

  return (
    <section className={styles.panel}>
      <div className={styles.head}>
        <span className={styles.title}>Domain intelligence</span>
        <span className={styles.sub}>
          {ins ? "Latest job · V2 signals" : "Waiting for signals"}
        </span>
      </div>

      {!ins ? (
        <div className={styles.empty}>
          <div className={styles.emptyTitle}>
            {stats.hasAnyJobs ? "No V2 intelligence yet" : "No jobs yet"}
          </div>
          <div className={styles.emptyBody}>{EMPTY_HINT}</div>
        </div>
      ) : (
        <div className={styles.body}>
          <Row
            tone="bad"
            label="Top risky domains"
            value={ins.domain_intelligence.risky.length}
            sample={ins.domain_intelligence.risky
              .slice(0, 3)
              .map((d) => d.domain)}
            emptyMsg="No risky domains flagged"
          />
          <Row
            tone="ok"
            label="Top reliable domains"
            value={ins.domain_intelligence.reliable.length}
            sample={ins.domain_intelligence.reliable
              .slice(0, 3)
              .map((d) => d.domain)}
            emptyMsg="Not enough signal yet to rank reliable domains"
          />
          <Row
            tone="warn"
            label="Catch-all suspected"
            value={ins.domain_intelligence.catch_all_suspected.length}
            sample={ins.domain_intelligence.catch_all_suspected
              .slice(0, 3)
              .map((d) => d.domain)}
            emptyMsg="No catch-all domains suspected"
          />
          <Row
            tone="info"
            label="Most common review reason"
            value={null}
            sample={[mostCommonReviewReason(ins)]}
            emptyMsg="Nothing flagged for review"
          />
        </div>
      )}

      {jobId && ins && (
        <Link
          href={`/insights/${encodeURIComponent(jobId)}`}
          className={styles.cta}
        >
          Open full insights →
        </Link>
      )}
    </section>
  );
}

function Row({
  label,
  value,
  sample,
  tone,
  emptyMsg,
}: {
  label: string;
  value: number | null;
  sample: string[];
  tone: "ok" | "warn" | "bad" | "info";
  emptyMsg: string;
}) {
  const cleanedSample = sample.filter(Boolean);
  const hasSample = cleanedSample.length > 0;
  const displaySample = hasSample ? cleanedSample.join(" · ") : emptyMsg;
  // Count the row as "alerting" only when its tone is a problem tone AND
  // the value is non-zero. "ok" always paints (it's good news).
  const isAlert =
    value !== null && value > 0 && (tone === "warn" || tone === "bad");
  const toneClass =
    tone === "ok"
      ? styles.toneOk
      : tone === "warn"
        ? styles.toneWarn
        : tone === "bad"
          ? styles.toneBad
          : styles.toneInfo;

  return (
    <div
      className={[
        styles.row,
        toneClass,
        isAlert && styles.rowAlert,
        !hasSample && styles.rowMuted,
      ]
        .filter(Boolean)
        .join(" ")}
    >
      <div className={styles.rowLeft}>
        <div className={styles.rowLabel}>// {label}</div>
        <div
          className={hasSample ? styles.rowSample : styles.rowEmpty}
          title={displaySample}
        >
          {displaySample}
        </div>
      </div>
      {value !== null && (
        <div className={styles.rowValue}>{value.toLocaleString("en-US")}</div>
      )}
    </div>
  );
}

function mostCommonReviewReason(ins: {
  rows: { source: string; reason_codes_v2?: string; review_subclass?: string }[];
}): string {
  const counts = new Map<string, number>();
  for (const r of ins.rows) {
    if (r.source !== "review") continue;
    const key = (r.review_subclass || r.reason_codes_v2 || "").split("|")[0].trim();
    if (!key) continue;
    counts.set(key, (counts.get(key) ?? 0) + 1);
  }
  if (counts.size === 0) return "";
  let bestKey = "";
  let bestCount = 0;
  for (const [k, v] of counts) {
    if (v > bestCount) {
      bestKey = k;
      bestCount = v;
    }
  }
  return `${bestKey} · ${bestCount}`;
}
