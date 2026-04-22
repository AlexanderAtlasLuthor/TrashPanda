"use client";

import Link from "next/link";
import type { WorkspaceStats } from "./useWorkspaceStats";
import styles from "./RiskHighlightsPanel.module.css";

/**
 * Risk & reliability snapshot for the most recent completed job that has
 * V2 insights available. If no V2 data is present the whole panel falls
 * back to a short, friendly empty state instead of hiding silently —
 * keeps the dashboard layout stable for new users.
 */

interface Props {
  stats: WorkspaceStats;
}

const EMPTY_HINT = "Run a file through the V2 engine to see domain signals here.";

export function RiskHighlightsPanel({ stats }: Props) {
  const ins = stats.latestInsights;
  const jobId = stats.latestCompletedJob?.job_id;

  return (
    <section className={styles.panel}>
      <div className={styles.head}>
        <span className={styles.title}>Domain intelligence</span>
        <span className={styles.sub}>
          {ins
            ? "// Latest job · V2 signals"
            : "// Waiting for signals"}
        </span>
      </div>

      {!ins ? (
        <div className={styles.empty}>
          <div className={styles.emptyTitle}>
            {stats.hasAnyJobs
              ? "No V2 intelligence yet"
              : "No jobs yet"}
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
          />
          <Row
            tone="ok"
            label="Top reliable domains"
            value={ins.domain_intelligence.reliable.length}
            sample={ins.domain_intelligence.reliable
              .slice(0, 3)
              .map((d) => d.domain)}
          />
          <Row
            tone="warn"
            label="Catch-all suspected"
            value={ins.domain_intelligence.catch_all_suspected.length}
            sample={ins.domain_intelligence.catch_all_suspected
              .slice(0, 3)
              .map((d) => d.domain)}
          />
          <Row
            tone="info"
            label="Most common review reason"
            value={null}
            sample={[mostCommonReviewReason(ins)]}
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
}: {
  label: string;
  value: number | null;
  sample: string[];
  tone: "ok" | "warn" | "bad" | "info";
}) {
  const displaySample = sample.filter(Boolean).join(" · ") || "—";
  const toneClass =
    tone === "ok"
      ? styles.toneOk
      : tone === "warn"
        ? styles.toneWarn
        : tone === "bad"
          ? styles.toneBad
          : styles.toneInfo;
  return (
    <div className={[styles.row, toneClass].join(" ")}>
      <div className={styles.rowLeft}>
        <div className={styles.rowLabel}>// {label}</div>
        <div className={styles.rowSample} title={displaySample}>
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
  if (counts.size === 0) return "none flagged";
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
