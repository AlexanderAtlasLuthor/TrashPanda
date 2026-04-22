"use client";

import type { WorkspaceStats } from "./useWorkspaceStats";
import styles from "./WorkspaceMetrics.module.css";

/**
 * "Recent performance" block — workspace-level counters aggregated across
 * the most recent completed jobs. No charts; just clear, scannable tiles.
 *
 * When the backend has not produced enough data yet, values degrade to
 * "0" or "—" without breaking the layout.
 */

interface Props {
  stats: WorkspaceStats;
}

function fmt(n: number): string {
  return n.toLocaleString("en-US");
}

function fmtPct(n: number | null): string {
  if (n === null) return "—";
  const s = n.toFixed(1);
  return `${s.endsWith(".0") ? s.slice(0, -2) : s}%`;
}

export function WorkspaceMetrics({ stats }: Props) {
  return (
    <section className={styles.section}>
      <div className={styles.head}>
        <span className={styles.title}>Recent performance</span>
        <span className={styles.sub}>
          {stats.loading
            ? "Loading workspace signals…"
            : stats.totalCompleted === 0
              ? "No completed jobs yet"
              : `Based on your last ${stats.recentCompletedSummaries.length} completed job${stats.recentCompletedSummaries.length !== 1 ? "s" : ""}`}
        </span>
      </div>

      <div className={styles.grid}>
        <Tile
          label="Jobs this week"
          value={fmt(stats.jobsThisWeek)}
          hint={stats.jobsThisWeek === 0 ? "no recent activity" : "last 7 days"}
        />
        <Tile
          label="Emails cleaned"
          value={fmt(stats.recordsThisWeek)}
          hint="this week"
        />
        <Tile
          label="Avg list quality"
          value={fmtPct(stats.avgReadyPctThisWeek ?? stats.avgReadyPct)}
          tone="ok"
          hint="ready-to-send share"
        />
        <Tile
          label="High-risk removed"
          value={fmt(stats.invalidThisWeek || stats.totalInvalid)}
          tone="bad"
          hint={stats.invalidThisWeek ? "this week" : "recent total"}
        />
        <Tile
          label="Review queue volume"
          value={fmt(stats.totalReview)}
          tone="warn"
          hint="needs attention"
        />
        <Tile
          label="Duplicates removed"
          value={fmt(stats.duplicatesRemoved)}
          hint="across recent jobs"
        />
        {stats.latestInsights ? (
          <>
            <Tile
              label="Catch-all detected"
              value={fmt(stats.catchAllDetectedCount)}
              tone="warn"
              hint="latest job · V2 signal"
            />
            <Tile
              label="SMTP tested"
              value={fmt(stats.smtpTestedCount)}
              tone="info"
              hint="latest job · V2 signal"
            />
          </>
        ) : (
          <>
            <Tile
              label="Typo corrections"
              value={fmt(stats.typoCorrections)}
              tone="ok"
              hint="domain typos auto-fixed"
            />
            <Tile
              label="Role-based"
              value={fmt(stats.roleBasedEmails)}
              tone="warn"
              hint="info@, admin@, support@"
            />
          </>
        )}
      </div>
    </section>
  );
}

function Tile({
  label,
  value,
  hint,
  tone,
}: {
  label: string;
  value: string;
  hint?: string;
  tone?: "ok" | "warn" | "bad" | "info";
}) {
  const toneClass =
    tone === "ok"
      ? styles.toneOk
      : tone === "warn"
        ? styles.toneWarn
        : tone === "bad"
          ? styles.toneBad
          : tone === "info"
            ? styles.toneInfo
            : "";
  return (
    <div className={[styles.tile, toneClass].filter(Boolean).join(" ")}>
      <div className={styles.tileLabel}>// {label}</div>
      <div className={styles.tileValue}>{value}</div>
      {hint && <div className={styles.tileHint}>{hint}</div>}
    </div>
  );
}
