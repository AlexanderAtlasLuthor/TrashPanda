"use client";

import type { WorkspaceStats } from "./useWorkspaceStats";
import styles from "./WorkspaceMetrics.module.css";

/**
 * "Recent performance" block — workspace-level counters aggregated across
 * the most recent completed jobs.
 *
 * Visual rules:
 *   - "Avg list quality" is the hero tile (wide, accented).
 *   - Every other tile is neutral until its counter actually has something
 *     to say. A disposable/catch-all counter at 0 shouldn't glow red.
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
  const avgQuality = stats.avgReadyPctThisWeek ?? stats.avgReadyPct;

  // No jobs at all — one friendly empty tile instead of six zeros.
  if (!stats.loading && !stats.hasAnyJobs) {
    return (
      <section className={styles.section}>
        <SectionHead stats={stats} />
        <div className={styles.empty}>
          <div className={styles.emptyTitle}>No performance data yet</div>
          <div className={styles.emptyBody}>
            Upload your first CSV or XLSX file to see list quality, review
            queue volume, and workspace-level signals here.
          </div>
        </div>
      </section>
    );
  }

  return (
    <section className={styles.section}>
      <SectionHead stats={stats} />

      <div className={styles.grid}>
        <Tile
          hero
          label="Avg list quality"
          value={fmtPct(avgQuality)}
          hint="ready-to-send share, recent jobs"
        />
        <Tile
          label="Jobs this week"
          value={fmt(stats.jobsThisWeek)}
          hint={
            stats.jobsThisWeek === 0 ? "No activity in the last 7 days" : "Last 7 days"
          }
        />
        <Tile
          label="Emails cleaned"
          value={fmt(stats.recordsThisWeek)}
          hint="This week"
        />
        <Tile
          label="Review queue volume"
          value={fmt(stats.totalReview)}
          tone={stats.totalReview > 0 ? "warn" : undefined}
          hint={
            stats.totalReview > 0
              ? "Needs your attention"
              : "No records flagged for review"
          }
        />
        <Tile
          label="High-risk removed"
          value={fmt(stats.invalidThisWeek || stats.totalInvalid)}
          tone={
            (stats.invalidThisWeek || stats.totalInvalid) > 0 ? "bad" : undefined
          }
          hint={
            (stats.invalidThisWeek || stats.totalInvalid) === 0
              ? "No high-risk addresses removed"
              : stats.invalidThisWeek
                ? "This week"
                : "Recent total"
          }
        />
        <Tile
          label="Duplicates removed"
          value={fmt(stats.duplicatesRemoved)}
          hint={
            stats.duplicatesRemoved > 0
              ? "Across recent jobs"
              : "No duplicates found"
          }
        />
        {stats.latestInsights ? (
          <>
            <Tile
              label="Catch-all detected"
              value={fmt(stats.catchAllDetectedCount)}
              tone={stats.catchAllDetectedCount > 0 ? "warn" : undefined}
              hint="Latest job, V2 signal"
            />
            <Tile
              label="SMTP tested"
              value={fmt(stats.smtpTestedCount)}
              hint="Latest job, V2 signal"
            />
          </>
        ) : (
          <>
            <Tile
              label="Typo corrections"
              value={fmt(stats.typoCorrections)}
              hint="Domain typos auto-fixed"
            />
            <Tile
              label="Role-based"
              value={fmt(stats.roleBasedEmails)}
              tone={stats.roleBasedEmails > 0 ? "warn" : undefined}
              hint="info@, admin@, support@"
            />
          </>
        )}
      </div>
    </section>
  );
}

function SectionHead({ stats }: { stats: WorkspaceStats }) {
  return (
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
  );
}

function Tile({
  label,
  value,
  hint,
  tone,
  hero,
}: {
  label: string;
  value: string;
  hint?: string;
  tone?: "ok" | "warn" | "bad" | "info";
  hero?: boolean;
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
    <div
      className={[
        styles.tile,
        hero && styles.tileHero,
        toneClass,
      ]
        .filter(Boolean)
        .join(" ")}
    >
      <div className={styles.tileLabel}>// {label}</div>
      <div className={hero ? styles.tileValueHero : styles.tileValue}>{value}</div>
      {hint && <div className={styles.tileHint}>{hint}</div>}
    </div>
  );
}
