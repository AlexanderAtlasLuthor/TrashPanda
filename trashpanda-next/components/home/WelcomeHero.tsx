"use client";

import type { WorkspaceStats } from "./useWorkspaceStats";
import styles from "./WelcomeHero.module.css";

/**
 * Welcome block at the top of the Home dashboard.
 *
 * Holds the greeting + headline + a single hero metric. The supporting
 * stats live in <WorkspaceMetrics/> below — duplicating them here would
 * flatten the hierarchy and force the reader to scan the same numbers
 * twice.
 */

interface Props {
  userName: string;
  stats: WorkspaceStats;
}

function pickHeadline(stats: WorkspaceStats): string {
  if (stats.loading) return "Your workspace is loading…";
  if (!stats.hasAnyJobs) {
    return "Your workspace is ready. Upload a file to start cleaning.";
  }
  if (stats.totalCompleted === 0) {
    return "Your first jobs are on the way — results land here.";
  }
  if (stats.avgReadyPct !== null) {
    if (stats.avgReadyPct >= 90) {
      return "Most of your recent jobs are in excellent shape.";
    }
    if (stats.avgReadyPct >= 75) {
      return "Your lists are in good shape — a few need closer review.";
    }
    return "Some of your lists need attention before you send.";
  }
  return "Your latest jobs, insights, and recommendations are ready.";
}

function formatPct(n: number | null): string | null {
  if (n === null) return null;
  const s = n.toFixed(1);
  return `${s.endsWith(".0") ? s.slice(0, -2) : s}%`;
}

export function WelcomeHero({ userName, stats }: Props) {
  const headline = pickHeadline(stats);
  const pct = formatPct(stats.avgReadyPct);
  const showHeroStat = !stats.loading && stats.totalCompleted > 0 && pct !== null;

  return (
    <div className={styles.hero}>
      <div className={styles.left}>
        <div className={styles.kicker}>// WORKSPACE OVERVIEW</div>
        <h2 className={styles.title}>
          Welcome back, <span className={styles.accent}>{userName}</span>
        </h2>
        <p className={styles.subtitle}>{headline}</p>
      </div>

      {showHeroStat && (
        <div className={styles.heroStat}>
          <div className={styles.heroValue}>{pct}</div>
          <div className={styles.heroLabel}>Average ready-to-send</div>
          <div className={styles.heroSub}>
            across your last{" "}
            {stats.recentCompletedSummaries.length} completed job
            {stats.recentCompletedSummaries.length !== 1 ? "s" : ""}
          </div>
        </div>
      )}
    </div>
  );
}
