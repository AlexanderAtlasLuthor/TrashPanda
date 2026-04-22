"use client";

import type { WorkspaceStats } from "./useWorkspaceStats";
import styles from "./WelcomeHero.module.css";

/**
 * Premium welcome block shown at the top of the Home/Dashboard.
 * Keeps the existing dark/neon/industrial visual language — no new tokens.
 *
 * The "headline" picks its copy from the current workspace state so it
 * always feels relevant (not a generic admin greeting).
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

export function WelcomeHero({ userName, stats }: Props) {
  const headline = pickHeadline(stats);
  return (
    <div className={styles.hero}>
      <div className={styles.left}>
        <div className={styles.kicker}>// WORKSPACE OVERVIEW</div>
        <h2 className={styles.title}>
          Welcome back, <span className={styles.accent}>{userName}</span>
        </h2>
        <p className={styles.subtitle}>{headline}</p>
      </div>

      <div className={styles.right}>
        <HeroStat
          label="Jobs processed"
          value={stats.loading ? "—" : String(stats.filesProcessed)}
        />
        <HeroStat
          label="Records cleaned"
          value={stats.loading ? "—" : stats.totalRecords.toLocaleString("en-US")}
        />
        <HeroStat
          label="Avg ready-to-send"
          value={
            stats.loading
              ? "—"
              : stats.avgReadyPct === null
                ? "—"
                : `${stats.avgReadyPct.toFixed(1).replace(/\.0$/, "")}%`
          }
          accent
        />
        <HeroStat
          label="High-risk removed"
          value={stats.loading ? "—" : stats.totalInvalid.toLocaleString("en-US")}
          danger
        />
      </div>
    </div>
  );
}

function HeroStat({
  label,
  value,
  accent,
  danger,
}: {
  label: string;
  value: string;
  accent?: boolean;
  danger?: boolean;
}) {
  return (
    <div className={styles.stat}>
      <div
        className={[
          styles.statValue,
          accent && styles.valueAccent,
          danger && styles.valueDanger,
        ]
          .filter(Boolean)
          .join(" ")}
      >
        {value}
      </div>
      <div className={styles.statLabel}>// {label}</div>
    </div>
  );
}
