"use client";

import type { WorkspaceStats } from "./useWorkspaceStats";
import styles from "./TipsPanel.module.css";

/**
 * Workspace recommendations. Mostly curated static tips; the first slot is
 * reserved for a contextual recommendation derived from current state.
 */

interface Props {
  stats: WorkspaceStats;
}

interface Tip {
  heading: string;
  body: string;
  tone?: "ok" | "warn" | "info";
}

function contextualTip(stats: WorkspaceStats): Tip | null {
  if (stats.loading || !stats.hasAnyJobs) return null;
  if (stats.totalReview > 0) {
    return {
      heading: "Clear your review queue before sending",
      body: `You have ${stats.totalReview.toLocaleString("en-US")} record${stats.totalReview !== 1 ? "s" : ""} flagged for manual review. Approve or remove them to maximise deliverability.`,
      tone: "warn",
    };
  }
  if (stats.latestInsights && stats.catchAllDetectedCount > 0) {
    return {
      heading: "Catch-all domains detected",
      body: `${stats.catchAllDetectedCount.toLocaleString("en-US")} address${stats.catchAllDetectedCount !== 1 ? "es sit" : " sits"} on catch-all domains. Open Insights to decide which ones are safe to keep.`,
      tone: "info",
    };
  }
  if (stats.avgReadyPct !== null && stats.avgReadyPct >= 90) {
    return {
      heading: "Your recent lists look clean",
      body: "Export the approved-original-format file to drop cleanly back into your CRM.",
      tone: "ok",
    };
  }
  return null;
}

const STATIC_TIPS: Tip[] = [
  {
    heading: "Run a quick scan before bulk sending",
    body: "Drop a small sample first to spot structural issues before processing a large file.",
  },
  {
    heading: "Use Insights to understand why records were flagged",
    body: "The V2 engine explains each decision with plain-English reasons and probabilistic scores.",
  },
  {
    heading: "Export the approved-original-format file",
    body: "Keeps the exact column layout of your original upload — ready to drop back into your CRM.",
  },
  {
    heading: "Review catch-all domains before bulk sending",
    body: "Catch-alls accept every address. Treat them with caution in high-volume campaigns.",
  },
];

export function TipsPanel({ stats }: Props) {
  const ctx = contextualTip(stats);
  const tips: Tip[] = ctx ? [ctx, ...STATIC_TIPS] : STATIC_TIPS;
  return (
    <section className={styles.panel}>
      <div className={styles.head}>
        <span className={styles.title}>Recommendations</span>
        <span className={styles.sub}>// Tips &amp; best practices</span>
      </div>
      <ul className={styles.list}>
        {tips.slice(0, 4).map((t, i) => (
          <li
            key={i}
            className={[styles.item, t.tone && styles[`tone_${t.tone}`]].filter(Boolean).join(" ")}
          >
            <span className={styles.bullet} aria-hidden />
            <div className={styles.itemBody}>
              <div className={styles.itemHead}>{t.heading}</div>
              <div className={styles.itemText}>{t.body}</div>
            </div>
          </li>
        ))}
      </ul>
    </section>
  );
}
