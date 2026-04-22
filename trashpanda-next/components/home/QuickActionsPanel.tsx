"use client";

import Link from "next/link";
import type { WorkspaceStats } from "./useWorkspaceStats";
import styles from "./QuickActionsPanel.module.css";

/**
 * Quick action shortcuts shown at the top of the Home/Dashboard.
 * Routes are wired to the real app surfaces; actions that require a
 * completed job degrade gracefully (rendered as disabled) when no job
 * is available yet.
 */

interface Props {
  stats: WorkspaceStats;
  onUploadClick?: () => void;
}

export function QuickActionsPanel({ stats, onUploadClick }: Props) {
  const latestId = stats.latestJob?.job_id ?? null;
  const latestCompletedId = stats.latestCompletedJob?.job_id ?? null;
  const reviewCount = stats.totalReview;

  return (
    <section className={styles.section}>
      <div className={styles.head}>
        <span className={styles.title}>Quick actions</span>
      </div>

      <div className={styles.grid}>
        <ActionTile
          icon={<IconUpload />}
          label="Upload a new file"
          hint="CSV or XLSX · up to 100 MB"
          onClick={onUploadClick}
          primary
        />
        <ActionTile
          icon={<IconBolt />}
          label="Start a quick scan"
          hint="Small list? Drop it in the uploader"
          onClick={onUploadClick}
        />
        <ActionTile
          icon={<IconResults />}
          label="Open latest results"
          hint={
            latestId
              ? stats.latestJob?.input_filename ?? "Most recent run"
              : "No jobs yet"
          }
          href={latestId ? `/results/${encodeURIComponent(latestId)}` : undefined}
          disabled={!latestId}
        />
        <ActionTile
          icon={<IconInsights />}
          label="Open latest insights"
          hint={
            stats.latestInsights
              ? "Deliverability intelligence · V2"
              : latestCompletedId
                ? "Open the V2 view"
                : "No completed runs yet"
          }
          href={
            latestCompletedId
              ? `/insights/${encodeURIComponent(latestCompletedId)}`
              : undefined
          }
          disabled={!latestCompletedId}
        />
        <ActionTile
          icon={<IconReview />}
          label="Review flagged emails"
          hint={
            reviewCount > 0
              ? `${reviewCount.toLocaleString("en-US")} waiting for review`
              : latestCompletedId
                ? "No items flagged in your latest run"
                : "No completed runs yet"
          }
          href={
            latestCompletedId
              ? `/review/${encodeURIComponent(latestCompletedId)}`
              : undefined
          }
          disabled={!latestCompletedId}
          tone={reviewCount > 0 ? "warn" : undefined}
        />
      </div>
    </section>
  );
}

interface ActionTileProps {
  icon: React.ReactNode;
  label: string;
  hint?: string;
  href?: string;
  onClick?: () => void;
  disabled?: boolean;
  primary?: boolean;
  tone?: "warn";
}

function ActionTile({
  icon,
  label,
  hint,
  href,
  onClick,
  disabled,
  primary,
  tone,
}: ActionTileProps) {
  const className = [
    styles.tile,
    primary && styles.tilePrimary,
    tone === "warn" && styles.toneWarn,
    disabled && styles.tileDisabled,
  ]
    .filter(Boolean)
    .join(" ");

  const content = (
    <>
      <span className={styles.iconWrap}>{icon}</span>
      <span className={styles.labelWrap}>
        <span className={styles.tileLabel}>{label}</span>
        {hint && <span className={styles.tileHint}>{hint}</span>}
      </span>
      <span className={styles.chevron}>→</span>
    </>
  );

  if (disabled) {
    return (
      <div className={className} aria-disabled="true">
        {content}
      </div>
    );
  }

  if (href) {
    return (
      <Link href={href} className={className}>
        {content}
      </Link>
    );
  }

  return (
    <button type="button" onClick={onClick} className={className}>
      {content}
    </button>
  );
}

// ── Icons (inline, stroke-based to match existing sidebar style) ────────

function IconUpload() {
  return (
    <svg viewBox="0 0 24 24">
      <path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4" />
      <polyline points="17 8 12 3 7 8" />
      <line x1="12" y1="3" x2="12" y2="15" />
    </svg>
  );
}

function IconBolt() {
  return (
    <svg viewBox="0 0 24 24">
      <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2" />
    </svg>
  );
}

function IconResults() {
  return (
    <svg viewBox="0 0 24 24">
      <ellipse cx="12" cy="5" rx="9" ry="3" />
      <path d="M3 5v6c0 1.66 4 3 9 3s9-1.34 9-3V5" />
      <path d="M3 11v6c0 1.66 4 3 9 3s9-1.34 9-3v-6" />
    </svg>
  );
}

function IconInsights() {
  return (
    <svg viewBox="0 0 24 24">
      <path d="M3 3v18h18" />
      <path d="M7 15l4-4 3 3 5-6" />
      <circle cx="7" cy="15" r="1.2" />
      <circle cx="11" cy="11" r="1.2" />
      <circle cx="14" cy="14" r="1.2" />
      <circle cx="19" cy="8" r="1.2" />
    </svg>
  );
}

function IconReview() {
  return (
    <svg viewBox="0 0 24 24">
      <path d="M9 11l3 3 7-7" />
      <path d="M21 12v7a2 2 0 01-2 2H5a2 2 0 01-2-2V5a2 2 0 012-2h11" />
    </svg>
  );
}
