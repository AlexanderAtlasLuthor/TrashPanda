"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import styles from "./Sidebar.module.css";

interface NavLinkProps {
  href?: string;
  label: string;
  icon: React.ReactNode;
  disabled?: boolean;
  active?: boolean;
  /** Short hint rendered in the disabled pill (e.g. "Pick a job"). */
  disabledHint?: string;
}

function NavLink({ href, label, icon, disabled, active, disabledHint }: NavLinkProps) {
  const className = [
    styles.navItem,
    active && styles.navItemActive,
    disabled && styles.disabled,
  ]
    .filter(Boolean)
    .join(" ");

  if (disabled || !href) {
    return (
      <button className={className} disabled aria-disabled>
        <span className={styles.icon}>{icon}</span>
        <span className={styles.navLabel}>{label}</span>
        {disabledHint && (
          <span className={styles.navHint}>{disabledHint}</span>
        )}
      </button>
    );
  }
  return (
    <Link href={href} className={className}>
      <span className={styles.icon}>{icon}</span>
      <span className={styles.navLabel}>{label}</span>
    </Link>
  );
}

interface SidebarProps {
  open?: boolean;
  onClose?: () => void;
}

export function Sidebar({ open, onClose }: SidebarProps) {
  const pathname = usePathname();
  const isConsole = pathname === "/";
  const isResults = pathname?.startsWith("/results");
  const isInsights = pathname?.startsWith("/insights");
  const isReview = pathname?.startsWith("/review");
  const isLeadDiscovery = pathname?.startsWith("/lead-discovery");
  const isDomainAudit = pathname?.startsWith("/domain-audit");
  const isPipelines = pathname?.startsWith("/pipelines");

  // Results / Insights / Review are all job-scoped. If we're inside any one
  // of them we can pull the jobId from the path and keep the other two
  // links live — that way a user reviewing a queue can jump to its Results
  // or Insights without going Home first.
  const jobContextMatch = pathname?.match(/^\/(results|insights|review)\/([^/]+)/);
  const jobId = jobContextMatch?.[2] ?? null;

  const resultsHref = isResults
    ? pathname
    : jobId
      ? `/results/${jobId}`
      : undefined;
  const insightsHref = isInsights
    ? pathname
    : jobId
      ? `/insights/${jobId}`
      : undefined;

  return (
    <>
      <div
        className={[styles.backdrop, open && styles.backdropVisible]
          .filter(Boolean)
          .join(" ")}
        onClick={onClose}
      />
      <aside
        className={[styles.sidebar, open && styles.sidebarOpen]
          .filter(Boolean)
          .join(" ")}
      >
        <div className={styles.brand}>
          {/* eslint-disable-next-line @next/next/no-img-element */}
          <img
            src="/trashpanda-logo.png"
            alt="TrashPanda"
            className={styles.logo}
          />
          <div className={styles.brandText}>
            <div className={styles.brandName}>
              Trash<span className={styles.accent}>Panda</span>
            </div>
            <div className={styles.brandTag}>// CLEAN YOUR DATA</div>
          </div>
        </div>

        <div className={styles.section}>
          <div className={styles.sectionLabel}>// Workspace</div>
          <NavLink
            href="/"
            active={isConsole}
            label="Console"
            icon={
              <svg viewBox="0 0 24 24">
                <path d="M3 12h4l3-9 4 18 3-9h4" />
              </svg>
            }
          />
          <NavLink
            href={resultsHref}
            active={!!isResults}
            label="Results"
            icon={
              <svg viewBox="0 0 24 24">
                <ellipse cx="12" cy="5" rx="9" ry="3" />
                <path d="M3 5v6c0 1.66 4 3 9 3s9-1.34 9-3V5" />
                <path d="M3 11v6c0 1.66 4 3 9 3s9-1.34 9-3v-6" />
              </svg>
            }
            disabled={!resultsHref}
            disabledHint={!resultsHref ? "Pick a job" : undefined}
          />
          <NavLink
            href={insightsHref}
            active={!!isInsights}
            label="Insights"
            icon={
              <svg viewBox="0 0 24 24">
                <path d="M3 3v18h18" />
                <path d="M7 15l4-4 3 3 5-6" />
                <circle cx="7" cy="15" r="1.2" />
                <circle cx="11" cy="11" r="1.2" />
                <circle cx="14" cy="14" r="1.2" />
                <circle cx="19" cy="8" r="1.2" />
              </svg>
            }
            disabled={!insightsHref}
            disabledHint={!insightsHref ? "Pick a job" : undefined}
          />
        </div>

        <div className={styles.section}>
          <div className={styles.sectionLabel}>// Tools</div>
          <NavLink
            href="/lead-discovery"
            active={isLeadDiscovery}
            label="Lead Discovery"
            icon={
              <svg viewBox="0 0 24 24">
                <path d="M12 2l3 7h7l-5.5 4.5L18 21l-6-4-6 4 1.5-7.5L2 9h7z" />
              </svg>
            }
          />
          <NavLink
            href="/domain-audit"
            active={isDomainAudit}
            label="Domain Audit"
            icon={
              <svg viewBox="0 0 24 24">
                <circle cx="11" cy="11" r="7" />
                <path d="M21 21l-4.35-4.35" />
              </svg>
            }
          />
          <NavLink
            href="/pipelines"
            active={isPipelines}
            label="Pipelines"
            icon={
              <svg viewBox="0 0 24 24">
                <circle cx="12" cy="12" r="9" />
                <path d="M12 7v5l3 3" />
              </svg>
            }
          />
        </div>

        <div className={styles.footer}>
          <span className={styles.statusDot}></span>Engine online
          <br />
          v0.4.2 — MVP
          <br />
          <span className={styles.copyright}>© Fuenmayor Industries</span>
        </div>
      </aside>
    </>
  );
}
