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
}

function NavLink({ href, label, icon, disabled, active }: NavLinkProps) {
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
        {label}
      </button>
    );
  }
  return (
    <Link href={href} className={className}>
      <span className={styles.icon}>{icon}</span>
      {label}
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
  const isLeadDiscovery = pathname?.startsWith("/lead-discovery");
  const isDomainAudit = pathname?.startsWith("/domain-audit");
  const isPipelines = pathname?.startsWith("/pipelines");

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
            href={isResults ? pathname : undefined}
            active={!!isResults}
            label="Results"
            icon={
              <svg viewBox="0 0 24 24">
                <ellipse cx="12" cy="5" rx="9" ry="3" />
                <path d="M3 5v6c0 1.66 4 3 9 3s9-1.34 9-3V5" />
                <path d="M3 11v6c0 1.66 4 3 9 3s9-1.34 9-3v-6" />
              </svg>
            }
            disabled={!isResults}
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
