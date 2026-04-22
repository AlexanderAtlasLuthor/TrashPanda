"use client";

import Link from "next/link";
import styles from "./Topbar.module.css";
import { useShell } from "./AppShell";

interface MetaItem {
  label: string;
  value: string;
  accent?: boolean;
  danger?: boolean;
}

/**
 * Breadcrumb items are either plain strings (current/static)
 * or `{ label, href }` objects to render a clickable link.
 */
export type BreadcrumbItem = string | { label: string; href: string };

interface TopbarProps {
  breadcrumb: BreadcrumbItem[];
  title: string;
  titleSlice?: string;
  subtitle?: string;
  meta?: MetaItem[];
}

/**
 * Top page header. `titleSlice` is the neon-green portion of the title,
 * usually a "/" separator to mirror the mockup brand feel.
 */
export function Topbar({
  breadcrumb,
  title,
  titleSlice,
  subtitle,
  meta,
}: TopbarProps) {
  const { toggleSidebar } = useShell();
  // Split the title around the slice so we can highlight it.
  let before = title;
  let after = "";
  if (titleSlice && title.includes(titleSlice)) {
    const idx = title.indexOf(titleSlice);
    before = title.slice(0, idx);
    after = title.slice(idx + titleSlice.length);
  }

  return (
    <header className={styles.topbar}>
      <div style={{ display: "flex", alignItems: "flex-start", gap: 14 }}>
        <button
          className={styles.mobileToggle}
          onClick={toggleSidebar}
          aria-label="Open menu"
        >
          ☰
        </button>
        <div className={styles.pageTitle}>
          <div className={styles.breadcrumb}>
            {breadcrumb.map((crumb, i) => {
              const isLast = i === breadcrumb.length - 1;
              const label = typeof crumb === "string" ? crumb : crumb.label;
              const href = typeof crumb === "string" ? null : crumb.href;
              return (
                <span key={i}>
                  {href ? (
                    <Link href={href} className={styles.crumbLink}>
                      {label}
                    </Link>
                  ) : (
                    label
                  )}
                  {!isLast && <span className={styles.slash}>/</span>}
                </span>
              );
            })}
          </div>
          <h1 className={styles.title}>
            {before}
            {titleSlice && <span className={styles.slice}>{titleSlice}</span>}
            {after}
          </h1>
          {subtitle && (
            <p className={styles.subtitle}>{subtitle}</p>
          )}
        </div>
      </div>

      {meta && meta.length > 0 && (
        <div className={styles.meta}>
          {meta.map((m, i) => (
            <div key={i}>
              <span className={styles.metaLabel}>{m.label}</span>{" "}
              <span
                className={[
                  m.accent && styles.metaVal,
                  m.danger && styles.metaDanger,
                ].filter(Boolean).join(" ") || undefined}
              >
                {m.value}
              </span>
            </div>
          ))}
        </div>
      )}
    </header>
  );
}
