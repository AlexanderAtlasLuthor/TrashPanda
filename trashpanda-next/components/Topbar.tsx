"use client";

import styles from "./Topbar.module.css";
import { useShell } from "./AppShell";

interface TopbarProps {
  breadcrumb: string[];
  title: string;
  titleSlice?: string;
  meta?: Array<{ label: string; value: string; accent?: boolean }>;
}

/**
 * Top page header. `titleSlice` is the neon-green portion of the title,
 * usually a "/" separator to mirror the mockup brand feel.
 */
export function Topbar({
  breadcrumb,
  title,
  titleSlice,
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
            {breadcrumb.map((crumb, i) => (
              <span key={i}>
                {crumb}
                {i < breadcrumb.length - 1 && (
                  <span className={styles.slash}>/</span>
                )}
              </span>
            ))}
          </div>
          <h1 className={styles.title}>
            {before}
            {titleSlice && <span className={styles.slice}>{titleSlice}</span>}
            {after}
          </h1>
        </div>
      </div>

      {meta && meta.length > 0 && (
        <div className={styles.meta}>
          {meta.map((m, i) => (
            <div key={i}>
              <span className={styles.metaLabel}>{m.label}</span>{" "}
              <span className={m.accent ? styles.metaVal : undefined}>
                {m.value}
              </span>
            </div>
          ))}
        </div>
      )}
    </header>
  );
}
