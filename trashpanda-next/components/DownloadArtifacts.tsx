"use client";

import { useState } from "react";
import type { JobArtifacts } from "@/lib/types";
import { CLIENT_OUTPUT_MANIFEST } from "@/lib/types";
import { artifactDownloadUrl, artifactZipUrl, buildZipFilename } from "@/lib/api";
import styles from "./DownloadArtifacts.module.css";

interface DownloadArtifactsProps {
  jobId: string;
  artifacts: JobArtifacts | null | undefined;
  inputFilename?: string | null;
  /**
   * When true (default) the panel renders the full grid of buttons
   * that has shipped historically. When false the panel collapses
   * into an accordion labelled "Show technical files (N)" so the
   * operator's primary attention stays on the giant
   * SendToClientButton.
   */
  expanded?: boolean;
}

// Preferred PRIMARY artifact, in order. The first one that the
// backend actually produced is highlighted with the ★ ribbon. The
// SendToClientButton uses the same preference list — keeping them
// aligned avoids "the big button gives me file X but the small ones
// say file Y is recommended".
const PRIMARY_KEY_PREFERENCES: ReadonlyArray<string> = [
  "approved_original_format",
  "valid_emails",
];

function pickPrimaryKey(
  available: Record<string, string | null>,
): string | null {
  for (const candidate of PRIMARY_KEY_PREFERENCES) {
    if (available[candidate]) return candidate;
  }
  return null;
}

export function DownloadArtifacts({
  jobId,
  artifacts,
  inputFilename,
  expanded = true,
}: DownloadArtifactsProps) {
  const clientOutputs = artifacts?.client_outputs ?? {};
  const technical = artifacts?.technical_csvs ?? {};
  const reports = artifacts?.reports ?? {};

  // Merge technical_csvs + reports into a single "technical" list.
  const technicalEntries: Array<{ key: string; filename: string }> = [
    ...Object.entries(technical)
      .filter(([, v]) => !!v)
      .map(([key, filename]) => ({ key, filename: filename as string })),
    ...Object.entries(reports)
      .filter(([, v]) => !!v)
      .map(([key, filename]) => ({ key, filename: filename as string })),
  ];

  const primaryKey = pickPrimaryKey(clientOutputs);
  const totalFiles =
    Object.values(clientOutputs).filter(Boolean).length + technicalEntries.length;

  // Collapsed mode: keep the panel out of the way until the operator
  // explicitly asks for the per-file breakdown. The giant
  // SendToClientButton is the primary action; this is for the rare
  // case when the customer asks for one of the supporting files.
  const [showAll, setShowAll] = useState(false);
  if (!expanded && !showAll) {
    return (
      <div className={styles.collapsed}>
        <button
          type="button"
          className={styles.expandBtn}
          onClick={() => setShowAll(true)}
          aria-expanded="false"
        >
          <span className={styles.expandLabel}>
            Show technical files ({totalFiles})
          </span>
          <span className={styles.expandHint}>
            Per-bucket XLSXs · CSVs · processing report · summary JSON
          </span>
        </button>
      </div>
    );
  }

  return (
    <div className={styles.panel}>
      <div className={styles.header}>
        <div className={styles.title}>DOWNLOADS</div>
        <div className={styles.headerActions}>
          <div className={styles.badge}>
            {CLIENT_OUTPUT_MANIFEST.length} client outputs
          </div>
          {!expanded && showAll && (
            <button
              type="button"
              className={styles.collapseBtn}
              onClick={() => setShowAll(false)}
              aria-expanded="true"
            >
              Hide
            </button>
          )}
        </div>
      </div>

      <div className={styles.body}>
        {/* ZIP download — full-width bar above individual files */}
        <div className={styles.zipBar}>
          <div className={styles.zipMeta}>
            <div className={styles.zipLabel}>All results</div>
            <div className={styles.zipDesc}>
              Client outputs · technical CSVs · reports · logs
            </div>
          </div>
          <a
            href={artifactZipUrl(jobId)}
            className={styles.zipBtn}
            download={buildZipFilename(inputFilename)}
          >
            <svg viewBox="0 0 24 24" aria-hidden>
              <path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4" />
              <polyline points="7 10 12 15 17 10" />
              <line x1="12" y1="15" x2="12" y2="3" />
            </svg>
            Download all (.zip)
          </a>
        </div>

        {CLIENT_OUTPUT_MANIFEST.map((item) => {
          const filename = clientOutputs[item.key] ?? item.filename;
          const available = !!clientOutputs[item.key];
          const isPrimary = available && item.key === primaryKey;
          const itemClass = [
            styles.item,
            styles[item.severity],
            !available && styles.itemDisabled,
            isPrimary && styles.itemPrimary,
          ]
            .filter(Boolean)
            .join(" ");

          return (
            <a
              key={item.key}
              href={available ? artifactDownloadUrl(jobId, item.key) : undefined}
              download={filename}
              className={itemClass}
              aria-disabled={!available}
            >
              <div className={styles.icon}>XLSX</div>
              <div className={styles.info}>
                {isPrimary && (
                  <div className={styles.primaryRibbon}>★ Recommended download</div>
                )}
                <div className={styles.label}>{item.label}</div>
                <div className={styles.filename}>{filename}</div>
                <div className={styles.description}>{item.description}</div>
              </div>
              <div className={styles.arrow}>
                <svg viewBox="0 0 24 24" aria-hidden>
                  <path d="M12 3v12M6 9l6 6 6-6" />
                  <path d="M3 21h18" />
                </svg>
              </div>
            </a>
          );
        })}

        {technicalEntries.length > 0 && (
          <div className={styles.technical} style={{ gridColumn: "1 / -1" }}>
            <div className={styles.techLabel}>// Technical outputs</div>
            <div className={styles.techList}>
              {technicalEntries.map((t) => (
                <a
                  key={t.key}
                  href={artifactDownloadUrl(jobId, t.key)}
                  download={t.filename}
                  className={styles.techItem}
                >
                  {t.filename}
                </a>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
