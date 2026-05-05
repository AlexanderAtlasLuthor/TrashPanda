"use client";

import { useState } from "react";
import type {
  ClientPackageWarning,
  FeedbackPreviewRecord,
  FeedbackPreviewResult,
} from "@/lib/types";
import { StatusBadge } from "./StatusBadge";
import { IssuesList } from "./IssuesList";
import { FeedbackBehaviorBadge } from "./FeedbackBehaviorBadge";
import {
  OperatorEmptyState,
  OperatorErrorState,
  OperatorLoadingState,
} from "./OperatorPanelStates";
import styles from "./FeedbackPreviewPanel.module.css";

interface FeedbackPreviewPanelProps {
  preview: FeedbackPreviewResult | null;
  loading?: boolean;
  error?: string | null;
}

interface IssueLike {
  severity?: string | null;
  code?: string | null;
  message?: string | null;
}

interface StatRow {
  label: string;
  value: string;
}

const RECORDS_PREVIEW_LIMIT = 100;

function formatNumber(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  return value.toLocaleString();
}

function formatScore(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  return value.toFixed(3);
}

function pushStat(
  out: StatRow[],
  label: string,
  value: number | null | undefined,
) {
  if (value === null || value === undefined) return;
  out.push({ label, value: formatNumber(value) });
}

function isClientPackageWarning(
  value: unknown,
): value is ClientPackageWarning {
  return (
    !!value &&
    typeof value === "object" &&
    typeof (value as ClientPackageWarning).code === "string" &&
    typeof (value as ClientPackageWarning).message === "string"
  );
}

export function FeedbackPreviewPanel({
  preview,
  loading,
  error,
}: FeedbackPreviewPanelProps) {
  const [showAll, setShowAll] = useState(false);

  if (loading) {
    return (
      <section className={styles.panel} aria-label="Feedback preview">
        <OperatorLoadingState message="Building feedback preview…" />
      </section>
    );
  }

  if (error) {
    return (
      <section className={styles.panel} aria-label="Feedback preview">
        <OperatorErrorState
          title="Feedback preview failed."
          message={error}
        />
      </section>
    );
  }

  if (!preview) {
    return (
      <section className={styles.panel} aria-label="Feedback preview">
        <OperatorEmptyState
          title="No feedback preview loaded yet."
          message="Load the current preview or build a preview from specific feedback inputs."
        />
      </section>
    );
  }

  // Backend feedback preview payload currently uses *_count and
  // record-level outcome fields before the TS contract was aligned;
  // keep this local bridge until a dedicated contract-alignment
  // subphase. Do NOT modify lib/types.ts here.
  const raw = preview as FeedbackPreviewResult & {
    report_version?: string | null;
    feedback_store_path?: string | null;
    output_path?: string | null;
    feedback_available?: boolean | null;
    known_good_count?: number | null;
    known_risky_count?: number | null;
    cold_start_count?: number | null;
    unknown_count?: number | null;
    warnings?: Array<string | ClientPackageWarning>;
  };

  type RawFeedbackRecord = FeedbackPreviewRecord & {
    delivered_count?: number | null;
    hard_bounce_count?: number | null;
    soft_bounce_count?: number | null;
    blocked_count?: number | null;
    complaint_count?: number | null;
    reputation_score?: number | null;
    risk_level?: string | null;
    cold_start?: boolean | null;
    reason?: string | null;
  };

  const status =
    raw.feedback_available === false ? "missing" : preview.status ?? "ok";

  const knownGood = raw.known_good_count ?? preview.known_good ?? null;
  const knownRisky = raw.known_risky_count ?? preview.known_risky ?? null;
  const coldStart = raw.cold_start_count ?? preview.cold_start ?? null;
  const unknown = raw.unknown_count ?? preview.unknown ?? null;

  const stats: StatRow[] = [];
  pushStat(stats, "Total domains", preview.total_domains);
  pushStat(stats, "Total observations", preview.total_observations);
  pushStat(stats, "Known good", knownGood);
  pushStat(stats, "Known risky", knownRisky);
  pushStat(stats, "Cold start", coldStart);
  pushStat(stats, "Unknown", unknown);

  const warningInputs = raw.warnings ?? [];
  const issues: IssueLike[] = warningInputs.map((w) => {
    if (isClientPackageWarning(w)) {
      return {
        severity: w.severity ?? "warn",
        code: w.code,
        message: w.message,
      };
    }
    const text = String(w);
    return { severity: "warn", code: text, message: text };
  });

  const records = (preview.records ?? []) as RawFeedbackRecord[];
  const overLimit = records.length > RECORDS_PREVIEW_LIMIT;
  const visibleRecords =
    showAll || !overLimit
      ? records
      : records.slice(0, RECORDS_PREVIEW_LIMIT);

  return (
    <section className={styles.panel} aria-label="Feedback preview">
      <header className={styles.header}>
        <div className={styles.titleWrap}>
          <div className={styles.eyebrow}>// FEEDBACK PREVIEW</div>
          <div className={styles.title}>Feedback preview</div>
        </div>
        <div className={styles.headMeta}>
          <StatusBadge status={status} />
        </div>
      </header>

      <div className={styles.previewNote} role="note">
        Preview only; does not mutate current run.
      </div>

      {(raw.report_version ||
        preview.generated_at ||
        raw.feedback_store_path ||
        raw.output_path ||
        raw.feedback_available !== null &&
          raw.feedback_available !== undefined) && (
        <div className={styles.metaRow}>
          {raw.report_version && (
            <div className={styles.metaItem}>
              <span className={styles.metaLabel}>Report version</span>
              <span className={styles.metaValue}>{raw.report_version}</span>
            </div>
          )}
          {preview.generated_at && (
            <div className={styles.metaItem}>
              <span className={styles.metaLabel}>Generated</span>
              <span className={styles.metaValue}>{preview.generated_at}</span>
            </div>
          )}
          {raw.feedback_store_path && (
            <div className={styles.metaItem}>
              <span className={styles.metaLabel}>Store</span>
              <span className={styles.metaValue}>
                {raw.feedback_store_path}
              </span>
            </div>
          )}
          {raw.output_path && (
            <div className={styles.metaItem}>
              <span className={styles.metaLabel}>Output</span>
              <span className={styles.metaValue}>{raw.output_path}</span>
            </div>
          )}
          {raw.feedback_available !== null &&
            raw.feedback_available !== undefined && (
              <div className={styles.metaItem}>
                <span className={styles.metaLabel}>Feedback available</span>
                <span className={styles.metaValue}>
                  {raw.feedback_available ? "true" : "false"}
                </span>
              </div>
            )}
        </div>
      )}

      {stats.length > 0 && (
        <div className={styles.statsGrid}>
          {stats.map((s) => (
            <div key={s.label} className={styles.stat}>
              <div className={styles.statValue}>{s.value}</div>
              <div className={styles.statLabel}>{s.label}</div>
            </div>
          ))}
        </div>
      )}

      <div className={styles.section}>
        <div className={styles.sectionLabel}>
          Warnings ({issues.length})
        </div>
        <IssuesList
          issues={issues}
          emptyLabel="No preview warnings reported."
        />
      </div>

      <div className={styles.section}>
        <div className={styles.recordsHeader}>
          <div className={styles.sectionLabel}>
            Domains ({records.length})
          </div>
          {overLimit && (
            <button
              type="button"
              className={styles.toggleBtn}
              onClick={() => setShowAll((v) => !v)}
            >
              {showAll
                ? `Show first ${RECORDS_PREVIEW_LIMIT}`
                : `Show all ${records.length} domains`}
            </button>
          )}
        </div>

        {records.length === 0 ? (
          <div className={styles.empty}>No domain records in preview.</div>
        ) : (
          <div className={styles.tableWrap}>
            <table className={styles.table}>
              <thead>
                <tr>
                  <th>Domain</th>
                  <th>Behavior</th>
                  <th>Obs.</th>
                  <th>Delivered</th>
                  <th>Hard</th>
                  <th>Soft</th>
                  <th>Blocked</th>
                  <th>Complaints</th>
                  <th>Reputation</th>
                  <th>Risk</th>
                  <th>Reason</th>
                </tr>
              </thead>
              <tbody>
                {visibleRecords.map((r, idx) => (
                  <tr key={`${r.domain}-${idx}`}>
                    <td className={styles.domainCell}>{r.domain}</td>
                    <td>
                      <FeedbackBehaviorBadge
                        behaviorClass={r.behavior_class}
                      />
                    </td>
                    <td>{formatNumber(r.total_observations)}</td>
                    <td>{formatNumber(r.delivered_count)}</td>
                    <td>{formatNumber(r.hard_bounce_count)}</td>
                    <td>{formatNumber(r.soft_bounce_count)}</td>
                    <td>{formatNumber(r.blocked_count)}</td>
                    <td>{formatNumber(r.complaint_count)}</td>
                    <td>{formatScore(r.reputation_score)}</td>
                    <td>{r.risk_level ?? "—"}</td>
                    <td className={styles.reasonCell}>{r.reason ?? "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            {overLimit && !showAll && (
              <div className={styles.tableFooter}>
                Showing first {RECORDS_PREVIEW_LIMIT} of {records.length}{" "}
                domains.
              </div>
            )}
          </div>
        )}
      </div>
    </section>
  );
}
