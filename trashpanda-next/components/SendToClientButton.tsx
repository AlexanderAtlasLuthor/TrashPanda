"use client";

import { useEffect, useState } from "react";

import {
  clientBundleDownloadUrl,
  extraStrictDownloadUrl,
  getClientBundleSummary,
  type ClientBundleSummary,
} from "@/lib/api";
import { RESULTS_COPY } from "@/lib/copy";
import styles from "./SendToClientButton.module.css";

interface SendToClientButtonProps {
  jobId: string;
  visible?: boolean;
}

function totalRows(summary: ClientBundleSummary): number {
  return summary.safe_count + summary.review_count + summary.rejected_count;
}

function safePercent(summary: ClientBundleSummary): number {
  const total = totalRows(summary);
  return total > 0 ? Math.round((summary.safe_count * 100) / total) : 0;
}

function DeliveryMetrics({ summary }: { summary: ClientBundleSummary }) {
  const total = totalRows(summary);
  const safePct = safePercent(summary);

  return (
    <>
      <div className={styles.metricsGrid}>
        <div className={[styles.metric, styles.metricOk].join(" ")}>
          <span className={styles.metricLabel}>Confirmed safe</span>
          <strong className={styles.metricValue}>
            {summary.safe_count.toLocaleString()}
          </strong>
          <span className={styles.metricNote}>Included in ZIP</span>
        </div>
        <div className={[styles.metric, styles.metricWarn].join(" ")}>
          <span className={styles.metricLabel}>Require review</span>
          <strong className={styles.metricValue}>
            {summary.review_count.toLocaleString()}
          </strong>
          <span className={styles.metricNote}>Held back</span>
        </div>
        <div className={[styles.metric, styles.metricBad].join(" ")}>
          <span className={styles.metricLabel}>Do not use</span>
          <strong className={styles.metricValue}>
            {summary.rejected_count.toLocaleString()}
          </strong>
          <span className={styles.metricNote}>Removed</span>
        </div>
        <div className={[styles.metric, styles.metricMuted].join(" ")}>
          <span className={styles.metricLabel}>Safe-only rate</span>
          <strong className={styles.metricValue}>{safePct}%</strong>
          <span className={styles.metricNote}>
            {total.toLocaleString()} rows processed
          </span>
        </div>
      </div>

      <div className={styles.deliveryMeter} aria-hidden>
        <span
          className={styles.deliveryMeterFill}
          style={{ width: `${safePct}%` }}
        />
      </div>
    </>
  );
}

/**
 * Primary delivery action for a completed job. The visual treatment is
 * intentionally calmer than the old giant warning-style button: the
 * package is the hero, the download affordance is clear, and the
 * partial-delivery warning stays separate from the click target.
 */
export function SendToClientButton({
  jobId,
  visible = true,
}: SendToClientButtonProps) {
  const [summary, setSummary] = useState<ClientBundleSummary | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!visible) return;
    let cancelled = false;
    setLoading(true);
    setError(null);
    getClientBundleSummary(jobId)
      .then((s) => {
        if (!cancelled) setSummary(s);
      })
      .catch((err: unknown) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "request failed");
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [jobId, visible]);

  if (!visible) return null;

  if (loading) {
    return (
      <div className={styles.loading}>
        Preparing the send-to-client bundle...
      </div>
    );
  }

  if (error || summary === null) {
    return (
      <div className={styles.errorCard}>
        <div className={styles.errorTitle}>Bundle not available yet</div>
        <div className={styles.errorMessage}>
          {error ??
            "The pipeline may still be finishing. Refresh the page in a few seconds."}
        </div>
      </div>
    );
  }

  if (!summary.available) {
    if (summary.delivery_state === "smtp_verification_pending") {
      return (
        <div className={styles.pendingCard}>
          <div className={styles.pendingTitle}>
            SMTP verification has not run yet
          </div>
          <div className={styles.pendingMessage}>
            {summary.operator_message ??
              `${summary.review_count.toLocaleString()} rows are pending verification.`}
          </div>
          <div className={styles.pendingAction}>
            Rerun with production SMTP config.
          </div>
        </div>
      );
    }

    if (summary.delivery_state === "cleaning_completed") {
      return (
        <div className={styles.pendingCard}>
          <div className={styles.pendingTitle}>Cleaning completed</div>
          <div className={styles.pendingMessage}>
            {summary.operator_message ??
              "Delivery readiness still needs verification."}
          </div>
        </div>
      );
    }

    return (
      <div className={styles.wrap}>
        <div className={styles.blockedCard}>
          <div className={styles.blockedTitle}>
            No rows passed the strict default policy
          </div>
          <div className={styles.blockedMessage}>
            {summary.operator_message ??
              summary.issues[0]?.message ??
              "Without live SMTP confirmation, the V2 decision engine holds most rows for manual review. Use Extra-Strict re-clean to produce a conservative fallback export."}
          </div>
        </div>

        <a
          href={extraStrictDownloadUrl(jobId)}
          className={[styles.button, styles.buttonPartial].join(" ")}
          download
          aria-label="Run extra-strict re-clean"
        >
          <span className={styles.packageIcon} aria-hidden>
            XLS
          </span>
          <span className={styles.buttonMain}>
            <span className={styles.buttonKicker}>Fallback export</span>
            <span className={styles.buttonHeadline}>
              Run extra-strict re-clean
            </span>
            <span className={styles.buttonSubline}>
              Different policy - drops Yahoo/AOL - keeps high-probability rows
            </span>
          </span>
          <span className={styles.downloadCta}>Download</span>
        </a>

        <DeliveryMetrics summary={summary} />
      </div>
    );
  }

  const isPartial = summary.delivery_mode === "safe_only_partial";
  const buttonClass = [
    styles.button,
    isPartial ? styles.buttonPartial : styles.buttonReady,
  ].join(" ");

  return (
    <div className={styles.wrap}>
      {isPartial && (
        <div className={styles.partialBanner}>
          <span className={styles.partialLabel}>Partial delivery</span>
          <span className={styles.partialText}>
            Includes {summary.safe_count.toLocaleString()} confirmed safe rows.
            {` ${summary.review_count.toLocaleString()} require review and `}
            {summary.rejected_count.toLocaleString()} were removed.
          </span>
        </div>
      )}

      <a
        href={clientBundleDownloadUrl(jobId)}
        className={buttonClass}
        download={summary.download_filename ?? undefined}
        aria-label="Send to client"
      >
        <span className={styles.packageIcon} aria-hidden>
          ZIP
        </span>
        <span className={styles.buttonMain}>
          <span className={styles.buttonKicker}>
            {isPartial ? "Safe-only delivery package" : "Client delivery package"}
          </span>
          <span className={styles.buttonHeadline}>Send to client</span>
          <span className={styles.buttonSubline}>
            Customer-safe ZIP with primary list, README, and summary report
          </span>
          {summary.primary_filename && (
            <span className={styles.deliveryMeta}>
              Primary file: {summary.primary_filename}
            </span>
          )}
        </span>
        <span className={styles.downloadCta}>Download ZIP</span>
      </a>

      <DeliveryMetrics summary={summary} />

      <div className={styles.secondary}>
        <a
          href={extraStrictDownloadUrl(jobId)}
          className={styles.secondaryBtn}
          download
          title={
            "Re-clean using the offline extra-strict policy: drops every " +
            "Yahoo / AOL / Verizon-class address, low-probability rows, " +
            "and structural rejects. Use this when the customer reported " +
            "bounces on a previous send."
          }
        >
          <span className={styles.secondaryIcon} aria-hidden>
            XLS
          </span>
          <span>
            <span className={styles.secondaryHeadline}>
              {RESULTS_COPY.extraStrict.title}
            </span>
            <span className={styles.secondarySubline}>
              {RESULTS_COPY.extraStrict.description}
            </span>
          </span>
        </a>
      </div>
    </div>
  );
}
