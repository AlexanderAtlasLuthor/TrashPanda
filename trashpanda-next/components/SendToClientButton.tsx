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
  /**
   * Hide while the job is still running. The button reveals itself
   * once the job completes — no point in clicking it earlier because
   * the client package isn't built yet.
   */
  visible?: boolean;
}

/**
 * The single primary action on a finished job. Replaces the legacy
 * "operator review gate → build package → download" three-click flow
 * with one giant button that auto-runs the gate, builds the package,
 * and ships a curated ZIP (PRIMARY artifact + README + summary).
 *
 * Loading states:
 *   - "loading"   while we fetch the bundle summary
 *   - "ready"     gate pass + safe rows present → green button
 *   - "partial"   gate WARN/BLOCK but safe rows exist → yellow button
 *                 with a "partial delivery" note above
 *   - "blocked"   no safe rows at all → red banner explaining why
 *   - "error"     summary endpoint failed → terse retry message
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
        Preparing the send-to-client bundle…
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
    // CRITICAL UX: when the V2 default policy produces 0 safe rows
    // (typical for SMTP-off runs on cold-start domains), surface the
    // Extra-Strict re-clean as the *primary* fallback so the operator
    // is never stuck staring at a blocked card.
    return (
      <div className={styles.wrap}>
        <div className={styles.blockedCard}>
          <div className={styles.blockedTitle}>
            No rows passed the strict default policy
          </div>
          <div className={styles.blockedMessage}>
            {summary.issues[0]?.message ??
              "Without live SMTP confirmation the V2 decision engine sends most rows to manual review. " +
                "Use the Extra-Strict re-clean below — it applies a different policy (probability ≥ 0.75 + " +
                "domain risk + provider class) and typically rescues several hundred rows."}
          </div>
        </div>

        <a
          href={extraStrictDownloadUrl(jobId)}
          className={[styles.button, styles.buttonPartial].join(" ")}
          download
          aria-label="Run extra-strict re-clean"
        >
          <span className={styles.buttonStar} aria-hidden>
            ⤓
          </span>
          <span className={styles.buttonMain}>
            <span className={styles.buttonHeadline}>
              Run extra-strict re-clean
            </span>
            <span className={styles.buttonSubline}>
              Different policy · drops Yahoo/AOL · keeps high-probability rows
            </span>
          </span>
          <span className={styles.buttonArrow} aria-hidden>
            ↓
          </span>
        </a>

        <div className={styles.tally}>
          <span className={styles.tallyMuted}>
            {summary.safe_count + summary.review_count + summary.rejected_count}{" "}
            rows scanned
          </span>
          <span className={styles.tallyDot}>·</span>
          <span className={styles.tallyWarn}>
            {summary.review_count} in review
          </span>
          <span className={styles.tallyDot}>·</span>
          <span className={styles.tallyBad}>
            {summary.rejected_count} rejected
          </span>
        </div>
      </div>
    );
  }

  const isPartial = summary.delivery_mode === "safe_only_partial";
  const buttonClass = [
    styles.button,
    isPartial ? styles.buttonPartial : styles.buttonReady,
  ].join(" ");

  const total =
    summary.safe_count + summary.review_count + summary.rejected_count;
  const safePct =
    total > 0 ? Math.round((summary.safe_count * 100) / total) : 0;

  return (
    <div className={styles.wrap}>
      {isPartial && (
        <div className={styles.partialBanner}>
          ⚠ Partial delivery — the operator review gate flagged warnings.
          The bundle includes only the {summary.safe_count} confirmed safe
          rows. {summary.review_count} require review (mostly unconfirmed
          B2B / catch-all consumer providers — see breakdown below) and{" "}
          {summary.rejected_count} were removed.
        </div>
      )}

      <a
        href={clientBundleDownloadUrl(jobId)}
        className={buttonClass}
        download={summary.download_filename ?? undefined}
        aria-label="Send to client"
      >
        <span className={styles.buttonStar} aria-hidden>
          ★
        </span>
        <span className={styles.buttonMain}>
          <span className={styles.buttonHeadline}>Send to client</span>
          <span className={styles.buttonSubline}>
            {summary.safe_count.toLocaleString()} ready · ZIP includes
            primary list + README + summary
          </span>
        </span>
        <span className={styles.buttonArrow} aria-hidden>
          ↓
        </span>
      </a>

      <div className={styles.tally}>
        <span className={styles.tallyOk}>
          {summary.safe_count.toLocaleString()} confirmed safe
        </span>
        <span className={styles.tallyDot}>·</span>
        <span className={styles.tallyWarn}>
          {summary.review_count.toLocaleString()} require review
        </span>
        <span className={styles.tallyDot}>·</span>
        <span className={styles.tallyBad}>
          {summary.rejected_count.toLocaleString()} do not use
        </span>
        {total > 0 && (
          <>
            <span className={styles.tallyDot}>·</span>
            <span className={styles.tallyMuted}>
              {safePct}% confirmed safe-only
            </span>
          </>
        )}
      </div>

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
          <span className={styles.secondaryIcon} aria-hidden>⤓</span>
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
