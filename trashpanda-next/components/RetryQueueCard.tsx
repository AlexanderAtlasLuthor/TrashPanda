"use client";

import { useCallback, useEffect, useState } from "react";

import {
  finalizeRetryQueue,
  getRetryQueueStatus,
  runRetryQueueDrain,
  setRetryQueueAutoRetry,
  type RetryQueueStatus,
} from "@/lib/api";
import styles from "./RetryQueueCard.module.css";

interface Props {
  jobId: string;
  /** Hide the card while the job is still running. */
  visible?: boolean;
  /** Callback fired after a successful finalize so the parent can
   * re-fetch the bundle summary / classification breakdown. */
  onFinalize?: () => void;
}

/**
 * V2.10.11 — render the per-job SMTP retry queue.
 *
 * Surfaces:
 *   * counts — pending / running / succeeded / exhausted / expired
 *   * `auto_retry_enabled` toggle (PATCH)
 *   * "Run retry pass now" — drains the queue once, ignoring the flag
 *   * "Re-clean with retry results" — operator-triggered finalize that
 *     regenerates valid_emails / review_* / second_pass_candidates
 *     XLSX files and rebuilds the client package. Always manual so a
 *     bundle a customer has already downloaded never silently
 *     changes content.
 */
export function RetryQueueCard({ jobId, visible = true, onFinalize }: Props) {
  const [status, setStatus] = useState<RetryQueueStatus | null>(null);
  const [busy, setBusy] = useState<
    "idle" | "toggling" | "draining" | "finalizing"
  >("idle");
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      const next = await getRetryQueueStatus(jobId);
      setStatus(next);
      setError(null);
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : "Failed to load");
    }
  }, [jobId]);

  useEffect(() => {
    if (!visible) return;
    refresh();
  }, [refresh, visible]);

  if (!visible) return null;
  if (!status) {
    return (
      <div className={styles.card}>
        <div className={styles.heading}>// SMTP retry queue</div>
        <div className={styles.muted}>
          {error ?? "Loading retry queue status…"}
        </div>
      </div>
    );
  }

  if (!status.available) {
    return (
      <div className={styles.card}>
        <div className={styles.heading}>// SMTP retry queue</div>
        <div className={styles.muted}>
          No retry queue for this job. Either SMTP did not run or
          every probed row returned a terminal verdict.
        </div>
      </div>
    );
  }

  const onToggle = async (enabled: boolean) => {
    setBusy("toggling");
    try {
      await setRetryQueueAutoRetry(jobId, enabled);
      await refresh();
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : "Toggle failed");
    } finally {
      setBusy("idle");
    }
  };

  const onDrain = async () => {
    setBusy("draining");
    try {
      await runRetryQueueDrain(jobId);
      await refresh();
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : "Drain failed");
    } finally {
      setBusy("idle");
    }
  };

  const onFinalizeClick = async () => {
    setBusy("finalizing");
    try {
      await finalizeRetryQueue(jobId);
      await refresh();
      onFinalize?.();
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : "Finalize failed");
    } finally {
      setBusy("idle");
    }
  };

  const c = status.counts;

  return (
    <div className={styles.card}>
      <div className={styles.heading}>// SMTP retry queue</div>

      <div className={styles.counts}>
        <span className={styles.statPending}>
          {c.pending.toLocaleString()} pending
        </span>
        <span className={styles.dot}>·</span>
        <span className={styles.statSuccess}>
          {c.succeeded.toLocaleString()} succeeded
        </span>
        <span className={styles.dot}>·</span>
        <span className={styles.statExhausted}>
          {c.exhausted.toLocaleString()} exhausted
        </span>
        {c.expired > 0 && (
          <>
            <span className={styles.dot}>·</span>
            <span className={styles.statExpired}>
              {c.expired.toLocaleString()} expired
            </span>
          </>
        )}
      </div>

      <label className={styles.toggleRow}>
        <input
          type="checkbox"
          checked={status.auto_retry_enabled}
          disabled={busy !== "idle"}
          onChange={(e) => onToggle(e.target.checked)}
        />
        <span>
          <strong>Auto-retry while I&apos;m away.</strong>{" "}
          When on, the background worker drains this queue every 15
          minutes (max 3 retries with 15 / 30 / 60 min backoff). When
          off, the queue is filled but only drained when you click the
          button below.
        </span>
      </label>

      <div className={styles.actions}>
        <button
          type="button"
          onClick={onDrain}
          disabled={busy !== "idle" || c.pending === 0}
          className={styles.btnSecondary}
        >
          {busy === "draining"
            ? "Draining…"
            : `Run retry pass now (${c.pending} pending)`}
        </button>
        <button
          type="button"
          onClick={onFinalizeClick}
          disabled={busy !== "idle" || c.succeeded + c.exhausted === 0}
          className={styles.btnPrimary}
          title={
            "Re-runs client output generation using the retry queue's " +
            "results. Regenerates valid_emails / review_* / " +
            "second_pass_candidates XLSX files and rebuilds the client " +
            "package. Always manual: never auto-applied to a bundle the " +
            "customer has already downloaded."
          }
        >
          {busy === "finalizing"
            ? "Re-cleaning…"
            : "Re-clean with retry results"}
        </button>
      </div>

      {error && <div className={styles.error}>{error}</div>}
    </div>
  );
}
