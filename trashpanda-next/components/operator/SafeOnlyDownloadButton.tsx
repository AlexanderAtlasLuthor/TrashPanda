"use client";

import { useState } from "react";
import { downloadSafeOnlyClientPackage } from "@/lib/api";
import type { OperatorReviewSummary, SafeOnlyDownloadError } from "@/lib/types";
import styles from "./SafeOnlyDownloadButton.module.css";

interface SafeOnlyDownloadButtonProps {
  jobId: string;
  review: OperatorReviewSummary | null;
}

interface BlockedPayload {
  error: string;
  message: string;
  status?: string | null;
  bad_files?: SafeOnlyDownloadError["bad_files"];
  missing_files?: string[];
}

const FILENAME_HEADER_RE = /filename\*?=(?:UTF-8'')?"?([^";\n]+)"?/i;

function parseFilenameFromContentDisposition(
  header: string | null,
): string | null {
  if (!header) return null;
  const match = FILENAME_HEADER_RE.exec(header);
  if (!match || !match[1]) return null;
  try {
    return decodeURIComponent(match[1].trim());
  } catch {
    return match[1].trim();
  }
}

function fallbackFilename(jobId: string): string {
  return `trashpanda_safe_only_client_package_${jobId}.zip`;
}

async function readBlockedPayload(
  response: Response,
): Promise<BlockedPayload | null> {
  try {
    const data = (await response.json()) as Partial<SafeOnlyDownloadError>;
    if (typeof data?.error !== "string" || typeof data?.message !== "string") {
      return null;
    }
    return {
      error: data.error,
      message: data.message,
      status: data.status ?? null,
      bad_files: data.bad_files,
      missing_files: data.missing_files,
    };
  } catch {
    return null;
  }
}

/**
 * V2.10.8.5 — Safe-only partial delivery download.
 *
 * SEPARATE from {@link DeliveryDownloadButton}; the two are mutually
 * exclusive contracts: full delivery for ``ready_for_client === true``
 * runs, safe-only partial delivery for runs whose review gate flagged
 * ``ready_for_client_partial === true`` with mode ``safe_only``.
 *
 * Three operator confirmations are required before any network request
 * is made. The wrapper in `lib/api.ts` enforces the same rule —
 * `downloadSafeOnlyClientPackage(jobId, false)` short-circuits with a
 * synthetic 409 — but we layer it again at the UI for clarity. A 409
 * response is parsed as JSON and surfaced inline; it is NEVER saved as
 * a corrupt ZIP.
 */
export function SafeOnlyDownloadButton({
  jobId,
  review,
}: SafeOnlyDownloadButtonProps) {
  const [confirmPartial, setConfirmPartial] = useState(false);
  const [confirmNotFullReady, setConfirmNotFullReady] = useState(false);
  const [confirmExcludedRows, setConfirmExcludedRows] = useState(false);
  const [loading, setLoading] = useState(false);
  const [blocked, setBlocked] = useState<BlockedPayload | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);

  const partialReady = review?.ready_for_client_partial === true;
  const fullReady = review?.ready_for_client === true;
  const includedCount = review?.partial_delivery_allowed_count ?? 0;
  const excludedCount = review?.partial_delivery_excluded_count ?? 0;
  const allConfirmed =
    confirmPartial && confirmNotFullReady && confirmExcludedRows;

  const buttonDisabled =
    !partialReady || fullReady || !allConfirmed || loading;

  const disabledReason = !review
    ? "Run the operator review gate first."
    : fullReady
      ? "Full delivery is ready. Use the standard client package download."
      : !partialReady
        ? "Safe-only delivery is not available for this run."
        : !allConfirmed
          ? "Confirm all three statements before downloading."
          : null;

  const handleDownload = async () => {
    if (buttonDisabled) return;
    setLoading(true);
    setBlocked(null);
    setErrorMessage(null);
    setSuccessMessage(null);

    try {
      const response = await downloadSafeOnlyClientPackage(jobId, true);

      if (response.status === 409 || !response.ok) {
        const payload = await readBlockedPayload(response);
        setBlocked(
          payload ?? {
            error: "blocked",
            message: "Safe-only download is unavailable.",
          },
        );
        return;
      }

      const blob = await response.blob();
      const filename =
        parseFilenameFromContentDisposition(
          response.headers.get("content-disposition"),
        ) ?? fallbackFilename(jobId);
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = filename;
      document.body.appendChild(anchor);
      anchor.click();
      anchor.remove();
      setTimeout(() => URL.revokeObjectURL(url), 0);
      setSuccessMessage(`Safe-only package downloaded: ${filename}`);
    } catch (err) {
      setErrorMessage(
        err instanceof Error ? err.message : "Unexpected download error.",
      );
    } finally {
      setLoading(false);
    }
  };

  return (
    <section
      className={styles.card}
      aria-label="Safe-only partial client-package download"
    >
      <header className={styles.header}>
        <div className={styles.titleWrap}>
          <div className={styles.eyebrow}>// SAFE-ONLY PARTIAL DELIVERY</div>
          <div className={styles.title}>Safe-only client package</div>
          <div className={styles.subtitle}>
            Strict subset of the full package. Only SMTP-confirmed safe
            rows are included.
          </div>
        </div>
        <div className={styles.headMeta}>
          <span
            className={[
              styles.status,
              partialReady && !fullReady
                ? styles.statusOk
                : styles.statusBlocked,
            ].join(" ")}
          >
            {fullReady
              ? "USE FULL"
              : partialReady
                ? "AVAILABLE"
                : "NOT AVAILABLE"}
          </span>
        </div>
      </header>

      {partialReady && !fullReady && (
        <>
          <p className={styles.warning}>
            Safe-only package available.
            <br />
            Only {includedCount} confirmed-safe rows will be included.
            <br />
            {excludedCount} rows are excluded from this package.
          </p>

          <div className={styles.counts}>
            <div className={styles.count}>
              <span className={styles.countValue}>{includedCount}</span>
              <span className={styles.countLabel}>Included</span>
            </div>
            <div className={styles.count}>
              <span
                className={[styles.countValue, styles.countExcluded].join(" ")}
              >
                {excludedCount}
              </span>
              <span className={styles.countLabel}>Excluded</span>
            </div>
          </div>

          <ul className={styles.confirmList}>
            <li className={styles.confirmItem}>
              <label>
                <input
                  type="checkbox"
                  checked={confirmPartial}
                  onChange={(e) => setConfirmPartial(e.target.checked)}
                  disabled={loading}
                />
                <span>I understand this is a partial safe-only delivery.</span>
              </label>
            </li>
            <li className={styles.confirmItem}>
              <label>
                <input
                  type="checkbox"
                  checked={confirmNotFullReady}
                  onChange={(e) => setConfirmNotFullReady(e.target.checked)}
                  disabled={loading}
                />
                <span>I understand the full run is not ready_for_client.</span>
              </label>
            </li>
            <li className={styles.confirmItem}>
              <label>
                <input
                  type="checkbox"
                  checked={confirmExcludedRows}
                  onChange={(e) => setConfirmExcludedRows(e.target.checked)}
                  disabled={loading}
                />
                <span>
                  I understand review/catch-all/rejected rows are excluded.
                </span>
              </label>
            </li>
          </ul>
        </>
      )}

      <button
        type="button"
        className={[
          styles.button,
          buttonDisabled ? styles.buttonDisabled : "",
        ].join(" ")}
        disabled={buttonDisabled}
        aria-disabled={buttonDisabled}
        onClick={() => {
          void handleDownload();
        }}
      >
        {loading ? "Downloading…" : "Download safe-only package"}
      </button>

      {disabledReason && !loading && (
        <div className={styles.disabledReason}>{disabledReason}</div>
      )}

      {blocked && (
        <div className={styles.error} role="alert">
          <div className={styles.errorTitle}>
            Download blocked: {blocked.error}
          </div>
          <div className={styles.errorMessage}>{blocked.message}</div>
          {blocked.status && (
            <div className={styles.errorMeta}>
              Backend status: {blocked.status}
            </div>
          )}
          {blocked.bad_files && blocked.bad_files.length > 0 && (
            <ul className={styles.errorList}>
              {blocked.bad_files.map((bf, idx) => (
                <li key={`${bf.filename ?? "unknown"}-${idx}`}>
                  {bf.filename ?? "(unknown filename)"} —{" "}
                  {bf.audience ?? "(unknown audience)"}
                </li>
              ))}
            </ul>
          )}
          {blocked.missing_files && blocked.missing_files.length > 0 && (
            <ul className={styles.errorList}>
              {blocked.missing_files.map((name, idx) => (
                <li key={`missing-${name}-${idx}`}>{name}</li>
              ))}
            </ul>
          )}
        </div>
      )}

      {errorMessage && !blocked && (
        <div className={styles.error} role="alert">
          {errorMessage}
        </div>
      )}

      {successMessage && !blocked && !errorMessage && (
        <div className={styles.success} role="status">
          {successMessage}
        </div>
      )}
    </section>
  );
}
