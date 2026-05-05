"use client";

import { useState } from "react";
import { downloadClientPackage } from "@/lib/api";
import type {
  ClientPackageDownloadError,
  OperatorReviewSummary,
} from "@/lib/types";
import styles from "./DeliveryDownloadButton.module.css";

interface DeliveryDownloadButtonProps {
  jobId: string;
  review: OperatorReviewSummary | null;
}

interface BlockedPayload {
  error: string;
  message: string;
  status?: string | null;
  bad_files?: ClientPackageDownloadError["bad_files"];
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
  return `trashpanda_client_delivery_package_${jobId}.zip`;
}

async function readBlockedPayload(
  response: Response,
): Promise<BlockedPayload | null> {
  try {
    const data = (await response.json()) as Partial<ClientPackageDownloadError>;
    if (typeof data?.error !== "string" || typeof data?.message !== "string") {
      return null;
    }
    return {
      error: data.error,
      message: data.message,
      status: data.status ?? null,
      bad_files: data.bad_files,
    };
  } catch {
    return null;
  }
}

async function readGenericErrorMessage(
  response: Response,
): Promise<string | null> {
  try {
    const data = await response.json();
    if (data && typeof data === "object") {
      const obj = data as Record<string, unknown>;
      if (typeof obj.message === "string") return obj.message;
      if (typeof obj.error === "string") return obj.error;
    }
    return null;
  } catch {
    return null;
  }
}

/**
 * The only safe-delivery surface in the UI.
 *
 * Enabled iff the backend's V2.9.7 operator review gate explicitly
 * green-lights the package — `review.ready_for_client === true`.
 * NEVER consults manifest fields, status strings, audience values, or
 * warnings/issues to decide enablement. Readiness is the backend's
 * boolean and nothing else.
 *
 * The download is performed via fetch + blob (NOT an `<a href>`), so
 * a 409 block payload is parsed and surfaced inline instead of being
 * silently saved as a corrupt ZIP.
 */
export function DeliveryDownloadButton({
  jobId,
  review,
}: DeliveryDownloadButtonProps) {
  const [loading, setLoading] = useState(false);
  const [blocked, setBlocked] = useState<BlockedPayload | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);

  const canDownload = review?.ready_for_client === true;

  const disabledReason = !review
    ? "Run the operator review gate first."
    : review.ready_for_client !== true
      ? "Gate did not green-light this package."
      : null;

  const handleDownload = async () => {
    if (!canDownload || loading) return;
    setLoading(true);
    setBlocked(null);
    setErrorMessage(null);
    setSuccessMessage(null);

    try {
      const response = await downloadClientPackage(jobId);

      if (response.status === 409) {
        const payload = await readBlockedPayload(response);
        setBlocked(
          payload ?? {
            error: "blocked",
            message:
              "The download endpoint returned a 409 block. The package is not deliverable.",
          },
        );
        return;
      }

      if (!response.ok) {
        const message =
          (await readGenericErrorMessage(response)) ??
          `Download failed (${response.status}).`;
        setErrorMessage(message);
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
      setSuccessMessage(`Downloaded ${filename}.`);
    } catch (err) {
      setErrorMessage(
        err instanceof Error ? err.message : "Unexpected download error.",
      );
    } finally {
      setLoading(false);
    }
  };

  return (
    <section className={styles.panel} aria-label="Safe client-package download">
      <header className={styles.header}>
        <div className={styles.titleWrap}>
          <div className={styles.eyebrow}>// SAFE DOWNLOAD</div>
          <div className={styles.title}>Client delivery package</div>
        </div>
        <div className={styles.headMeta}>
          <span
            className={[
              styles.readyChip,
              canDownload ? styles.readyChipOk : styles.readyChipBlocked,
            ].join(" ")}
          >
            {canDownload ? "READY" : "BLOCKED"}
          </span>
        </div>
      </header>

      <p className={styles.notice}>
        Delivery is blocked unless ready_for_client is true.
      </p>

      <button
        type="button"
        className={styles.downloadBtn}
        disabled={!canDownload || loading}
        onClick={() => {
          void handleDownload();
        }}
        aria-disabled={!canDownload}
      >
        {loading ? "Downloading…" : "Download client delivery package"}
      </button>

      {disabledReason && !loading && (
        <div className={styles.disabledReason}>{disabledReason}</div>
      )}

      {blocked && (
        <div className={styles.blocked} role="alert">
          <div className={styles.blockedTitle}>
            Download blocked: {blocked.error}
          </div>
          <div className={styles.blockedMessage}>{blocked.message}</div>
          {blocked.status && (
            <div className={styles.blockedMeta}>
              Backend status: {blocked.status}
            </div>
          )}
          {blocked.bad_files && blocked.bad_files.length > 0 && (
            <ul className={styles.badFiles}>
              {blocked.bad_files.map((bf, idx) => (
                <li key={`${bf.filename ?? "unknown"}-${idx}`}>
                  {bf.filename ?? "(unknown filename)"} —{" "}
                  {bf.audience ?? "(unknown audience)"}
                </li>
              ))}
            </ul>
          )}
        </div>
      )}

      {errorMessage && !blocked && (
        <div className={styles.errorBox} role="alert">
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
