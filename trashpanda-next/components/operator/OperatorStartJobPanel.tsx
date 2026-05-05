"use client";

import { useEffect, useState } from "react";
import type { PreflightResult } from "@/lib/types";
import { UploadDropzone } from "@/components/UploadDropzone";
import styles from "./OperatorStartJobPanel.module.css";

interface OperatorStartJobPanelProps {
  result: PreflightResult | null;
  confirmedWarn: boolean;
  /**
   * `input_path` from the most recent successful preflight run, used
   * to remind the operator which server-side path the result attests
   * to. Null until the operator has run preflight successfully at
   * least once.
   */
  lastInputPath?: string | null;
  /**
   * `config_path` from the most recent successful preflight run.
   * Threaded through `UploadDropzone` -> `uploadFile` -> BFF -> backend
   * `POST /jobs` so cleaning runs against the same config that gated
   * the launch. Null when the operator preflighted with default config.
   */
  configPath?: string | null;
}

type Tone = "muted" | "success" | "warn" | "danger";

function normalizeStatus(status: unknown): string {
  return (status ?? "").toString().trim().toLowerCase();
}

function reasonFor(
  result: PreflightResult | null,
  status: string,
  confirmedWarn: boolean,
  canStartFromPreflight: boolean,
  matchConfirmed: boolean,
): { tone: Tone; text: string } {
  if (result === null) {
    return {
      tone: "muted",
      text: "Run preflight before starting a cleaning job.",
    };
  }
  if (status === "block") {
    return {
      tone: "danger",
      text: "Preflight blocked this run. Resolve blocking issues first.",
    };
  }
  if (status === "warn" && !confirmedWarn) {
    return {
      tone: "warn",
      text: "Confirm the warnings before starting this cleaning job.",
    };
  }
  if (!canStartFromPreflight) {
    return {
      tone: "muted",
      text: "Preflight status is not recognized. Re-run preflight before starting.",
    };
  }
  if (!matchConfirmed) {
    return {
      tone: "warn",
      text: "Confirm that the uploaded file matches the file/path that passed preflight.",
    };
  }
  if (status === "warn" && confirmedWarn) {
    return {
      tone: "warn",
      text: "Warnings were operator-confirmed for cleaning start only.",
    };
  }
  return {
    tone: "success",
    text: "Preflight passed. You may start the cleaning job.",
  };
}

/**
 * Gated upload + cleaning-start surface for operators.
 *
 * The gate is for cleaning-start ONLY. It must never be reused as a
 * delivery-readiness signal. Delivery readiness is decided exclusively
 * by the V2.9.7 operator review gate via
 * ``OperatorReviewSummary.ready_for_client`` and is checked on
 * ``/operator/jobs/[jobId]`` — never here.
 *
 * Two-layer gate:
 *   Layer A — preflight outcome: result.status `pass`, or `warn` with
 *             `confirmedWarn === true`.
 *   Layer B — explicit operator confirmation that the uploaded file
 *             matches the file/path that passed preflight. The
 *             frontend cannot prove equivalence between the
 *             server-side preflight `input_path` and the
 *             browser-uploaded file, so this checkbox is the only
 *             record of operator intent.
 *
 * The component reuses the existing `UploadDropzone` component and the
 * existing `uploadFile` -> `POST /api/jobs` pipeline. It does not
 * import `uploadFile` directly, does not build packages, does not run
 * the review gate, and does not download anything.
 */
export function OperatorStartJobPanel({
  result,
  confirmedWarn,
  lastInputPath,
  configPath,
}: OperatorStartJobPanelProps) {
  const status = normalizeStatus(result?.status);
  const canStartFromPreflight =
    result !== null &&
    (status === "pass" || (status === "warn" && confirmedWarn === true));

  const [matchConfirmed, setMatchConfirmed] = useState(false);

  // Reset the file-match confirmation whenever any of the gating
  // inputs change. A fresh preflight, a flipped warn confirmation,
  // or a different preflighted path/config all invalidate the prior
  // operator attestation.
  useEffect(() => {
    setMatchConfirmed(false);
  }, [result, confirmedWarn, lastInputPath, configPath]);

  const canUploadStart = canStartFromPreflight && matchConfirmed;

  const reason = reasonFor(
    result,
    status,
    confirmedWarn,
    canStartFromPreflight,
    matchConfirmed,
  );
  const warnConfirmed = status === "warn" && confirmedWarn === true;

  const trimmedLastInput = (lastInputPath ?? "").trim();
  const trimmedConfigPath = (configPath ?? "").trim();

  return (
    <section
      className={[styles.panel, styles[reason.tone]].join(" ")}
      data-tone={reason.tone}
      aria-label="Operator cleaning start"
    >
      <header className={styles.head}>
        <div className={styles.eyebrow}>// CLEANING START</div>
        <div className={styles.title}>Start operator cleaning job</div>
      </header>

      <ul className={styles.safety}>
        <li>Start cleaning is not delivery.</li>
        <li>
          Client package still requires build + operator review gate after
          cleaning completes.
        </li>
        <li>Do not deliver unless ready_for_client is true.</li>
        <li>
          Uploaded file must match the file/path that passed preflight. The
          frontend cannot verify equivalence.
        </li>
      </ul>

      {(trimmedLastInput || trimmedConfigPath) && (
        <div className={styles.preflightSnapshot}>
          {trimmedLastInput && (
            <div className={styles.snapshotRow}>
              <span className={styles.snapshotLabel}>Preflight passed against</span>
              <code className={styles.snapshotValue}>{trimmedLastInput}</code>
            </div>
          )}
          {trimmedConfigPath && (
            <div className={styles.snapshotRow}>
              <span className={styles.snapshotLabel}>Config path</span>
              <code className={styles.snapshotValue}>{trimmedConfigPath}</code>
            </div>
          )}
        </div>
      )}

      <div className={styles.reason} role="status">
        {reason.text}
      </div>

      {warnConfirmed && (
        <div className={styles.warnConfirmedNote}>
          Warnings were operator-confirmed for cleaning start only. They do
          not satisfy the delivery review gate.
        </div>
      )}

      {canStartFromPreflight && (
        <label className={styles.matchRow}>
          <input
            type="checkbox"
            checked={matchConfirmed}
            onChange={(event) => setMatchConfirmed(event.target.checked)}
          />
          <span>
            I confirm the uploaded file matches the file/path that passed
            preflight.
          </span>
        </label>
      )}

      {canUploadStart ? (
        <div className={styles.uploadWrap}>
          <UploadDropzone
            configPath={trimmedConfigPath || null}
            redirectTo={(jobId) =>
              `/operator/jobs/${encodeURIComponent(jobId)}`
            }
            ctaLabel="START OPERATOR CLEANING"
          />
        </div>
      ) : (
        <div className={styles.disabledRow}>
          <span
            className={styles.disabledBtn}
            aria-disabled="true"
            role="button"
          >
            Start operator cleaning
          </span>
          <div className={styles.disabledHint}>
            Upload is hidden until preflight allows starting and the file
            match is confirmed.
          </div>
        </div>
      )}
    </section>
  );
}
