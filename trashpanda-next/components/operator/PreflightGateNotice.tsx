"use client";

import type { PreflightResult } from "@/lib/types";
import styles from "./PreflightGateNotice.module.css";

interface PreflightGateNoticeProps {
  result: PreflightResult | null;
  confirmedWarn: boolean;
  onConfirmedWarnChange: (value: boolean) => void;
}

type Tone = "muted" | "success" | "warn" | "danger";

interface Branch {
  tone: Tone;
  title: string;
  message: string;
  showWarnConfirm: boolean;
}

function branchFor(result: PreflightResult | null): Branch {
  if (result === null) {
    return {
      tone: "muted",
      title: "Run preflight before starting a cleaning job.",
      message:
        "Preflight results will determine whether the next step is blocked, warned, or clear.",
      showWarnConfirm: false,
    };
  }
  const status = (result.status ?? "").toString().trim().toLowerCase();
  if (status === "block") {
    return {
      tone: "danger",
      title: "Preflight blocked this run.",
      message:
        "Do not start cleaning until blocking issues are resolved.",
      showWarnConfirm: false,
    };
  }
  if (status === "warn") {
    return {
      tone: "warn",
      title: "Preflight returned warnings.",
      message: "Operator confirmation is required before continuing.",
      showWarnConfirm: true,
    };
  }
  if (status === "pass") {
    return {
      tone: "success",
      title: "Preflight passed.",
      message: "This run can proceed to the cleaning job step.",
      showWarnConfirm: false,
    };
  }
  return {
    tone: "muted",
    title: "Preflight status requires review.",
    message:
      "The preflight returned an unrecognised status. Inspect issues above before continuing.",
    showWarnConfirm: false,
  };
}

/**
 * Visual gate that branches on the preflight result.
 *
 * V2.10.6 contract: this component is purely presentational. It
 * surfaces the preflight outcome (pass/warn/block/null/unknown) and,
 * on the warn branch, the operator-confirmation checkbox. The actual
 * gated upload + cleaning-start UI lives in
 * `OperatorStartJobPanel`, rendered as a sibling immediately below
 * this notice on the preflight page. This component therefore never
 * calls any API, never starts a job, and never enables a real action.
 *
 * The warn-branch checkbox controls `confirmedWarn`, which the
 * sibling `OperatorStartJobPanel` reads to decide whether to mount
 * the upload dropzone.
 */
export function PreflightGateNotice({
  result,
  confirmedWarn,
  onConfirmedWarnChange,
}: PreflightGateNoticeProps) {
  const branch = branchFor(result);

  return (
    <section
      className={[styles.notice, styles[branch.tone]].join(" ")}
      data-tone={branch.tone}
      aria-label="Preflight gate notice"
    >
      <div className={styles.head}>
        <div className={styles.eyebrow}>// NEXT STEP</div>
        <div className={styles.title}>{branch.title}</div>
        <div className={styles.message}>{branch.message}</div>
      </div>

      {branch.showWarnConfirm && (
        <label className={styles.confirmRow}>
          <input
            type="checkbox"
            checked={confirmedWarn}
            onChange={(event) => onConfirmedWarnChange(event.target.checked)}
          />
          <span>I understand the warnings and want to continue.</span>
        </label>
      )}
    </section>
  );
}
