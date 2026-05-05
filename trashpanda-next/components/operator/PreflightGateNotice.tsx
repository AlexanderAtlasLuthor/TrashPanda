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
  helper: string;
}

const HELPER_NOT_WIRED = "Cleaning job start is not wired in V2.10.4.";
const HELPER_NOT_WIRED_DETAILED =
  "Cleaning job start is not wired in V2.10.4 — operator-side cleaning launch is deferred.";

function branchFor(result: PreflightResult | null): Branch {
  if (result === null) {
    return {
      tone: "muted",
      title: "Run preflight before starting a cleaning job.",
      message:
        "Preflight results will determine whether the next step is blocked, warned, or clear.",
      showWarnConfirm: false,
      helper: HELPER_NOT_WIRED,
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
      helper: HELPER_NOT_WIRED,
    };
  }
  if (status === "warn") {
    return {
      tone: "warn",
      title: "Preflight returned warnings.",
      message: "Operator confirmation is required before continuing.",
      showWarnConfirm: true,
      helper: HELPER_NOT_WIRED,
    };
  }
  if (status === "pass") {
    return {
      tone: "success",
      title: "Preflight passed.",
      message: "This run can proceed to the cleaning job step.",
      showWarnConfirm: false,
      helper: HELPER_NOT_WIRED_DETAILED,
    };
  }
  return {
    tone: "muted",
    title: "Preflight status requires review.",
    message:
      "The preflight returned an unrecognised status. Inspect issues above before continuing.",
    showWarnConfirm: false,
    helper: HELPER_NOT_WIRED,
  };
}

/**
 * Visual gate that branches on the preflight result.
 *
 * V2.10.4 contract: the "Start cleaning job" button is ALWAYS a
 * disabled placeholder. There is no operator-side endpoint to start a
 * cleaning job from a server-side path today — operator-side cleaning
 * launch is deferred to V2.10.6 alongside multipart upload. This
 * component therefore never calls any API and never enables a real
 * action.
 *
 * The warn-branch confirmation checkbox is a UX preview of the future
 * gating (so the operator can practice the flow) — it does not unlock
 * any action in V2.10.4.
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

      <div className={styles.actionRow}>
        <button
          type="button"
          className={styles.placeholderBtn}
          disabled
          aria-disabled="true"
        >
          Start cleaning job
        </button>
        <div className={styles.helper}>{branch.helper}</div>
      </div>
    </section>
  );
}
