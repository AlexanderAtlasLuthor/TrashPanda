"use client";

import type { JobResult } from "@/lib/types";
import styles from "./JobHelperBanner.module.css";

interface JobHelperBannerProps {
  result: JobResult;
  /** True when the operator-side cancellation flag has been flipped. */
  cancelled?: boolean;
  /** True when the run is using live SMTP probes. */
  smtpLive?: boolean;
  /** True when the BFF cannot reach the backend (tunnel dropped). */
  backendUnreachable?: boolean;
}

interface BannerCopy {
  tone: "info" | "warn" | "danger" | "ok";
  icon: string;
  message: string;
}

function pickCopy(props: JobHelperBannerProps): BannerCopy {
  const { result, cancelled, smtpLive, backendUnreachable } = props;

  if (backendUnreachable) {
    return {
      tone: "danger",
      icon: "⚠",
      message:
        "VPS connection lost. The badge in the top-right will turn green again once the SSH tunnel is back up.",
    };
  }
  if (cancelled) {
    return {
      tone: "warn",
      icon: "⏹",
      message:
        "Cancellation requested — SMTP probes will unwind in a few seconds and the job will finalise with whatever is processed so far.",
    };
  }

  switch (result.status) {
    case "queued":
      return {
        tone: "info",
        icon: "⏳",
        message:
          "Upload received. The pipeline will pick this up in a few seconds — you can leave this page and come back, the job runs server-side.",
      };
    case "running":
      if (smtpLive) {
        return {
          tone: "warn",
          icon: "📡",
          message:
            "SMTP live probes are running. Yahoo / AOL / Verizon-class addresses are skipped automatically. If a probe stalls, click Cancel below.",
        };
      }
      return {
        tone: "info",
        icon: "🐼",
        message:
          "Cleaning in progress. This page polls every 2 s and switches to the download view automatically when the run finishes.",
      };
    case "completed":
      return {
        tone: "ok",
        icon: "✓",
        message:
          "Done. Click the big green Send to client button below — that's the only file your customer needs.",
      };
    case "failed": {
      const errorMsg = result.error?.message;
      if (errorMsg && /wall.?clock|cancel/i.test(errorMsg)) {
        return {
          tone: "danger",
          icon: "⏱",
          message:
            "Job timed out (wall-clock watchdog). Re-upload, or raise TRASHPANDA_MAX_JOB_SECONDS in /etc/trashpanda/backend.env if your lists are very large.",
        };
      }
      return {
        tone: "danger",
        icon: "✕",
        message:
          errorMsg ??
          "The job failed before producing artifacts. Re-upload the file; if the error repeats, check the backend logs.",
      };
    }
  }
  return {
    tone: "info",
    icon: "·",
    message: "",
  };
}

/**
 * One sentence that always tells the operator what to do next. Tone
 * (info / ok / warn / danger) drives the colour scheme. Copy is
 * picked from a small lookup so the user never has to interpret
 * raw status flags.
 */
export function JobHelperBanner(props: JobHelperBannerProps) {
  const copy = pickCopy(props);
  if (!copy.message) return null;
  return (
    <div
      className={[styles.banner, styles[copy.tone]].join(" ")}
      role={copy.tone === "danger" ? "alert" : "status"}
    >
      <span className={styles.icon} aria-hidden>
        {copy.icon}
      </span>
      <span className={styles.message}>{copy.message}</span>
    </div>
  );
}
