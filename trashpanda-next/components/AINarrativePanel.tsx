"use client";

import { useState } from "react";
import { getAIJobSummary } from "@/lib/api";
import styles from "./AINarrativePanel.module.css";

/**
 * Plain-English, AI-generated summary of a completed job.
 *
 * Sits under <ExecutiveSummary/>. User presses "Generate summary" and the
 * backend calls Claude Haiku 4.5 with a cached system prompt — one call per
 * job, cheap enough to re-run freely.
 *
 * Deliberately opt-in (not auto-run on page load) so that:
 *   1. Users without an ANTHROPIC_API_KEY configured never see a failure.
 *   2. No tokens are spent on Results pages the user opens and closes.
 */

type State =
  | { status: "idle" }
  | { status: "loading" }
  | { status: "ready"; narrative: string }
  | { status: "error"; message: string };

export function AINarrativePanel({ jobId }: { jobId: string }) {
  const [state, setState] = useState<State>({ status: "idle" });

  const generate = async () => {
    setState({ status: "loading" });
    try {
      const result = await getAIJobSummary(jobId);
      setState({ status: "ready", narrative: result.narrative });
    } catch (err) {
      const message = err instanceof Error ? err.message : "AI summary failed.";
      setState({ status: "error", message });
    }
  };

  return (
    <div className={styles.panel}>
      <div className={styles.head}>
        <span className={styles.kicker}>// AI SUMMARY</span>
        {state.status !== "loading" && (
          <button
            type="button"
            className={styles.btn}
            onClick={generate}
            aria-label="Generate an AI summary of this job"
          >
            {state.status === "ready" ? "Regenerate" : "Generate summary"}
          </button>
        )}
      </div>

      {state.status === "idle" && (
        <p className={styles.idle}>
          Get a plain-English, one-paragraph take on this list — what&apos;s
          ready, what needs your attention, and what to do next.
        </p>
      )}

      {state.status === "loading" && (
        <p className={styles.loading}>Asking Claude Haiku…</p>
      )}

      {state.status === "ready" && (
        <p className={styles.narrative}>{state.narrative}</p>
      )}

      {state.status === "error" && (
        <p className={styles.error}>
          Couldn&apos;t generate a summary: {state.message}
        </p>
      )}
    </div>
  );
}
