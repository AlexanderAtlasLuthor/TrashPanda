"use client";

import { useState } from "react";
import { ApiError, runPreflight, type RunPreflightInput } from "@/lib/api";
import type { PreflightResult } from "@/lib/types";
import { OperatorConsoleShell } from "@/components/operator/OperatorConsoleShell";
import { PreflightForm } from "@/components/operator/PreflightForm";
import { PreflightResultPanel } from "@/components/operator/PreflightResultPanel";
import { PreflightGateNotice } from "@/components/operator/PreflightGateNotice";
import { OperatorStartJobPanel } from "@/components/operator/OperatorStartJobPanel";
import styles from "./page.module.css";

function formatOperatorError(error: unknown): string {
  if (error instanceof ApiError) {
    if (error.status === 503) {
      return "Operator endpoints require the Python backend. Set TRASHPANDA_BACKEND_URL.";
    }
    return error.message;
  }
  return error instanceof Error ? error.message : "Unexpected error.";
}

export default function OperatorPreflightPage() {
  const [result, setResult] = useState<PreflightResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [confirmedWarn, setConfirmedWarn] = useState(false);
  // Snapshot of the input that produced `result`. Captured AFTER a
  // successful preflight so the start panel always reads the
  // input_path / config_path that the result actually attests to —
  // not whatever the operator subsequently typed into the form.
  const [lastPreflightInput, setLastPreflightInput] =
    useState<RunPreflightInput | null>(null);

  const handleRun = async (input: RunPreflightInput) => {
    setLoading(true);
    setError(null);
    setConfirmedWarn(false);
    try {
      const next = await runPreflight(input);
      setResult(next);
      setLastPreflightInput(input);
    } catch (err) {
      setError(formatOperatorError(err));
    } finally {
      setLoading(false);
    }
  };

  return (
    <OperatorConsoleShell>
      <section className={styles.intro}>
        <div className={styles.eyebrow}>// PREFLIGHT</div>
        <div className={styles.title}>Preflight a rollout run</div>
        <p className={styles.desc}>
          Run preflight before cleaning to catch large-run, SMTP, and
          configuration risks. Preflight is local-only — it does not run
          the pipeline, open sockets, or mutate outputs.
        </p>
      </section>

      <PreflightForm onSubmit={handleRun} loading={loading} />

      <PreflightResultPanel
        result={result}
        loading={loading}
        error={error}
      />

      <PreflightGateNotice
        result={result}
        confirmedWarn={confirmedWarn}
        onConfirmedWarnChange={setConfirmedWarn}
      />

      <OperatorStartJobPanel
        result={result}
        confirmedWarn={confirmedWarn}
        lastInputPath={lastPreflightInput?.input_path ?? null}
        configPath={lastPreflightInput?.config_path ?? null}
      />
    </OperatorConsoleShell>
  );
}
