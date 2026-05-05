"use client";

import { useState } from "react";
import type { RunPreflightInput } from "@/lib/api";
import styles from "./PreflightForm.module.css";

interface PreflightFormProps {
  loading?: boolean;
  onSubmit: (input: RunPreflightInput) => Promise<void> | void;
}

/**
 * Operator preflight form. Server-side path input only — browser
 * upload support is deferred to V2.10.6. This component never calls
 * the API itself; it builds a {@link RunPreflightInput} from local
 * state and hands it to its parent via {@link onSubmit}.
 *
 * Optional text fields are stripped from the payload when empty so
 * the backend doesn't have to interpret empty strings. Boolean fields
 * are always sent because `false` is a meaningful value to the
 * V2.9.2 preflight checker (e.g. `smtp_port_verified: false` toggles
 * a warn issue).
 */
export function PreflightForm({ loading, onSubmit }: PreflightFormProps) {
  const [inputPath, setInputPath] = useState("");
  const [outputDir, setOutputDir] = useState("");
  const [configPath, setConfigPath] = useState("");
  const [largeRunConfirmed, setLargeRunConfirmed] = useState(false);
  const [smtpPortVerified, setSmtpPortVerified] = useState(false);

  const trimmedInputPath = inputPath.trim();
  const submittable = !loading && trimmedInputPath.length > 0;

  const handleSubmit = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!submittable) return;

    const input: RunPreflightInput = {
      input_path: trimmedInputPath,
      operator_confirmed_large_run: largeRunConfirmed,
      smtp_port_verified: smtpPortVerified,
    };
    const trimmedOutputDir = outputDir.trim();
    if (trimmedOutputDir) input.output_dir = trimmedOutputDir;
    const trimmedConfigPath = configPath.trim();
    if (trimmedConfigPath) input.config_path = trimmedConfigPath;

    void onSubmit(input);
  };

  return (
    <form
      className={styles.form}
      onSubmit={handleSubmit}
      aria-label="Run operator preflight"
    >
      <header className={styles.header}>
        <div className={styles.eyebrow}>// PREFLIGHT INPUT</div>
        <div className={styles.title}>Run preflight</div>
      </header>

      <div className={styles.field}>
        <label htmlFor="preflight-input-path" className={styles.label}>
          Input path
        </label>
        <input
          id="preflight-input-path"
          type="text"
          value={inputPath}
          onChange={(e) => setInputPath(e.target.value)}
          placeholder="/data/incoming/wyoming_sample.csv"
          autoComplete="off"
          spellCheck={false}
          required
          className={styles.input}
        />
        <div className={styles.helper}>
          Server-side path only. Browser upload support is deferred.
        </div>
      </div>

      <div className={styles.field}>
        <label htmlFor="preflight-output-dir" className={styles.label}>
          Output directory <span className={styles.optional}>(optional)</span>
        </label>
        <input
          id="preflight-output-dir"
          type="text"
          value={outputDir}
          onChange={(e) => setOutputDir(e.target.value)}
          placeholder="Leave blank for default"
          autoComplete="off"
          spellCheck={false}
          className={styles.input}
        />
      </div>

      <div className={styles.field}>
        <label htmlFor="preflight-config-path" className={styles.label}>
          Config path <span className={styles.optional}>(optional)</span>
        </label>
        <input
          id="preflight-config-path"
          type="text"
          value={configPath}
          onChange={(e) => setConfigPath(e.target.value)}
          placeholder="Leave blank for default"
          autoComplete="off"
          spellCheck={false}
          className={styles.input}
        />
      </div>

      <div className={styles.checkRow}>
        <label className={styles.checkLabel}>
          <input
            type="checkbox"
            checked={largeRunConfirmed}
            onChange={(e) => setLargeRunConfirmed(e.target.checked)}
          />
          <span>Operator confirmed large run</span>
        </label>
        <label className={styles.checkLabel}>
          <input
            type="checkbox"
            checked={smtpPortVerified}
            onChange={(e) => setSmtpPortVerified(e.target.checked)}
          />
          <span>SMTP port verified</span>
        </label>
      </div>

      <footer className={styles.footer}>
        <button
          type="submit"
          className={styles.submit}
          disabled={!submittable}
        >
          {loading ? "Running preflight…" : "Run preflight"}
        </button>
      </footer>
    </form>
  );
}
