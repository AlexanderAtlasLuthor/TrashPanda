"use client";

import { useState } from "react";
import type { IngestFeedbackInput } from "@/lib/api";
import styles from "./FeedbackIngestForm.module.css";

interface FeedbackIngestFormProps {
  loading?: boolean;
  onSubmit: (input: IngestFeedbackInput) => Promise<void> | void;
}

export function FeedbackIngestForm({
  loading,
  onSubmit,
}: FeedbackIngestFormProps) {
  const [feedbackCsvPath, setFeedbackCsvPath] = useState("");
  const [configPath, setConfigPath] = useState("");

  const trimmedFeedbackCsvPath = feedbackCsvPath.trim();
  const submittable = !loading && trimmedFeedbackCsvPath.length > 0;

  const handleSubmit = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!submittable) return;

    const input: IngestFeedbackInput = {
      feedback_csv_path: trimmedFeedbackCsvPath,
    };
    const trimmedConfigPath = configPath.trim();
    if (trimmedConfigPath) input.config_path = trimmedConfigPath;

    void onSubmit(input);
  };

  return (
    <form
      className={styles.form}
      onSubmit={handleSubmit}
      aria-label="Ingest operator feedback"
    >
      <header className={styles.header}>
        <div className={styles.eyebrow}>// FEEDBACK INGEST</div>
        <div className={styles.title}>Ingest bounce feedback</div>
      </header>

      <div className={styles.field}>
        <label htmlFor="feedback-csv-path" className={styles.label}>
          Feedback CSV path
        </label>
        <input
          id="feedback-csv-path"
          type="text"
          value={feedbackCsvPath}
          onChange={(e) => setFeedbackCsvPath(e.target.value)}
          placeholder="/data/feedback/bounces.csv"
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
        <label htmlFor="feedback-config-path" className={styles.label}>
          Config path <span className={styles.optional}>(optional)</span>
        </label>
        <input
          id="feedback-config-path"
          type="text"
          value={configPath}
          onChange={(e) => setConfigPath(e.target.value)}
          placeholder="Leave blank for default"
          autoComplete="off"
          spellCheck={false}
          className={styles.input}
        />
      </div>

      <footer className={styles.footer}>
        <button
          type="submit"
          className={styles.submit}
          disabled={!submittable}
        >
          {loading ? "Ingesting feedback…" : "Ingest feedback"}
        </button>
      </footer>
    </form>
  );
}
