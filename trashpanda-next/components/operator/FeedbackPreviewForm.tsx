"use client";

import { useState } from "react";
import type { FeedbackPreviewInput } from "@/lib/api";
import styles from "./FeedbackPreviewForm.module.css";

interface FeedbackPreviewFormProps {
  loading?: boolean;
  onLoadCurrent: () => Promise<void> | void;
  onBuildPreview: (input?: FeedbackPreviewInput) => Promise<void> | void;
}

export function FeedbackPreviewForm({
  loading,
  onLoadCurrent,
  onBuildPreview,
}: FeedbackPreviewFormProps) {
  const [feedbackStorePath, setFeedbackStorePath] = useState("");
  const [configPath, setConfigPath] = useState("");
  const [outputDir, setOutputDir] = useState("");

  const handleSubmit = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (loading) return;

    const input: FeedbackPreviewInput = {};
    const trimmedStore = feedbackStorePath.trim();
    if (trimmedStore) input.feedback_store_path = trimmedStore;
    const trimmedConfig = configPath.trim();
    if (trimmedConfig) input.config_path = trimmedConfig;
    const trimmedOutputDir = outputDir.trim();
    if (trimmedOutputDir) input.output_dir = trimmedOutputDir;

    if (Object.keys(input).length === 0) {
      void onBuildPreview(undefined);
      return;
    }
    void onBuildPreview(input);
  };

  return (
    <form
      className={styles.form}
      onSubmit={handleSubmit}
      aria-label="Feedback preview"
    >
      <header className={styles.header}>
        <div className={styles.eyebrow}>// FEEDBACK PREVIEW</div>
        <div className={styles.title}>Domain intelligence preview</div>
      </header>

      <div className={styles.previewNote} role="note">
        Preview only; does not mutate current run.
      </div>

      <div className={styles.field}>
        <label htmlFor="feedback-store-path" className={styles.label}>
          Feedback store path{" "}
          <span className={styles.optional}>(optional)</span>
        </label>
        <input
          id="feedback-store-path"
          type="text"
          value={feedbackStorePath}
          onChange={(e) => setFeedbackStorePath(e.target.value)}
          placeholder="Leave blank for current/default store"
          autoComplete="off"
          spellCheck={false}
          className={styles.input}
        />
      </div>

      <div className={styles.field}>
        <label htmlFor="feedback-preview-config-path" className={styles.label}>
          Config path <span className={styles.optional}>(optional)</span>
        </label>
        <input
          id="feedback-preview-config-path"
          type="text"
          value={configPath}
          onChange={(e) => setConfigPath(e.target.value)}
          placeholder="Leave blank for default"
          autoComplete="off"
          spellCheck={false}
          className={styles.input}
        />
      </div>

      <div className={styles.field}>
        <label htmlFor="feedback-output-dir" className={styles.label}>
          Output directory <span className={styles.optional}>(optional)</span>
        </label>
        <input
          id="feedback-output-dir"
          type="text"
          value={outputDir}
          onChange={(e) => setOutputDir(e.target.value)}
          placeholder="Leave blank for default"
          autoComplete="off"
          spellCheck={false}
          className={styles.input}
        />
      </div>

      <footer className={styles.footer}>
        <button
          type="button"
          className={styles.secondary}
          disabled={loading}
          onClick={() => {
            void onLoadCurrent();
          }}
        >
          {loading ? "Loading preview…" : "Load current preview"}
        </button>
        <button
          type="submit"
          className={styles.submit}
          disabled={loading}
        >
          {loading ? "Building preview…" : "Build preview"}
        </button>
      </footer>
    </form>
  );
}
