"use client";

import { useState } from "react";
import {
  ApiError,
  getFeedbackPreview,
  ingestFeedback,
  type FeedbackPreviewInput,
  type IngestFeedbackInput,
} from "@/lib/api";
import type {
  FeedbackIngestionSummary,
  FeedbackPreviewResult,
} from "@/lib/types";
import { OperatorConsoleShell } from "@/components/operator/OperatorConsoleShell";
import { FeedbackIngestForm } from "@/components/operator/FeedbackIngestForm";
import { FeedbackIngestionSummaryPanel } from "@/components/operator/FeedbackIngestionSummaryPanel";
import { FeedbackPreviewForm } from "@/components/operator/FeedbackPreviewForm";
import { FeedbackPreviewPanel } from "@/components/operator/FeedbackPreviewPanel";
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

export default function OperatorFeedbackPage() {
  const [ingestion, setIngestion] = useState<FeedbackIngestionSummary | null>(
    null,
  );
  const [ingestionLoading, setIngestionLoading] = useState(false);
  const [ingestionError, setIngestionError] = useState<string | null>(null);

  const [preview, setPreview] = useState<FeedbackPreviewResult | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewError, setPreviewError] = useState<string | null>(null);

  const handleIngest = async (input: IngestFeedbackInput) => {
    setIngestionLoading(true);
    setIngestionError(null);
    try {
      const next = await ingestFeedback(input);
      setIngestion(next);
    } catch (err) {
      setIngestionError(formatOperatorError(err));
    } finally {
      setIngestionLoading(false);
    }
  };

  const handleLoadPreview = async () => {
    setPreviewLoading(true);
    setPreviewError(null);
    try {
      const next = await getFeedbackPreview();
      setPreview(next);
    } catch (err) {
      setPreviewError(formatOperatorError(err));
    } finally {
      setPreviewLoading(false);
    }
  };

  const handleBuildPreview = async (input?: FeedbackPreviewInput) => {
    setPreviewLoading(true);
    setPreviewError(null);
    try {
      const next = input
        ? await getFeedbackPreview(input)
        : await getFeedbackPreview();
      setPreview(next);
    } catch (err) {
      setPreviewError(formatOperatorError(err));
    } finally {
      setPreviewLoading(false);
    }
  };

  return (
    <OperatorConsoleShell>
      <section className={styles.intro}>
        <div className={styles.eyebrow}>// FEEDBACK</div>
        <div className={styles.title}>Feedback ingest &amp; preview</div>
        <p className={styles.desc}>
          Ingest bounce feedback and preview domain intelligence before
          applying changes to future validation runs.
        </p>
        <div className={styles.previewNote} role="note">
          Preview only; does not mutate current run.
        </div>
      </section>

      <section className={styles.block}>
        <div className={styles.blockLabel}>// INGEST</div>
        <div className={styles.row}>
          <FeedbackIngestForm
            loading={ingestionLoading}
            onSubmit={handleIngest}
          />
          <FeedbackIngestionSummaryPanel
            summary={ingestion}
            loading={ingestionLoading}
            error={ingestionError}
          />
        </div>
      </section>

      <section className={styles.block}>
        <div className={styles.blockLabel}>// PREVIEW</div>
        <div className={styles.row}>
          <FeedbackPreviewForm
            loading={previewLoading}
            onLoadCurrent={handleLoadPreview}
            onBuildPreview={handleBuildPreview}
          />
          <FeedbackPreviewPanel
            preview={preview}
            loading={previewLoading}
            error={previewError}
          />
        </div>
      </section>
    </OperatorConsoleShell>
  );
}
