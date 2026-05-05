"use client";

import { useState } from "react";
import {
  ApiError,
  buildClientPackage,
  getClientPackageManifest,
  getOperatorJob,
  getOperatorReviewSummary,
  runOperatorReviewGate,
} from "@/lib/api";
import type {
  ClientPackageManifest,
  JobResult,
  OperatorReviewSummary,
} from "@/lib/types";
import { OperatorConsoleShell } from "@/components/operator/OperatorConsoleShell";
import { ClientPackageCard } from "@/components/operator/ClientPackageCard";
import { OperatorReviewPanel } from "@/components/operator/OperatorReviewPanel";
import { DeliveryDownloadButton } from "@/components/operator/DeliveryDownloadButton";
import { SafeOnlyDownloadButton } from "@/components/operator/SafeOnlyDownloadButton";
import { StatusBadge } from "@/components/operator/StatusBadge";
import styles from "./OperatorJobClient.module.css";

interface OperatorJobClientProps {
  jobId: string;
  initialJob: JobResult | null;
  initialManifest: ClientPackageManifest | null;
  initialReview: OperatorReviewSummary | null;
}

function formatOperatorError(error: unknown): string {
  if (error instanceof ApiError) {
    if (error.status === 503) {
      return "Operator endpoints require the Python backend. Set TRASHPANDA_BACKEND_URL.";
    }
    if (error.status === 404) {
      return "Not found.";
    }
    return error.message;
  }
  return error instanceof Error ? error.message : "Unexpected error.";
}

export function OperatorJobClient({
  jobId,
  initialJob,
  initialManifest,
  initialReview,
}: OperatorJobClientProps) {
  const [job, setJob] = useState<JobResult | null>(initialJob);
  const [manifest, setManifest] = useState<ClientPackageManifest | null>(
    initialManifest,
  );
  const [review, setReview] = useState<OperatorReviewSummary | null>(
    initialReview,
  );

  const [reviewStaleSinceBuild, setReviewStaleSinceBuild] = useState(false);

  const [loadingJob, setLoadingJob] = useState(false);
  const [loadingPackage, setLoadingPackage] = useState(false);
  const [loadingReview, setLoadingReview] = useState(false);

  const [jobError, setJobError] = useState<string | null>(null);
  const [packageError, setPackageError] = useState<string | null>(null);
  const [reviewError, setReviewError] = useState<string | null>(null);

  const refreshJob = async () => {
    setLoadingJob(true);
    setJobError(null);
    try {
      const next = await getOperatorJob(jobId);
      setJob(next);
    } catch (err) {
      if (err instanceof ApiError && err.status === 404) {
        setJob(null);
        setJobError("Job not found.");
        return;
      }
      setJobError(formatOperatorError(err));
    } finally {
      setLoadingJob(false);
    }
  };

  const refreshManifest = async () => {
    try {
      const next = await getClientPackageManifest(jobId);
      setManifest(next);
      setPackageError(null);
    } catch (err) {
      if (err instanceof ApiError && err.status === 404) {
        setManifest(null);
        setPackageError(null);
        return;
      }
      setPackageError(formatOperatorError(err));
    }
  };

  const refreshReview = async () => {
    try {
      const next = await getOperatorReviewSummary(jobId);
      setReview(next);
      setReviewError(null);
    } catch (err) {
      if (err instanceof ApiError && err.status === 404) {
        setReview(null);
        setReviewError(null);
        return;
      }
      setReviewError(formatOperatorError(err));
    }
  };

  const handleBuildPackage = async () => {
    setLoadingPackage(true);
    setPackageError(null);
    try {
      await buildClientPackage(jobId);
      await refreshManifest();
      setReviewStaleSinceBuild(true);
    } catch (err) {
      setPackageError(formatOperatorError(err));
    } finally {
      setLoadingPackage(false);
    }
  };

  const handleRunGate = async () => {
    setLoadingReview(true);
    setReviewError(null);
    try {
      await runOperatorReviewGate(jobId);
      await refreshReview();
      setReviewStaleSinceBuild(false);
    } catch (err) {
      setReviewError(formatOperatorError(err));
    } finally {
      setLoadingReview(false);
    }
  };

  return (
    <OperatorConsoleShell>
      <section className={styles.jobHeader}>
        <div className={styles.jobMeta}>
          <div className={styles.eyebrow}>// JOB</div>
          <div className={styles.jobId}>{jobId}</div>
          {job?.input_filename && (
            <div className={styles.inputFilename}>{job.input_filename}</div>
          )}
        </div>
        <div className={styles.jobControls}>
          <StatusBadge status={job?.status ?? "unknown"} />
          <button
            type="button"
            className={styles.refreshBtn}
            disabled={loadingJob}
            onClick={() => {
              void refreshJob();
            }}
          >
            {loadingJob ? "Refreshing…" : "Refresh job"}
          </button>
        </div>
      </section>

      {jobError && (
        <div className={styles.jobErrorBox} role="alert">
          {jobError}
        </div>
      )}

      <div className={styles.grid}>
        <ClientPackageCard
          jobStatus={job?.status ?? null}
          manifest={manifest}
          loading={loadingPackage}
          error={packageError}
          reviewStaleSinceBuild={reviewStaleSinceBuild}
          onBuildPackage={handleBuildPackage}
        />
        <OperatorReviewPanel
          review={review}
          loading={loadingReview}
          error={reviewError}
          reviewStaleSinceBuild={reviewStaleSinceBuild}
          onRunGate={handleRunGate}
        />
      </div>

      <DeliveryDownloadButton jobId={jobId} review={review} />
      <SafeOnlyDownloadButton jobId={jobId} review={review} />
    </OperatorConsoleShell>
  );
}
