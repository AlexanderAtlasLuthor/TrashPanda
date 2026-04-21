"use client";

import { useEffect, useRef, useState } from "react";
import { getJob, ApiError } from "@/lib/api";
import type { JobResult } from "@/lib/types";
import { Topbar } from "@/components/Topbar";
import { JobStatusPanel } from "@/components/JobStatusPanel";
import {
  MetricsCards,
  SecondaryMetrics,
} from "@/components/MetricsCards";
import { DownloadArtifacts } from "@/components/DownloadArtifacts";
import { ErrorState } from "@/components/ErrorState";

const POLL_INTERVAL_MS = 2000;

interface ResultsClientProps {
  jobId: string;
  initialJob: JobResult | null;
}

/**
 * Polls /api/jobs/:jobId every 2s. Stops as soon as status is terminal
 * (completed / failed) or when the component unmounts.
 *
 * Renders one of three UIs based on status:
 *   queued | running  -> JobStatusPanel (pipeline visual + hints)
 *   completed         -> MetricsCards + SecondaryMetrics + DownloadArtifacts
 *   failed            -> ErrorState
 *   null              -> 404 copy
 */
export function ResultsClient({ jobId, initialJob }: ResultsClientProps) {
  const [job, setJob] = useState<JobResult | null>(initialJob);
  const [fetchError, setFetchError] = useState<string | null>(null);
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);

  useEffect(() => {
    const isTerminal =
      job?.status === "completed" || job?.status === "failed";
    if (isTerminal) {
      if (timerRef.current) {
        clearInterval(timerRef.current);
        timerRef.current = null;
      }
      return;
    }

    // kick off polling
    async function fetchOnce() {
      try {
        const next = await getJob(jobId);
        setJob(next);
        setFetchError(null);
      } catch (err) {
        if (err instanceof ApiError && err.status === 404) {
          setJob(null);
          setFetchError(null);
          return;
        }
        const message =
          err instanceof Error ? err.message : "Could not fetch job status.";
        setFetchError(message);
      }
    }

    // fetch immediately on mount if we don't have an initial snapshot,
    // then start the interval
    if (!job) fetchOnce();

    timerRef.current = setInterval(fetchOnce, POLL_INTERVAL_MS);
    return () => {
      if (timerRef.current) clearInterval(timerRef.current);
    };
  }, [job, jobId]);

  // 404 / not found
  if (!job) {
    return (
      <>
        <div className="fade-up">
          <Topbar
            breadcrumb={["WORKSPACE", "RESULTS", jobId]}
            title="JOB/NOT FOUND"
            titleSlice="/"
          />
        </div>
        <div className="fade-up">
          <ErrorState
            jobId={jobId}
            error={{
              error_type: "NotFound",
              message:
                "No job with this ID. It may have been pruned, or the URL is wrong.",
            }}
          />
        </div>
      </>
    );
  }

  // Build topbar meta from whatever we have
  const meta: Array<{ label: string; value: string; accent?: boolean }> = [
    { label: "JOB", value: jobId.slice(0, 14) + (jobId.length > 14 ? "…" : "") },
    {
      label: "STATUS",
      value: job.status.toUpperCase(),
      accent: job.status === "completed",
    },
  ];
  if (job.input_filename) {
    meta.unshift({ label: "FILE", value: job.input_filename });
  }

  const titleByStatus: Record<JobResult["status"], { title: string; crumb: string }> = {
    queued: { title: "JOB/QUEUED", crumb: "QUEUED" },
    running: { title: "JOB/RUNNING", crumb: "RUNNING" },
    completed: { title: "JOB/RESULTS", crumb: "RESULTS" },
    failed: { title: "JOB/FAILED", crumb: "FAILED" },
  };
  const { title, crumb } = titleByStatus[job.status];

  return (
    <>
      <div className="fade-up">
        <Topbar
          breadcrumb={["WORKSPACE", crumb]}
          title={title}
          titleSlice="/"
          meta={meta}
        />
      </div>

      {fetchError && (
        <div
          className="fade-up"
          style={{
            padding: "10px 14px",
            marginBottom: 20,
            border: "1px solid rgba(255, 184, 58, 0.4)",
            background: "rgba(255, 184, 58, 0.06)",
            color: "var(--warn)",
            fontFamily: "var(--font-mono)",
            fontSize: 11,
            borderRadius: 3,
          }}
        >
          ⚠ {fetchError} · retrying in 2s
        </div>
      )}

      {job.status === "failed" ? (
        <div className="fade-up">
          <ErrorState error={job.error} jobId={jobId} />
        </div>
      ) : job.status === "completed" ? (
        <>
          <div className="fade-up">
            <MetricsCards summary={job.summary} />
          </div>
          <div className="fade-up">
            <SecondaryMetrics summary={job.summary} />
          </div>
          <div className="fade-up">
            <DownloadArtifacts jobId={jobId} artifacts={job.artifacts} />
          </div>
        </>
      ) : (
        <>
          <div className="fade-up">
            <JobStatusPanel result={job} />
          </div>
          <div className="fade-up">
            <MetricsCards summary={null} />
          </div>
        </>
      )}
    </>
  );
}
