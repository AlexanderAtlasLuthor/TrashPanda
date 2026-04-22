"use client";

import Link from "next/link";
import { useEffect, useRef, useState } from "react";
import { getJob, getJobLogs, ApiError } from "@/lib/api";
import type { JobResult } from "@/lib/types";
import { Topbar } from "@/components/Topbar";
import { JobStatusPanel } from "@/components/JobStatusPanel";
import { LiveLogsPanel } from "@/components/LiveLogsPanel";
import {
  MetricsCards,
  SecondaryMetrics,
} from "@/components/MetricsCards";
import { DownloadArtifacts } from "@/components/DownloadArtifacts";
import { ExecutiveSummary } from "@/components/ExecutiveSummary";
import { ClassificationBreakdown } from "@/components/ClassificationBreakdown";
import { TypoCorrectionsPanel } from "@/components/TypoCorrectionsPanel";
import { ErrorState } from "@/components/ErrorState";

const POLL_INTERVAL_MS = 2000;
const LOG_LIMIT = 25;

function shouldPoll(job: JobResult | null): boolean {
  return job?.status === "queued" || job?.status === "running";
}

function formatDuration(
  start: string | null | undefined,
  end: string | null | undefined,
): string | null {
  if (!start || !end) return null;
  const ms = new Date(end).getTime() - new Date(start).getTime();
  if (isNaN(ms) || ms <= 0) return null;
  const s = Math.floor(ms / 1000);
  const m = Math.floor(s / 60);
  if (m === 0) return `${s}s`;
  return `${m}m ${s % 60}s`;
}

interface ResultsClientProps {
  jobId: string;
  initialJob: JobResult | null;
}

/**
 * Polls /api/jobs/:jobId every 2s. Stops as soon as status is terminal
 * (completed / failed) or when the component unmounts.
 *
 * Log lines are fetched in the same poll cycle via Promise.all — no second
 * timer, no extra effect, no risk of the multiple-polling bug.
 *
 * Renders one of three UIs based on status:
 *   queued | running  -> JobStatusPanel + LiveLogsPanel
 *   completed         -> MetricsCards + SecondaryMetrics + DownloadArtifacts
 *   failed            -> ErrorState + LiveLogsPanel (last lines for debugging)
 *   null              -> 404 copy
 */
export function ResultsClient({ jobId, initialJob }: ResultsClientProps) {
  const [job, setJob] = useState<JobResult | null>(initialJob);
  const [fetchError, setFetchError] = useState<string | null>(null);
  const [logLines, setLogLines] = useState<string[]>([]);
  const jobRef = useRef<JobResult | null>(initialJob);

  useEffect(() => {
    jobRef.current = job;
  }, [job]);

  useEffect(() => {
    let cancelled = false;
    let timeoutId: ReturnType<typeof setTimeout> | null = null;

    const schedule = (delay: number) => {
      if (cancelled) return;
      if (timeoutId) clearTimeout(timeoutId);
      timeoutId = setTimeout(poll, delay);
    };

    const poll = async () => {
      if (cancelled) return;
      if (jobRef.current && !shouldPoll(jobRef.current)) return;

      try {
        const [next, logsData] = await Promise.all([
          getJob(jobId),
          getJobLogs(jobId, LOG_LIMIT).catch(() => null),
        ]);
        if (cancelled) return;

        jobRef.current = next;
        setJob(next);
        if (logsData) setLogLines(logsData.lines);
        setFetchError(null);

        if (shouldPoll(next)) {
          schedule(POLL_INTERVAL_MS);
        } else {
          getJobLogs(jobId, LOG_LIMIT)
            .then((ld) => { if (!cancelled) setLogLines(ld.lines); })
            .catch(() => undefined);
        }
      } catch (err) {
        if (cancelled) return;

        if (err instanceof ApiError && err.status === 404) {
          jobRef.current = null;
          setJob(null);
          setFetchError(null);
          return;
        }
        const message =
          err instanceof Error ? err.message : "Could not fetch job status.";
        setFetchError(message);
        schedule(POLL_INTERVAL_MS);
      }
    };

    if (!jobRef.current || shouldPoll(jobRef.current)) {
      schedule(POLL_INTERVAL_MS);
    }

    return () => {
      cancelled = true;
      if (timeoutId) {
        clearTimeout(timeoutId);
        timeoutId = null;
      }
    };
  }, [jobId]);

  // ── 404 ──────────────────────────────────────────────────────────────────
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

  // ── Shared topbar data ────────────────────────────────────────────────────
  const duration =
    job.status === "completed"
      ? formatDuration(job.started_at, job.finished_at)
      : null;

  const meta = [
    ...(job.input_filename
      ? [{ label: "FILE", value: job.input_filename }]
      : []),
    {
      label: "JOB",
      value: jobId.slice(0, 14) + (jobId.length > 14 ? "…" : ""),
    },
    {
      label: "STATUS",
      value: job.status.toUpperCase(),
      accent: job.status === "completed" || job.status === "running",
      danger: job.status === "failed",
    },
    ...(duration
      ? [{ label: "DURATION", value: duration, accent: true }]
      : []),
  ];

  const titleByStatus: Record<
    JobResult["status"],
    { title: string; crumb: string }
  > = {
    queued:    { title: "JOB/QUEUED",   crumb: "QUEUED"    },
    running:   { title: "JOB/RUNNING",  crumb: "RUNNING"   },
    completed: { title: "JOB/RESULTS",  crumb: "RESULTS"   },
    failed:    { title: "JOB/FAILED",   crumb: "FAILED"    },
  };
  const { title, crumb } = titleByStatus[job.status];

  return (
    <>
      <div className="fade-up">
        <Topbar
          breadcrumb={["WORKSPACE", crumb]}
          title={title}
          titleSlice="/"
          subtitle={job.input_filename ?? undefined}
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
            fontFamily: "var(--font-ui)",
            fontSize: 13,
            letterSpacing: 0.1,
            borderRadius: 3,
          }}
        >
          ⚠ {fetchError} — retrying in 2s
        </div>
      )}

      {/* ── FAILED ── */}
      {job.status === "failed" ? (
        <>
          <div className="fade-up">
            <ErrorState error={job.error} jobId={jobId} />
          </div>
          <div className="fade-up">
            <LiveLogsPanel lines={logLines} status={job.status} />
          </div>
          <div className="fade-up">
            <ProcessAnotherButton />
          </div>
        </>

      /* ── COMPLETED ── */
      ) : job.status === "completed" ? (
        <>
          <div className="fade-up">
            <ExecutiveSummary summary={job.summary} />
          </div>
          <div className="fade-up">
            <MetricsCards summary={job.summary} />
          </div>
          <div className="fade-up">
            <SecondaryMetrics summary={job.summary} />
          </div>
          <div className="fade-up">
            <ClassificationBreakdown summary={job.summary} />
          </div>
          {(job.summary?.total_review ?? 0) > 0 && (
            <div className="fade-up">
              <ReviewQueueBanner jobId={jobId} count={job.summary!.total_review!} />
            </div>
          )}
          <div className="fade-up">
            <InsightsBanner jobId={jobId} />
          </div>
          <div className="fade-up">
            <TypoCorrectionsPanel jobId={jobId} />
          </div>
          <div className="fade-up">
            <DownloadArtifacts
              jobId={jobId}
              artifacts={job.artifacts}
              inputFilename={job.input_filename}
            />
          </div>
          <div className="fade-up">
            <LiveLogsPanel
              lines={logLines}
              status={job.status}
              defaultCollapsed
            />
          </div>
          <div className="fade-up">
            <ProcessAnotherButton />
          </div>
        </>

      /* ── QUEUED / RUNNING ── */
      ) : (
        <>
          <div className="fade-up">
            <JobStatusPanel result={job} />
          </div>
          <div className="fade-up">
            <LiveLogsPanel lines={logLines} status={job.status} />
          </div>
          <div className="fade-up">
            <MetricsCards summary={null} />
          </div>
        </>
      )}
    </>
  );
}

// ── Review Queue CTA banner ──────────────────────────────────────────────────

function ReviewQueueBanner({ jobId, count }: { jobId: string; count: number }) {
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        gap: 16,
        padding: "14px 20px",
        marginBottom: 28,
        background: "rgba(255, 184, 58, 0.05)",
        border: "1px solid rgba(255, 184, 58, 0.25)",
        borderRadius: 3,
      }}
    >
      <div>
        <div style={{ fontFamily: "var(--font-ui)", fontWeight: 600, fontSize: 15, color: "var(--ink-high)", marginBottom: 3, letterSpacing: 0.1 }}>
          {count.toLocaleString()} emails need manual review
        </div>
        <div style={{ fontFamily: "var(--font-ui)", fontSize: 13, color: "var(--ink-mid)", letterSpacing: 0.1 }}>
          Catch-all domains, role-based addresses, and unverified SMTP.
        </div>
      </div>
      <Link
        href={`/review/${encodeURIComponent(jobId)}`}
        style={{
          display: "inline-flex",
          alignItems: "center",
          gap: 8,
          padding: "9px 18px",
          fontFamily: "var(--font-mono)",
          fontSize: 11,
          letterSpacing: "1px",
          textTransform: "uppercase",
          color: "var(--warn)",
          background: "transparent",
          border: "1px solid rgba(255, 184, 58, 0.5)",
          borderRadius: 3,
          textDecoration: "none",
          whiteSpace: "nowrap",
          flexShrink: 0,
          transition: "all 0.15s ease",
        }}
      >
        Review queue →
      </Link>
    </div>
  );
}

// ── "Process another file" CTA ───────────────────────────────────────────────

function ProcessAnotherButton() {
  return (
    <div
      style={{
        display: "flex",
        justifyContent: "flex-start",
        marginBottom: 28,
      }}
    >
      <Link
        href="/"
        style={{
          display: "inline-flex",
          alignItems: "center",
          gap: 8,
          padding: "10px 22px",
          fontFamily: "var(--font-display)",
          fontSize: 13,
          letterSpacing: "1px",
          textTransform: "uppercase",
          color: "var(--bg-void)",
          background: "var(--neon)",
          border: "none",
          borderRadius: 3,
          textDecoration: "none",
          cursor: "pointer",
          boxShadow: "0 0 20px rgba(142, 255, 58, 0.25)",
          transition: "box-shadow 0.2s ease, opacity 0.2s ease",
        }}
        onMouseEnter={(e) => {
          (e.currentTarget as HTMLElement).style.boxShadow =
            "0 0 32px rgba(142, 255, 58, 0.5)";
        }}
        onMouseLeave={(e) => {
          (e.currentTarget as HTMLElement).style.boxShadow =
            "0 0 20px rgba(142, 255, 58, 0.25)";
        }}
      >
        ↩ Process another file
      </Link>
    </div>
  );
}

// ── Insights CTA banner (links to /insights/[jobId]) ─────────────────────────

function InsightsBanner({ jobId }: { jobId: string }) {
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        gap: 16,
        padding: "14px 20px",
        marginBottom: 28,
        background: "linear-gradient(135deg, rgba(142,255,58,0.04), rgba(95,180,255,0.03))",
        border: "1px solid rgba(142, 255, 58, 0.22)",
        borderLeft: "3px solid var(--neon)",
        borderRadius: 3,
      }}
    >
      <div>
        <div style={{ fontFamily: "var(--font-ui)", fontWeight: 600, fontSize: 15, color: "var(--ink-high)", marginBottom: 3, letterSpacing: 0.1 }}>
          See the full intelligence behind every record
        </div>
        <div style={{ fontFamily: "var(--font-ui)", fontSize: 13, color: "var(--ink-mid)", letterSpacing: 0.1 }}>
          Deliverability probability, catch-all detection, SMTP probe, and domain history.
        </div>
      </div>
      <Link
        href={`/insights/${encodeURIComponent(jobId)}`}
        style={{
          display: "inline-flex",
          alignItems: "center",
          gap: 8,
          padding: "9px 18px",
          fontFamily: "var(--font-mono)",
          fontSize: 11,
          letterSpacing: "1px",
          textTransform: "uppercase",
          color: "var(--neon)",
          background: "transparent",
          border: "1px solid rgba(142, 255, 58, 0.5)",
          borderRadius: 3,
          textDecoration: "none",
          whiteSpace: "nowrap",
          flexShrink: 0,
        }}
      >
        Open insights →
      </Link>
    </div>
  );
}
