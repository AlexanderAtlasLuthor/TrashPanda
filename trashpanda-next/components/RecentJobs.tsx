"use client";

import Link from "next/link";
import { useEffect, useRef, useState } from "react";
import { getJobList } from "@/lib/api";
import type { JobListItem } from "@/lib/types";
import styles from "./RecentJobs.module.css";

const POLL_MS = 3000;

function isActive(status: JobListItem["status"]): boolean {
  return status === "queued" || status === "running";
}

function formatRelativeDate(iso: string | null | undefined): string {
  if (!iso) return "—";
  const diff = Date.now() - new Date(iso).getTime();
  if (diff < 60_000) return "just now";
  if (diff < 3_600_000) return `${Math.floor(diff / 60_000)}m ago`;
  if (diff < 86_400_000) return `${Math.floor(diff / 3_600_000)}h ago`;
  const days = Math.floor(diff / 86_400_000);
  if (days < 30) return `${days}d ago`;
  return `${Math.floor(days / 30)}mo ago`;
}

function StatusPill({ status }: { status: JobListItem["status"] }) {
  const cls =
    status === "completed" ? styles.pillCompleted
    : status === "running"   ? styles.pillRunning
    : status === "failed"    ? styles.pillFailed
    :                          styles.pillQueued;

  const label =
    status === "completed" ? "done"
    : status === "running"   ? "running"
    : status === "failed"    ? "failed"
    :                          "queued";

  return <span className={`${styles.pill} ${cls}`}>{label}</span>;
}

function FileIcon({ filename }: { filename: string }) {
  const isXlsx = filename?.toLowerCase().endsWith(".xlsx");
  return (
    <div className={styles.fileIcon}>
      {isXlsx ? (
        <svg viewBox="0 0 24 24">
          <path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z" />
          <polyline points="14 2 14 8 20 8" />
          <line x1="9" y1="15" x2="15" y2="15" />
          <line x1="9" y1="11" x2="15" y2="11" />
        </svg>
      ) : (
        <svg viewBox="0 0 24 24">
          <path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z" />
          <polyline points="14 2 14 8 20 8" />
          <line x1="12" y1="18" x2="12" y2="12" />
          <line x1="9" y1="15" x2="15" y2="15" />
        </svg>
      )}
    </div>
  );
}

export function RecentJobs() {
  const [jobs, setJobs] = useState<JobListItem[] | null>(null);
  const jobsRef = useRef<JobListItem[] | null>(null);

  useEffect(() => {
    jobsRef.current = jobs;
  }, [jobs]);

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
      try {
        const data = await getJobList(20);
        if (cancelled) return;
        jobsRef.current = data.jobs;
        setJobs(data.jobs);
        if (data.jobs.some((j) => isActive(j.status))) {
          schedule(POLL_MS);
        }
      } catch {
        if (!cancelled) schedule(POLL_MS);
      }
    };

    poll();

    return () => {
      cancelled = true;
      if (timeoutId) clearTimeout(timeoutId);
    };
  }, []);

  return (
    <section style={{ marginBottom: 0 }}>
      <div className={styles.sectionHead}>
        <span className={styles.sectionTitle}>Recent Jobs</span>
        {jobs && jobs.length > 0 && (
          <span className={styles.count}>{jobs.length} job{jobs.length !== 1 ? "s" : ""}</span>
        )}
      </div>

      {jobs === null ? (
        <div className={styles.list}>
          {[0, 1, 2].map((i) => (
            <SkeletonRow key={i} />
          ))}
        </div>
      ) : jobs.length === 0 ? (
        <div className={styles.empty}>
          <svg className={styles.emptyIcon} viewBox="0 0 24 24">
            <path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z" />
            <polyline points="14 2 14 8 20 8" />
          </svg>
          <div className={styles.emptyTitle}>No jobs yet</div>
          <div className={styles.emptyDesc}>
            Upload a CSV or XLSX above to start your first cleaning run.
          </div>
        </div>
      ) : (
        <div className={styles.list}>
          {jobs.map((job) => (
            <JobRow key={job.job_id} job={job} />
          ))}
        </div>
      )}
    </section>
  );
}

function JobRow({ job }: { job: JobListItem }) {
  const href = `/results/${encodeURIComponent(job.job_id)}`;
  const filename = job.input_filename ?? "unknown file";

  return (
    <div className={styles.row}>
      <FileIcon filename={filename} />
      <div className={styles.info}>
        <div className={styles.filename}>{filename}</div>
        <div className={styles.meta}>
          <span>{formatRelativeDate(job.started_at)}</span>
          <span className={styles.metaSep}>·</span>
          <span>{job.job_id.slice(0, 18)}</span>
        </div>
      </div>
      <StatusPill status={job.status} />
      <Link href={href} className={styles.viewBtn}>
        VIEW →
      </Link>
    </div>
  );
}

function SkeletonRow() {
  return (
    <div
      className={styles.row}
      style={{ opacity: 0.35 }}
      aria-hidden
    >
      <div
        style={{
          width: 28,
          height: 28,
          background: "var(--bg-elevated)",
          borderRadius: 3,
          flexShrink: 0,
        }}
      />
      <div style={{ display: "flex", flexDirection: "column", gap: 5 }}>
        <div style={{ width: 180, height: 10, background: "var(--bg-elevated)", borderRadius: 2 }} />
        <div style={{ width: 100, height: 8,  background: "var(--bg-elevated)", borderRadius: 2 }} />
      </div>
      <div style={{ width: 46, height: 20, background: "var(--bg-elevated)", borderRadius: 2 }} />
      <div style={{ width: 52, height: 24, background: "var(--bg-elevated)", borderRadius: 2 }} />
    </div>
  );
}
