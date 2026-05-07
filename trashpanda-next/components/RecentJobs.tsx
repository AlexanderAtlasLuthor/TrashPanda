"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { getJobList, listBatches } from "@/lib/api";
import type { BatchProgress, JobListItem } from "@/lib/types";
import styles from "./RecentJobs.module.css";

const POLL_MS = 3000;

// localStorage key for the list of job IDs the user chose to hide from the UI.
// NOTE: this only hides jobs in the UI. No backend files or job metadata are
// deleted. Clearing browser storage brings them back.
const HIDDEN_JOBS_KEY = "trashpanda_recent_jobs";

function loadHiddenIds(): Set<string> {
  if (typeof window === "undefined") return new Set();
  try {
    const raw = window.localStorage.getItem(HIDDEN_JOBS_KEY);
    if (!raw) return new Set();
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) return new Set(parsed.filter((x) => typeof x === "string"));
    return new Set();
  } catch {
    return new Set();
  }
}

function saveHiddenIds(ids: Set<string>): void {
  if (typeof window === "undefined") return;
  try {
    window.localStorage.setItem(HIDDEN_JOBS_KEY, JSON.stringify(Array.from(ids)));
  } catch {
    // storage may be unavailable (private mode, quota) — fail silently
  }
}

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
  const [batches, setBatches] = useState<BatchProgress[] | null>(null);
  const jobsRef = useRef<JobListItem[] | null>(null);
  const [hiddenIds, setHiddenIds] = useState<Set<string>>(() => new Set());
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [toastOpen, setToastOpen] = useState(false);

  // Load hidden IDs from localStorage after mount (avoids SSR hydration issues).
  useEffect(() => {
    setHiddenIds(loadHiddenIds());
  }, []);

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
        // Fetch jobs and batches in parallel; one failing shouldn't
        // block the other from rendering. Each branch tolerates its
        // own error.
        const [jobsResult, batchesResult] = await Promise.allSettled([
          getJobList(20),
          listBatches(),
        ]);
        if (cancelled) return;
        let anyActive = false;
        if (jobsResult.status === "fulfilled") {
          jobsRef.current = jobsResult.value.jobs;
          setJobs(jobsResult.value.jobs);
          if (jobsResult.value.jobs.some((j) => isActive(j.status))) {
            anyActive = true;
          }
        }
        if (batchesResult.status === "fulfilled") {
          setBatches(batchesResult.value.batches);
          if (
            batchesResult.value.batches.some(
              (b) => b.status === "running" || b.status === "queued",
            )
          ) {
            anyActive = true;
          }
        }
        if (anyActive) schedule(POLL_MS);
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

  // Jobs the user has not chosen to hide from their view.
  const visibleJobs = useMemo(() => {
    if (!jobs) return null;
    if (hiddenIds.size === 0) return jobs;
    return jobs.filter((j) => !hiddenIds.has(j.job_id));
  }, [jobs, hiddenIds]);

  const canClear = !!visibleJobs && visibleJobs.length > 0;

  const handleClear = useCallback(() => {
    setConfirmOpen(false);
    const current = jobsRef.current ?? [];
    // Hide every job currently visible from server — purely client-side.
    setHiddenIds((prev) => {
      const next = new Set(prev);
      for (const j of current) next.add(j.job_id);
      saveHiddenIds(next);
      return next;
    });
    setToastOpen(true);
  }, []);

  // Auto-dismiss the confirmation toast after a short delay.
  useEffect(() => {
    if (!toastOpen) return;
    const t = setTimeout(() => setToastOpen(false), 2500);
    return () => clearTimeout(t);
  }, [toastOpen]);

  return (
    <section style={{ marginBottom: 0 }}>
      <div className={styles.sectionHead}>
        <span className={styles.sectionTitle}>Recent Jobs</span>
        <div className={styles.sectionActions}>
          {visibleJobs && visibleJobs.length > 0 && (
            <span className={styles.count}>
              {visibleJobs.length} job{visibleJobs.length !== 1 ? "s" : ""}
            </span>
          )}
          {canClear && (
            <button
              type="button"
              className={styles.clearBtn}
              onClick={() => setConfirmOpen(true)}
              aria-label="Clear recent jobs from this view"
            >
              Clear recent jobs
            </button>
          )}
        </div>
      </div>

      {visibleJobs === null ? (
        <div className={styles.list}>
          {[0, 1, 2].map((i) => (
            <SkeletonRow key={i} />
          ))}
        </div>
      ) : visibleJobs.length === 0 ? (
        <div className={styles.empty}>
          <svg className={styles.emptyIcon} viewBox="0 0 24 24">
            <path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z" />
            <polyline points="14 2 14 8 20 8" />
          </svg>
          <div className={styles.emptyTitle}>No recent jobs yet</div>
          <div className={styles.emptyDesc}>
            Process a file to see results here
          </div>
        </div>
      ) : (
        <div className={styles.list}>
          {visibleJobs.map((job) => (
            <JobRow key={job.job_id} job={job} />
          ))}
        </div>
      )}

      {confirmOpen && (
        <ClearConfirmDialog
          onCancel={() => setConfirmOpen(false)}
          onConfirm={handleClear}
        />
      )}

      {toastOpen && <ClearToast message="Job history cleared" />}

      {batches && batches.length > 0 && (
        <BatchesSection batches={batches} />
      )}
    </section>
  );
}

// ── Recent batches section (V2.10.18 auto-chunked jobs) ────────────────────

function BatchesSection({ batches }: { batches: BatchProgress[] }) {
  // Newest first.
  const ordered = useMemo(
    () => [...batches].reverse(),
    [batches],
  );

  return (
    <div style={{ marginTop: 24 }}>
      <div className={styles.sectionHead}>
        <span className={styles.sectionTitle}>Recent Batches</span>
        <span className={styles.count}>
          {ordered.length} batch{ordered.length !== 1 ? "es" : ""}
        </span>
      </div>
      <div className={styles.list}>
        {ordered.map((b) => (
          <BatchRow key={b.batch_id} batch={b} />
        ))}
      </div>
    </div>
  );
}

function BatchRow({ batch }: { batch: BatchProgress }) {
  const href = `/batches/${encodeURIComponent(batch.batch_id)}`;
  const filename = batch.batch_id;
  // Map BatchStatus → JobListItem-compatible pill labels for visual reuse.
  const pillStatus: JobListItem["status"] =
    batch.status === "completed"
      ? "completed"
      : batch.status === "failed" || batch.status === "partial_failure"
        ? "failed"
        : batch.status === "running"
          ? "running"
          : "queued";
  return (
    <div className={styles.row}>
      <FileIcon filename="batch.csv" />
      <div className={styles.info}>
        <div className={styles.filename}>{filename}</div>
        <div className={styles.meta}>
          <span>{formatRelativeDate(batch.started_at ?? null)}</span>
          <span className={styles.metaSep}>·</span>
          <span>
            {batch.n_completed}/{batch.n_chunks} chunks
          </span>
          {batch.merged_counts && (
            <>
              <span className={styles.metaSep}>·</span>
              <span>
                {batch.merged_counts.clean_deliverable.toLocaleString()} clean
              </span>
            </>
          )}
        </div>
      </div>
      <StatusPill status={pillStatus} />
      <Link href={href} className={styles.viewBtn}>
        VIEW →
      </Link>
    </div>
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

// ── Clear confirmation dialog ────────────────────────────────────────────────

function ClearConfirmDialog({
  onCancel,
  onConfirm,
}: {
  onCancel: () => void;
  onConfirm: () => void;
}) {
  // Dismiss on Escape key for keyboard users.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onCancel();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onCancel]);

  return (
    <div
      className={styles.confirmBackdrop}
      onClick={onCancel}
      role="dialog"
      aria-modal="true"
      aria-labelledby="clear-jobs-title"
    >
      <div
        className={styles.confirmCard}
        onClick={(e) => e.stopPropagation()}
      >
        <div className={styles.confirmTitle} id="clear-jobs-title">
          Clear job history from this view?
        </div>
        <div className={styles.confirmBody}>
          This only hides jobs in your browser. No files or results are deleted.
        </div>
        <div className={styles.confirmActions}>
          <button
            type="button"
            className={styles.confirmCancel}
            onClick={onCancel}
          >
            Cancel
          </button>
          <button
            type="button"
            className={styles.confirmOk}
            onClick={onConfirm}
            autoFocus
          >
            Clear
          </button>
        </div>
      </div>
    </div>
  );
}

// ── Toast (transient confirmation) ───────────────────────────────────────────

function ClearToast({ message }: { message: string }) {
  return (
    <div className={styles.toast} role="status" aria-live="polite">
      {message}
    </div>
  );
}
