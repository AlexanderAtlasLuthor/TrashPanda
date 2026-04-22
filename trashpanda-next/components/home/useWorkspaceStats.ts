"use client";

/**
 * useWorkspaceStats
 * ─────────────────
 * Single source of truth for the Home/Dashboard page. Pulls the recent job
 * list and, for the most-recent completed jobs, their per-job summaries so
 * we can aggregate workspace-wide counters.
 *
 * Design goals:
 *   - Fail soft: every field is optional and falls back to 0 / null.
 *   - No new backend endpoints — reuses existing `/api/jobs`, `/api/jobs/:id`
 *     and (optionally) `/api/jobs/:id/insights`.
 *   - Keep network cost bounded: we only hydrate a small window of jobs.
 */

import { useEffect, useMemo, useRef, useState } from "react";
import {
  getJob,
  getJobInsights,
  getJobList,
} from "@/lib/api";
import type {
  InsightsResponse,
  JobListItem,
  JobResult,
  JobSummary,
} from "@/lib/types";

const MAX_DETAIL_FETCH = 10;
const ONE_WEEK_MS = 7 * 24 * 60 * 60 * 1000;

export interface WorkspaceStats {
  loading: boolean;
  hasAnyJobs: boolean;

  // ── lifetime counters (across the window we hydrate) ────────────────
  totalJobs: number;
  totalCompleted: number;
  totalFailed: number;
  filesProcessed: number;
  totalRecords: number;
  totalReady: number;
  totalReview: number;
  totalInvalid: number;
  avgReadyPct: number | null;

  // ── pipeline counters ───────────────────────────────────────────────
  duplicatesRemoved: number;
  typoCorrections: number;
  disposableEmails: number;
  placeholderEmails: number;
  roleBasedEmails: number;

  // ── week-scoped counters ────────────────────────────────────────────
  jobsThisWeek: number;
  recordsThisWeek: number;
  readyThisWeek: number;
  invalidThisWeek: number;
  avgReadyPctThisWeek: number | null;

  // ── most recent job shortcuts ───────────────────────────────────────
  latestJob: JobListItem | null;
  latestCompletedJob: JobListItem | null;
  latestCompletedSummary: JobSummary | null;
  latestInsights: InsightsResponse | null;

  // ── domain intelligence (latest completed job, optional) ────────────
  totalDomainsAnalyzed: number;
  catchAllDetectedCount: number;
  smtpTestedCount: number;

  // ── rollup list for display ─────────────────────────────────────────
  recentCompletedSummaries: Array<{ job: JobListItem; summary: JobSummary }>;
}

function n(x: number | null | undefined): number {
  return typeof x === "number" && Number.isFinite(x) ? x : 0;
}

function isWithinLastWeek(iso: string | null | undefined): boolean {
  if (!iso) return false;
  const t = new Date(iso).getTime();
  if (Number.isNaN(t)) return false;
  return Date.now() - t <= ONE_WEEK_MS;
}

const EMPTY_STATS: WorkspaceStats = {
  loading: true,
  hasAnyJobs: false,
  totalJobs: 0,
  totalCompleted: 0,
  totalFailed: 0,
  filesProcessed: 0,
  totalRecords: 0,
  totalReady: 0,
  totalReview: 0,
  totalInvalid: 0,
  avgReadyPct: null,
  duplicatesRemoved: 0,
  typoCorrections: 0,
  disposableEmails: 0,
  placeholderEmails: 0,
  roleBasedEmails: 0,
  jobsThisWeek: 0,
  recordsThisWeek: 0,
  readyThisWeek: 0,
  invalidThisWeek: 0,
  avgReadyPctThisWeek: null,
  latestJob: null,
  latestCompletedJob: null,
  latestCompletedSummary: null,
  latestInsights: null,
  totalDomainsAnalyzed: 0,
  catchAllDetectedCount: 0,
  smtpTestedCount: 0,
  recentCompletedSummaries: [],
};

export function useWorkspaceStats(hiddenIds?: Set<string>): WorkspaceStats {
  const [stats, setStats] = useState<WorkspaceStats>(EMPTY_STATS);
  const ranRef = useRef(false);

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        const list = await getJobList(50);
        if (cancelled) return;

        // Honor the user's "Clear recent jobs" view preference if provided.
        const visible = hiddenIds && hiddenIds.size > 0
          ? list.jobs.filter((j) => !hiddenIds.has(j.job_id))
          : list.jobs;

        const completed = visible.filter((j) => j.status === "completed");
        const failed   = visible.filter((j) => j.status === "failed");
        const latestJob = visible[0] ?? null;
        const latestCompletedJob = completed[0] ?? null;

        // Hydrate summaries for the N most-recent completed jobs.
        const window_ = completed.slice(0, MAX_DETAIL_FETCH);
        const settled = await Promise.allSettled(
          window_.map((j) => getJob(j.job_id)),
        );
        if (cancelled) return;

        const hydrated: Array<{ job: JobListItem; result: JobResult }> = [];
        settled.forEach((r, i) => {
          if (r.status === "fulfilled") {
            hydrated.push({ job: window_[i], result: r.value });
          }
        });

        const recentCompletedSummaries: Array<{
          job: JobListItem;
          summary: JobSummary;
        }> = hydrated
          .filter((h) => h.result.summary)
          .map((h) => ({ job: h.job, summary: h.result.summary! }));

        // Aggregate lifetime (windowed) counters.
        let totalRecords = 0;
        let totalReady = 0;
        let totalReview = 0;
        let totalInvalid = 0;
        let duplicatesRemoved = 0;
        let typoCorrections = 0;
        let disposableEmails = 0;
        let placeholderEmails = 0;
        let roleBasedEmails = 0;

        // Week-scoped counters.
        let jobsThisWeek = 0;
        let recordsThisWeek = 0;
        let readyThisWeek = 0;
        let invalidThisWeek = 0;

        for (const { job, summary } of recentCompletedSummaries) {
          const records = n(summary.total_input_rows);
          const ready = n(summary.total_valid);
          const review = n(summary.total_review);
          const invalid = n(summary.total_invalid_or_bounce_risk);

          totalRecords += records;
          totalReady += ready;
          totalReview += review;
          totalInvalid += invalid;

          duplicatesRemoved += n(summary.duplicates_removed);
          typoCorrections += n(summary.typo_corrections);
          disposableEmails += n(summary.disposable_emails);
          placeholderEmails += n(summary.placeholder_or_fake_emails);
          roleBasedEmails += n(summary.role_based_emails);

          if (isWithinLastWeek(job.started_at)) {
            jobsThisWeek += 1;
            recordsThisWeek += records;
            readyThisWeek += ready;
            invalidThisWeek += invalid;
          }
        }

        // Count jobs this week across *all* visible jobs, not just hydrated.
        jobsThisWeek = visible.filter((j) => isWithinLastWeek(j.started_at)).length;

        const avgReadyPct =
          totalRecords > 0 ? (totalReady / totalRecords) * 100 : null;
        const avgReadyPctThisWeek =
          recordsThisWeek > 0 ? (readyThisWeek / recordsThisWeek) * 100 : null;

        const latestCompletedSummary =
          recentCompletedSummaries[0]?.summary ?? null;

        // Best-effort V2 insights for the latest completed job — optional.
        let latestInsights: InsightsResponse | null = null;
        if (latestCompletedJob) {
          try {
            const ins = await getJobInsights(latestCompletedJob.job_id);
            if (!cancelled && ins.v2_available) {
              latestInsights = ins;
            }
          } catch {
            // insights not available — hide advanced panels
          }
        }

        const totalDomainsAnalyzed = latestInsights
          ? Object.values(latestInsights.domain_intelligence).reduce(
              (s, l) => s + l.length,
              0,
            )
          : 0;

        if (cancelled) return;
        setStats({
          loading: false,
          hasAnyJobs: visible.length > 0,
          totalJobs: visible.length,
          totalCompleted: completed.length,
          totalFailed: failed.length,
          filesProcessed: completed.length,
          totalRecords,
          totalReady,
          totalReview,
          totalInvalid,
          avgReadyPct,
          duplicatesRemoved,
          typoCorrections,
          disposableEmails,
          placeholderEmails,
          roleBasedEmails,
          jobsThisWeek,
          recordsThisWeek,
          readyThisWeek,
          invalidThisWeek,
          avgReadyPctThisWeek,
          latestJob,
          latestCompletedJob,
          latestCompletedSummary,
          latestInsights,
          totalDomainsAnalyzed,
          catchAllDetectedCount: latestInsights?.catch_all_count ?? 0,
          smtpTestedCount: latestInsights?.smtp_tested_count ?? 0,
          recentCompletedSummaries,
        });
      } catch {
        if (!cancelled) {
          setStats((prev) => ({ ...prev, loading: false }));
        }
      }
    }

    if (!ranRef.current) {
      ranRef.current = true;
      load();
    }

    return () => {
      cancelled = true;
    };
    // We intentionally only run once per mount; hiddenIds changes are rare
    // and the dashboard is not expected to live-refresh aggregates.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return useMemo(() => stats, [stats]);
}
