/**
 * Mock adapter. Simulates the Python backend in-memory so the frontend
 * works end-to-end before the HTTP layer is built.
 *
 * When TRASHPANDA_BACKEND_URL is set, the route handlers bypass this and
 * proxy directly. This file is only used in dev.
 */

import type { JobResult } from "./types";

interface StoredJob {
  result: JobResult;
  /** epoch ms when the job was created */
  createdAt: number;
}

// Module-level map. In Next.js dev, module state persists across requests
// within the same Node process. Not safe for multi-instance production,
// but this adapter is only a dev stub.
const jobs = new Map<string, StoredJob>();

function genId(): string {
  // readable, short, no external deps
  const ts = Date.now().toString(36);
  const rnd = Math.random().toString(36).slice(2, 8);
  return `job_${ts}_${rnd}`;
}

export function createMockJob(filename: string): string {
  const jobId = genId();
  const now = new Date().toISOString();
  jobs.set(jobId, {
    createdAt: Date.now(),
    result: {
      job_id: jobId,
      status: "queued",
      input_filename: filename,
      run_dir: `/runs/${jobId}`,
      started_at: now,
      finished_at: null,
      summary: null,
      artifacts: null,
      error: null,
    },
  });
  return jobId;
}

/**
 * Return the current state, advancing the mock state machine based on age.
 *   0 - 2s   : queued
 *   2 - 7s   : running
 *   > 7s     : completed
 * Rolls a 5% chance of "failed" on first completion read (deterministic per job).
 */
export function getMockJob(jobId: string): JobResult | null {
  const stored = jobs.get(jobId);
  if (!stored) return null;

  const ageMs = Date.now() - stored.createdAt;
  const current = stored.result;

  // Terminal states don't advance.
  if (current.status === "completed" || current.status === "failed") {
    return current;
  }

  if (ageMs < 2000) {
    return { ...current, status: "queued" };
  }
  if (ageMs < 7000) {
    const next: JobResult = { ...current, status: "running" };
    stored.result = next;
    return next;
  }

  // Transition to terminal. Deterministic: hash job id to a fixed outcome.
  const roll = hashString(jobId) % 20;
  if (roll === 0) {
    const failed: JobResult = {
      ...current,
      status: "failed",
      finished_at: new Date().toISOString(),
      error: {
        error_type: "ParseError",
        message:
          "Could not parse uploaded file. Expected CSV or XLSX with at least one email column.",
      },
    };
    stored.result = failed;
    return failed;
  }

  const completed: JobResult = {
    ...current,
    status: "completed",
    finished_at: new Date().toISOString(),
    summary: {
      total_input_rows: 114398,
      total_valid: 75112,
      total_review: 9284,
      total_invalid_or_bounce_risk: 30002,
      duplicates_removed: 7284,
      typo_corrections: 4112,
      disposable_emails: 842,
      placeholder_or_fake_emails: 317,
      role_based_emails: 1903,
    },
    artifacts: {
      run_dir: current.run_dir,
      client_outputs: {
        valid_emails: "valid_emails.xlsx",
        review_emails: "review_emails.xlsx",
        invalid_or_bounce_risk: "invalid_or_bounce_risk.xlsx",
        summary_report: "summary_report.xlsx",
      },
      technical_csvs: {
        raw_normalized: "raw_normalized.csv",
        mx_results: "mx_results.csv",
        dedup_log: "dedup_log.csv",
      },
      reports: {
        pipeline_report: "pipeline_report.json",
      },
    },
  };
  stored.result = completed;
  return completed;
}

function hashString(s: string): number {
  let h = 0;
  for (let i = 0; i < s.length; i++) {
    h = (h * 31 + s.charCodeAt(i)) | 0;
  }
  return Math.abs(h);
}

/**
 * Produce an empty "downloaded" file so the download buttons actually
 * resolve in the mock adapter. Real backend will stream the real xlsx.
 */
export function mockArtifactResponse(
  jobId: string,
  key: string,
): { filename: string; body: string } | null {
  const job = jobs.get(jobId);
  if (!job) return null;
  const filename = job.result.artifacts?.client_outputs?.[key];
  if (!filename) return null;
  const body = `TrashPanda mock artifact\nJob: ${jobId}\nKey: ${key}\nFilename: ${filename}\n\nThis is a dev placeholder. The real backend will stream the .xlsx.`;
  return { filename, body };
}
