/**
 * API client. Single entry point for all frontend -> backend calls.
 *
 * Architecture:
 *   - Frontend components ONLY import from this file.
 *   - Internally we route through Next.js route handlers under /api/jobs/*.
 *   - Those route handlers decide (at runtime) whether to:
 *       (a) serve a mock response (local dev, no Python backend yet)
 *       (b) proxy to the real Python backend at TRASHPANDA_BACKEND_URL
 *
 * When the Python HTTP service comes online (FastAPI recommended), flip
 * NEXT_PUBLIC_TRASHPANDA_ADAPTER=proxy and set TRASHPANDA_BACKEND_URL.
 * No UI code needs to change.
 */

import type { JobList, JobLogs, JobResult, ReviewQueue } from "./types";

export interface UploadResponse {
  job_id: string;
}

export class ApiError extends Error {
  constructor(
    message: string,
    public status: number,
    public body?: unknown,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

async function handleResponse<T>(res: Response): Promise<T> {
  if (!res.ok) {
    let body: unknown = null;
    try {
      body = await res.json();
    } catch {
      /* ignore */
    }
    const message =
      (body as { message?: string })?.message ??
      `Request failed with ${res.status}`;
    throw new ApiError(message, res.status, body);
  }
  return res.json() as Promise<T>;
}

/**
 * Upload a file and start a cleaning job.
 *
 * Backend contract:
 *   POST /api/jobs (multipart/form-data, field name "file")
 *   -> { job_id: string }
 *
 * When the Python service is wired up, this route proxies to something like:
 *   POST {TRASHPANDA_BACKEND_URL}/jobs
 */
export async function uploadFile(file: File): Promise<UploadResponse> {
  const form = new FormData();
  form.append("file", file);

  const res = await fetch("/api/jobs", {
    method: "POST",
    body: form,
  });
  return handleResponse<UploadResponse>(res);
}

/**
 * Fetch current state of a job. Used for polling.
 *
 * Backend contract:
 *   GET /api/jobs/:jobId -> JobResult
 */
export async function getJob(jobId: string): Promise<JobResult> {
  const res = await fetch(`/api/jobs/${encodeURIComponent(jobId)}`, {
    cache: "no-store",
  });
  return handleResponse<JobResult>(res);
}

/**
 * Fetch the last N log lines for a job. Non-fatal: callers should catch.
 *
 * Backend contract:
 *   GET /api/jobs/:jobId/logs?limit=N -> JobLogs
 */
export async function getJobLogs(jobId: string, limit = 20): Promise<JobLogs> {
  const res = await fetch(
    `/api/jobs/${encodeURIComponent(jobId)}/logs?limit=${limit}`,
    { cache: "no-store" },
  );
  return handleResponse<JobLogs>(res);
}

/**
 * Fetch the list of recent jobs (most-recent first).
 *
 * Backend contract:
 *   GET /api/jobs?limit=N -> JobList
 */
export async function getJobList(limit = 20): Promise<JobList> {
  const res = await fetch(`/api/jobs?limit=${limit}`, { cache: "no-store" });
  return handleResponse<JobList>(res);
}

/**
 * Resolve an artifact key to a URL the browser can download.
 * The route handler decides whether to stream from local disk, signed URL,
 * or proxy from the Python service.
 */
export function artifactDownloadUrl(jobId: string, key: string): string {
  return `/api/jobs/${encodeURIComponent(jobId)}/artifacts/${encodeURIComponent(key)}`;
}

export async function getReviewEmails(jobId: string): Promise<ReviewQueue> {
  const res = await fetch(
    `/api/jobs/${encodeURIComponent(jobId)}/review`,
    { cache: "no-store" },
  );
  return handleResponse<ReviewQueue>(res);
}

/** URL that streams a ZIP of all artifacts for a completed job. */
export function artifactZipUrl(jobId: string): string {
  return `/api/jobs/${encodeURIComponent(jobId)}/artifacts/zip`;
}

/**
 * Build the client-side fallback ZIP filename. Mirrors the backend rule in
 * `app/server.py::_build_zip_filename` so the <a download> attribute matches
 * the Content-Disposition the server sends back.
 *
 * Format: `<clean_stem>_trashpanda_results_<YYYY-MM-DD_HH-MM>.zip`
 * Fallback when no input filename: `trashpanda_results_<timestamp>.zip`
 */
export function buildZipFilename(
  inputFilename: string | null | undefined,
  now: Date = new Date(),
): string {
  const pad = (n: number) => String(n).padStart(2, "0");
  const ts =
    `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())}` +
    `_${pad(now.getHours())}-${pad(now.getMinutes())}`;

  let stem = "";
  if (inputFilename) {
    const base = inputFilename.split(/[\\/]/).pop() ?? "";
    const dot = base.lastIndexOf(".");
    const rawStem = dot > 0 ? base.slice(0, dot) : base;
    stem = rawStem
      .toLowerCase()
      .replace(/\s+/g, "_")
      .replace(/[^a-z0-9_-]+/g, "")
      .replace(/_+/g, "_")
      .replace(/^[_-]+|[_-]+$/g, "");
  }

  return stem
    ? `${stem}_trashpanda_results_${ts}.zip`
    : `trashpanda_results_${ts}.zip`;
}
