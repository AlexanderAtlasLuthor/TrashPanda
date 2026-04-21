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

import type { JobResult } from "./types";

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
 * Resolve an artifact key to a URL the browser can download.
 * The route handler decides whether to stream from local disk, signed URL,
 * or proxy from the Python service.
 */
export function artifactDownloadUrl(jobId: string, key: string): string {
  return `/api/jobs/${encodeURIComponent(jobId)}/artifacts/${encodeURIComponent(key)}`;
}
