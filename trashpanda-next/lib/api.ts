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

import type {
  JobCancelResponse,
  JobList,
  JobLogs,
  JobProgress,
  JobResult,
  ReviewDecision,
  ReviewDecisions,
  ReviewQueue,
  TypoCorrections,
  InsightsResponse,
  PreflightResult,
  SmtpRuntimeSummary,
  ArtifactConsistencyResult,
  ClientPackageManifest,
  OperatorReviewSummary,
  FeedbackIngestionSummary,
  FeedbackPreviewResult,
} from "./types";

export interface UploadResponse {
  job_id: string;
}

/** Wire-shape of GET /api/system/info. */
export interface SystemInfo {
  backend_label: string;
  deployment: string;
  auth_enabled: boolean;
  wall_clock_seconds: number;
  smtp_default_dry_run: boolean;
  adapter_mode: "proxy" | "mock";
  backend_url: string | null;
  operator_token_configured: boolean;
}

/**
 * Fetch deployment metadata about the backend the BFF is talking to.
 * Used by the Topbar badge to surface "VPS via tunnel" vs "local mock".
 */
export async function getSystemInfo(): Promise<SystemInfo> {
  const res = await fetch("/api/system/info", { cache: "no-store" });
  return handleResponse<SystemInfo>(res);
}

export interface UploadFileOptions {
  config_path?: string;
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
export async function uploadFile(
  file: File,
  options?: UploadFileOptions,
): Promise<UploadResponse> {
  const form = new FormData();
  form.append("file", file);
  const configPath = options?.config_path?.trim();
  if (configPath) {
    form.append("config_path", configPath);
  }

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
 * Live progress for a running job (probe counters + cancel state).
 *
 * Backend contract:
 *   GET /api/jobs/:jobId/progress -> JobProgress
 */
export async function getJobProgress(jobId: string): Promise<JobProgress> {
  const res = await fetch(
    `/api/jobs/${encodeURIComponent(jobId)}/progress`,
    { cache: "no-store" },
  );
  return handleResponse<JobProgress>(res);
}

/**
 * Cooperatively cancel a running job. The SMTP probe loop respects the
 * flag and unwinds within seconds; structural pipeline stages may
 * complete the current chunk before exiting.
 */
export async function cancelJob(jobId: string): Promise<JobCancelResponse> {
  const res = await fetch(
    `/api/jobs/${encodeURIComponent(jobId)}/cancel`,
    { method: "POST" },
  );
  return handleResponse<JobCancelResponse>(res);
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

export async function getReviewDecisions(jobId: string): Promise<ReviewDecisions> {
  const res = await fetch(
    `/api/jobs/${encodeURIComponent(jobId)}/review/decisions`,
    { cache: "no-store" },
  );
  return handleResponse<ReviewDecisions>(res);
}

export async function saveReviewDecisions(
  jobId: string,
  decisions: Record<string, ReviewDecision>,
): Promise<{ job_id: string; saved: number }> {
  const res = await fetch(
    `/api/jobs/${encodeURIComponent(jobId)}/review/decisions`,
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ decisions }),
    },
  );
  return handleResponse<{ job_id: string; saved: number }>(res);
}

export function reviewExportUrl(jobId: string): string {
  return `/api/jobs/${encodeURIComponent(jobId)}/review/export`;
}

export async function getTypoCorrections(jobId: string): Promise<TypoCorrections> {
  const res = await fetch(
    `/api/jobs/${encodeURIComponent(jobId)}/typo-corrections`,
    { cache: "no-store" },
  );
  return handleResponse<TypoCorrections>(res);
}

/**
 * Fetch V2 Deliverability Intelligence aggregate + per-row feed.
 * Safe to call on legacy runs: response.v2_available will be false and the UI
 * should render an empty-state panel.
 */
export async function getJobInsights(jobId: string): Promise<InsightsResponse> {
  const res = await fetch(
    `/api/jobs/${encodeURIComponent(jobId)}/insights`,
    { cache: "no-store" },
  );
  return handleResponse<InsightsResponse>(res);
}

// ── AI endpoints (Claude Haiku 4.5 via FastAPI backend) ─────────────────────

export interface AIReviewSuggestion {
  id: string;
  decision: "approve" | "reject" | "uncertain";
  confidence: number;
  reasoning: string;
}

export interface AIReviewResult {
  job_id: string;
  total: number;
  suggestions: AIReviewSuggestion[];
}

export interface AISummaryResult {
  job_id: string;
  narrative: string;
}

/** Ask Claude to stack-rank the review queue. */
export async function getAIReviewSuggestions(
  jobId: string,
): Promise<AIReviewResult> {
  const res = await fetch(
    `/api/jobs/${encodeURIComponent(jobId)}/ai-review`,
    { method: "POST", cache: "no-store" },
  );
  return handleResponse<AIReviewResult>(res);
}

/** One-paragraph plain-English summary of a completed job. */
export async function getAIJobSummary(
  jobId: string,
): Promise<AISummaryResult> {
  const res = await fetch(
    `/api/jobs/${encodeURIComponent(jobId)}/ai-summary`,
    { method: "POST", cache: "no-store" },
  );
  return handleResponse<AISummaryResult>(res);
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

// ── V2.10 Operator API wrappers ───────────────────────────────────────────
//
// Client-side fetch wrappers for the /api/operator/* surface. Every
// wrapper hits a Next.js BFF route under /api/operator/... — those
// route handlers are added in V2.10.1.3 and proxy to the Python backend.
//
// Delivery decisions live in the backend's V2.9.7 operator review
// gate. These wrappers MUST NOT compute readiness; they only carry
// payloads.

export interface RunPreflightInput {
  input_path: string;
  output_dir?: string;
  config_path?: string;
  operator_confirmed_large_run?: boolean;
  smtp_port_verified?: boolean;
}

export interface IngestFeedbackInput {
  feedback_csv_path: string;
  config_path?: string;
}

export interface FeedbackPreviewInput {
  feedback_store_path?: string;
  config_path?: string;
  output_dir?: string;
}

export async function runPreflight(
  input: RunPreflightInput,
): Promise<PreflightResult> {
  const res = await fetch("/api/operator/preflight", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(input),
  });
  return handleResponse<PreflightResult>(res);
}

export async function getOperatorJob(jobId: string): Promise<JobResult> {
  const res = await fetch(
    `/api/operator/jobs/${encodeURIComponent(jobId)}`,
    { cache: "no-store" },
  );
  return handleResponse<JobResult>(res);
}

export async function getSmtpRuntime(
  jobId: string,
): Promise<SmtpRuntimeSummary> {
  const res = await fetch(
    `/api/operator/jobs/${encodeURIComponent(jobId)}/smtp-runtime`,
    { cache: "no-store" },
  );
  return handleResponse<SmtpRuntimeSummary>(res);
}

export async function getArtifactConsistency(
  jobId: string,
): Promise<ArtifactConsistencyResult> {
  const res = await fetch(
    `/api/operator/jobs/${encodeURIComponent(jobId)}/artifact-consistency`,
    { cache: "no-store" },
  );
  return handleResponse<ArtifactConsistencyResult>(res);
}

export async function buildClientPackage(
  jobId: string,
): Promise<ClientPackageManifest> {
  const res = await fetch(
    `/api/operator/jobs/${encodeURIComponent(jobId)}/client-package`,
    { method: "POST" },
  );
  return handleResponse<ClientPackageManifest>(res);
}

export async function getClientPackageManifest(
  jobId: string,
): Promise<ClientPackageManifest> {
  const res = await fetch(
    `/api/operator/jobs/${encodeURIComponent(jobId)}/client-package`,
    { cache: "no-store" },
  );
  return handleResponse<ClientPackageManifest>(res);
}

export async function runOperatorReviewGate(
  jobId: string,
): Promise<OperatorReviewSummary> {
  const res = await fetch(
    `/api/operator/jobs/${encodeURIComponent(jobId)}/review-gate`,
    { method: "POST" },
  );
  return handleResponse<OperatorReviewSummary>(res);
}

export async function getOperatorReviewSummary(
  jobId: string,
): Promise<OperatorReviewSummary> {
  const res = await fetch(
    `/api/operator/jobs/${encodeURIComponent(jobId)}/operator-review`,
    { cache: "no-store" },
  );
  return handleResponse<OperatorReviewSummary>(res);
}

/**
 * Fetches the only safe client-delivery ZIP endpoint.
 *
 * Returns the raw Response because the endpoint can return either:
 * - 200 application/zip with Content-Disposition
 * - 409 JSON ClientPackageDownloadError explaining why delivery is blocked
 *
 * Callers must branch on response.ok / response.status before reading the body.
 * Always read Content-Disposition from the response; do not reconstruct filenames.
 */
export async function downloadClientPackage(
  jobId: string,
): Promise<Response> {
  return fetch(
    `/api/operator/jobs/${encodeURIComponent(jobId)}/client-package/download`,
    { cache: "no-store" },
  );
}

// ── "Send to client" — one-click bundle ─────────────────────────────────
//
// Wire-shape returned by GET /api/operator/jobs/{id}/client-bundle/summary.
// Used by the SendToClientButton to render counts + state before the
// operator clicks the giant download button.
//
// V2.10.10 — `review_breakdown` carries the per-decision_reason
// counts emitted by the package builder so the UI can show *why*
// each review row was held back instead of presenting a single
// "X need review" lump. Missing keys are treated as "no rows of
// this kind" (the operator may genuinely have zero catch-all rows,
// for example).
//
// `smtp_runtime` is the customer-safe subset of the SMTP runtime
// summary (coverage + valid/inconclusive split). Absent when SMTP
// did not run or the file is unreadable.
export interface ReviewBreakdown {
  review_cold_start_b2b?: number | null;
  review_smtp_inconclusive?: number | null;
  review_catch_all?: number | null;
  review_medium_probability?: number | null;
  review_domain_high_risk?: number | null;
}

export interface SmtpRuntimePublic {
  smtp_enabled?: boolean;
  smtp_dry_run?: boolean;
  smtp_candidates_seen?: number;
  smtp_candidates_attempted?: number;
  smtp_valid_count?: number;
  smtp_inconclusive_count?: number;
}

export interface ClientBundleSummary {
  available: boolean;
  ready_for_client: boolean;
  delivery_mode: "full" | "safe_only_partial";
  primary_filename: string | null;
  download_filename: string | null;
  safe_count: number;
  review_count: number;
  rejected_count: number;
  review_breakdown?: ReviewBreakdown;
  smtp_runtime?: SmtpRuntimePublic | null;
  issues: Array<{ severity: string; code: string; message: string }>;
}

export async function getClientBundleSummary(
  jobId: string,
): Promise<ClientBundleSummary> {
  const res = await fetch(
    `/api/operator/jobs/${encodeURIComponent(jobId)}/client-bundle/summary`,
    { cache: "no-store" },
  );
  return handleResponse<ClientBundleSummary>(res);
}

/** URL the giant "Send to client" download button targets. */
export function clientBundleDownloadUrl(jobId: string): string {
  return `/api/operator/jobs/${encodeURIComponent(jobId)}/client-bundle/download`;
}

/**
 * URL for the Extra Strict Offline ZIP — the "re-clean stricter"
 * action surfaced as a secondary button next to the primary
 * SendToClientButton. Runs in-process on the finished run dir.
 */
export function extraStrictDownloadUrl(jobId: string): string {
  return `/api/operator/jobs/${encodeURIComponent(jobId)}/extra-strict/download`;
}

/**
 * Fetches the *partial* safe-only client-delivery ZIP endpoint.
 *
 * V2.10.8.3 contract: this is a separate channel, not a fallback for
 * ``downloadClientPackage``. It is only valid when the run reports
 * ``ready_for_client=false`` AND ``ready_for_client_partial=true`` AND
 * ``partial_delivery_mode=safe_only`` AND the operator has explicitly
 * confirmed the override. The two endpoints are mutually exclusive:
 * a full-ready run uses the standard endpoint, and the safe-only
 * endpoint will reject ``ready_for_client=true`` runs with
 * ``safe_only_not_required``.
 *
 * Returns the raw Response. The endpoint can return either:
 * - 200 application/zip with Content-Disposition (and the V2.10.8.3
 *   X-TrashPanda-Delivery-Mode / -Ready-For-Client / -Ready-For-Client-Partial
 *   advisory headers)
 * - 409 application/json SafeOnlyDownloadError explaining the gate.
 *
 * Callers must branch on response.ok / response.status before reading
 * the body. Always read Content-Disposition from the response; do not
 * reconstruct filenames.
 *
 * Override semantics:
 * - When ``overrideConfirmed=false`` no network request is made; a
 *   synthetic 409 Response is returned so the UI can render the
 *   "operator must confirm" branch without a round-trip. This means
 *   accidental clicks on a not-yet-confirmed control cannot leak any
 *   bytes, even client-safe ones.
 * - When ``overrideConfirmed=true`` the wire request carries the
 *   exact ``X-TrashPanda-Operator-Override: safe-only`` header the
 *   backend requires. No other override values are sent.
 */
export async function downloadSafeOnlyClientPackage(
  jobId: string,
  overrideConfirmed: boolean,
): Promise<Response> {
  if (!overrideConfirmed) {
    return new Response(
      JSON.stringify({
        error: "safe_only_override_not_confirmed",
        message: "Operator must confirm the safe-only delivery override.",
        ready_for_client: false,
        ready_for_client_partial: false,
      }),
      {
        status: 409,
        headers: { "content-type": "application/json" },
      },
    );
  }

  return fetch(
    `/api/operator/jobs/${encodeURIComponent(jobId)}/client-package/download-safe-only`,
    {
      cache: "no-store",
      headers: {
        "X-TrashPanda-Operator-Override": "safe-only",
      },
    },
  );
}

export async function ingestFeedback(
  input: IngestFeedbackInput,
): Promise<FeedbackIngestionSummary> {
  const res = await fetch("/api/operator/feedback/ingest", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(input),
  });
  return handleResponse<FeedbackIngestionSummary>(res);
}

export async function getFeedbackPreview(
  input?: FeedbackPreviewInput,
): Promise<FeedbackPreviewResult> {
  if (input === undefined) {
    const res = await fetch("/api/operator/feedback/preview", {
      cache: "no-store",
    });
    return handleResponse<FeedbackPreviewResult>(res);
  }
  const res = await fetch("/api/operator/feedback/preview", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(input),
  });
  return handleResponse<FeedbackPreviewResult>(res);
}
