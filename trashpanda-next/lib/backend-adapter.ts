/**
 * Server-side adapter switch. Route handlers call these functions;
 * they either hit the mock in-memory store or proxy to the real Python HTTP
 * service depending on TRASHPANDA_BACKEND_URL.
 *
 * Keep this file server-only (used only from route handlers under app/api).
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
import type {
  RunPreflightInput,
  IngestFeedbackInput,
  FeedbackPreviewInput,
  PilotFinalizeResult,
  PilotLaunchResult,
  PilotPollBouncesResult,
  PilotPreviewResult,
  PilotSendConfigInput,
  PilotSendStatus,
  RetryQueueDrainResult,
  RetryQueueFinalizeResult,
  RetryQueueStatus,
} from "./api";
import {
  createMockJob,
  getMockJob,
  getMockJobList,
  getMockJobLogs,
  getMockReviewEmails,
  mockArtifactResponse,
} from "./mock-adapter";

const backendUrl = process.env.TRASHPANDA_BACKEND_URL?.replace(/\/$/, "");
const useProxy = Boolean(backendUrl);

// Operator bearer token. When set, every /api/operator/* proxy request
// carries it as ``Authorization: Bearer <token>``. The backend's
// ``app.auth.require_operator_token`` dependency rejects requests
// without a matching token. Leave unset for local dev where the
// backend has no auth configured.
const operatorToken = (process.env.TRASHPANDA_OPERATOR_TOKEN || "").trim();
const defaultJobConfigPath = (
  process.env.TRASHPANDA_DEFAULT_JOB_CONFIG_PATH || ""
).trim();

function operatorAuthHeaders(): Record<string, string> {
  return operatorToken
    ? { Authorization: `Bearer ${operatorToken}` }
    : {};
}

/**
 * The Python backend serialises Path objects to absolute strings.
 * Extract the basename so the UI shows "valid_emails.xlsx" not "/runs/.../valid_emails.xlsx".
 */
function basename(p: string | null | undefined): string | null {
  if (!p) return p ?? null;
  return p.split(/[/\\]/).pop() ?? p;
}

function normalizeArtifactPaths(result: JobResult): JobResult {
  if (!result.artifacts) return result;
  const { client_outputs, technical_csvs, reports } = result.artifacts;
  return {
    ...result,
    artifacts: {
      ...result.artifacts,
      client_outputs: client_outputs
        ? Object.fromEntries(Object.entries(client_outputs).map(([k, v]) => [k, basename(v)]))
        : client_outputs,
      technical_csvs: technical_csvs
        ? Object.fromEntries(Object.entries(technical_csvs).map(([k, v]) => [k, basename(v)]))
        : technical_csvs,
      reports: reports
        ? Object.fromEntries(Object.entries(reports).map(([k, v]) => [k, basename(v)]))
        : reports,
    },
  };
}

export interface AdapterStartJobOptions {
  config_path?: string;
}

/** POST file (and optional config_path), return job id. */
export async function adapterStartJob(
  file: File,
  options?: AdapterStartJobOptions,
): Promise<{ job_id: string }> {
  const effectiveConfigPath =
    options?.config_path?.trim() || defaultJobConfigPath || undefined;

  if (useProxy) {
    const form = new FormData();
    form.append("file", file);
    if (effectiveConfigPath) {
      form.append("config_path", effectiveConfigPath);
    }
    const res = await fetch(`${backendUrl}/jobs`, {
      method: "POST",
      body: form,
    });
    if (!res.ok) {
      throw new Error(`Backend rejected upload (${res.status})`);
    }
    return (await res.json()) as { job_id: string };
  }
  const jobId = createMockJob(file.name);
  return { job_id: jobId };
}

export async function adapterGetJob(jobId: string): Promise<JobResult | null> {
  if (useProxy) {
    const res = await fetch(
      `${backendUrl}/jobs/${encodeURIComponent(jobId)}`,
      { cache: "no-store" },
    );
    if (res.status === 404) return null;
    if (!res.ok) throw new Error(`Backend error (${res.status})`);
    const result = (await res.json()) as JobResult;
    return normalizeArtifactPaths(result);
  }
  return getMockJob(jobId);
}

/** Live progress (status + SMTP probe counters). */
export async function adapterGetJobProgress(
  jobId: string,
): Promise<JobProgress | null> {
  if (useProxy) {
    const res = await fetch(
      `${backendUrl}/jobs/${encodeURIComponent(jobId)}/progress`,
      { cache: "no-store" },
    );
    if (res.status === 404) return null;
    if (!res.ok) throw new Error(`Backend error (${res.status})`);
    return (await res.json()) as JobProgress;
  }
  // Mock mode: synthesise a minimal payload from the in-memory job.
  const job = getMockJob(jobId);
  if (!job) return null;
  return {
    job_id: jobId,
    status: job.status,
    cancelled: false,
    started_at: job.started_at ?? null,
    finished_at: job.finished_at ?? null,
    smtp: null,
  };
}

/** Cooperatively cancel a running job. */
export async function adapterCancelJob(
  jobId: string,
): Promise<JobCancelResponse | null> {
  if (useProxy) {
    const res = await fetch(
      `${backendUrl}/jobs/${encodeURIComponent(jobId)}/cancel`,
      { method: "POST" },
    );
    if (res.status === 404) return null;
    if (!res.ok) throw new Error(`Backend error (${res.status})`);
    return (await res.json()) as JobCancelResponse;
  }
  // Mock mode: pretend the cancellation flag was flipped so the UI
  // exercise the disabled-cancel-button transition without a backend.
  const job = getMockJob(jobId);
  if (!job) return null;
  return {
    job_id: jobId,
    status: job.status,
    cancelled: job.status !== "completed" && job.status !== "failed",
    reason: "mock cancellation",
  };
}

/**
 * Return a Response for artifact download. In proxy mode, streams from the
 * backend. In mock mode, returns a plaintext placeholder.
 */
export async function adapterGetArtifact(
  jobId: string,
  key: string,
): Promise<Response> {
  if (useProxy) {
    const res = await fetch(
      `${backendUrl}/jobs/${encodeURIComponent(jobId)}/artifacts/${encodeURIComponent(key)}`,
    );
    return res;
  }

  const artifact = mockArtifactResponse(jobId, key);
  if (!artifact) {
    return new Response(
      JSON.stringify({ message: "Artifact not found" }),
      { status: 404, headers: { "content-type": "application/json" } },
    );
  }
  return new Response(artifact.body, {
    status: 200,
    headers: {
      "content-type": "text/plain; charset=utf-8",
      "content-disposition": `attachment; filename="${artifact.filename}"`,
    },
  });
}

export async function adapterGetJobLogs(
  jobId: string,
  limit: number,
): Promise<JobLogs> {
  if (useProxy) {
    const res = await fetch(
      `${backendUrl}/jobs/${encodeURIComponent(jobId)}/logs?limit=${limit}`,
      { cache: "no-store" },
    );
    if (res.status === 404) return { job_id: jobId, lines: [] };
    if (!res.ok) throw new Error(`Backend error (${res.status})`);
    return (await res.json()) as JobLogs;
  }
  return getMockJobLogs(jobId, limit);
}

export async function adapterGetArtifactZip(jobId: string): Promise<Response> {
  if (useProxy) {
    return fetch(
      `${backendUrl}/jobs/${encodeURIComponent(jobId)}/artifacts/zip`,
    );
  }
  return new Response(
    JSON.stringify({ message: "ZIP download requires the Python backend." }),
    { status: 501, headers: { "content-type": "application/json" } },
  );
}

// ---------------------------------------------------------------------------
// V2.10.18 — auto-chunked batch jobs.
// All adapters require the Python backend (no mock — the orchestrator runs
// in a worker thread on the backend; there's nothing meaningful to mock).
// ---------------------------------------------------------------------------

const BATCH_BACKEND_REQUIRED =
  "Batch jobs require the Python backend. Set TRASHPANDA_BACKEND_URL.";

export interface AdapterBatchUploadOptions {
  chunk_size?: number;
  threshold_rows?: number;
  allow_partial?: boolean;
  cleanup?: boolean;
}

export async function adapterUploadBatch(
  file: File,
  options?: AdapterBatchUploadOptions,
): Promise<import("./types").BatchUploadResponse> {
  if (!useProxy) {
    throw new Error(BATCH_BACKEND_REQUIRED);
  }
  const form = new FormData();
  form.append("file", file);
  if (options?.chunk_size !== undefined) {
    form.append("chunk_size", String(options.chunk_size));
  }
  if (options?.threshold_rows !== undefined) {
    form.append("threshold_rows", String(options.threshold_rows));
  }
  if (options?.allow_partial !== undefined) {
    form.append("allow_partial", String(options.allow_partial));
  }
  if (options?.cleanup !== undefined) {
    form.append("cleanup", String(options.cleanup));
  }
  const res = await fetch(`${backendUrl}/batches/upload`, {
    method: "POST",
    body: form,
  });
  if (!res.ok) {
    throw new Error(`Backend rejected batch upload (${res.status})`);
  }
  return (await res.json()) as import("./types").BatchUploadResponse;
}

export async function adapterGetBatchProgress(
  batchId: string,
): Promise<import("./types").BatchProgress | null> {
  if (!useProxy) {
    throw new Error(BATCH_BACKEND_REQUIRED);
  }
  const res = await fetch(
    `${backendUrl}/batches/${encodeURIComponent(batchId)}/progress`,
    { cache: "no-store" },
  );
  if (res.status === 404) return null;
  if (!res.ok) throw new Error(`Backend error (${res.status})`);
  return (await res.json()) as import("./types").BatchProgress;
}

export async function adapterGetBatchStatusDoc(
  batchId: string,
): Promise<import("./types").BatchStatusDoc | null> {
  if (!useProxy) {
    throw new Error(BATCH_BACKEND_REQUIRED);
  }
  const res = await fetch(
    `${backendUrl}/batches/${encodeURIComponent(batchId)}`,
    { cache: "no-store" },
  );
  if (res.status === 404) return null;
  if (!res.ok) throw new Error(`Backend error (${res.status})`);
  return (await res.json()) as import("./types").BatchStatusDoc;
}

export async function adapterListBatches(): Promise<
  import("./types").BatchList
> {
  if (!useProxy) {
    throw new Error(BATCH_BACKEND_REQUIRED);
  }
  const res = await fetch(`${backendUrl}/batches`, { cache: "no-store" });
  if (!res.ok) throw new Error(`Backend error (${res.status})`);
  return (await res.json()) as import("./types").BatchList;
}

export async function adapterDownloadBatchBundle(
  batchId: string,
): Promise<Response> {
  if (!useProxy) {
    return new Response(
      JSON.stringify({ message: BATCH_BACKEND_REQUIRED }),
      { status: 501, headers: { "content-type": "application/json" } },
    );
  }
  return fetch(
    `${backendUrl}/batches/${encodeURIComponent(batchId)}/customer-bundle/download`,
  );
}

export async function adapterGetJobList(limit: number): Promise<JobList> {
  if (useProxy) {
    const res = await fetch(`${backendUrl}/jobs?limit=${limit}`, {
      cache: "no-store",
    });
    if (!res.ok) throw new Error(`Backend error (${res.status})`);
    return (await res.json()) as JobList;
  }
  return getMockJobList(limit);
}

export async function adapterGetReviewEmails(jobId: string): Promise<ReviewQueue> {
  if (useProxy) {
    const res = await fetch(
      `${backendUrl}/jobs/${encodeURIComponent(jobId)}/review`,
      { cache: "no-store" },
    );
    if (res.status === 404) return { job_id: jobId, total: 0, emails: [] };
    if (!res.ok) throw new Error(`Backend error (${res.status})`);
    return (await res.json()) as ReviewQueue;
  }
  return getMockReviewEmails(jobId);
}

export async function adapterGetReviewDecisions(jobId: string): Promise<ReviewDecisions> {
  if (useProxy) {
    const res = await fetch(
      `${backendUrl}/jobs/${encodeURIComponent(jobId)}/review/decisions`,
      { cache: "no-store" },
    );
    if (res.status === 404) return { job_id: jobId, decisions: {} };
    if (!res.ok) throw new Error(`Backend error (${res.status})`);
    return (await res.json()) as ReviewDecisions;
  }
  // Mock mode: decisions live only in localStorage on the client.
  return { job_id: jobId, decisions: {} };
}

export async function adapterSaveReviewDecisions(
  jobId: string,
  decisions: Record<string, ReviewDecision>,
): Promise<{ job_id: string; saved: number }> {
  if (useProxy) {
    const res = await fetch(
      `${backendUrl}/jobs/${encodeURIComponent(jobId)}/review/decisions`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ decisions }),
      },
    );
    if (!res.ok) throw new Error(`Backend error (${res.status})`);
    return (await res.json()) as { job_id: string; saved: number };
  }
  return { job_id: jobId, saved: Object.keys(decisions).length };
}

export async function adapterGetReviewExport(jobId: string): Promise<Response> {
  if (useProxy) {
    return fetch(`${backendUrl}/jobs/${encodeURIComponent(jobId)}/review/export`);
  }
  return new Response(
    JSON.stringify({ message: "Final export requires the Python backend." }),
    { status: 501, headers: { "content-type": "application/json" } },
  );
}

export async function adapterGetTypoCorrections(jobId: string): Promise<TypoCorrections> {
  if (useProxy) {
    const res = await fetch(
      `${backendUrl}/jobs/${encodeURIComponent(jobId)}/typo-corrections`,
      { cache: "no-store" },
    );
    if (res.status === 404) return { job_id: jobId, total: 0, corrections: [] };
    if (!res.ok) throw new Error(`Backend error (${res.status})`);
    return (await res.json()) as TypoCorrections;
  }
  return { job_id: jobId, total: 0, corrections: [] };
}

export interface AIReviewSuggestion {
  id: string;
  decision: "approve" | "reject" | "uncertain";
  confidence: number;
  reasoning: string;
}

export interface AIReviewResponse {
  job_id: string;
  total: number;
  suggestions: AIReviewSuggestion[];
}

export interface AISummaryResponse {
  job_id: string;
  narrative: string;
}

export class AIDisabledError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "AIDisabledError";
  }
}

/**
 * The Python backend serialises errors in a few different shapes depending on
 * whether the custom exception handler unwrapped them:
 *   1. `{error: {error_type, message, details}}`  ← custom handler output
 *   2. `{detail: {error: {...}}}`                 ← raw FastAPI HTTPException
 *   3. `{message: "..."}`                         ← Next.js proxy fallback
 *   4. `{detail: "some string"}`                  ← FastAPI default
 * Try each shape and return the first message we find so the UI shows the
 * actual backend reason instead of a generic "Backend error (500)".
 */
function extractBackendMessage(body: unknown): string | null {
  if (!body || typeof body !== "object") return null;
  const obj = body as Record<string, unknown>;
  // (3)
  if (typeof obj.message === "string") return obj.message;
  // (1) custom handler: {error: {message}}
  const errorTop = obj.error as Record<string, unknown> | undefined;
  if (errorTop && typeof errorTop.message === "string") return errorTop.message;
  // (2) raw HTTPException: {detail: {error: {message}}}
  const detail = obj.detail as Record<string, unknown> | undefined;
  const errorInDetail = detail?.error as Record<string, unknown> | undefined;
  if (errorInDetail && typeof errorInDetail.message === "string") {
    return errorInDetail.message;
  }
  // (4) bare detail string
  if (typeof detail === "string") return detail;
  return null;
}

async function _callAI<T>(
  jobId: string,
  suffix: string,
): Promise<T> {
  if (!useProxy) {
    throw new AIDisabledError(
      "AI features require the Python backend. Set TRASHPANDA_BACKEND_URL.",
    );
  }
  const res = await fetch(
    `${backendUrl}/jobs/${encodeURIComponent(jobId)}/${suffix}`,
    { method: "POST", cache: "no-store" },
  );
  if (res.status === 503) {
    const body = await res.json().catch(() => ({}));
    throw new AIDisabledError(
      extractBackendMessage(body) ?? "AI features are not configured on the backend.",
    );
  }
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(
      extractBackendMessage(body) ?? `Backend error (${res.status})`,
    );
  }
  return (await res.json()) as T;
}

export async function adapterAIReviewSuggestions(
  jobId: string,
): Promise<AIReviewResponse> {
  return _callAI<AIReviewResponse>(jobId, "ai-review");
}

export async function adapterAIJobSummary(
  jobId: string,
): Promise<AISummaryResponse> {
  return _callAI<AISummaryResponse>(jobId, "ai-summary");
}

export async function adapterGetJobInsights(
  jobId: string,
): Promise<InsightsResponse> {
  if (useProxy) {
    const res = await fetch(
      `${backendUrl}/jobs/${encodeURIComponent(jobId)}/insights`,
      { cache: "no-store" },
    );
    if (res.status === 404) {
      return _emptyInsights(jobId);
    }
    if (!res.ok) throw new Error(`Backend error (${res.status})`);
    return (await res.json()) as InsightsResponse;
  }
  return _emptyInsights(jobId);
}

function _emptyInsights(jobId: string): InsightsResponse {
  return {
    job_id: jobId,
    v2_available: false,
    totals: { all: 0, valid: 0, review: 0, invalid: 0 },
    confidence_tiers: { high: 0, medium: 0, low: 0, unknown: 0 },
    final_actions: {},
    catch_all_count: 0,
    smtp_tested_count: 0,
    smtp_suspicious_count: 0,
    domain_intelligence: {
      reliable: [], risky: [], unstable: [], catch_all_suspected: [],
    },
    rows: [],
  };
}

export const adapterMode = useProxy ? "proxy" : "mock";

// ── System info ──────────────────────────────────────────────────────────
// Wire-shape mirrored from app/server.py::system_info().
export interface BackendSystemInfo {
  backend_label: string;
  deployment: string;
  auth_enabled: boolean;
  wall_clock_seconds: number;
  smtp_default_dry_run: boolean;
}

export interface FrontendSystemInfo extends BackendSystemInfo {
  /** "proxy" when TRASHPANDA_BACKEND_URL is set; "mock" otherwise. */
  adapter_mode: "proxy" | "mock";
  /** Effective backend URL the BFF talks to (basename only, no token). */
  backend_url: string | null;
  /** True when the operator passes a token to /api/operator/*. */
  operator_token_configured: boolean;
}

const _MOCK_SYSTEM_INFO: BackendSystemInfo = {
  backend_label: "mock",
  deployment: "mock",
  auth_enabled: false,
  wall_clock_seconds: 0,
  smtp_default_dry_run: true,
};

export async function adapterGetSystemInfo(): Promise<FrontendSystemInfo> {
  let backend: BackendSystemInfo = _MOCK_SYSTEM_INFO;
  if (useProxy && backendUrl) {
    try {
      const res = await fetch(`${backendUrl}/system/info`, {
        cache: "no-store",
      });
      if (res.ok) {
        backend = (await res.json()) as BackendSystemInfo;
      }
    } catch {
      // Tunnel down or backend unreachable — leave the mock defaults
      // in place so the UI can still render.
      backend = { ..._MOCK_SYSTEM_INFO, backend_label: "unreachable" };
    }
  }

  return {
    ...backend,
    adapter_mode: useProxy ? "proxy" : "mock",
    backend_url: backendUrl ?? null,
    operator_token_configured: Boolean(operatorToken),
  };
}

// ── V2.10 Operator adapter functions ───────────────────────────────────────
//
// Server-side proxy functions for the /api/operator/* BFF routes.
// These require the Python backend and intentionally do not synthesize
// mock operator state — operator workflows depend on real pipeline
// state (review summary, package manifest, SMTP runtime, …) that mock
// mode cannot fabricate convincingly. Without TRASHPANDA_BACKEND_URL,
// every operator JSON adapter throws OperatorBackendUnavailableError;
// the BFF routes map that to a 503 with a clear message.

export class OperatorBackendUnavailableError extends Error {
  constructor() {
    super(
      "Operator endpoints require the Python backend. Set TRASHPANDA_BACKEND_URL to enable /api/operator proxy routes.",
    );
    this.name = "OperatorBackendUnavailableError";
  }
}

function assertOperatorBackendAvailable(): string {
  if (!backendUrl) {
    throw new OperatorBackendUnavailableError();
  }
  return backendUrl;
}

async function operatorJson<T>(
  path: string,
  init?: RequestInit,
): Promise<T> {
  const baseUrl = assertOperatorBackendAvailable();
  const res = await fetch(`${baseUrl}${path}`, {
    cache: "no-store",
    ...init,
    headers: {
      ...(init?.body ? { "content-type": "application/json" } : {}),
      ...operatorAuthHeaders(),
      ...(init?.headers ?? {}),
    },
  });

  if (!res.ok) {
    let body: unknown = null;
    try {
      body = await res.json();
    } catch {
      body = null;
    }
    const message =
      extractBackendMessage(body) ?? `Backend error (${res.status})`;
    throw new Error(message);
  }

  return (await res.json()) as T;
}

export async function adapterRunOperatorPreflight(
  input: RunPreflightInput,
): Promise<PreflightResult> {
  return operatorJson<PreflightResult>("/api/operator/preflight", {
    method: "POST",
    body: JSON.stringify(input),
  });
}

export async function adapterGetOperatorJob(
  jobId: string,
): Promise<JobResult> {
  return operatorJson<JobResult>(
    `/api/operator/jobs/${encodeURIComponent(jobId)}`,
  );
}

export async function adapterGetOperatorSmtpRuntime(
  jobId: string,
): Promise<SmtpRuntimeSummary> {
  return operatorJson<SmtpRuntimeSummary>(
    `/api/operator/jobs/${encodeURIComponent(jobId)}/smtp-runtime`,
  );
}

export async function adapterGetOperatorArtifactConsistency(
  jobId: string,
): Promise<ArtifactConsistencyResult> {
  return operatorJson<ArtifactConsistencyResult>(
    `/api/operator/jobs/${encodeURIComponent(jobId)}/artifact-consistency`,
  );
}

export async function adapterBuildOperatorClientPackage(
  jobId: string,
): Promise<ClientPackageManifest> {
  return operatorJson<ClientPackageManifest>(
    `/api/operator/jobs/${encodeURIComponent(jobId)}/client-package`,
    { method: "POST" },
  );
}

export async function adapterGetOperatorClientPackageManifest(
  jobId: string,
): Promise<ClientPackageManifest> {
  return operatorJson<ClientPackageManifest>(
    `/api/operator/jobs/${encodeURIComponent(jobId)}/client-package`,
  );
}

/**
 * Streams the safe client-package ZIP straight back to the BFF route.
 *
 * Critical: returns the raw fetch Response so status, Content-Type,
 * Content-Disposition, and X-TrashPanda-Audience flow through unchanged.
 * The endpoint can return either:
 *   - 200 application/zip with Content-Disposition
 *   - 409 application/json ClientPackageDownloadError
 * Never wrap, never parse, never reconstruct the filename. Never fall
 * back to /jobs/{id}/artifacts/zip or /results/{id} — those are not the
 * client delivery contract.
 */
export async function adapterDownloadOperatorClientPackage(
  jobId: string,
): Promise<Response> {
  if (backendUrl) {
    return fetch(
      `${backendUrl}/api/operator/jobs/${encodeURIComponent(jobId)}/client-package/download`,
      {
        cache: "no-store",
        headers: { ...operatorAuthHeaders() },
      },
    );
  }
  return new Response(
    JSON.stringify({
      error: "client_package_download_requires_backend",
      message:
        "The safe client-package download requires the Python backend. Set TRASHPANDA_BACKEND_URL to enable /api/operator proxy routes.",
      ready_for_client: false,
    }),
    {
      status: 501,
      headers: { "content-type": "application/json" },
    },
  );
}

/**
 * V2.10.8.3 — Streams the *safe-only partial* client-package ZIP back
 * to the BFF route. Parallel to ``adapterDownloadOperatorClientPackage``
 * but for the partial-delivery channel.
 *
 * Critical: returns the raw fetch Response so status, Content-Type,
 * Content-Disposition, X-TrashPanda-Audience,
 * X-TrashPanda-Delivery-Mode, X-TrashPanda-Ready-For-Client, and
 * X-TrashPanda-Ready-For-Client-Partial all flow through unchanged.
 * The endpoint can return either:
 *   - 200 application/zip with Content-Disposition
 *   - 409 application/json SafeOnlyDownloadError
 * Never wrap, never parse, never reconstruct the filename. Never fall
 * back to /jobs/{id}/artifacts/zip or /results/{id}.
 *
 * The override header is forwarded only when non-empty; the backend
 * rejects the request with ``safe_only_override_required`` when the
 * value isn't exactly ``safe-only``, so an empty header pass-through
 * would just be wasted bytes.
 */
export async function adapterDownloadOperatorClientPackageSafeOnly(
  jobId: string,
  overrideHeader: string,
): Promise<Response> {
  if (!backendUrl) {
    return new Response(
      JSON.stringify({
        error: "backend_not_configured",
        message: "Backend API base URL is not configured.",
        ready_for_client: false,
        ready_for_client_partial: false,
      }),
      {
        status: 501,
        headers: { "content-type": "application/json" },
      },
    );
  }

  const headers: Record<string, string> = { ...operatorAuthHeaders() };
  if (overrideHeader) {
    headers["X-TrashPanda-Operator-Override"] = overrideHeader;
  }

  return fetch(
    `${backendUrl}/api/operator/jobs/${encodeURIComponent(jobId)}/client-package/download-safe-only`,
    {
      cache: "no-store",
      headers,
    },
  );
}

// ── "Send to client" — one-click bundle ─────────────────────────────────
// Mirrors the wire-shape of GET /api/operator/jobs/{id}/client-bundle/summary.
export interface ClientBundleSummary {
  available: boolean;
  ready_for_client: boolean;
  delivery_mode: "full" | "safe_only_partial";
  primary_filename: string | null;
  download_filename: string | null;
  safe_count: number;
  review_count: number;
  rejected_count: number;
  delivery_state?:
    | "cleaning_completed"
    | "smtp_verification_pending"
    | "smtp_verified"
    | "ready_to_send"
    | "blocked"
    | "failed";
  smtp_verification_status?: "disabled" | "dry_run" | "not_run" | "verified";
  smtp_enabled?: boolean;
  smtp_dry_run?: boolean;
  smtp_candidates_seen?: number;
  smtp_candidates_attempted?: number;
  smtp_not_tested_count?: number;
  high_risk_count?: number;
  blocking_reason?: string | null;
  operator_message?: string;
  issues: Array<{ severity: string; code: string; message: string }>;
}

export async function adapterGetClientBundleSummary(
  jobId: string,
): Promise<ClientBundleSummary> {
  return operatorJson<ClientBundleSummary>(
    `/api/operator/jobs/${encodeURIComponent(jobId)}/client-bundle/summary`,
  );
}

export async function adapterDownloadClientBundle(
  jobId: string,
): Promise<Response> {
  if (!backendUrl) {
    return new Response(
      JSON.stringify({
        error: "backend_not_configured",
        message: "Backend API base URL is not configured.",
        ready_for_client: false,
      }),
      { status: 501, headers: { "content-type": "application/json" } },
    );
  }
  return fetch(
    `${backendUrl}/api/operator/jobs/${encodeURIComponent(jobId)}/client-bundle/download`,
    {
      cache: "no-store",
      headers: { ...operatorAuthHeaders() },
    },
  );
}

export async function adapterDownloadExtraStrict(
  jobId: string,
): Promise<Response> {
  if (!backendUrl) {
    return new Response(
      JSON.stringify({
        error: "backend_not_configured",
        message: "Backend API base URL is not configured.",
      }),
      { status: 501, headers: { "content-type": "application/json" } },
    );
  }
  return fetch(
    `${backendUrl}/api/operator/jobs/${encodeURIComponent(jobId)}/extra-strict/download`,
    {
      cache: "no-store",
      headers: { ...operatorAuthHeaders() },
    },
  );
}


export async function adapterRunOperatorReviewGate(
  jobId: string,
): Promise<OperatorReviewSummary> {
  return operatorJson<OperatorReviewSummary>(
    `/api/operator/jobs/${encodeURIComponent(jobId)}/review-gate`,
    { method: "POST" },
  );
}

export async function adapterGetOperatorReviewSummary(
  jobId: string,
): Promise<OperatorReviewSummary> {
  return operatorJson<OperatorReviewSummary>(
    `/api/operator/jobs/${encodeURIComponent(jobId)}/operator-review`,
  );
}

export async function adapterGetOperatorRetryQueueStatus(
  jobId: string,
): Promise<RetryQueueStatus> {
  return operatorJson<RetryQueueStatus>(
    `/api/operator/jobs/${encodeURIComponent(jobId)}/retry-queue/status`,
  );
}

export async function adapterRunOperatorRetryQueueDrain(
  jobId: string,
): Promise<RetryQueueDrainResult> {
  return operatorJson<RetryQueueDrainResult>(
    `/api/operator/jobs/${encodeURIComponent(jobId)}/retry-queue/run`,
    { method: "POST" },
  );
}

export async function adapterSetOperatorRetryQueueAutoRetry(
  jobId: string,
  enabled: boolean,
): Promise<{ auto_retry_enabled: boolean }> {
  return operatorJson<{ auto_retry_enabled: boolean }>(
    `/api/operator/jobs/${encodeURIComponent(jobId)}/retry-queue/auto-retry?enabled=${encodeURIComponent(String(enabled))}`,
    { method: "PATCH" },
  );
}

export async function adapterFinalizeOperatorRetryQueue(
  jobId: string,
): Promise<RetryQueueFinalizeResult> {
  return operatorJson<RetryQueueFinalizeResult>(
    `/api/operator/jobs/${encodeURIComponent(jobId)}/retry-queue/finalize`,
    { method: "POST" },
  );
}

export async function adapterGetOperatorPilotSendStatus(
  jobId: string,
): Promise<PilotSendStatus> {
  return operatorJson<PilotSendStatus>(
    `/api/operator/jobs/${encodeURIComponent(jobId)}/pilot-send/status`,
  );
}

export async function adapterSetOperatorPilotSendConfig(
  jobId: string,
  input: PilotSendConfigInput,
): Promise<{ saved: boolean; config_ready: boolean }> {
  return operatorJson<{ saved: boolean; config_ready: boolean }>(
    `/api/operator/jobs/${encodeURIComponent(jobId)}/pilot-send/config`,
    {
      method: "PUT",
      body: JSON.stringify(input),
    },
  );
}

export async function adapterPreviewOperatorPilotCandidates(
  jobId: string,
  batchSize: number,
): Promise<PilotPreviewResult> {
  return operatorJson<PilotPreviewResult>(
    `/api/operator/jobs/${encodeURIComponent(jobId)}/pilot-send/preview?batch_size=${encodeURIComponent(String(batchSize))}`,
    { method: "POST" },
  );
}

export async function adapterLaunchOperatorPilot(
  jobId: string,
  batchSize: number,
): Promise<PilotLaunchResult> {
  return operatorJson<PilotLaunchResult>(
    `/api/operator/jobs/${encodeURIComponent(jobId)}/pilot-send/launch?batch_size=${encodeURIComponent(String(batchSize))}`,
    { method: "POST" },
  );
}

export async function adapterPollOperatorPilotBounces(
  jobId: string,
): Promise<PilotPollBouncesResult> {
  return operatorJson<PilotPollBouncesResult>(
    `/api/operator/jobs/${encodeURIComponent(jobId)}/pilot-send/poll-bounces`,
    { method: "POST" },
  );
}

export async function adapterFinalizeOperatorPilot(
  jobId: string,
): Promise<PilotFinalizeResult> {
  return operatorJson<PilotFinalizeResult>(
    `/api/operator/jobs/${encodeURIComponent(jobId)}/pilot-send/finalize`,
    { method: "POST" },
  );
}

export async function adapterIngestOperatorFeedback(
  input: IngestFeedbackInput,
): Promise<FeedbackIngestionSummary> {
  return operatorJson<FeedbackIngestionSummary>(
    "/api/operator/feedback/ingest",
    {
      method: "POST",
      body: JSON.stringify(input),
    },
  );
}

export async function adapterGetOperatorFeedbackPreview(): Promise<FeedbackPreviewResult> {
  return operatorJson<FeedbackPreviewResult>(
    "/api/operator/feedback/preview",
  );
}

export async function adapterBuildOperatorFeedbackPreview(
  input: FeedbackPreviewInput,
): Promise<FeedbackPreviewResult> {
  return operatorJson<FeedbackPreviewResult>(
    "/api/operator/feedback/preview",
    {
      method: "POST",
      body: JSON.stringify(input),
    },
  );
}
