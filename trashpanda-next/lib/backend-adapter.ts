/**
 * Server-side adapter switch. Route handlers call these functions;
 * they either hit the mock in-memory store or proxy to the real Python HTTP
 * service depending on TRASHPANDA_BACKEND_URL.
 *
 * Keep this file server-only (used only from route handlers under app/api).
 */

import type {
  JobList,
  JobLogs,
  JobResult,
  ReviewDecision,
  ReviewDecisions,
  ReviewQueue,
  TypoCorrections,
  InsightsResponse,
} from "./types";
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

/** POST file, return job id. */
export async function adapterStartJob(
  file: File,
): Promise<{ job_id: string }> {
  if (useProxy) {
    const form = new FormData();
    form.append("file", file);
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
 * FastAPI wraps HTTPException errors as `{detail: {error: {message, ...}}}`.
 * Pull the human-readable message out of either shape so the UI shows the
 * actual backend error instead of a generic "Backend error (500)".
 */
function extractBackendMessage(body: unknown): string | null {
  if (!body || typeof body !== "object") return null;
  const obj = body as Record<string, unknown>;
  if (typeof obj.message === "string") return obj.message;
  const detail = obj.detail as Record<string, unknown> | undefined;
  const error = detail?.error as Record<string, unknown> | undefined;
  if (error && typeof error.message === "string") return error.message;
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
