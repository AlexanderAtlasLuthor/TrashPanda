/**
 * Server-side adapter switch. Route handlers call these functions;
 * they either hit the mock in-memory store or proxy to the real Python HTTP
 * service depending on TRASHPANDA_BACKEND_URL.
 *
 * Keep this file server-only (used only from route handlers under app/api).
 */

import type { JobResult } from "./types";
import {
  createMockJob,
  getMockJob,
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

export const adapterMode = useProxy ? "proxy" : "mock";
