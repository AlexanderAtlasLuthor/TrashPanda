import { adapterDownloadExtraStrict } from "@/lib/backend-adapter";

export const runtime = "nodejs";

/** Streams the Extra Strict Offline ZIP straight back to the browser. */
export async function GET(
  _req: Request,
  { params }: { params: Promise<{ jobId: string }> },
) {
  const { jobId } = await params;
  const upstream = await adapterDownloadExtraStrict(jobId);
  const headers = new Headers(upstream.headers);
  return new Response(upstream.body, {
    status: upstream.status,
    headers,
  });
}
