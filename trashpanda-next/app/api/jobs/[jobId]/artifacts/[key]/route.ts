import { adapterGetArtifact } from "@/lib/backend-adapter";

export const runtime = "nodejs";

export async function GET(
  _req: Request,
  { params }: { params: Promise<{ jobId: string; key: string }> },
) {
  const { jobId, key } = await params;
  return adapterGetArtifact(jobId, key);
}
