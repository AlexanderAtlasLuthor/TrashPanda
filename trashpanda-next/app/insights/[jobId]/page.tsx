import { adapterGetJobInsights, adapterGetJob } from "@/lib/backend-adapter";
import { InsightsClient } from "./InsightsClient";

export default async function InsightsPage({
  params,
}: {
  params: Promise<{ jobId: string }>;
}) {
  const { jobId } = await params;
  let initial = null;
  let job = null;
  try {
    [initial, job] = await Promise.all([
      adapterGetJobInsights(jobId),
      adapterGetJob(jobId).catch(() => null),
    ]);
  } catch {
    initial = null;
  }
  return (
    <InsightsClient
      jobId={jobId}
      initial={initial}
      inputFilename={job?.input_filename ?? null}
    />
  );
}
