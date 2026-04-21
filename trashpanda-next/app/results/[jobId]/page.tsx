import { adapterGetJob } from "@/lib/backend-adapter";
import { ResultsClient } from "./ResultsClient";

/**
 * Results page. Runs as a server component so we can do the first fetch
 * on the server (SSR) and hand a hydrated snapshot to the client. The
 * client then takes over with polling for live updates.
 *
 * If the initial fetch fails, we still render the client and let it
 * handle the retry / error display. This keeps the UX consistent.
 */
export default async function ResultsPage({
  params,
}: {
  params: Promise<{ jobId: string }>;
}) {
  const { jobId } = await params;
  let initialJob = null;
  try {
    initialJob = await adapterGetJob(jobId);
  } catch {
    // swallow; client will retry via polling
    initialJob = null;
  }
  return <ResultsClient jobId={jobId} initialJob={initialJob} />;
}
