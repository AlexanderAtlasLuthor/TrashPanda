import { ReviewQueueClient } from "./ReviewQueueClient";

export default async function ReviewQueuePage({
  params,
}: {
  params: Promise<{ jobId: string }>;
}) {
  const { jobId } = await params;
  return <ReviewQueueClient jobId={jobId} />;
}
