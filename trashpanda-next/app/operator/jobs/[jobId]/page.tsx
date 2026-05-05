import {
  adapterGetOperatorClientPackageManifest,
  adapterGetOperatorJob,
  adapterGetOperatorReviewSummary,
} from "@/lib/backend-adapter";
import { OperatorJobClient } from "./OperatorJobClient";

export default async function OperatorJobPage({
  params,
}: {
  params: Promise<{ jobId: string }>;
}) {
  const { jobId } = await params;

  const [initialJob, initialManifest, initialReview] = await Promise.all([
    adapterGetOperatorJob(jobId).catch(() => null),
    adapterGetOperatorClientPackageManifest(jobId).catch(() => null),
    adapterGetOperatorReviewSummary(jobId).catch(() => null),
  ]);

  return (
    <OperatorJobClient
      jobId={jobId}
      initialJob={initialJob}
      initialManifest={initialManifest}
      initialReview={initialReview}
    />
  );
}
