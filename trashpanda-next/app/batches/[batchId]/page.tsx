"use client";

import { useParams } from "next/navigation";

import BatchProgressPanel from "@/components/BatchProgressPanel";

export default function BatchPage() {
  const params = useParams<{ batchId: string }>();
  const batchId = params?.batchId;
  if (!batchId) {
    return <div style={{ padding: "2rem" }}>Missing batch id.</div>;
  }
  return (
    <div style={{ padding: "2rem", maxWidth: 960, margin: "0 auto" }}>
      <BatchProgressPanel batchId={batchId} />
    </div>
  );
}
