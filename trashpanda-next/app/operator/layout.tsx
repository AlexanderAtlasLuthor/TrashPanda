import type { ReactNode } from "react";
import { OperatorAdvancedBanner } from "@/components/operator/OperatorAdvancedBanner";

/**
 * Layout for the /operator/* surface. Mounts the
 * "you-probably-want-Home" banner above every operator screen so a
 * first-time user who wandered into Advanced from the sidebar doesn't
 * try to make sense of the audit jargon. The banner is
 * non-dismissable on purpose — its copy is short and the CTA links
 * straight back to /.
 */
export default function OperatorLayout({
  children,
}: {
  children: ReactNode;
}) {
  return (
    <>
      <OperatorAdvancedBanner />
      {children}
    </>
  );
}
