"use client";

import { useRef } from "react";
import { Topbar } from "@/components/Topbar";
import { UploadDropzone } from "@/components/UploadDropzone";
import { RecentJobs } from "@/components/RecentJobs";
import { useWorkspaceStats } from "./home/useWorkspaceStats";
import { WelcomeHero } from "./home/WelcomeHero";
import { WorkspaceMetrics } from "./home/WorkspaceMetrics";
import { QuickActionsPanel } from "./home/QuickActionsPanel";
import { TipsPanel } from "./home/TipsPanel";
import { RiskHighlightsPanel } from "./home/RiskHighlightsPanel";
import styles from "./HomeDashboard.module.css";

/**
 * The Home / Dashboard surface.
 *
 * Composes the already-shipped Topbar, UploadDropzone and RecentJobs with
 * the new welcome / metrics / quick actions / tips / risk panels. No new
 * design tokens, sidebar items, or backend contracts are introduced.
 */

// User greeting name. Can be customised per deployment through
// NEXT_PUBLIC_USER_NAME — falls back to the product owner's name.
const USER_NAME =
  (process.env.NEXT_PUBLIC_USER_NAME && process.env.NEXT_PUBLIC_USER_NAME.trim()) ||
  "Miguel";

export function HomeDashboard() {
  const stats = useWorkspaceStats();
  const uploadRef = useRef<HTMLDivElement>(null);

  const scrollToUpload = () => {
    uploadRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
  };

  return (
    <>
      <div className="fade-up">
        <Topbar
          breadcrumb={["WORKSPACE", "HOME"]}
          title="DATA/HYGIENE WORKSPACE"
          titleSlice="/"
          meta={[
            { label: "ENGINE", value: "ONLINE", accent: true },
            { label: "READY FOR", value: "CSV · XLSX" },
          ]}
        />
      </div>

      <div className="fade-up">
        <WelcomeHero userName={USER_NAME} stats={stats} />
      </div>

      <div className="fade-up">
        <QuickActionsPanel stats={stats} onUploadClick={scrollToUpload} />
      </div>

      <div className="fade-up">
        <WorkspaceMetrics stats={stats} />
      </div>

      <div className={`${styles.twoCol} fade-up`}>
        <RiskHighlightsPanel stats={stats} />
        <TipsPanel stats={stats} />
      </div>

      <div className="fade-up" ref={uploadRef}>
        <UploadDropzone />
      </div>

      <div className="fade-up">
        <RecentJobs />
      </div>
    </>
  );
}
