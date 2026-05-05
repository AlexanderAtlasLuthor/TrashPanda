"use client";

import type { ReactNode } from "react";
import { Topbar } from "@/components/Topbar";
import { SafetyBanner } from "./SafetyBanner";
import styles from "./OperatorConsoleShell.module.css";

interface OperatorConsoleShellProps {
  children: ReactNode;
}

/**
 * Common chrome for every page under /operator. Mounts the Topbar with
 * operator branding and the mandatory SafetyBanner above the page-
 * specific content, so future operator routes inherit the delivery
 * warning automatically without each page having to remember to add it.
 *
 * Stateless. No API imports. No readiness logic.
 */
export function OperatorConsoleShell({ children }: OperatorConsoleShellProps) {
  return (
    <>
      <div className="fade-up">
        <Topbar
          breadcrumb={["OPERATOR", "CONSOLE"]}
          title="OPERATOR/CONSOLE"
          titleSlice="/"
          subtitle="Safe delivery review surface for V2 operator workflows."
        />
      </div>
      <div className={`fade-up ${styles.bannerWrap}`}>
        <SafetyBanner />
      </div>
      <div className={styles.body}>{children}</div>
    </>
  );
}
