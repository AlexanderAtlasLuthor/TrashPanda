import type { FeedbackBehaviorClass } from "@/lib/types";
import styles from "./FeedbackBehaviorBadge.module.css";

interface FeedbackBehaviorBadgeProps {
  behaviorClass?: FeedbackBehaviorClass | string | null;
}

const TONE_BY_CLASS: Record<string, string> = {
  known_good: "success",
  known_risky: "danger",
  cold_start: "warn",
  unknown: "muted",
};

// Display-only badge. Feedback behavior class must never be used as
// delivery readiness.
export function FeedbackBehaviorBadge({
  behaviorClass,
}: FeedbackBehaviorBadgeProps) {
  const raw = (behaviorClass ?? "").toString().trim().toLowerCase();
  const tone = TONE_BY_CLASS[raw] ?? "muted";
  const label = raw ? raw.replace(/_/g, " ").toUpperCase() : "UNKNOWN";
  return (
    <span className={[styles.badge, styles[tone]].join(" ")} data-tone={tone}>
      {label}
    </span>
  );
}
