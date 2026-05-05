import type { ArtifactAudience } from "@/lib/types";
import styles from "./ArtifactAudienceBadge.module.css";

interface ArtifactAudienceBadgeProps {
  audience?: ArtifactAudience | string | null;
}

const TONE_BY_AUDIENCE: Record<string, string> = {
  client_safe: "safe",
  operator_only: "operator",
  technical_debug: "debug",
  internal_only: "internal",
};

const LABEL_BY_AUDIENCE: Record<string, string> = {
  client_safe: "CLIENT-SAFE",
  operator_only: "OPERATOR-ONLY",
  technical_debug: "TECHNICAL-DEBUG",
  internal_only: "INTERNAL-ONLY",
};

/**
 * Presentational pill describing an artifact's audience classification.
 * Display-only — never inspect this value to decide whether delivery
 * is permitted. Delivery is gated server-side by the V2.9.7 operator
 * review gate.
 */
export function ArtifactAudienceBadge({ audience }: ArtifactAudienceBadgeProps) {
  const raw = (audience ?? "").toString().trim().toLowerCase();
  const tone = TONE_BY_AUDIENCE[raw] ?? "unknown";
  const label = LABEL_BY_AUDIENCE[raw] ?? (raw ? raw.toUpperCase() : "UNKNOWN");
  return (
    <span className={[styles.badge, styles[tone]].join(" ")} data-tone={tone}>
      {label}
    </span>
  );
}
