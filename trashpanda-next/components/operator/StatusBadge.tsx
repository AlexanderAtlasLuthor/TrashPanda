import styles from "./StatusBadge.module.css";

interface StatusBadgeProps {
  status?: string | null;
}

const TONE_BY_STATUS: Record<string, string> = {
  ready: "success",
  pass: "success",
  completed: "success",
  success: "success",
  client_safe: "success",
  ok: "success",

  warn: "warn",
  warning: "warn",

  block: "danger",
  fail: "danger",
  failed: "danger",
  error: "danger",

  missing: "muted",
  queued: "info",
  running: "info",
  pending: "info",
  unknown: "muted",
};

/**
 * Presentational status pill. Maps a free-form status string to a
 * neutral colour tone via {@link TONE_BY_STATUS}. NEVER use this to
 * decide whether delivery is allowed — readiness is the backend's
 * `OperatorReviewSummary.ready_for_client` boolean and nothing else.
 */
export function StatusBadge({ status }: StatusBadgeProps) {
  const raw = (status ?? "").toString().trim().toLowerCase();
  const tone = TONE_BY_STATUS[raw] ?? "muted";
  const label = raw ? raw.toUpperCase() : "UNKNOWN";
  return (
    <span className={[styles.badge, styles[tone]].join(" ")} data-tone={tone}>
      {label}
    </span>
  );
}
