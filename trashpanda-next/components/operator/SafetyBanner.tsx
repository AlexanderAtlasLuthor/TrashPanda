import styles from "./SafetyBanner.module.css";

/**
 * Mandatory delivery-safety copy for every operator surface.
 *
 * Exported so the page body can repeat the strings without drifting
 * out of sync with the banner. Do NOT add helpers, derived flags, or
 * audience constants here — readiness is decided server-side by the
 * V2.9.7 operator review gate.
 */
export const OPERATOR_SAFETY_COPY = [
  "WARN is not ready_for_client.",
  "Do not deliver unless ready_for_client is true.",
  "Only client_delivery_package is deliverable.",
] as const;

export function SafetyBanner() {
  return (
    <section
      className={styles.banner}
      role="note"
      aria-label="Delivery safety"
    >
      <div className={styles.iconWrap} aria-hidden="true">
        <svg viewBox="0 0 24 24" className={styles.icon}>
          <path d="M12 3l9 4v5c0 5-3.5 8.5-9 9-5.5-.5-9-4-9-9V7l9-4z" />
          <path d="M12 8v5" />
          <circle cx="12" cy="16" r="0.6" fill="currentColor" stroke="none" />
        </svg>
      </div>
      <div className={styles.body}>
        <div className={styles.eyebrow}>// DELIVERY SAFETY</div>
        <div className={styles.heading}>Read before acting on any operator screen</div>
        <ul className={styles.list}>
          {OPERATOR_SAFETY_COPY.map((line) => (
            <li key={line} className={styles.item}>
              {line}
            </li>
          ))}
        </ul>
      </div>
    </section>
  );
}
