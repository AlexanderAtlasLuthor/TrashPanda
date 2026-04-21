"use client";

import styles from "./RaccoonMascot.module.css";

export type RaccoonState =
  | "idle"
  | "hover"
  | "ready"
  | "processing"
  | "happy"
  | "neutral"
  | "worried";

interface Props {
  state: RaccoonState;
  className?: string;
}

export function RaccoonMascot({ state, className }: Props) {
  return (
    <svg
      viewBox="0 0 100 100"
      fill="none"
      stroke="currentColor"
      strokeWidth="2.2"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={[styles.raccoon, styles[state], className]
        .filter(Boolean)
        .join(" ")}
      aria-hidden
    >
      {/* Head */}
      <circle cx="50" cy="58" r="34" />

      {/* Ears */}
      <path d="M20,44 L27,13 L42,40" />
      <path d="M58,40 L73,13 L80,44" />

      {/* Raccoon eye-mask rings */}
      <ellipse cx="37" cy="55" rx="12" ry="9" />
      <ellipse cx="63" cy="55" rx="12" ry="9" />

      {/* Eyes (inside the rings) */}
      <circle className={`${styles.eye} ${styles.eyeLeft}`}  cx="37" cy="55" r="4" />
      <circle className={`${styles.eye} ${styles.eyeRight}`} cx="63" cy="55" r="4" />

      {/* Nose */}
      <path d="M47,69 Q50,65 53,69 Q51,73 50,74 Q49,73 47,69 Z" />

      {/* Mouth: idle — gentle closed smile */}
      <path className={styles.mouthIdle}    d="M43,79 Q50,84 57,79" />

      {/* Mouth: open — hover / ready */}
      <path className={styles.mouthOpen}    d="M43,79 Q50,90 57,79" />

      {/* Mouth: happy — wide smile */}
      <path className={styles.mouthHappy}   d="M40,79 Q50,91 60,79" />

      {/* Mouth: worried — slight frown */}
      <path className={styles.mouthWorried} d="M43,83 Q50,78 57,83" />

      {/* Whiskers */}
      <line className={styles.whisker} x1="6"  y1="64" x2="34" y2="66" />
      <line className={styles.whisker} x1="6"  y1="70" x2="34" y2="70" />
      <line className={styles.whisker} x1="66" y1="66" x2="94" y2="64" />
      <line className={styles.whisker} x1="66" y1="70" x2="94" y2="70" />
    </svg>
  );
}
