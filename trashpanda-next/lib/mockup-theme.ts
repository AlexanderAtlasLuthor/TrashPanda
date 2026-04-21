/**
 * Design tokens lifted from the TrashPanda mockup. Centralized so every
 * component can reference them without copy-pasting hex codes.
 *
 * These values are also mirrored in app/globals.css as CSS custom properties
 * (e.g. var(--neon)), so most components should prefer the CSS var. This
 * module exists for cases where we need the literal value in TS (inline
 * styles, charts, etc).
 */

export const theme = {
  colors: {
    bg: {
      void: "#05070a",
      deep: "#0a0e14",
      panel: "#0f141c",
      panel2: "#131a24",
      elevated: "#1a2230",
    },
    ink: {
      high: "#e8f4ff",
      mid: "#9ab0c7",
      low: "#5a6a80",
      ghost: "#2a3445",
    },
    accent: {
      neon: "#8eff3a",
      neonBright: "#b4ff5c",
      neonDeep: "#5fc21d",
      neonGlow: "rgba(142, 255, 58, 0.45)",
    },
    steel: {
      blue: "#4a7ba8",
      light: "#7da8d0",
      deep: "#2d4a68",
    },
    semantic: {
      danger: "#ff3a5c",
      warn: "#ffb83a",
      info: "#5fb4ff",
    },
    stroke: {
      soft: "rgba(142, 255, 58, 0.12)",
      strong: "rgba(142, 255, 58, 0.28)",
      steel: "rgba(125, 168, 208, 0.15)",
    },
  },
  fonts: {
    display: "'Bungee', 'Impact', sans-serif",
    ui: "'Chakra Petch', system-ui, sans-serif",
    mono: "'JetBrains Mono', 'Courier New', monospace",
  },
} as const;

export type Severity = "ok" | "warn" | "bad" | "info" | "neutral";
