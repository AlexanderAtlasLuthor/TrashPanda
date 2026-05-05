"use client";

import { useEffect, useState } from "react";

import { getSystemInfo, type SystemInfo } from "@/lib/api";
import styles from "./BackendBadge.module.css";

/**
 * Surfaces which backend the BFF is currently talking to. Three
 * variants:
 *
 *   - "VPS · racknerd"   — adapter_mode=proxy + deployment=vps. Auth
 *     state is also surfaced (lock icon when operator token is wired).
 *   - "Local backend"    — adapter_mode=proxy + deployment=local.
 *   - "Mock"             — adapter_mode=mock (no Python backend).
 *
 * The badge polls /api/system/info every 30s so a tunnel drop is
 * visible without a hard refresh.
 */
export function BackendBadge() {
  const [info, setInfo] = useState<SystemInfo | null>(null);
  const [stale, setStale] = useState(false);

  useEffect(() => {
    let cancelled = false;
    const tick = async () => {
      try {
        const next = await getSystemInfo();
        if (!cancelled) {
          setInfo(next);
          setStale(false);
        }
      } catch {
        if (!cancelled) setStale(true);
      }
    };
    tick();
    const id = setInterval(tick, 30_000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  if (info === null && !stale) {
    // Don't flash an empty badge during the very first render.
    return null;
  }

  const variant = stale
    ? "stale"
    : info?.adapter_mode === "mock"
      ? "mock"
      : info?.deployment === "vps"
        ? "vps"
        : "local";

  const label = stale
    ? "Backend unreachable"
    : info?.adapter_mode === "mock"
      ? "Mock backend"
      : info?.deployment === "vps"
        ? `VPS · ${info?.backend_label || "remote"}`
        : `Local · ${info?.backend_label || "local"}`;

  const lockTitle = info?.operator_token_configured
    ? "Operator token configured"
    : info?.auth_enabled
      ? "Backend requires a token but the BFF has none configured"
      : "No operator auth (open backend)";

  return (
    <div
      className={[styles.badge, styles[variant]].join(" ")}
      role="status"
      aria-label={label}
      title={`${label} · ${lockTitle}`}
    >
      <span className={styles.dot} aria-hidden />
      <span className={styles.label}>{label}</span>
      {info && (
        <span
          className={styles.lock}
          aria-hidden
          data-locked={info.operator_token_configured ? "true" : "false"}
        >
          {info.operator_token_configured ? "🔒" : "🔓"}
        </span>
      )}
    </div>
  );
}
