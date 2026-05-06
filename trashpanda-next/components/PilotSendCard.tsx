"use client";

import { useCallback, useEffect, useState } from "react";

import {
  finalizePilot,
  getPilotSendStatus,
  launchPilot,
  pollPilotBounces,
  previewPilotCandidates,
  setPilotSendConfig,
  type PilotPreviewCandidate,
  type PilotSendConfigInput,
  type PilotSendStatus,
} from "@/lib/api";
import { PILOT_SEND_COPY } from "@/lib/copy";
import styles from "./PilotSendCard.module.css";

interface Props {
  jobId: string;
  visible?: boolean;
}

/**
 * V2.10.12 — Controlled in-house pilot send + bounce-proven verification.
 *
 * Sends real email from the operator's configured sender to a small
 * batch of rescatable rows. Captures bounces via IMAP and feeds the
 * results into the existing bounce_ingestion store + new XLSX
 * deliverables (delivery_verified.xlsx, pilot_hard_bounces.xlsx,
 * updated_do_not_send.xlsx).
 *
 * Card is mounted on the results page below RetryQueueCard. It is
 * intentionally collapsable — most jobs won't run a pilot send and
 * the form is large.
 */
export function PilotSendCard({ jobId, visible = true }: Props) {
  const [status, setStatus] = useState<PilotSendStatus | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState<
    "idle" | "saving" | "previewing" | "launching" | "polling" | "finalizing"
  >("idle");
  const [showConfig, setShowConfig] = useState(false);
  const [preview, setPreview] = useState<PilotPreviewCandidate[] | null>(null);
  const [batchSize, setBatchSize] = useState(50);

  // Form state (mirrors PilotSendConfigInput).
  const [form, setForm] = useState<PilotSendConfigInput>({
    template: {
      subject: "",
      body_text: "",
      body_html: "",
      sender_address: "",
      sender_name: "TrashPanda",
      reply_to: "",
    },
    imap: {
      host: "",
      port: 993,
      use_ssl: true,
      username: "",
      password_env: "TRASHPANDA_BOUNCE_IMAP_PASSWORD",
      folder: "INBOX",
    },
    return_path_domain: "",
    wait_window_hours: 48,
    expiry_hours: 168,
    max_batch_size: 100,
    authorization_confirmed: false,
    authorization_note: "",
  });

  const refresh = useCallback(async () => {
    try {
      const next = await getPilotSendStatus(jobId);
      setStatus(next);
      setError(null);
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : "Failed to load");
    }
  }, [jobId]);

  useEffect(() => {
    if (!visible) return;
    refresh();
  }, [refresh, visible]);

  if (!visible) return null;

  const onSaveConfig = async () => {
    setBusy("saving");
    try {
      await setPilotSendConfig(jobId, form);
      await refresh();
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : "Save failed");
    } finally {
      setBusy("idle");
    }
  };

  const onPreview = async () => {
    setBusy("previewing");
    try {
      const result = await previewPilotCandidates(jobId, batchSize);
      setPreview(result.candidates);
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : "Preview failed");
    } finally {
      setBusy("idle");
    }
  };

  const onLaunch = async () => {
    if (!window.confirm(PILOT_SEND_COPY.launchWarning)) return;
    setBusy("launching");
    try {
      await launchPilot(jobId, batchSize);
      await refresh();
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : "Launch failed");
    } finally {
      setBusy("idle");
    }
  };

  const onPoll = async () => {
    setBusy("polling");
    try {
      await pollPilotBounces(jobId);
      await refresh();
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : "Poll failed");
    } finally {
      setBusy("idle");
    }
  };

  const onFinalize = async () => {
    if (!window.confirm(PILOT_SEND_COPY.finalizeWarning)) return;
    setBusy("finalizing");
    try {
      await finalizePilot(jobId);
      await refresh();
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : "Finalize failed");
    } finally {
      setBusy("idle");
    }
  };

  const c = status?.counts;
  const launchDisabled =
    busy !== "idle" || !status?.config_ready || !status?.authorization_confirmed;

  return (
    <div className={styles.card}>
      <div className={styles.heading}>// {PILOT_SEND_COPY.title}</div>
      <div className={styles.description}>{PILOT_SEND_COPY.description}</div>

      {/* Counts row */}
      {c && (
        <div className={styles.counts}>
          <span className={styles.statPending}>
            {c.pending_send.toLocaleString()} pending_send
          </span>
          <span className={styles.dot}>·</span>
          <span className={styles.statSent}>
            {c.sent.toLocaleString()} sent
          </span>
          <span className={styles.dot}>·</span>
          <span className={styles.statSuccess}>
            {c.delivered.toLocaleString()} delivery_verified
          </span>
          <span className={styles.dot}>·</span>
          <span className={styles.statBad}>
            {c.hard_bounce.toLocaleString()} hard_bounce
          </span>
          {c.blocked > 0 && (
            <>
              <span className={styles.dot}>·</span>
              <span className={styles.statBad}>
                {c.blocked.toLocaleString()} blocked
              </span>
            </>
          )}
          {c.soft_bounce + c.deferred > 0 && (
            <>
              <span className={styles.dot}>·</span>
              <span className={styles.statWarn}>
                {(c.soft_bounce + c.deferred).toLocaleString()} soft / deferred
              </span>
            </>
          )}
          <span className={styles.dot}>·</span>
          <span className={styles.muted}>
            hard-bounce rate {(c.hard_bounce_rate * 100).toFixed(1)}%
          </span>
        </div>
      )}

      {/* State warnings */}
      {status && !status.config_ready && (
        <div className={styles.warning}>
          {status.authorization_confirmed
            ? PILOT_SEND_COPY.state.notConfigured
            : PILOT_SEND_COPY.state.notAuthorized}
        </div>
      )}
      <div className={styles.portWarning}>
        {PILOT_SEND_COPY.state.portWarning}
      </div>

      {/* Config toggle */}
      <button
        type="button"
        className={styles.btnSecondary}
        onClick={() => setShowConfig((v) => !v)}
      >
        {showConfig ? "Hide config" : "Edit config"}
      </button>

      {showConfig && (
        <div className={styles.configForm}>
          <label className={styles.field}>
            <span>Subject</span>
            <input
              type="text"
              value={form.template?.subject ?? ""}
              onChange={(e) =>
                setForm((f) => ({
                  ...f,
                  template: { ...f.template, subject: e.target.value },
                }))
              }
            />
          </label>
          <label className={styles.field}>
            <span>Body (text)</span>
            <textarea
              rows={4}
              value={form.template?.body_text ?? ""}
              onChange={(e) =>
                setForm((f) => ({
                  ...f,
                  template: { ...f.template, body_text: e.target.value },
                }))
              }
            />
          </label>
          <label className={styles.field}>
            <span>Body (HTML, optional)</span>
            <textarea
              rows={3}
              value={form.template?.body_html ?? ""}
              onChange={(e) =>
                setForm((f) => ({
                  ...f,
                  template: { ...f.template, body_html: e.target.value },
                }))
              }
            />
          </label>
          <label className={styles.field}>
            <span>Sender address</span>
            <input
              type="email"
              value={form.template?.sender_address ?? ""}
              onChange={(e) =>
                setForm((f) => ({
                  ...f,
                  template: { ...f.template, sender_address: e.target.value },
                }))
              }
            />
          </label>
          <label className={styles.field}>
            <span>Sender name</span>
            <input
              type="text"
              value={form.template?.sender_name ?? ""}
              onChange={(e) =>
                setForm((f) => ({
                  ...f,
                  template: { ...f.template, sender_name: e.target.value },
                }))
              }
            />
          </label>
          <label className={styles.field}>
            <span>Reply-To (optional)</span>
            <input
              type="email"
              value={form.template?.reply_to ?? ""}
              onChange={(e) =>
                setForm((f) => ({
                  ...f,
                  template: { ...f.template, reply_to: e.target.value },
                }))
              }
            />
          </label>
          <label className={styles.field}>
            <span>Return-Path domain (VERP)</span>
            <input
              type="text"
              placeholder="bounces.example.com"
              value={form.return_path_domain ?? ""}
              onChange={(e) =>
                setForm((f) => ({ ...f, return_path_domain: e.target.value }))
              }
            />
          </label>

          <div className={styles.subheading}>IMAP bounce mailbox</div>
          <label className={styles.field}>
            <span>Host</span>
            <input
              type="text"
              value={form.imap?.host ?? ""}
              onChange={(e) =>
                setForm((f) => ({
                  ...f,
                  imap: { ...f.imap, host: e.target.value },
                }))
              }
            />
          </label>
          <label className={styles.field}>
            <span>Port</span>
            <input
              type="number"
              value={form.imap?.port ?? 993}
              onChange={(e) =>
                setForm((f) => ({
                  ...f,
                  imap: {
                    ...f.imap,
                    port: parseInt(e.target.value, 10) || 993,
                  },
                }))
              }
            />
          </label>
          <label className={styles.field}>
            <span>Username</span>
            <input
              type="text"
              value={form.imap?.username ?? ""}
              onChange={(e) =>
                setForm((f) => ({
                  ...f,
                  imap: { ...f.imap, username: e.target.value },
                }))
              }
            />
          </label>
          <label className={styles.field}>
            <span>
              Password env var{" "}
              <small>(name only — never persisted)</small>
            </span>
            <input
              type="text"
              value={form.imap?.password_env ?? ""}
              onChange={(e) =>
                setForm((f) => ({
                  ...f,
                  imap: { ...f.imap, password_env: e.target.value },
                }))
              }
            />
          </label>
          <label className={styles.field}>
            <span>Folder</span>
            <input
              type="text"
              value={form.imap?.folder ?? "INBOX"}
              onChange={(e) =>
                setForm((f) => ({
                  ...f,
                  imap: { ...f.imap, folder: e.target.value },
                }))
              }
            />
          </label>

          <div className={styles.subheading}>Limits</div>
          <label className={styles.field}>
            <span>Max batch size</span>
            <input
              type="number"
              value={form.max_batch_size ?? 100}
              onChange={(e) =>
                setForm((f) => ({
                  ...f,
                  max_batch_size: parseInt(e.target.value, 10) || 100,
                }))
              }
            />
          </label>
          <label className={styles.field}>
            <span>Wait window (hours)</span>
            <input
              type="number"
              value={form.wait_window_hours ?? 48}
              onChange={(e) =>
                setForm((f) => ({
                  ...f,
                  wait_window_hours: parseInt(e.target.value, 10) || 48,
                }))
              }
            />
          </label>

          <label className={styles.authRow}>
            <input
              type="checkbox"
              checked={form.authorization_confirmed ?? false}
              onChange={(e) =>
                setForm((f) => ({
                  ...f,
                  authorization_confirmed: e.target.checked,
                }))
              }
            />
            <span>{PILOT_SEND_COPY.authConfirm}</span>
          </label>

          <button
            type="button"
            className={styles.btnPrimary}
            onClick={onSaveConfig}
            disabled={busy !== "idle"}
          >
            {busy === "saving"
              ? "Saving…"
              : PILOT_SEND_COPY.cta.saveConfig}
          </button>
        </div>
      )}

      {/* Action row */}
      <div className={styles.actions}>
        <label className={styles.batchInput}>
          <span>Batch size</span>
          <input
            type="number"
            min={1}
            max={status?.max_batch_size ?? 100}
            value={batchSize}
            onChange={(e) =>
              setBatchSize(parseInt(e.target.value, 10) || 1)
            }
          />
        </label>
        <button
          type="button"
          onClick={onPreview}
          disabled={busy !== "idle"}
          className={styles.btnSecondary}
        >
          {busy === "previewing"
            ? "Previewing…"
            : PILOT_SEND_COPY.cta.preview}
        </button>
        <button
          type="button"
          onClick={onLaunch}
          disabled={launchDisabled}
          className={styles.btnDanger}
        >
          {busy === "launching"
            ? "Launching…"
            : PILOT_SEND_COPY.cta.launch}
        </button>
        <button
          type="button"
          onClick={onPoll}
          disabled={busy !== "idle" || !status?.imap_configured}
          className={styles.btnSecondary}
        >
          {busy === "polling"
            ? "Polling…"
            : PILOT_SEND_COPY.cta.pollBounces}
        </button>
        <button
          type="button"
          onClick={onFinalize}
          disabled={busy !== "idle" || (c?.verdict_ready ?? 0) === 0}
          className={styles.btnPrimary}
        >
          {busy === "finalizing"
            ? "Re-cleaning…"
            : PILOT_SEND_COPY.cta.finalize}
        </button>
      </div>

      {preview && preview.length > 0 && (
        <div className={styles.previewBlock}>
          <div className={styles.subheading}>
            {preview.length} candidates ready for launch:
          </div>
          <ul className={styles.previewList}>
            {preview.slice(0, 20).map((c, i) => (
              <li key={i}>
                <code>{c.email}</code>{" "}
                <span className={styles.muted}>
                  {c.action} · {c.provider_family} ·{" "}
                  prob {c.deliverability_probability.toFixed(2)}
                </span>
              </li>
            ))}
            {preview.length > 20 && (
              <li className={styles.muted}>
                … and {preview.length - 20} more
              </li>
            )}
          </ul>
        </div>
      )}

      {error && <div className={styles.error}>{error}</div>}
    </div>
  );
}
