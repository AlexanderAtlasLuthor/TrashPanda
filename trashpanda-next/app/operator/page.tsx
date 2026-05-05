"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { OperatorConsoleShell } from "@/components/operator/OperatorConsoleShell";
import s from "../mockup-page.module.css";
import locator from "./page.module.css";

interface OperatorCard {
  key: string;
  title: string;
  desc: string;
  badge: string;
}

const OPERATOR_CARDS: ReadonlyArray<OperatorCard> = [
  {
    key: "preflight",
    title: "Preflight",
    desc: "Run large-list safety checks before cleaning starts.",
    badge: "COMING NEXT",
  },
  {
    key: "job_review",
    title: "Job Review",
    desc:
      "Inspect package, SMTP runtime, artifact consistency, and review-gate status.",
    badge: "READY",
  },
  {
    key: "feedback",
    title: "Feedback",
    desc: "Ingest bounce feedback and preview domain intelligence impact.",
    badge: "COMING NEXT",
  },
];

export default function OperatorConsolePage() {
  const router = useRouter();
  const [jobIdInput, setJobIdInput] = useState("");

  const trimmed = jobIdInput.trim();
  const canSubmit = trimmed.length > 0;

  const handleSubmit = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!canSubmit) return;
    router.push(`/operator/jobs/${encodeURIComponent(trimmed)}`);
  };

  return (
    <OperatorConsoleShell>
      <div className={`fade-up ${s.hero}`}>
        <div className={s.sectionHead}>
          <span className={s.sectionTitle}>V2 Operator Console</span>
        </div>
        <p className={s.heroDesc}>
          Client delivery flows exclusively through the safe download
          endpoint. Operator review must surface{" "}
          <strong>ready_for_client === true</strong> before any package
          leaves this console. Operator workflows are job-scoped — paste a
          job id below to inspect its package and review-gate status.
        </p>
      </div>

      <div className="fade-up">
        <form
          onSubmit={handleSubmit}
          aria-label="Open operator job"
          className={locator.locator}
        >
          <label htmlFor="operator-job-id" className={locator.label}>
            Job ID
          </label>
          <input
            id="operator-job-id"
            type="text"
            value={jobIdInput}
            onChange={(e) => setJobIdInput(e.target.value)}
            placeholder="job_20260101_120000_abc123"
            autoComplete="off"
            spellCheck={false}
            className={locator.input}
          />
          <button
            type="submit"
            disabled={!canSubmit}
            className={locator.submit}
          >
            Open job
          </button>
        </form>
      </div>

      <div className="fade-up">
        <div className={s.sectionHead}>
          <span className={s.sectionTitle}>Workflows</span>
        </div>
        <div className={`${s.featureGrid} ${s.cols3}`}>
          {OPERATOR_CARDS.map((card) => (
            <div key={card.key} className={s.featureCard}>
              <div className={s.cardTitle}>{card.title}</div>
              <div className={s.cardDesc}>{card.desc}</div>
              <span className={s.cardBadge}>{card.badge}</span>
            </div>
          ))}
        </div>
      </div>
    </OperatorConsoleShell>
  );
}
