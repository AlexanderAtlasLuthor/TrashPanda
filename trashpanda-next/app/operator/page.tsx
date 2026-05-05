"use client";

import { useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { OperatorConsoleShell } from "@/components/operator/OperatorConsoleShell";
import s from "./page.module.css";

interface OperatorCard {
  key: string;
  title: string;
  desc: string;
  badge: string;
  href?: string;
  primary?: boolean;
}

const OPERATOR_CARDS: ReadonlyArray<OperatorCard> = [
  {
    key: "preflight",
    title: "Preflight",
    desc: "Run large-list safety checks before cleaning starts.",
    badge: "READY",
    href: "/operator/preflight",
    primary: true,
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
    badge: "READY",
    href: "/operator/feedback",
  },
];

function operatorJobRedirect(jobId: string): string {
  return `/operator/jobs/${encodeURIComponent(jobId)}`;
}

export default function OperatorConsolePage() {
  const router = useRouter();
  const [jobIdInput, setJobIdInput] = useState("");

  const trimmed = jobIdInput.trim();
  const canSubmit = trimmed.length > 0;

  const handleSubmit = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!canSubmit) return;
    router.push(operatorJobRedirect(trimmed));
  };

  return (
    <OperatorConsoleShell>
      <section className={`fade-up ${s.hero}`}>
        <div className={s.heroLeft}>
          <div className={s.kicker}>// V2 OPERATOR CONSOLE</div>
          <h2 className={s.heroTitle}>
            Cleared for <span className={s.accent}>operator review</span>
          </h2>
          <p className={s.heroSubtitle}>
            Operator launches are gated. Run preflight before uploading the
            cleaning input. Client delivery still flows exclusively through
            the safe download endpoint when{" "}
            <strong>ready_for_client === true</strong>.
          </p>
        </div>
        <div className={s.heroActions}>
          <Link href="/operator/preflight" className={s.heroPrimary}>
            Start with preflight
          </Link>
        </div>
      </section>

      <section className="fade-up">
        <div className={s.sectionHead}>
          <span className={s.sectionTitle}>Workflows</span>
        </div>
        <div className={s.workflowGrid}>
          {OPERATOR_CARDS.map((card) => {
            const className = [
              s.workflowCard,
              card.primary && s.workflowCardPrimary,
              !card.href && s.workflowCardDisabled,
            ]
              .filter(Boolean)
              .join(" ");
            const inner = (
              <>
                <div className={s.workflowHead}>
                  <span className={s.workflowTitle}>{card.title}</span>
                  <span
                    className={
                      card.badge === "READY"
                        ? `${s.workflowBadge} ${s.workflowBadgeReady}`
                        : s.workflowBadge
                    }
                  >
                    {card.badge}
                  </span>
                </div>
                <div className={s.workflowDesc}>{card.desc}</div>
                {card.href && (
                  <span className={s.workflowChevron} aria-hidden>
                    →
                  </span>
                )}
              </>
            );
            if (card.href) {
              return (
                <Link key={card.key} href={card.href} className={className}>
                  {inner}
                </Link>
              );
            }
            return (
              <div key={card.key} className={className} aria-disabled="true">
                {inner}
              </div>
            );
          })}
        </div>
      </section>

      <section className="fade-up">
        <div className={s.sectionHead}>
          <span className={s.sectionTitle}>Open an existing job</span>
        </div>
        <p className={s.sectionLead}>
          Already have a job id? Jump straight to its operator surface
          without uploading again.
        </p>
        <form
          onSubmit={handleSubmit}
          aria-label="Open operator job"
          className={s.locator}
        >
          <label htmlFor="operator-job-id" className={s.label}>
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
            className={s.input}
          />
          <button
            type="submit"
            disabled={!canSubmit}
            className={s.submit}
          >
            Open job
          </button>
        </form>
      </section>
    </OperatorConsoleShell>
  );
}
