"use client";

import { Topbar } from "@/components/Topbar";
import s from "../mockup-page.module.css";

export default function DomainAuditPage() {
  return (
    <>
      <div className="fade-up">
        <Topbar
          breadcrumb={["TOOLS", "DOMAIN AUDIT"]}
          title="DOMAIN/AUDIT"
          titleSlice="/"
          subtitle="Inspect deliverability infrastructure for any domain"
        />
      </div>

      {/* Dev banner */}
      <div className={`fade-up ${s.devBanner}`}>
        <div className={s.devDot} />
        <span className={s.devText}>MODULE IN DEVELOPMENT — PREVIEW MODE</span>
        <span className={s.devRelease}>ETA v0.6.0</span>
      </div>

      {/* Stats row */}
      <div className={`fade-up ${s.statsRow}`}>
        <div className={s.stat}>
          <div className={`${s.statValue} ${s.accent}`}>12,840</div>
          <div className={s.statLabel}>Domains Audited</div>
        </div>
        <div className={s.stat}>
          <div className={`${s.statValue} ${s.info}`}>61%</div>
          <div className={s.statLabel}>Pass Rate (SPF)</div>
        </div>
        <div className={s.stat}>
          <div className={`${s.statValue} ${s.warn}`}>34%</div>
          <div className={s.statLabel}>Missing DMARC</div>
        </div>
        <div className={s.stat}>
          <div className={`${s.statValue} ${s.muted}`}>OFFLINE</div>
          <div className={s.statLabel}>Module Status</div>
        </div>
      </div>

      {/* Deliverability score (mock) */}
      <div className={`fade-up ${s.scorePanel}`}>
        <div className={s.scoreRing}>
          <svg viewBox="0 0 90 90">
            <circle cx="45" cy="45" r="38" fill="none" stroke="var(--stroke-steel)" strokeWidth="6" />
            <circle
              cx="45" cy="45" r="38"
              fill="none"
              stroke="var(--warn)"
              strokeWidth="6"
              strokeDasharray={`${2 * Math.PI * 38 * 0.62} ${2 * Math.PI * 38 * 0.38}`}
              strokeLinecap="round"
            />
          </svg>
          <span className={s.scoreNumber}>62</span>
        </div>
        <div className={s.scoreMeta}>
          <div className={s.scoreTitle}>DELIVERABILITY SCORE</div>
          <div className={s.scoreDesc}>
            Sample domain <strong>acme.com</strong> — SPF present, DKIM misconfigured,
            DMARC policy missing. Three MX records found. Not on any major blacklist.
            Estimated inbox rate: 61–68%.
          </div>
        </div>
      </div>

      {/* Domain input (disabled) */}
      <div className={`fade-up ${s.hero}`}>
        <div className={s.sectionHead}>
          <span className={s.sectionTitle}>Audit a Domain</span>
        </div>
        <div className={s.inputArea}>
          <div className={s.fakeInput}>e.g. example.com</div>
          <button className={s.disabledBtn} disabled>RUN AUDIT →</button>
        </div>
      </div>

      {/* Mock check results */}
      <div className="fade-up">
        <div className={s.sectionHead}>
          <span className={s.sectionTitle}>Audit Results — acme.com (sample)</span>
        </div>
        <div className={s.checkGrid}>
          <div className={s.checkRow}>
            <div>
              <div className={s.checkName}>SPF Record</div>
              <div className={s.checkSub}>v=spf1 include:_spf.google.com ~all</div>
            </div>
            <div className={s.checkValue}>1 record</div>
            <span className={`${s.pill} ${s.pass}`}>PASS</span>
          </div>

          <div className={s.checkRow}>
            <div>
              <div className={s.checkName}>DKIM</div>
              <div className={s.checkSub}>google._domainkey — key present, rotation overdue</div>
            </div>
            <div className={s.checkValue}>2048-bit</div>
            <span className={`${s.pill} ${s.warn}`}>WARN</span>
          </div>

          <div className={s.checkRow}>
            <div>
              <div className={s.checkName}>DMARC Policy</div>
              <div className={s.checkSub}>No _dmarc TXT record found</div>
            </div>
            <div className={s.checkValue}>—</div>
            <span className={`${s.pill} ${s.fail}`}>FAIL</span>
          </div>

          <div className={s.checkRow}>
            <div>
              <div className={s.checkName}>MX Records</div>
              <div className={s.checkSub}>aspmx.l.google.com (pri 1), alt1, alt2</div>
            </div>
            <div className={s.checkValue}>3 records</div>
            <span className={`${s.pill} ${s.pass}`}>PASS</span>
          </div>

          <div className={s.checkRow}>
            <div>
              <div className={s.checkName}>Blacklist Check</div>
              <div className={s.checkSub}>Spamhaus, Barracuda, SURBL, MXToolbox</div>
            </div>
            <div className={s.checkValue}>0 / 4 listed</div>
            <span className={`${s.pill} ${s.pass}`}>PASS</span>
          </div>

          <div className={s.checkRow}>
            <div>
              <div className={s.checkName}>Reverse DNS (PTR)</div>
              <div className={s.checkSub}>mail.acme.com → 74.125.28.27 — forward confirmed</div>
            </div>
            <div className={s.checkValue}>match</div>
            <span className={`${s.pill} ${s.pass}`}>PASS</span>
          </div>

          <div className={s.checkRow}>
            <div>
              <div className={s.checkName}>BIMI Record</div>
              <div className={s.checkSub}>Brand Indicators for Message Identification</div>
            </div>
            <div className={s.checkValue}>—</div>
            <span className={`${s.pill} ${s.muted}`}>N/A</span>
          </div>
        </div>
      </div>

      {/* Feature cards */}
      <div className="fade-up">
        <div className={s.sectionHead}>
          <span className={s.sectionTitle}>Capabilities</span>
        </div>
        <div className={s.featureGrid}>
          <div className={s.featureCard}>
            <svg className={s.cardIcon} viewBox="0 0 24 24">
              <path d="M9 11l3 3L22 4" />
              <path d="M21 12v7a2 2 0 01-2 2H5a2 2 0 01-2-2V5a2 2 0 012-2h11" />
            </svg>
            <div className={s.cardTitle}>Full DNS Health Check</div>
            <div className={s.cardDesc}>
              SPF, DKIM, DMARC, MX, PTR, and BIMI — all in a single request.
              Returns structured pass/warn/fail per check with remediation hints.
            </div>
            <span className={s.cardBadge}>PLANNED</span>
          </div>

          <div className={s.featureCard}>
            <svg className={s.cardIcon} viewBox="0 0 24 24">
              <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z" />
            </svg>
            <div className={s.cardTitle}>Blacklist Monitor</div>
            <div className={s.cardDesc}>
              Check against 40+ IP and domain reputation lists in real time.
              Alerts when a domain appears on a new list between audit runs.
            </div>
            <span className={s.cardBadge}>PLANNED</span>
          </div>

          <div className={s.featureCard}>
            <svg className={s.cardIcon} viewBox="0 0 24 24">
              <polyline points="22 12 18 12 15 21 9 3 6 12 2 12" />
            </svg>
            <div className={s.cardTitle}>Deliverability Score</div>
            <div className={s.cardDesc}>
              Composite 0–100 score derived from DNS config, blacklist status,
              and historical bounce data. Tracks change over time.
            </div>
            <span className={s.cardBadge}>PLANNED</span>
          </div>

          <div className={s.featureCard}>
            <svg className={s.cardIcon} viewBox="0 0 24 24">
              <path d="M13 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V9z" />
              <polyline points="13 2 13 9 20 9" />
            </svg>
            <div className={s.cardTitle}>Audit Reports</div>
            <div className={s.cardDesc}>
              Export full audit reports as PDF or JSON. Share a shareable link
              with your team or attach to a client deliverable.
            </div>
            <span className={s.cardBadge}>PLANNED</span>
          </div>
        </div>
      </div>
    </>
  );
}
