"use client";

import { Topbar } from "@/components/Topbar";
import s from "../mockup-page.module.css";

export default function LeadDiscoveryPage() {
  return (
    <>
      <div className="fade-up">
        <Topbar
          breadcrumb={["TOOLS", "LEAD DISCOVERY"]}
          title="LEAD/DISCOVERY"
          titleSlice="/"
          subtitle="Find and verify leads from domain intelligence"
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
          <div className={`${s.statValue} ${s.accent}`}>4.2M</div>
          <div className={s.statLabel}>Domains Indexed</div>
        </div>
        <div className={s.stat}>
          <div className={`${s.statValue} ${s.info}`}>18.4</div>
          <div className={s.statLabel}>Avg Leads / Domain</div>
        </div>
        <div className={s.stat}>
          <div className={`${s.statValue} ${s.warn}`}>73%</div>
          <div className={s.statLabel}>Verify Rate</div>
        </div>
        <div className={s.stat}>
          <div className={`${s.statValue} ${s.muted}`}>OFFLINE</div>
          <div className={s.statLabel}>Module Status</div>
        </div>
      </div>

      {/* Search input (disabled) */}
      <div className={`fade-up ${s.hero}`}>
        <div className={s.sectionHead}>
          <span className={s.sectionTitle}>Domain Search</span>
        </div>
        <div className={s.inputArea}>
          <div className={s.fakeInput}>e.g. acme.com, techcorp.io…</div>
          <button className={s.disabledBtn} disabled>DISCOVER →</button>
        </div>
      </div>

      {/* Feature cards */}
      <div className="fade-up">
        <div className={s.sectionHead}>
          <span className={s.sectionTitle}>Capabilities</span>
        </div>
        <div className={`${s.featureGrid} ${s.cols3}`}>
          <div className={s.featureCard}>
            <svg className={s.cardIcon} viewBox="0 0 24 24">
              <circle cx="11" cy="11" r="7" />
              <path d="M21 21l-4.35-4.35" />
            </svg>
            <div className={s.cardTitle}>Domain-Based Discovery</div>
            <div className={s.cardDesc}>
              Scan any domain for email patterns, MX records, and contact
              signals. Extracts verified leads from public DNS and WHOIS data.
            </div>
            <span className={s.cardBadge}>PLANNED</span>
          </div>

          <div className={s.featureCard}>
            <svg className={s.cardIcon} viewBox="0 0 24 24">
              <rect x="2" y="7" width="20" height="14" rx="2" />
              <path d="M16 7V5a2 2 0 00-4 0v2" />
              <path d="M12 12v3" />
            </svg>
            <div className={s.cardTitle}>Company Lookup</div>
            <div className={s.cardDesc}>
              Resolve a company name to its primary domain and infer standard
              email formats using pattern analysis from your existing clean data.
            </div>
            <span className={s.cardBadge}>PLANNED</span>
          </div>

          <div className={s.featureCard}>
            <svg className={s.cardIcon} viewBox="0 0 24 24">
              <path d="M4 6h16M4 12h10M4 18h6" />
            </svg>
            <div className={s.cardTitle}>Industry Filter</div>
            <div className={s.cardDesc}>
              Narrow discovery by SIC code, company size, or tech stack signals.
              Integrates with enrichment sources to qualify leads before export.
            </div>
            <span className={s.cardBadge}>PLANNED</span>
          </div>

          <div className={s.featureCard}>
            <svg className={s.cardIcon} viewBox="0 0 24 24">
              <path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4" />
              <polyline points="7 10 12 15 17 10" />
              <line x1="12" y1="15" x2="12" y2="3" />
            </svg>
            <div className={s.cardTitle}>Bulk Export</div>
            <div className={s.cardDesc}>
              Export discovered leads as CSV or push directly into your CRM via
              webhook. Includes confidence score and source attribution for each
              contact.
            </div>
            <span className={s.cardBadge}>PLANNED</span>
          </div>

          <div className={s.featureCard}>
            <svg className={s.cardIcon} viewBox="0 0 24 24">
              <path d="M12 2l3 7h7l-5.5 4.5L18 21l-6-4-6 4 1.5-7.5L2 9h7z" />
            </svg>
            <div className={s.cardTitle}>Lead Scoring</div>
            <div className={s.cardDesc}>
              Automatic quality score based on DNS health, email pattern
              confidence, engagement signals, and historical deliverability from
              past TrashPanda runs.
            </div>
            <span className={s.cardBadge}>PLANNED</span>
          </div>

          <div className={s.featureCard}>
            <svg className={s.cardIcon} viewBox="0 0 24 24">
              <circle cx="12" cy="12" r="3" />
              <path d="M12 1v4M12 19v4M4.22 4.22l2.83 2.83M16.95 16.95l2.83 2.83M1 12h4M19 12h4M4.22 19.78l2.83-2.83M16.95 7.05l2.83-2.83" />
            </svg>
            <div className={s.cardTitle}>Real-Time Enrichment</div>
            <div className={s.cardDesc}>
              Stream enrichment data as you process — no waiting for batch
              jobs. Works alongside the existing TrashPanda cleaning pipeline.
            </div>
            <span className={s.cardBadge}>PLANNED</span>
          </div>
        </div>
      </div>

      {/* Empty state */}
      <div className={`fade-up ${s.emptyState}`}>
        <svg className={s.emptyIcon} viewBox="0 0 24 24">
          <circle cx="11" cy="11" r="7" />
          <path d="M21 21l-4.35-4.35" />
        </svg>
        <div className={s.emptyTitle}>No leads discovered yet</div>
        <div className={s.emptyDesc}>
          Lead Discovery is coming in a future release. In the meantime, use
          the Console to clean and verify your existing contact lists.
        </div>
      </div>
    </>
  );
}
