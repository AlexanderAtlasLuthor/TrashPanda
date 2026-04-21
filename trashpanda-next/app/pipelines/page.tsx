"use client";

import { Topbar } from "@/components/Topbar";
import s from "../mockup-page.module.css";

export default function PipelinesPage() {
  return (
    <>
      <div className="fade-up">
        <Topbar
          breadcrumb={["TOOLS", "PIPELINES"]}
          title="AUTO/PIPELINES"
          titleSlice="/"
          subtitle="Schedule and automate recurring data cleaning jobs"
        />
      </div>

      {/* Dev banner */}
      <div className={`fade-up ${s.devBanner}`}>
        <div className={s.devDot} />
        <span className={s.devText}>MODULE IN DEVELOPMENT — PREVIEW MODE</span>
        <span className={s.devRelease}>ETA v0.7.0</span>
      </div>

      {/* Stats row */}
      <div className={`fade-up ${s.statsRow}`}>
        <div className={s.stat}>
          <div className={`${s.statValue} ${s.accent}`}>3</div>
          <div className={s.statLabel}>Active Pipelines</div>
        </div>
        <div className={s.stat}>
          <div className={`${s.statValue} ${s.info}`}>2.1M</div>
          <div className={s.statLabel}>Rows Processed</div>
        </div>
        <div className={s.stat}>
          <div className={`${s.statValue} ${s.warn}`}>1</div>
          <div className={s.statLabel}>Needs Attention</div>
        </div>
        <div className={s.stat}>
          <div className={`${s.statValue} ${s.muted}`}>OFFLINE</div>
          <div className={s.statLabel}>Scheduler Status</div>
        </div>
      </div>

      {/* Pipeline list */}
      <div className="fade-up">
        <div className={s.pipelineListHeader}>
          <div className={s.sectionHead} style={{ marginBottom: 0 }}>
            <span className={s.sectionTitle}>Configured Pipelines</span>
          </div>
          <button className={s.disabledBtn} disabled style={{ padding: "7px 16px", fontSize: 11 }}>
            + NEW PIPELINE
          </button>
        </div>
        <div className={s.pipelineList}>
          <div className={s.pipelineCard}>
            <div>
              <div className={s.pipelineName}>Weekly CRM Sync</div>
              <div className={s.pipelineMeta}>
                <span>SCHEDULE: Every Monday 03:00 UTC</span>
                <span>TRIGGER: Cron</span>
                <span>LAST RUN: 6 days ago</span>
                <span>ROWS: 48,200</span>
              </div>
            </div>
            <div className={s.pipelineActions}>
              <span className={`${s.pill} ${s.pass}`}>ACTIVE</span>
              <div className={s.iconBtn}>
                <svg viewBox="0 0 24 24"><path d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7" /><path d="M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z" /></svg>
              </div>
              <div className={s.iconBtn}>
                <svg viewBox="0 0 24 24"><polyline points="3 6 5 6 21 6" /><path d="M19 6l-1 14a2 2 0 01-2 2H8a2 2 0 01-2-2L5 6" /><path d="M10 11v6M14 11v6" /><path d="M9 6V4a1 1 0 011-1h4a1 1 0 011 1v2" /></svg>
              </div>
            </div>
          </div>

          <div className={s.pipelineCard}>
            <div>
              <div className={s.pipelineName}>New Signup Validator</div>
              <div className={s.pipelineMeta}>
                <span>SCHEDULE: On webhook trigger</span>
                <span>TRIGGER: Webhook</span>
                <span>LAST RUN: 2 hours ago</span>
                <span>ROWS: 1 (real-time)</span>
              </div>
            </div>
            <div className={s.pipelineActions}>
              <span className={`${s.pill} ${s.warn}`}>WARN</span>
              <div className={s.iconBtn}>
                <svg viewBox="0 0 24 24"><path d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7" /><path d="M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z" /></svg>
              </div>
              <div className={s.iconBtn}>
                <svg viewBox="0 0 24 24"><polyline points="3 6 5 6 21 6" /><path d="M19 6l-1 14a2 2 0 01-2 2H8a2 2 0 01-2-2L5 6" /><path d="M10 11v6M14 11v6" /><path d="M9 6V4a1 1 0 011-1h4a1 1 0 011 1v2" /></svg>
              </div>
            </div>
          </div>

          <div className={s.pipelineCard}>
            <div>
              <div className={s.pipelineName}>Quarterly Hygiene</div>
              <div className={s.pipelineMeta}>
                <span>SCHEDULE: 1st of Jan, Apr, Jul, Oct</span>
                <span>TRIGGER: Cron</span>
                <span>LAST RUN: 18 days ago</span>
                <span>ROWS: 1,200,000</span>
              </div>
            </div>
            <div className={s.pipelineActions}>
              <span className={`${s.pill} ${s.muted}`}>IDLE</span>
              <div className={s.iconBtn}>
                <svg viewBox="0 0 24 24"><path d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7" /><path d="M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z" /></svg>
              </div>
              <div className={s.iconBtn}>
                <svg viewBox="0 0 24 24"><polyline points="3 6 5 6 21 6" /><path d="M19 6l-1 14a2 2 0 01-2 2H8a2 2 0 01-2-2L5 6" /><path d="M10 11v6M14 11v6" /><path d="M9 6V4a1 1 0 011-1h4a1 1 0 011 1v2" /></svg>
              </div>
            </div>
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
              <circle cx="12" cy="12" r="9" />
              <path d="M12 7v5l3 3" />
            </svg>
            <div className={s.cardTitle}>Cron Scheduling</div>
            <div className={s.cardDesc}>
              Define pipelines with cron expressions — hourly, daily, weekly,
              or custom intervals. Runs are tracked with full audit logs.
            </div>
            <span className={s.cardBadge}>PLANNED</span>
          </div>

          <div className={s.featureCard}>
            <svg className={s.cardIcon} viewBox="0 0 24 24">
              <path d="M5 12h14M12 5l7 7-7 7" />
            </svg>
            <div className={s.cardTitle}>Webhook Triggers</div>
            <div className={s.cardDesc}>
              Kick off a pipeline via HTTP POST. Plug directly into your CRM,
              form platform, or CI/CD pipeline for real-time validation.
            </div>
            <span className={s.cardBadge}>PLANNED</span>
          </div>

          <div className={s.featureCard}>
            <svg className={s.cardIcon} viewBox="0 0 24 24">
              <path d="M18 20V10M12 20V4M6 20v-6" />
            </svg>
            <div className={s.cardTitle}>Run History</div>
            <div className={s.cardDesc}>
              Browse past runs with duration, row counts, clean rates, and
              error breakdowns. Drill into any run to view full logs.
            </div>
            <span className={s.cardBadge}>PLANNED</span>
          </div>

          <div className={s.featureCard}>
            <svg className={s.cardIcon} viewBox="0 0 24 24">
              <path d="M22 16.92v3a2 2 0 01-2.18 2 19.79 19.79 0 01-8.63-3.07A19.5 19.5 0 013.07 9.82a19.79 19.79 0 01-3.07-8.67A2 2 0 012 1h3a2 2 0 012 1.72c.127.96.361 1.903.7 2.81a2 2 0 01-.45 2.11L6.09 8.91a16 16 0 006 6l1.27-1.27a2 2 0 012.11-.45c.907.339 1.85.573 2.81.7A2 2 0 0122 16.92z" />
            </svg>
            <div className={s.cardTitle}>Alerts & Notifications</div>
            <div className={s.cardDesc}>
              Get notified on failure, high bounce rates, or blacklist hits via
              email or Slack. Configurable thresholds per pipeline.
            </div>
            <span className={s.cardBadge}>PLANNED</span>
          </div>
        </div>
      </div>

      {/* Empty state */}
      <div className={`fade-up ${s.emptyState}`}>
        <svg className={s.emptyIcon} viewBox="0 0 24 24">
          <circle cx="12" cy="12" r="9" />
          <path d="M12 7v5l3 3" />
        </svg>
        <div className={s.emptyTitle}>No pipeline runs yet</div>
        <div className={s.emptyDesc}>
          Automated pipelines are coming in a future release. For now, process
          files manually from the Console.
        </div>
      </div>
    </>
  );
}
