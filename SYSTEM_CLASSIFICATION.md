# System Classification

## Purpose

This document classifies current repository components so future SaaS
transformation work does not confuse core product logic with temporary
infrastructure, mock behavior, or preview UI.

## Classification table

| Component | Location | Classification | Notes |
|---|---|---|---|
| Main pipeline | `app/pipeline.py` | Core engine | Primary processing orchestration through `EmailCleaningPipeline` |
| Stage framework | `app/engine/` | Core engine | Stage execution, context, payload, stage abstractions |
| Stage implementations | `app/engine/stages/` | Core engine | Preprocess, email processing, enrichment, scoring, postprocessing |
| Input utilities | `app/io_utils.py` | Core engine support | CSV/XLSX reading and chunk handling |
| Configuration loader | `app/config.py` | Engine support with platform risk | Valid for engine config, but public `config_path` exposure is not SaaS-safe |
| Default config | `configs/default.yaml` | Engine defaults | Captures conservative product behavior |
| Normalizers | `app/normalizers.py` | Core engine | Data cleanup behavior |
| Validators/rules | `app/validators.py`, `app/email_rules.py`, `app/rules.py` | Core engine | Validation and rule logic |
| DNS utility | `app/dns_utils.py` | Core engine | DNS/MX enrichment |
| Deduplication | `app/dedupe.py` | Core engine | Duplicate detection and winner selection |
| Baseline scoring | `app/scoring.py` | Core engine | Deterministic scoring/bucket behavior |
| Scoring V2 | `app/scoring_v2/` | Core engine | Explainable scoring layer |
| Validation V2 | `app/validation_v2/` | Core engine | Domain intelligence, SMTP-related signals, probability, decision annotations |
| Calibration/evaluation modules | `app/calibration_v2/`, `app/evaluation_v2/`, `app/rollout_v2/`, `evaluation_v2_2.py` | Engine research/support | Product-adjacent analysis and rollout support, not SaaS platform |
| Staging database | `app/storage.py` | Engine-local processing support | Useful for per-run materialization; not SaaS product persistence |
| Reporting | `app/reporting.py` | Core engine output | Technical reports |
| Client output | `app/client_output.py` | Core engine output | Client-facing XLSX exports |
| API boundary | `app/api_boundary.py` | Platform seed | Useful seam around engine execution, not full SaaS |
| FastAPI server | `app/server.py` | Temporary platform wrapper | Local HTTP API with temporary job/store/storage behavior |
| In-memory job store | `app/server.py` | Temporary infra | Not durable, not multi-tenant |
| Local background tasks | `app/server.py` | Temporary infra | Not a production queue |
| Runtime uploads/jobs/history | `runtime/` | Temporary/local infra | Local generated state, not SaaS storage |
| Output/log directories | `output/`, `logs/` | Temporary/local artifacts | Local artifacts/logs |
| CLI | `app/cli.py`, `app/__main__.py` | Local/operator interface | Useful internal execution path |
| Dev launch scripts | `start_trashpanda.*`, `stop_trashpanda.*`, `scripts/dev_launcher.py` | Local dev tooling | Not production platform |
| Frontend API client | `trashpanda-next/lib/api.ts` | Platform seed | Central browser-to-API contract |
| Backend adapter | `trashpanda-next/lib/backend-adapter.ts` | Platform seed/temporary bridge | Proxy/mock switch; not final platform boundary |
| Mock adapter | `trashpanda-next/lib/mock-adapter.ts` | Frontend mock feature | Development-only simulation |
| Shared frontend types | `trashpanda-next/lib/types.ts` | Platform seed | Contract types for frontend surfaces |
| App shell/sidebar/topbar | `trashpanda-next/components/AppShell.tsx`, `Sidebar.tsx`, `Topbar.tsx` | Frontend platform seed | Early SaaS shell, not complete account/org shell |
| Home dashboard | `trashpanda-next/components/HomeDashboard.tsx` | Frontend real feature with local assumptions | Useful entry point but not tenant-aware |
| Upload UI | `trashpanda-next/components/UploadDropzone.tsx` | Frontend real feature | Real upload flow |
| Recent jobs UI | `trashpanda-next/components/RecentJobs.tsx` | Frontend real feature with temporary behavior | Uses real job list but localStorage hiding is not real deletion |
| Results page | `trashpanda-next/app/results/[jobId]/ResultsClient.tsx` | Frontend real feature | Real job status/results surface |
| Review page | `trashpanda-next/app/review/[jobId]/ReviewQueueClient.tsx` | Frontend real feature | Real review workflow surface |
| Insights page | `trashpanda-next/app/insights/[jobId]/InsightsClient.tsx` | Frontend real feature | Real insights surface where data exists |
| Download UI | `trashpanda-next/components/DownloadArtifacts.tsx` | Frontend real feature | Real artifact download surface |
| Metrics/summary/breakdown UI | `trashpanda-next/components/MetricsCards.tsx`, `ExecutiveSummary.tsx`, `ClassificationBreakdown.tsx` | Frontend real feature | Real result presentation |
| AI narrative/review UI | `trashpanda-next/components/AINarrativePanel.tsx`, AI endpoints | Product adjunct | Useful optional feature, depends on external AI key and policy |
| Domain audit page | `trashpanda-next/app/domain-audit/page.tsx` | Frontend preview/mock feature | Preview/offline/planned, not real module |
| Lead discovery page | `trashpanda-next/app/lead-discovery/page.tsx` | Frontend preview/mock feature | Preview/offline/planned, not real module |
| Pipelines page | `trashpanda-next/app/pipelines/page.tsx` | Frontend preview/mock feature | Preview/offline/planned, not real scheduler |
| Static HTML files | `trashpanda.html`, `trashpanda-mobile.html` | Legacy/static artifact | Not the current SaaS app surface |
| Local datasets/assets | `WY.csv`, `test_subphase3.csv`, `TrashPanda logo.png`, `assets/` | Local data/assets | Not platform logic |
| Python/Node dependency folders | `.venv/`, `.vendor_py/`, `vendor_site/`, `node_modules/` | Local dependencies | Not product code |

## Classification rules

## Core engine

Core engine components produce data-hygiene value.

They should be protected during SaaS transformation.

## Temporary infra

Temporary infra supports local operation but is not production-compatible.

It should be replaced or surrounded by platform layers during SaaS
transformation.

## Platform seeds

Platform seeds are useful boundaries or UI shells that can inform SaaS work,
but they are not complete platform infrastructure.

## Frontend real features

Frontend real features are user-visible flows connected to actual job behavior.

They should be protected as product flows even if their data access model
changes.

## Frontend preview/mock features

Preview/mock features must not be used as proof of completed SaaS capability.

They may remain as clearly labeled previews or be hidden during SaaS V1 work.

## Pilot send verdict vocabulary

The pilot send tracker (`app/db/pilot_send_tracker.py`) classifies each
DSN into one of these verdicts:

| Verdict | Meaning | Routing |
|---|---|---|
| `delivered` | Wait window elapsed without a bounce. | `delivery_verified` |
| `hard_bounce` | Recipient-level 5xx (e.g. user unknown). | `do_not_send` |
| `soft_bounce` | Recipient-level 4xx. | review (transient) |
| `blocked` | Content / policy rejection (DMARC, spam keyword, content filter). | `do_not_send` |
| `deferred` | DSN `Action: delayed`. | review (transient) |
| `complaint` | ARF abuse report. | `do_not_send` |
| `infrastructure_blocked` | **Sender-side.** Recipient provider rejected our sending IP / network (e.g. Microsoft S3150 "block list", Spamhaus listing). Says nothing about the recipient. | review — re-test from clean IP |
| `provider_deferred` | **Sender-side.** Recipient provider throttled mail due to sender volume / reputation (e.g. Yahoo `TSS04`). Always transient. | review — re-test later |
| `unknown` | DSN couldn't be parsed. | review |

**Critical rule:** SMTP 4xx/5xx replies that reference the sender IP /
network (`messages from [ip] weren't sent`, `block list`, `TSS\d+`)
describe the *sender*, not the recipient. They must NEVER feed
`do_not_send` and never count toward `hard_bounce_rate`. Detection
patterns live in `app/pilot_send/bounce_parser.py`
(`_INFRA_BLOCK_PATTERNS`, `_PROVIDER_DEFER_PATTERNS`).

Operational details: see `deploy/PILOT_RUNBOOK.md`.
