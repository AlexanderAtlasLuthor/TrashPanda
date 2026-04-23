# Engine Vs Platform

## Purpose

This document separates the value-generating processing engine from the future
SaaS platform layers.

The transformation must preserve this boundary. Mixing SaaS concerns directly
into engine logic would make the system harder to test, harder to reuse, and
more dangerous to evolve.

## Definitions

## Engine

The Engine is the product logic that processes input data and produces hygiene,
validation, classification, review, reporting, and export outputs.

The Engine answers questions like:

- What columns and values are present?
- Which field is the email field?
- Is the email syntactically valid?
- What is the domain?
- Does the domain have usable DNS/MX signals?
- Is the address likely disposable, fake, role-based, duplicated, or risky?
- Should this record be valid, review, or removed?
- What explanation should be shown?
- What files and reports should be generated?

## Platform

The Platform is the SaaS layer that lets customers safely use the Engine.

The Platform answers questions like:

- Who is the user?
- Which organization owns this job?
- Is this user allowed to upload, view, review, export, or delete this job?
- Where are files stored durably?
- How are jobs persisted?
- How are jobs queued and retried?
- What plan or limit applies?
- What audit trail exists?
- What operational events should be observed?

## Existing Engine components

| Component | Location | Classification |
|---|---|---|
| Pipeline orchestration | `app/pipeline.py` | Engine |
| Generic stage runner | `app/engine/` | Engine |
| Stage implementations | `app/engine/stages/` | Engine |
| Input parsing | `app/io_utils.py` | Engine support |
| Normalization | `app/normalizers.py` | Engine |
| Validation rules | `app/validators.py`, `app/email_rules.py`, `app/rules.py` | Engine |
| DNS/MX logic | `app/dns_utils.py` | Engine |
| Deduplication | `app/dedupe.py` | Engine |
| Baseline scoring | `app/scoring.py` | Engine |
| Scoring V2 | `app/scoring_v2/` | Engine |
| Validation V2 | `app/validation_v2/` | Engine |
| Reporting | `app/reporting.py` | Engine output |
| Client exports | `app/client_output.py` | Engine output |
| Staging DB | `app/storage.py` | Engine-local processing support, not SaaS persistence |

## Existing Platform seeds

These components are early platform seeds, but they are not full SaaS platform
components yet:

| Component | Location | Current role |
|---|---|---|
| API boundary | `app/api_boundary.py` | Wraps engine execution and serializes results |
| FastAPI server | `app/server.py` | Local HTTP API wrapper |
| Frontend API client | `trashpanda-next/lib/api.ts` | Central frontend access point |
| Backend adapter | `trashpanda-next/lib/backend-adapter.ts` | Proxy/mock switch |
| App shell | `trashpanda-next/components/AppShell.tsx` | Early navigation shell |
| Results UI | `trashpanda-next/app/results/[jobId]/ResultsClient.tsx` | Real job result surface |
| Review UI | `trashpanda-next/app/review/[jobId]/ReviewQueueClient.tsx` | Real review workflow surface |
| Insights UI | `trashpanda-next/app/insights/[jobId]/InsightsClient.tsx` | Real insights surface |

## Missing Platform components

The following Platform components do not exist yet:

- Authentication.
- User accounts.
- Organizations/workspaces.
- Memberships and roles.
- Invitations.
- Tenant-scoped job persistence.
- Tenant-scoped file/artifact storage.
- Authorization for reads, writes, reviews, and downloads.
- Durable job queue.
- Job event log.
- Audit log.
- Billing/usage tracking.
- Customer settings.
- Admin/support tooling.
- Production observability.

## Boundary rules

These rules are strict.

1. The Engine must not know about SaaS users.
2. The Engine must not know about organizations.
3. The Engine must not know about billing plans.
4. The Engine must not perform authorization checks.
5. The Engine must not depend on web sessions or cookies.
6. The Engine must not assume local filesystem storage as a SaaS contract.
7. The Platform must not reimplement validation/scoring logic that already lives in the Engine.
8. The Platform may pass input paths, configuration, and output destinations to the Engine through stable contracts.
9. The Platform owns persistence, ownership, access control, and job lifecycle.
10. The Engine owns data-hygiene decisions, classification, reports, and exports.

## Correct relationship

The Platform should call the Engine.

The Engine should return structured results, artifacts, summaries, and signals.

The Platform should persist, authorize, expose, and operate those results.

## Incorrect relationship

The following would be architectural mistakes:

- Adding `organization_id` checks inside scoring logic.
- Adding billing logic inside `EmailCleaningPipeline`.
- Duplicating scoring rules in frontend code.
- Letting route handlers mutate engine classification rules.
- Treating local runtime paths as permanent product identifiers.
- Letting engine modules read authenticated user/session state.

## Current best boundary

The current best boundary is `app/api_boundary.py`.

It is not a complete SaaS boundary, but it already separates direct engine
execution from HTTP route handling. Future transformation should preserve this
conceptual seam while replacing temporary infrastructure around it.
