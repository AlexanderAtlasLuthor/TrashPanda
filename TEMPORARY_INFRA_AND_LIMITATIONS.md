# Temporary Infrastructure And Limitations

## Purpose

This document identifies the parts of the current system that are temporary,
local-only, single-user, mock-based, or otherwise not compatible with a real
SaaS product.

These components may be useful during development. They must not be mistaken
for production SaaS infrastructure.

## Summary

The current system has a valuable processing engine, but its surrounding
infrastructure is local and transitional.

The current infrastructure is not SaaS-compatible because it lacks:

- Authentication.
- Authorization.
- Tenant isolation.
- Durable job persistence.
- Production-grade storage.
- Production-grade background job execution.
- Auditable review decisions.
- Secure artifact access.
- Billing/usage tracking.
- Operational observability.

## Temporary/local-only components

| Component | Location | Why it is temporary |
|---|---|---|
| In-memory job store | `app/server.py`, `InMemoryJobStore` | Loses state on process restart; cannot support multiple app instances; no tenant ownership |
| FastAPI background tasks | `app/server.py`, `BackgroundTasks` usage | Not a queue; no retries, backpressure, cancellation, priority, or worker isolation |
| Local runtime storage | `runtime/uploads`, `runtime/jobs`, `runtime/history` | Local disk is not durable SaaS storage and does not support horizontal scaling |
| Global job endpoints | `app/server.py` | Jobs are accessed by `job_id` only; no user or organization boundary |
| Public `config_path` input | `app/server.py`, `app/config.py` | Lets request input influence server-side config loading; not acceptable for SaaS |
| JSON review decisions | `runtime/jobs/{job_id}/review_decisions.json` via `app/server.py` | No actor, role, organization, audit trail, or conflict model |
| Artifact ZIP over run directory | `app/server.py`, `_build_zip` | Risks exposing internal artifacts and runtime data as customer downloads |
| Mock frontend backend | `trashpanda-next/lib/mock-adapter.ts` | Development simulation only; not durable, secure, or multi-user |
| Adapter fallback to mock | `trashpanda-next/lib/backend-adapter.ts` | Useful transition mechanism, but dangerous if treated as production behavior |
| Preview product pages | `trashpanda-next/app/domain-audit`, `lead-discovery`, `pipelines` | Static or disabled preview surfaces, not real product features |
| Local dev launch scripts | `start_trashpanda.ps1`, `start_trashpanda.bat`, `scripts/dev_launcher.py` | Useful for local operation only |

## In-memory job store limitation

`app/server.py` defines `InMemoryJobStore`.

This is not SaaS-compatible because:

- Job state disappears on process restart.
- Multiple backend instances would each have different job state.
- There is no persistence boundary.
- There is no job ownership.
- There is no organization scope.
- There is no audit trail.

The current disk reconstruction logic is useful for local recovery, but it is
not a substitute for durable product persistence.

## Background task limitation

Job execution currently uses FastAPI `BackgroundTasks`.

This is not SaaS-compatible because:

- It runs inside the web application process.
- It does not provide durable queue semantics.
- It does not provide retries.
- It does not provide job cancellation.
- It does not provide concurrency control by tenant or plan.
- It does not provide backpressure during load.
- It does not isolate long-running processing from HTTP serving.

This is acceptable for local/internal use. It is not acceptable as production
SaaS job infrastructure.

## Local filesystem limitation

The system currently writes uploads, job outputs, history data, logs, and
runtime artifacts to local directories such as:

- `runtime/uploads`
- `runtime/jobs`
- `runtime/history`
- `output`
- `logs`

This is not SaaS-compatible because:

- Files are not scoped by authenticated tenant.
- Files are not governed by retention policy.
- Files are not protected by signed access.
- Files are not durable across deployments unless explicitly preserved.
- Local paths cannot be shared safely across horizontally scaled instances.
- Internal runtime artifacts can be confused with customer-facing artifacts.

## Global access limitation

The backend exposes job and artifact endpoints by `job_id`.

Examples include:

- `GET /jobs`
- `GET /jobs/{job_id}`
- `GET /results/{job_id}`
- `GET /jobs/{job_id}/review`
- `POST /jobs/{job_id}/review/decisions`
- `GET /jobs/{job_id}/artifacts/{key}`
- `GET /jobs/{job_id}/artifacts/zip`

These routes currently have no authentication or authorization boundary.

This is production-incompatible. In a SaaS product, job access must be scoped to
an authenticated user and organization.

## Config exposure limitation

`POST /jobs` accepts `config_path` as form input in `app/server.py`.
`app/config.py` resolves that path and loads configuration.

This is not SaaS-compatible.

For SaaS, request input must not be allowed to choose arbitrary server-side
configuration files. Runtime configuration must be controlled by the platform,
not by public upload requests.

## Mock adapter limitation

The frontend can run against `trashpanda-next/lib/mock-adapter.ts` when
`TRASHPANDA_BACKEND_URL` is not set.

This is useful for UI development.

It is not SaaS-compatible because:

- It stores jobs in module memory.
- It simulates job progress.
- It returns fake logs.
- It returns fake review queue data.
- It returns placeholder artifacts.
- It has no user, organization, security, or persistence model.

Mock mode must remain clearly marked as development-only.

## Preview feature limitation

The following frontend sections are not real product modules today:

- `trashpanda-next/app/domain-audit/page.tsx`
- `trashpanda-next/app/lead-discovery/page.tsx`
- `trashpanda-next/app/pipelines/page.tsx`

They include preview/offline/planned behavior and disabled controls.

These must not be represented as completed SaaS capabilities.

## Security limitations

Current security limitations include:

- No login.
- No sessions.
- No user identity.
- No role model.
- No organization model.
- No tenant isolation.
- No secure artifact authorization.
- No per-tenant rate limiting.
- No audit trail for review decisions.
- No production secrets management.
- No upload malware scanning.
- No policy governing PII retention.

These are not minor gaps. They define the difference between an internal tool
and a SaaS platform.

## Operational limitations

Current operational limitations include:

- No production queue.
- No durable job event log.
- No migration discipline.
- No multi-instance coordination.
- No structured metrics/tracing surface for operations.
- No support/admin tooling.
- No backup/restore contract for product state.
- No customer-facing SLA posture.

## Non-goals of this document

This document does not define replacements.

It only identifies the temporary pieces and why they cannot be considered SaaS
infrastructure.
