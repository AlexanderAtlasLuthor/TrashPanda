# SaaS Target V1

## Purpose

This document defines what SaaS V1 means for this repository.

It does not define implementation. It does not define database schema. It does
not define auth provider choice. It defines the required product and platform
capabilities that must exist before this system can be considered a real SaaS
V1.

## Current starting point

The system currently has:

- A valuable data hygiene/email validation engine.
- A working local HTTP API.
- A working Next.js frontend for upload, results, review, exports, and insights.
- Local runtime storage.
- In-memory job state.
- Mock frontend fallback behavior.
- No authentication.
- No organizations.
- No tenant isolation.
- No billing.

SaaS V1 must preserve the working engine flows while replacing the temporary
single-user infrastructure around them.

## SaaS V1 product definition

SaaS V1 for this system means:

A customer can securely sign in, upload a contact list, run the existing
TrashPanda engine, view job results, review ambiguous records, download exports,
and see job history within the boundaries of their own organization.

## Required capabilities

SaaS V1 requires the following capabilities.

### User access

- Users can authenticate.
- Users have an identity.
- Users belong to at least one organization/workspace.
- Users can access only data they are authorized to access.

### Organization boundary

- Jobs belong to an organization.
- Uploaded files belong to an organization.
- Artifacts belong to an organization.
- Review decisions belong to an organization and job.
- Job history is scoped to an organization.

### Job lifecycle

- A user can upload a supported file.
- The system creates a durable job record.
- The engine can process the job.
- Job status is visible.
- Job failure is visible.
- Completed jobs expose summaries, review queues, insights, and artifacts.

### Results and review

- Results remain tied to the job that produced them.
- Review queue access is authorized.
- Review decisions are durable.
- Review decisions are attributable to an actor.
- Exports reflect the intended product semantics.

### Artifact access

- Artifacts are not publicly accessible by guessing a job id.
- Downloads require authorization.
- Customer-facing exports are distinguishable from internal runtime files.

### Persistence

- Product state survives app restarts.
- Product state does not depend on a single web process.
- Job ownership, status, artifacts, and decisions are durable.

### Operational safety

- Long-running processing does not depend on the HTTP request lifecycle.
- The system can represent queued, running, completed, and failed jobs.
- Failures are visible without exposing unsafe internals to customers.

## Required guarantees

SaaS V1 must guarantee:

- No cross-organization job listing.
- No cross-organization result access.
- No cross-organization artifact download.
- No unauthenticated job creation.
- No unauthenticated review decision write.
- No public request-controlled server config path.
- No customer access to internal runtime-only artifacts.
- No reliance on mock data in production.
- No reliance on browser localStorage for authoritative product state.

## Product flows that must remain available

SaaS V1 must preserve these existing flows:

- Upload CSV/XLSX.
- Create job.
- Execute processing through the existing engine.
- Poll or view job status.
- View completed results.
- View failure state.
- View logs or customer-safe processing events.
- View review queue.
- Save review decisions.
- Download exports.
- View insights where generated.

## Explicitly out of scope for SaaS V1

The following are not required for SaaS V1 based on the current repository:

- Full enterprise SSO.
- Advanced billing packaging.
- Lead discovery as a real module.
- Domain audit as a real module.
- Automated recurring pipelines as a real module.
- Complex enterprise admin hierarchy.
- Marketplace integrations.
- CRM sync.
- Real-time validation API for external applications.
- Guaranteed bounce prediction.
- Aggressive SMTP validation.
- Enterprise deliverability modeling claims.

## Current features that must not be marketed as SaaS V1 capabilities

These current UI areas are preview/mock surfaces and must not be treated as
completed SaaS V1 features:

- `trashpanda-next/app/domain-audit/page.tsx`
- `trashpanda-next/app/lead-discovery/page.tsx`
- `trashpanda-next/app/pipelines/page.tsx`

They can exist as placeholders, but they are not SaaS V1 product commitments.

## SaaS V1 success definition

SaaS V1 is achieved when the existing engine-backed upload-to-export workflow
is secure, durable, organization-scoped, and usable by more than one customer
without data leakage or local-process dependence.

This document intentionally does not specify how to implement that target.
