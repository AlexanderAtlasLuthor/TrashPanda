# Critical Flows

## Purpose

This document lists the current end-to-end flows that must not break during the
SaaS transformation.

The goal is not to freeze implementation details. The goal is to protect user
visible behavior and engine-backed value.

## Flow 1: Upload

## Description

A user selects or drops a CSV/XLSX file in the frontend. The frontend validates
basic file type and size, then sends the file to the API.

Current locations:

- Frontend: `trashpanda-next/components/UploadDropzone.tsx`
- Frontend API client: `trashpanda-next/lib/api.ts`
- Next route handlers: `trashpanda-next/app/api/jobs/route.ts`
- Backend endpoint: `POST /jobs` in `app/server.py`

## Non-regression expectations

- CSV upload remains supported.
- XLSX upload remains supported.
- Empty files are rejected.
- Unsupported file extensions are rejected.
- The user receives a job id or a clear error.
- Upload remains the entry point to the processing workflow.

## Flow 2: Job creation

## Description

The backend creates a job id, stores initial job metadata, saves the uploaded
file, and begins processing.

Current locations:

- `create_job` in `app/server.py`
- `InMemoryJobStore` in `app/server.py`
- Upload storage under `runtime/uploads`

## Non-regression expectations

- A job id is created for each accepted upload.
- Job status starts in a queued/running-compatible state.
- Input filename is retained for user-facing display.
- Failed job creation returns a structured error.
- Future SaaS persistence must preserve the user-visible job lifecycle.

## Flow 3: Processing execution

## Description

The uploaded file is processed by the engine through the API boundary.

Current locations:

- `_run_job` in `app/server.py`
- `run_cleaning_job` in `app/api_boundary.py`
- `EmailCleaningPipeline` in `app/pipeline.py`

## Non-regression expectations

- The existing engine remains the processor.
- Pipeline stages still run in their established order.
- Processing produces summaries, artifacts, and errors in a structured form.
- Engine failures result in failed jobs, not silent disappearance.
- SaaS infrastructure changes must not rewrite the engine as a side effect.

## Flow 4: Results access

## Description

The frontend polls or fetches job results and renders status, metrics, summaries,
download links, and error states.

Current locations:

- Frontend: `trashpanda-next/app/results/[jobId]/ResultsClient.tsx`
- Backend endpoints: `/jobs/{job_id}`, `/results/{job_id}`, `/status/{job_id}`
- Shared types: `trashpanda-next/lib/types.ts`

## Non-regression expectations

- Queued jobs render as queued.
- Running jobs render as running.
- Completed jobs render summaries and metrics.
- Failed jobs render useful error state.
- Results remain tied to the correct job.
- Future SaaS authorization must restrict access without removing the flow.

## Flow 5: Logs or processing events

## Description

The UI can show live or recent processing logs for a job.

Current locations:

- Backend endpoint: `/jobs/{job_id}/logs` in `app/server.py`
- Frontend component: `trashpanda-next/components/LiveLogsPanel.tsx`
- Results page integration: `ResultsClient.tsx`

## Non-regression expectations

- Users can see useful progress/failure information.
- Terminal job states still show final diagnostic context.
- Future SaaS logs/events must avoid exposing unsafe internal details.
- Customer-visible logs must remain job-scoped.

## Flow 6: Review queue

## Description

Completed jobs may expose records requiring manual review. The user can inspect
the reason, confidence, risk explanation, and recommendation.

Current locations:

- Backend endpoint: `/jobs/{job_id}/review` in `app/server.py`
- Frontend: `trashpanda-next/app/review/[jobId]/ReviewQueueClient.tsx`

## Non-regression expectations

- Review records remain accessible for jobs that produce them.
- Review reasons remain visible.
- Confidence/risk/recommendation fields remain visible where available.
- Review queue remains linked to the source job.
- Review queue semantics remain based on engine output.

## Flow 7: Review decisions

## Description

The user can approve or remove review items and save decisions.

Current locations:

- Backend endpoints: `/jobs/{job_id}/review/decisions` in `app/server.py`
- Frontend: `ReviewQueueClient.tsx`
- Current persistence: local JSON file under the job runtime directory

## Non-regression expectations

- Users can make approve/remove decisions.
- Decisions can be saved.
- Saved decisions can be reloaded.
- Export generation can use saved decisions.
- Future SaaS persistence must add ownership/audit without changing the user
  meaning of a review decision.

## Flow 8: Artifact/export download

## Description

Completed jobs expose downloadable artifacts and exports.

Current locations:

- Backend endpoints: `/jobs/{job_id}/artifacts/{key}` and
  `/jobs/{job_id}/artifacts/zip` in `app/server.py`
- Frontend component: `trashpanda-next/components/DownloadArtifacts.tsx`
- Export generation: `app/client_output.py`

## Non-regression expectations

- Completed jobs expose valid client-facing exports.
- Artifact keys continue to map to the intended files.
- Download filenames remain useful to users.
- Customer-facing exports remain distinguishable from internal files.
- Future SaaS authorization must secure downloads without removing them.

## Flow 9: Insights

## Description

The system exposes V2 deliverability/intelligence annotations where available.

Current locations:

- Backend endpoint: `/jobs/{job_id}/insights` in `app/server.py`
- Frontend: `trashpanda-next/app/insights/[jobId]/InsightsClient.tsx`
- Types: `trashpanda-next/lib/types.ts`

## Non-regression expectations

- Jobs with V2 data can show insights.
- Jobs without V2 data show an appropriate empty/unavailable state.
- Insight rows remain filterable/searchable in the frontend.
- Insight fields remain derived from engine output, not frontend invention.

## Flow 10: Recent jobs

## Description

The frontend lists recent jobs and links to their results.

Current locations:

- Backend endpoint: `GET /jobs` in `app/server.py`
- Frontend component: `trashpanda-next/components/RecentJobs.tsx`

## Non-regression expectations

- Users can find recent jobs.
- Active jobs can update over time.
- Completed/failed jobs remain reachable.
- Future SaaS job lists must become organization-scoped.
- Current localStorage hiding behavior must not be treated as real deletion.

## Flow protection rule

During SaaS transformation, infrastructure may change, but these user-visible
flows must remain intact or be intentionally replaced by equivalent flows.
