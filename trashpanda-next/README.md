# TrashPanda — Next.js MVP

Frontend MVP for TrashPanda. Upload a CSV/XLSX, run it through the Python cleaning pipeline, download the deliverables.

## Status

Phase 6 deliverable. The frontend is feature-complete for the MVP scope:

- Upload console (drag/drop + click-to-pick, file validation)
- Live job polling with queued / running state visuals
- Results view with primary + secondary metrics
- Download section for the 4 client xlsx outputs + technical artifacts
- Failed state with retry

The Python backend does not yet expose an HTTP layer, so the frontend currently runs against an **in-memory mock adapter** that simulates the job lifecycle. Switching to the real backend is one env variable — see [Connecting the real backend](#connecting-the-real-backend).

## Stack

- **Next.js 15** (App Router, React Server Components)
- **React 19**
- **TypeScript** (strict)
- **CSS Modules** (per-component), no Tailwind
- **No extra UI libraries** — the visual system is hand-built from the mockup tokens

## Setup

```bash
cd trashpanda-next
npm install
npm run dev
```

Open http://localhost:3000.

## Project structure

```
trashpanda-next/
├── app/
│   ├── layout.tsx                    # Root layout, wraps everything in <AppShell>
│   ├── page.tsx                      # "/" — Upload Console
│   ├── globals.css                   # Palette, fonts, reset, atmospheric bg
│   ├── results/
│   │   └── [jobId]/
│   │       ├── page.tsx              # Server component: SSR initial fetch
│   │       └── ResultsClient.tsx     # Client component: polling + status routing
│   └── api/
│       └── jobs/
│           ├── route.ts              # POST /api/jobs (upload)
│           └── [jobId]/
│               ├── route.ts          # GET  /api/jobs/:jobId
│               └── artifacts/[key]/route.ts  # GET artifact stream
├── components/
│   ├── AppShell.tsx                  # Sidebar + main layout, ShellContext
│   ├── Sidebar.tsx                   # Navigation, disabled placeholders for Phase 7+
│   ├── Topbar.tsx                    # Breadcrumb + title + meta
│   ├── UploadDropzone.tsx            # Drag/drop + file preview + submit
│   ├── MetricsCards.tsx              # Primary 4-up + secondary 5-up stats
│   ├── JobStatusPanel.tsx            # 7-stage pipeline visual
│   ├── DownloadArtifacts.tsx         # 4 client xlsx + technical section
│   ├── ErrorState.tsx                # Failed job UI
│   └── *.module.css                  # Co-located styles per component
├── lib/
│   ├── types.ts                      # JobResult contract (must match Python)
│   ├── api.ts                        # Frontend API client (single import surface)
│   ├── backend-adapter.ts            # Server-side: mock vs proxy switch
│   ├── mock-adapter.ts               # In-memory mock job store
│   └── mockup-theme.ts               # TS-side design tokens
└── public/
    └── trashpanda-logo.png           # Transparent logo
```

## Architecture

### How the mockup was converted to components

The original TrashPanda mockup was a single ~330 KB HTML file with every view, style, and script inlined. To turn that into a maintainable Next.js app:

1. **CSS tokens extracted** into `app/globals.css` as CSS custom properties (`--neon`, `--bg-panel`, `--font-display`, etc.) and mirrored in `lib/mockup-theme.ts` for inline-style cases.
2. **Atmospheric layers** (grid, noise, gradients) moved to `body::before`/`::after` so they render under every page without being repeated per component.
3. **Views decomposed**. The mockup had 7 views; only 4 map to the MVP scope (Console, Processing, Results, Failed). The other 3 (Lead Discovery, Domain Audit, Pipelines) appear in the sidebar as disabled `SOON` placeholders, preserving the visual hierarchy without shipping dead functionality.
4. **Each visual block became its own component** with a co-located `.module.css`. No global selectors leak between components.
5. **Animations kept deliberate**. Page-level `fade-up` stagger on mount. No loose micro-interactions.

### Frontend → backend contract

```
Frontend (React)                    Route Handler              Adapter              Backend
─────────────────                   ─────────────              ───────              ───────
UploadDropzone                      POST /api/jobs             adapterStartJob      POST /jobs (Python)
  └─ uploadFile(file)               (multipart/form-data)      ├─ mock: createMockJob
                                                               └─ proxy: fetch backend
                                    │
ResultsClient (every 2s)            GET /api/jobs/:id          adapterGetJob        GET /jobs/:id
  └─ getJob(id)                                                ├─ mock: getMockJob
                                                               └─ proxy: fetch backend
                                    │
DownloadArtifacts                   GET /api/jobs/:id/         adapterGetArtifact   GET /jobs/:id/artifacts/:key
  └─ artifactDownloadUrl(id, key)   artifacts/:key             ├─ mock: plaintext
                                                               └─ proxy: stream
```

Every component talks only to `lib/api.ts`. The API client talks only to Next.js route handlers. Route handlers talk only to `backend-adapter.ts`. The adapter decides whether to hit the mock or proxy to Python.

**This means swapping in the real backend requires zero component changes.**

### The `JobResult` contract

`lib/types.ts` defines the contract. It mirrors the Python backend's `JobResult` field-for-field:

```ts
interface JobResult {
  job_id: string;
  status: "queued" | "running" | "completed" | "failed";
  input_filename?: string | null;
  run_dir?: string | null;
  summary?: JobSummary | null;
  artifacts?: JobArtifacts | null;
  error?: JobError | null;
  started_at?: string | null;
  finished_at?: string | null;
}
```

`JobSummary` matches the Python summary counters one-to-one (`total_input_rows`, `total_valid`, `duplicates_removed`, `typo_corrections`, `disposable_emails`, etc.). `JobArtifacts.client_outputs` is keyed by the manifest in `CLIENT_OUTPUT_MANIFEST`.

**If the Python contract changes, update `lib/types.ts` first and let TypeScript show every UI site that needs to adjust.**

## Connecting the real backend

When the Python HTTP service is online:

1. Expose three endpoints matching the contract above:
   - `POST /jobs` — multipart `file`, returns `{ job_id }`
   - `GET /jobs/{id}` — returns `JobResult` as JSON
   - `GET /jobs/{id}/artifacts/{key}` — streams the xlsx with `Content-Disposition`
2. Set the env variable:
   ```
   TRASHPANDA_BACKEND_URL=http://localhost:8000
   ```
3. Restart `npm run dev`.

The adapter detects the env var and proxies all requests. No component or route handler code changes.

**Recommended:** use FastAPI. `run_cleaning_job(...)` wraps cleanly into an async background task, and FastAPI's `UploadFile` handles multipart uploads without ceremony.

## Screens

### Console (`/`)
- Dropzone with mascot and keyboard hints
- File preview card (icon, name, size) once a file is selected
- `START CLEANING` button posts to `/api/jobs` and navigates to `/results/:id`
- Empty metrics block below (stable layout when nothing has run yet)

### Processing (`/results/:id` with status = queued | running)
- Topbar shows filename + status badge
- `JobStatusPanel`: 7 pipeline stages with a swept sheen on the active stage
- Stage estimate from `started_at` delta — when the backend later exposes per-stage progress, wire it into `estimateStage()`
- Hint box explaining the polling behavior

### Results (`/results/:id` with status = completed)
- Primary metrics: total rows / valid / recoverable / purged
- Secondary metrics: duplicates, typo fixes, disposable, placeholder, role-based
- Download grid: the 4 client xlsx outputs as severity-colored cards (ok / warn / bad / info)
- Technical outputs list for CSVs and reports

### Failed (`/results/:id` with status = failed)
- `ErrorState` with `error.message`, `error.error_type`, and job id
- `TRY AGAIN` CTA routing back to `/`

## What was intentionally left out

Per the Phase 6 spec, these are deferred:

- Authentication, accounts, billing
- Multi-tenant isolation
- Websocket / SSE for progress (current approach: 2s polling)
- Lead Discovery, Domain Audit, Pipelines editor
- Cloudflare deployment

The sidebar shows them as disabled placeholders so the surface is there when we build them out.

## Dev notes

- **CSS Modules over Tailwind**: the mockup aesthetic relies on gradient stops, precise shadows, and clip-paths that are painful to express as utility classes. Modules keep the mockup's visual language intact without sacrificing scope isolation.
- **No client-side data fetching library**: polling is simple and the payload is small. `useEffect` + `setInterval` is the right size.
- **SSR initial fetch** on the results page (`page.tsx` is a server component) so the first paint shows real data, not a loading spinner. Polling picks up from there.
- **Mock adapter failure roll**: `~5% deterministic`, so the same job id always resolves to the same outcome. Useful for reproducing bugs.

## Next phases

- Phase 7: Python FastAPI HTTP layer (unblocks proxy mode)
- Phase 8: Progress events (websocket or polling with stage field)
- Phase 9: Auth + accounts
- Phase 10: Cloudflare deploy
