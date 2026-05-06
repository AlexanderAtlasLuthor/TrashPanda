# Plan: Auto-chunked processing — Fases 1 (CLI) y 2 (UI)

## Contexto

El 6 de mayo de 2026 el job `job_20260506_190530_65f44142` (input
`WY.csv`, 100k filas) corrió correctamente el primer bloque de 50k,
quedó persistido en `staging.sqlite3`, entró al segundo bloque, y a
las 19:39:55 UTC fue matado por el OOM killer de Linux durante la
`DecisionStage`. systemd reinició el backend y el API quedó
re-hidratando el estado a partir de artefactos parciales — reportando
`completed` pero sin los CSVs/XLSXs de salida.

Workaround manual ya disponible (commit `c1f55d8`):

1. Splittear el input en chunks (25k recomendado).
2. Correr `scripts/defensive_clean.py` por cada chunk como proceso
   independiente.
3. Mergear los `customer_bundle/` con
   `scripts/merge_customer_bundles.py`.

Ese flujo funciona pero requiere que el operador acuerde de hacer 6
pasos manualmente y se acuerde de splittear con sed/awk
correctamente. Este documento planifica las dos fases para
automatizarlo.

---

## Fase 1 — CLI auto-chunker (entrega rápida)

### Objetivo

Un solo comando que toma un input file grande y produce un
`customer_bundle/` mergeado, con OOM-safety y progreso visible en
stdout.

### Esfuerzo estimado

2-3 horas (script + tests + smoke run sobre los 100k de WY.csv).

### Archivos a crear

* `scripts/auto_chunked_clean.py` — CLI orchestrator (~150-200
  líneas).
* `tests/test_auto_chunked_clean.py` — unit tests con un input
  sintético chico (mock de `defensive_clean` y `merge_customer_bundles`).

### Archivos a modificar

Ninguno. Reutiliza:
* `scripts/defensive_clean.py` (ya existe en `main`)
* `scripts/merge_customer_bundles.py` (ya existe en `main`,
  commit `c1f55d8`)

### CLI shape

```
python -m scripts.auto_chunked_clean \
    --input-file /path/to/WY.csv \
    --output-dir runtime/jobs/wy-100k \
    [--chunk-size 25000] \
    [--threshold-rows 50000] \
    [--max-parallel 1]
```

* `--chunk-size`: filas por chunk (default 25,000).
* `--threshold-rows`: si el input es menor que esto, NO se splittea —
  pasa derecho a `defensive_clean` (default 50,000).
* `--max-parallel`: cuántos chunks correr en paralelo (default 1
  para máxima OOM-safety; subir a 2-4 si el VPS tiene RAM).

### Comportamiento

1. **Detección de tamaño**: cuenta filas del input sin cargar el
   archivo entero (line count para CSV, `openpyxl.load_workbook(...,
   read_only=True)` para XLSX).
2. **Decisión**: si `total_rows < threshold_rows`, delega a
   `defensive_clean` directamente y termina. Caso contrario pasa al
   modo chunked.
3. **Split**: divide el input en N chunks de `chunk_size` filas en
   `<output_dir>/_chunks/<name>_part_<i>.csv`. Preserva el header en
   cada uno.
4. **Run**: por cada chunk, lanza un subprocess
   `python -m scripts.defensive_clean --input-file <chunk> --output-dir
   <output_dir>/_chunks/run_<i>`. Subprocess para OOM-safety
   (cada uno arranca con memoria fresca; un OOM en chunk 3 no mata
   chunks 4-N).
5. **Progreso**: el orquestador escribe a stdout una línea por evento
   y mantiene un state file actualizado (formato abajo).
6. **Merge**: cuando todos los chunks terminan exitosamente, llama a
   `merge_customer_bundles.merge_bundles(...)` para producir
   `<output_dir>/customer_bundle/` con los CSVs mergeados.
7. **Cleanup opcional**: borrar `_chunks/` si el operador pasa
   `--cleanup`. Default: dejarlos para auditoría.

### Estado en disco — state file

`<output_dir>/auto_chunked_status.json`:

```json
{
  "started_at": "2026-05-06T20:00:00Z",
  "completed_at": null,
  "input_file": "/path/to/WY.csv",
  "input_format": "csv",
  "total_rows": 100000,
  "threshold_rows": 50000,
  "chunk_size": 25000,
  "status": "running",
  "chunks": [
    {
      "index": 1,
      "input_path": ".../wy_part_1.csv",
      "run_dir": ".../run_1",
      "status": "completed",
      "started_at": "2026-05-06T20:00:01Z",
      "completed_at": "2026-05-06T20:01:30Z",
      "exit_code": 0,
      "counts": {
        "clean_deliverable": 21800,
        "review_provider_limited": 250,
        "high_risk_removed": 2950
      }
    },
    {
      "index": 2,
      "input_path": ".../wy_part_2.csv",
      "run_dir": ".../run_2",
      "status": "running",
      "started_at": "2026-05-06T20:01:31Z",
      "completed_at": null,
      "exit_code": null,
      "counts": null
    },
    {"index": 3, "status": "pending", ...},
    {"index": 4, "status": "pending", ...}
  ],
  "merged_at": null,
  "merged_counts": null
}
```

Estados de chunk: `pending` → `running` → (`completed` | `failed`).
Estado del batch: `running` → (`completed` | `failed` |
`partial_failure`).

Este JSON es el **contrato** que la Fase 2 (UI) lee. Una vez que la
Fase 1 está estable, la Fase 2 no necesita reinventar el data model
— solo expone este JSON sobre HTTP.

### Output stdout

```
auto_chunked_clean: input=/path/WY.csv rows=100000 → 4 chunks of 25000
[1/4] split: wy_part_1.csv (25,000 rows)
[2/4] split: wy_part_2.csv (25,000 rows)
[3/4] split: wy_part_3.csv (25,000 rows)
[4/4] split: wy_part_4.csv (25,000 rows)
[1/4] start: defensive_clean wy_part_1.csv
[1/4] done:  21,800 clean / 2,950 removed / 250 review (89s)
[2/4] start: defensive_clean wy_part_2.csv
[2/4] done:  21,750 clean / 2,980 removed / 270 review (92s)
[3/4] start: defensive_clean wy_part_3.csv
[3/4] done:  21,820 clean / 2,930 removed / 250 review (88s)
[4/4] start: defensive_clean wy_part_4.csv
[4/4] done:  21,810 clean / 2,940 removed / 250 review (90s)
merge: 4 bundles → runtime/jobs/wy-100k/customer_bundle/
merge: 87,180 clean / 1,020 review / 11,800 removed
ok: 6m 12s
```

### Manejo de fallos

* Chunk fail (subprocess returns non-zero):
  * Marca el chunk como `failed` en el state file.
  * Si `--strict` (default), aborta el batch sin mergear.
  * Si `--allow-partial`, sigue con los chunks restantes y emite
    `partial_failure` con un merge de los que sí completaron.
* Orchestrator interrumpido (Ctrl+C):
  * Estado se preserva en el JSON.
  * `--resume <output_dir>` reanuda desde donde quedó (saltea chunks
    `completed`, re-ejecuta los `pending` y los `failed`).

### Tests

`tests/test_auto_chunked_clean.py`:
* **Detección de tamaño**: file con 1k filas → no se splittea.
* **Splitting**: file de 100k → 4 chunks de 25k cada uno con header
  preservado.
* **Run**: stub `defensive_clean` para que no llame al pipeline real.
  Verificar que se invoca por chunk.
* **Merge**: verificar que llama a `merge_bundles` con los run_dirs
  correctos.
* **State file**: verificar shape JSON después de cada fase.
* **Resume**: kill durante chunk 2, resume → completa solo chunks
  2-4, no re-corre 1.
* **Stdout format**: verificar que las líneas `[i/N] ...` salen en
  el orden esperado (regex match).

### Risks / mitigaciones

| Riesgo | Mitigación |
|---|---|
| Subprocess stdout buffering oculta progreso | `PYTHONUNBUFFERED=1` en el env del subprocess + `flush=True` en los prints del orchestrator |
| Chunk size de 25k sigue OOM-eando algún VPS | Default 25k pero `--chunk-size` configurable. Doc en runbook: si OOMea, bajar a 12k |
| Disk fill por dejar `_chunks/` | `--cleanup` flag; doc en README |
| Operador corre 2 instancias en paralelo sobre el mismo output_dir | flock en el state file |

---

## Fase 2 — UI batch job (entrega completa)

### Objetivo

El operador sube un archivo grande en la UI, ve "este archivo es
grande, se procesará en N partes", y mientras corre ve un panel con
status per-chunk en tiempo real. Cuando termina, descarga el bundle
mergeado igual que un job normal.

### Esfuerzo estimado

12-14 horas en total:
* Backend (orquestador + endpoints + tests): 4-6 horas.
* Frontend (componente nuevo + integración): 4-6 horas.
* Smoke tests / polish: 2 horas.

### Decisión clave: batch ≠ job

Un batch contiene N child jobs. Cada child es un job normal con su
propio job_id. El batch agrega:
* Su propio `batch_id`.
* Una lista ordenada de child job_ids.
* El state file de Fase 1 (mismo formato).
* Una API que agrega el progreso de los children.

Beneficio: los children siguen siendo jobs normales que la UI sabe
renderizar. La pieza nueva es solo la agregación.

### Backend — archivos a crear

* `app/batches.py` — `BatchStore`, `BatchOrchestrator`, status models.
* `app/batch_routes.py` — FastAPI router con endpoints (abajo).
* `tests/test_batches.py` — tests del orquestador.
* `tests/test_batch_routes.py` — tests de los endpoints (TestClient).

### Backend — archivos a modificar

* `app/server.py`: `app.include_router(batch_router)`.
* `app/api_boundary.py`: opcionalmente exponer
  `submit_batch_job(...)` por simetría con `run_job(...)`.

### API contract

```
POST   /batches/upload
       multipart/form-data: input file
       optional fields: chunk_size, threshold_rows, max_parallel
       returns: { "batch_id": "...", "status": "queued",
                  "total_rows": ..., "n_chunks": ... }

GET    /batches/{batch_id}
       returns: full state JSON (same shape as Fase 1's state file,
                + child_job_ids per chunk for cross-link)

GET    /batches/{batch_id}/progress
       returns: aggregated progress (lightweight version of /
                state, optimized for polling every 2-3s)
       {
         "batch_id": "...",
         "status": "running",
         "n_chunks": 4,
         "n_completed": 2,
         "n_failed": 0,
         "n_running": 1,
         "n_pending": 1,
         "current_chunk_index": 3,
         "current_chunk_phase": "domain_intelligence",
         "current_chunk_progress_percent": 60,
         "merged_counts": null
       }

GET    /batches/{batch_id}/customer-bundle/download
       streams the merged customer_bundle/ as a zip
       (404 until status == "completed")

POST   /batches/{batch_id}/cancel
       interrupts: in-flight chunk killed, pending chunks skipped
```

### Backend — orchestrator design

`BatchOrchestrator.start(batch_id, input_path, **opts)`:
* Crea el directorio del batch en `runtime/batches/{batch_id}/`.
* Lee el input, decide chunks (reutiliza la lógica de Fase 1).
* Por cada chunk, llama internamente a `api_boundary.run_cleaning_job`
  (no subprocess — un thread/asyncio task per chunk con un
  `multiprocessing.Pool` de tamaño `max_parallel`).
* Captura el progreso del child job leyendo su
  `/jobs/{child_id}/progress` y lo proxy-ea al state del batch.
* Cuando todos los children completan, invoca `merge_bundles` y emite
  el bundle final en `runtime/batches/{batch_id}/customer_bundle/`.

Decisión: thread pool vs subprocess. El thread pool es más simple
para la integración FastAPI/asyncio. El subprocess es más OOM-seguro.
**Recomendación**: subprocess (igual que Fase 1) — el costo de OOM
es alto y la cláusula que los hace iguales es lo que evita el bug
del 6 de mayo.

Persistencia: `runtime/batches/{batch_id}/batch_state.json` (same
shape Fase 1). El orchestrator escribe; los endpoints leen. Si el
backend reinicia mid-batch, leer el JSON al startup permite reanudar.

### Frontend — archivos a crear

* `trashpanda-next/components/BatchProgressPanel.tsx` — el componente
  que muestra los chunks en grid.
* `trashpanda-next/components/BatchProgressPanel.module.css` —
  estilos.

### Frontend — archivos a modificar

* `trashpanda-next/lib/api.ts` — agregar `getBatchProgress(id)`,
  `uploadBatch(file, opts)`, `downloadBatchBundle(id)`.
* `trashpanda-next/lib/types.ts` — tipos de batch.
* `trashpanda-next/components/UploadDropzone.tsx` — detección
  client-side de tamaño, mostrar mensaje "Will be processed in N
  chunks" antes del submit.
* `trashpanda-next/components/RecentJobs.tsx` — listar también
  batches.
* `trashpanda-next/app/results/[jobId]/ResultsClient.tsx` —
  branch para batches: si la URL es `/results/batch_xxx`, render
  `BatchProgressPanel`; si es un job_id, render lo de siempre.

### UI — el panel

```
┌─────────────────────────────────────────────────────┐
│ Batch wy-100k-abc123 — running                       │
│ ▓▓▓▓▓▓▓░░░ 65%   2 of 4 chunks complete              │
│                                                      │
│  #  status        rows    clean   review  removed    │
│  1  ✅ done       25,000   21,800     250    2,950   │
│  2  ✅ done       25,000   21,750     270    2,980   │
│  3  🟡 running    25,000   …       processing 60%   │
│  4  ⏳ pending    25,000   —         —        —      │
│                                                      │
│  [Cancel batch]  [View merged bundle when ready]    │
└─────────────────────────────────────────────────────┘
```

Polling cada 3s al endpoint `/batches/{id}/progress` (el liviano).
Backoff exponencial cuando `status == completed` o `failed`
(probablemente solo deja de pollear).

### Tests — backend

* `test_batches.py`:
  * Orchestrator start: crea state, splittea, invoca run per chunk.
  * Status flow: pending → running → completed para un chunk.
  * Merge: state.merged_counts populado al final.
  * Cancellation: pending chunks no se ejecutan.
  * Resume on restart: leer state existente y reanudar.
* `test_batch_routes.py`:
  * POST /upload con un CSV chico de prueba → returns batch_id.
  * GET /progress después de un tick → shape correcta.
  * GET /customer-bundle/download → 404 si no terminó.

### Tests — frontend

* `BatchProgressPanel.test.tsx`:
  * Renderiza los chunks correctamente.
  * Muestra porcentaje agregado.
  * Botón cancel solo activo si status == running.
* Smoke E2E: subir un CSV chico → batch corre → bundle descargable.

---

## Cómo conectan las dos fases

```
Fase 1 ships          state file format       Fase 2 reuses
   ↓                     ↓                        ↓
CLI orchestrator → auto_chunked_status.json → batch_state.json
   ↓                                              ↓
defensive_clean                              api_boundary.run_cleaning_job
   ↓                                              ↓
merge_customer_bundles                       merge_customer_bundles
```

Mismo data model, mismo merge, mismo output final. La Fase 2
agrega un wrapper FastAPI alrededor de la Fase 1, pero la lógica
de splitting / running / merging vive en el código de Fase 1 y se
reutiliza tal cual.

Implicación: si la Fase 1 funciona en producción durante días/semanas
antes de la Fase 2, la confianza está acumulada. La Fase 2 solo
agrega UX, no cambia el comportamiento.

---

## Decisiones que vale la pena revisar antes de implementar

1. **Threshold para auto-chunkear**: 50k filas es el default
   recomendado por la evidencia del OOM (50k corrió ok, 100k murió).
   ¿Conservador a 30k? ¿Agresivo a 75k?

2. **Default chunk_size**: 25k es seguro. ¿Bajar a 20k para tener
   más margen en VPSs chicos? Tradeoff: más chunks = más overhead
   de import del proceso Python (~3-5s por chunk).

3. **max_parallel default**: 1 es máxima OOM-safety. 2-4 sería
   ~2-4× más rápido pero con más riesgo. Recomendación: 1 default,
   documentar cómo subirlo.

4. **Frontend: poll vs WebSocket**: poll es simple y suficiente
   para batches que duran minutos. WebSocket sería mejor UX para
   batches de horas, pero agregar infra de WS por esto solo es
   over-engineering. Recomendación: poll cada 3s.

5. **Cleanup automático de `_chunks/`**: para 2.7M filas serían
   ~108 chunks × ~100MB cada uno = ~10GB transient. Decisión:
   `--cleanup` activo por default cuando el batch completa OK,
   off cuando hay failures (para auditoría).

---

## Próximos pasos

Cuando se decida implementar:

* **Fase 1**: 1 sesión de 2-3 horas. Empezar por
  `scripts/auto_chunked_clean.py` con la API expuesta arriba, los
  tests, y un smoke run sobre los 100k de WY.csv para confirmar
  que se mergean correctamente.
* **Fase 2**: 2-3 sesiones. Backend primero (orchestrator + routes
  + tests), después frontend (panel + integración + tests),
  después smoke E2E con un upload real.

Cada fase es shippable independiente. Fase 1 sin Fase 2 ya elimina
el OOM y da progreso por stdout — eso solo justifica el trabajo.
Fase 2 es UX encima de eso.
