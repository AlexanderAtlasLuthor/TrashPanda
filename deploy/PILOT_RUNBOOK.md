# TrashPanda — Pilot Send Runbook

Procedimiento operacional para correr un **pilot send controlado**
después de que un job termina la limpieza V2 y produce los archivos
de acción (V2.10.10.b / V2.10.11). Aplica al stack productivo
descrito en `PRODUCTION_BOOTSTRAP_LOG.md`.

> **Regla de oro:** nunca lances un pilot mayor a 100 emails. La
> primera vez que tocas un dominio sender nuevo, queda en **50**.
> Cuando un batch de 50 termina con `hard_bounce_rate < 2%`, sube
> al siguiente.

---

## Pre-flight checklist (T-30 minutos)

Verifica los 7 pre-requisitos. Si alguno falla, **NO LANCES**.

| # | Check | Cómo verificar |
|---|-------|----------------|
| 1 | VPS responde y backend healthy | `ssh root@192.3.105.145 'curl -fsS http://127.0.0.1:8000/healthz'` debe imprimir `{"status":"ok"...}` |
| 2 | Timers systemd activos | `ssh root@192.3.105.145 'systemctl list-timers --no-pager "trashpanda-*"'` debe mostrar los dos timers con `active` |
| 3 | Port 25 outbound abierto | `ssh root@192.3.105.145 'nc -vz smtp.gmail.com 25'` → `succeeded!` |
| 4 | SPF / DKIM / DMARC publicados | `dig +short TXT fuenmayorindustries.com`, `dig +short TXT google._domainkey.fuenmayorindustries.com`, `dig +short TXT _dmarc.fuenmayorindustries.com` — los tres deben devolver contenido |
| 5 | Job tiene action files (V2.10.11) | En la UI del operator, el job muestra `review_ready_probable.xlsx` / `review_low_risk.xlsx` con conteos > 0 |
| 6 | IMAP password configurada en systemd | `ssh root@192.3.105.145 'systemctl cat trashpanda-pilot-bounce-poller.service \| grep -c TRASHPANDA_BOUNCE_IMAP_PASSWORD'` debe ser ≥ 1 |
| 7 | Tunnel laptop → VPS abierto | Ícono **TrashPanda (VPS)** corriendo, ventana *TrashPanda - Tunnel* sin errores |

Si algo falla, vuelve a `PRODUCTION_BOOTSTRAP_LOG.md` antes de continuar.

---

## Fase 0 — Configurar el pilot (una sola vez por job)

Hay dos paths: por la UI del operator (recomendado) o por API directa
(auditable / scriptable).

### 0.1 — Path UI

1. Abre `http://localhost:3000` (con el ícono VPS).
2. Click en el job destino. En la card "Pilot send" presiona **Configurar**.
3. Llena:
   - **Subject** y **body** del mensaje (ver guía de copy abajo).
   - **Sender address:** `team@fuenmayorindustries.com`
   - **Sender name:** `TrashPanda Outreach` (o el que prefieras).
   - **Reply-To:** `team@fuenmayorindustries.com`
   - **Return-Path domain:** `fuenmayorindustries.com` (mismo del sender, simplifica IMAP).
   - **IMAP host:** `imap.gmail.com`, port `993`, SSL on.
   - **IMAP username:** `team@fuenmayorindustries.com`.
   - **IMAP password env var:** `TRASHPANDA_BOUNCE_IMAP_PASSWORD` (ya está seteada via systemd override).
   - **Wait window:** `48` horas (default; primer pilot mantenlo).
   - **Expiry:** `168` horas (default).
   - **Max batch size:** `100` (hard cap; nunca subir).
4. Marca el checkbox **"Confirmo autorización del cliente para enviar"** y guarda.

### 0.2 — Path API directa

```bash
ssh -L 8001:127.0.0.1:8000 root@192.3.105.145   # tunnel separado
export TOK=<el operator token>

curl -X PUT http://localhost:8001/api/operator/jobs/<JOB_ID>/pilot-send/config \
  -H "Authorization: Bearer $TOK" \
  -H "Content-Type: application/json" \
  -d '{
    "template": {
      "subject": "Quick question about <topic>",
      "body_text": "...",
      "sender_address": "team@fuenmayorindustries.com",
      "sender_name": "TrashPanda Outreach",
      "reply_to": "team@fuenmayorindustries.com"
    },
    "imap": {
      "host": "imap.gmail.com",
      "port": 993,
      "use_ssl": true,
      "username": "team@fuenmayorindustries.com",
      "password_env": "TRASHPANDA_BOUNCE_IMAP_PASSWORD"
    },
    "return_path_domain": "fuenmayorindustries.com",
    "wait_window_hours": 48,
    "expiry_hours": 168,
    "max_batch_size": 100,
    "authorization_confirmed": true,
    "authorization_note": "Lista X aprobada por <cliente> el 2026-05-04"
  }'
```

Respuesta esperada: `{"saved": true, "config_ready": true}`.

### Guía de copy del mensaje

- **Subject:** corto (≤ 6 palabras), no all-caps, sin signos `!`, sin
  emoji, sin "FREE" / "URGENT" / "RE:" sin contexto. Conjuga acción
  ("Quick question about X", "Following up on Y").
- **Body text:** mínimo 80 palabras (filtros anti-thin), saludo
  personalizado si tienes el nombre, link único máximo, **sin imágenes**
  en el primer pilot, footer con dirección física + opt-out claro
  (`Reply STOP to opt out`). Plain text en el primer pilot — HTML
  agrega vector de spam-score.
- **No** uses palabras tipo "Click here", "Limited time", "100% free",
  "Guaranteed". Pasa el subject+body por
  [mail-tester.com](https://www.mail-tester.com) antes de lanzar.

---

## Fase 1 — Preview (T-5 minutos)

**Antes de lanzar**, audita los candidatos que el selector va a tomar.

```bash
curl -X POST "http://localhost:8001/api/operator/jobs/<JOB_ID>/pilot-send/preview?batch_size=50" \
  -H "Authorization: Bearer $TOK"
```

Devuelve la lista de hasta 50 candidatos con:
- `email`, `domain`, `provider_family`
- `action` (`ready_probable` / `low_risk` / `timeout_retry` / `catch_all_consumer`)
- `deliverability_probability`

**Sanidad mental:**
1. ¿Hay duplicados? No debería — el selector deduplica.
2. ¿Hay emails de tu propio dominio (`fuenmayorindustries.com`)?
   Eso causa loop. Si los ves, abre un issue.
3. ¿La distribución de provider_family es razonable? Un primer pilot
   con 90% gmail+yahoo es agresivo; idealmente <60% consumer mail
   en el primer batch para no quemar la IP del VPS contra los
   filtros B2C.
4. ¿`deliverability_probability` está mayoritariamente arriba de
   0.7? Si no, considera bajar `batch_size` o usar
   `actions=("ready_probable",)` en una llamada custom para
   restringir aún más.

> **El selector NUNCA toma rows de `review_high_risk` o
> `do_not_send`** — es un guardrail del código, no una promesa
> humana. Confirmar regardless.

---

## Fase 2 — Launch (T-0)

```bash
curl -X POST "http://localhost:8001/api/operator/jobs/<JOB_ID>/pilot-send/launch?batch_size=50" \
  -H "Authorization: Bearer $TOK"
```

**Síncrono.** Tarda ~50 × 1.0s + handshake SMTP por dominio = entre
1 y 3 minutos para 50 emails. El endpoint no responde hasta que
terminó el último envío.

Respuesta esperada:
```json
{
  "batch_id": "abc123def456",
  "candidates_selected": 50,
  "candidates_added": 50,
  "sent": 48,
  "failed": 2,
  "counts": { ... }
}
```

**Anota el `batch_id`.** Lo usas para filtrar el tracker en pasos
posteriores.

### Modos de fallo del launch

| reason code | qué pasó | acción |
|-------------|----------|--------|
| `authorization_required` | Olvidaste el checkbox | volver al config con `authorization_confirmed: true` |
| `template_incomplete` | Subject / body / sender vacío | completar |
| `return_path_domain_missing` | Sin return-path | rellenar |
| `batch_size_exceeds_max` | Pediste >100 | bajar a 100 |
| `no_candidates_found` | Selector no halló nada elegible | revisar action XLSX, posiblemente el job no tiene rows con `action ∈ {ready_probable, low_risk, timeout_retry, catch_all_consumer}` |

Si `failed > 0` en una respuesta de `200 OK`, mira el tracker: las
filas en `state=verdict_ready` con `dsn_status` distinto de
`delivered` son los rechazos al momento del send (no esperaron al
poller IMAP — fueron rejects síncronos del MX destino).

---

## Fase 3 — Monitoreo

Tres puntos de medición fijos: **24h**, **48h**, **72h** después del
launch. El bounce-poller corre solo (timer systemd), no necesitas
hacer nada salvo mirar los conteos.

### Endpoint de status

```bash
curl -s "http://localhost:8001/api/operator/jobs/<JOB_ID>/pilot-send/status" \
  -H "Authorization: Bearer $TOK" | jq '.counts'
```

Devuelve:
```json
{
  "pending_send":  0,
  "sent":          18,
  "verdict_ready": 32,
  "expired":       0,
  "delivered":     22,
  "hard_bounce":    3,
  "soft_bounce":    4,
  "blocked":        2,
  "deferred":       1,
  "complaint":      0,
  "unknown":        0,
  "total":         50,
  "hard_bounce_rate": 0.12
}
```

`hard_bounce_rate = hard_bounce / (delivered + hard_bounce)`. Es
el KPI principal — el resto contextualiza.

### T+24h — checkpoint inicial

| KPI | Verde | Amarillo | Rojo |
|-----|-------|----------|------|
| `hard_bounce_rate` | < 2% | 2–5% | ≥ 5% |
| `blocked` | 0–1 | 2–4 | ≥ 5 |
| `complaint` | 0 | 1 | ≥ 2 |
| `verdict_ready / total` | ≥ 60% | 30–60% | < 30% (poller no llega a la mailbox) |
| `pending_send` | 0 | 0 | > 0 (algo está atascado en el sender) |

**Acciones:**
- **Verde:** sigue al T+48h sin tocar nada.
- **Amarillo:** corre `poll-bounces` manualmente (abajo) para acelerar el
  consumo de la mailbox; revisa `dsn_diagnostic` de los hard_bounce
  para ver si hay un patrón de dominio (catch-all que rebota todo).
- **Rojo:** **detente.** Tu sender está siendo penalizado o la lista
  tiene un problema sistémico. Pasa al protocolo de pause (abajo).

### T+48h — checkpoint principal (default `wait_window`)

A esta hora la ventana de espera por defecto se cumple. Las filas
que no recibieron DSN se consideran `delivered` *implícitamente*
cuando llega el `finalize` de la Fase 4.

KPIs y umbrales **idénticos a T+24h** — pero ahora son la decisión
final del batch:

| `hard_bounce_rate` | Veredicto |
|--------------------|-----------|
| **< 2%** | ✅ Sigue al siguiente batch del segmento (puedes subir tamaño a 100). |
| **2–5%** | ⚠️ Mantén batch en 50. Investiga los 2–5 hard_bounces. Probablemente el segmento tiene un sub-grupo malo que vale la pena excluir. |
| **5–10%** | ❌ Pause. El segmento como conjunto no es deliverable. Reclasificación necesaria. |
| **≥ 10%** | 🛑 ABORT global. Algo está fundamentalmente mal — no es el segmento, es la lista o la limpieza. Investigación obligatoria antes de cualquier nuevo pilot. |

### T+72h — cleanup checkpoint

A las 72h cualquier rebote tardío ya llegó. Las filas que sigan en
`state=sent` son fantasmas — las marca el `finalize` como `expired`.

**Acción única en T+72h:** correr `finalize` (Fase 4).

### Forzar un poll IMAP manual

Si el timer systemd se rezagó o quieres ver el estado vivo:

```bash
curl -X POST "http://localhost:8001/api/operator/jobs/<JOB_ID>/pilot-send/poll-bounces" \
  -H "Authorization: Bearer $TOK"
```

Respuesta:
```json
{
  "fetched": 12,
  "parsed":  11,
  "matched": 9,
  "unmatched_tokens": 0,
  "parse_errors": 1,
  "verdict_breakdown": { "hard_bounce": 6, "soft_bounce": 3 }
}
```

- `unmatched_tokens > 0` → el VERP token del rebote no aparece en el
  tracker. Causas: rebote de un batch viejo, mailbox tiene basura
  manual del operator. No urgente.
- `parse_errors > 0` → algún email del mailbox no es un DSN válido.
  Normal en una mailbox compartida.

---

## Fase 4 — Finalize (T+72h)

Aplica los veredictos de wait-window y emite los XLSX entregables.

```bash
curl -X POST "http://localhost:8001/api/operator/jobs/<JOB_ID>/pilot-send/finalize" \
  -H "Authorization: Bearer $TOK"
```

Genera en el `run_dir` del job:

| archivo | contenido |
|---------|-----------|
| `delivery_verified.xlsx` | **El entregable bueno.** Rows que el pilot probó como deliverable. Pásalas al cliente. |
| `pilot_hard_bounces.xlsx` | Rows con DSN `hard_bounce`. Mover a `do_not_send`. |
| `pilot_soft_bounces.xlsx` | Rows con DSN `soft_bounce`. Considerar reintento en 48h. |
| `pilot_blocked_or_deferred.xlsx` | Rows con `blocked` / `deferred` (rechazo de contenido / política, no de IP). |
| `pilot_infrastructure_blocked.xlsx` | **NUEVO.** Rows con `infrastructure_blocked` o `provider_deferred`. El proveedor receptor rechazó/throttleó nuestra IP/red, no al recipient. **No mover a `do_not_send`** — re-test desde otra IP antes de decidir. |
| `pilot_summary_report.xlsx` | KPI consolidado del batch — pásalo al cliente como anexo. |
| `updated_do_not_send.xlsx` | Union del `do_not_send.xlsx` viejo + las hard_bounces / blocked / complaints nuevas. **Es el `do_not_send` que debe usar el cliente de aquí en adelante.** |
| `pilot_send_candidates.xlsx` | Snapshot del cohort enviado (auditoría). |

`finalize` también escribe un CSV agregado por dominio que se ingiere
en el aggregate de `bounce_ingestion` V2.7. Eso afecta a futuros jobs
del MISMO cliente (el sistema aprende dominios malos).

---

## Protocolos de excepción

### Pause (T+24h amarillo / T+48h ≥ 5%)

1. **No** lances un nuevo batch.
2. Corre `finalize` igual — necesitas los XLSX para auditoría.
3. Inspecciona `pilot_hard_bounces.xlsx`:
   - ¿Mismo dominio repetido? Probable catch-all malo o dominio cerrado.
   - ¿Mismo provider_family? Probable problema de reputación con ese
     proveedor (Gmail bloquea más agresivo que Yahoo).
4. Excluye los dominios problemáticos del próximo selector run, o
   sube `review_high_risk` la frontera.
5. Re-corre la limpieza V2 con la nueva info en
   `bounce_ingestion` (la ingesta del finalize lo hace automático).

### Abort (T+48h ≥ 10% o complaint ≥ 2)

1. Detén el bounce-poller para esa mailbox (no más sweeps que
   confundan):
   ```bash
   ssh root@192.3.105.145 'systemctl stop trashpanda-pilot-bounce-poller.timer'
   ```
2. **No corras finalize.** Los datos del batch están envenenados.
3. Investiga:
   - ¿Cambió DKIM/SPF/DMARC entre el bootstrap y ahora? Re-corre el
     `dig` de la checklist.
   - ¿La IP `192.3.105.145` aparece en
     [mxtoolbox blacklist check](https://mxtoolbox.com/blacklists.aspx)?
   - ¿La lista que enviaste viene de una fuente nueva no probada?
4. Reactiva el poller solo cuando haya un plan claro:
   ```bash
   ssh root@192.3.105.145 'systemctl start trashpanda-pilot-bounce-poller.timer'
   ```

### Tracker corrompido / pollster trabado

```bash
# Backup del tracker
ssh root@192.3.105.145 \
  'cp /root/trashpanda/runtime/jobs/<JOB_ID>/pilot_send_tracker.sqlite{,.bak}'

# Inspección manual
ssh root@192.3.105.145 \
  'sqlite3 /root/trashpanda/runtime/jobs/<JOB_ID>/pilot_send_tracker.sqlite \
   "SELECT state, count(*) FROM pilot_send_tracker GROUP BY state;"'
```

---

## Métricas que SÍ importan vs. ruido

**Importan** (fila a fila, decisión de seguir o parar):
- `hard_bounce_rate`
- `complaint`
- `blocked` con keywords spam/policy/blacklist en `dsn_diagnostic`

**Contexto** (no actúes solo en base a esto):
- `soft_bounce` — temporal, suele resolverse con retry-worker.
- `deferred` — greylisting, normal en B2B.
- `unknown` — ambigüedad de network. Si > 5% del batch, verifica
  `journalctl -u trashpanda-pilot-bounce-poller`.

**Sender-side — no son señal del recipient:**
- `infrastructure_blocked` — el proveedor receptor (Microsoft S3150,
  Spamhaus listed, etc.) rechazó nuestra IP/red. No dice nada del
  email individual. **Acción**: revisar reputación de la IP de envío;
  delist en Microsoft SNDS / Spamhaus si aplica; re-test desde IP
  limpia antes de marcar nada como `do_not_send`. **No** se cuenta
  para `hard_bounce_rate`.
- `provider_deferred` — Yahoo/AOL `TSS04` (volumen / quejas /
  reputación) o equivalente. Transitorio. **Acción**: bajar volumen,
  esperar, verificar postmaster del proveedor. **No** se cuenta para
  `hard_bounce_rate`.

> Regla: cuando el código SMTP (4xx/5xx) referencia explícitamente la
> IP del sender (`messages from [...] weren't sent`, `block list`,
> `TSS\d+`), **NO confiar** en él como señal del recipient. Ese rechazo
> describe al sender, no al recipient. El clasificador
> (`bounce_parser._INFRA_BLOCK_PATTERNS` /
> `_PROVIDER_DEFER_PATTERNS`) ya lo separa, pero auditá los XLSX si
> ves un volumen anómalo.

**Ruido** (no actúes nunca):
- Variaciones de timing en `sent_at` (el sleep entre rcpt es 1s).
- `unmatched_tokens > 0` aislado (basura de mailbox).
- `parse_errors > 0` aislado.

---

## Cadencia de pilots

Después del primer batch verde a 50:

| Batch # | Tamaño | Espaciado mínimo | Criterio para subir |
|---------|--------|------------------|---------------------|
| 1 | 50 | — | `hard_bounce_rate < 2%` en T+48h |
| 2 | 50 | 24h después de finalize del #1 | `hard_bounce_rate < 2%` × 2 batches consecutivos |
| 3 | 100 | 24h después de finalize del #2 | mismo |
| 4+ | 100 | 24h después de finalize del previo | revisar reputación cada 5 batches |

**Nunca subir a 200+ sin migrar a un relay con IP warmup**
(ESP tipo Postmark / SES / Sendgrid). Direct-to-MX desde un VPS
RackNerd no escala más allá de batches de 100 sin riesgo
reputacional.

---

## Checklist de "GO" antes de cada launch

```
[ ] Pre-flight (7 checks) verde
[ ] Config saved (config_ready: true)
[ ] Authorization confirmed = true (escrito en authorization_note)
[ ] Subject + body pasaron por mail-tester.com (≥ 8/10)
[ ] Preview revisada — sin duplicados, sin emails propios
[ ] batch_size ≤ tamaño autorizado para esta fase
[ ] Hora local: días de semana, 9-17 hora destino (no domingo,
    no madrugada)
[ ] Tunnel + UI corriendo, ojos en el dashboard mientras lanzas
```

Si los 8 están marcados, lanza. Si no, no.

---

## Referencias

- Schema del tracker: `app/db/pilot_send_tracker.py`
- Selector criteria: `app/pilot_send/selector.py`
- Sender (direct-to-MX o relay V2.10.13): `app/pilot_send/sender.py`
- Bounce poller IMAP: `app/pilot_send/bounce_poller.py`
- Finalize / artifacts: `app/pilot_send/finalize.py`
- Endpoints HTTP: `app/operator_routes.py` (`/api/operator/jobs/{id}/pilot-send/*`)
- Snapshot ejemplo de tracker: `examples/pilot_send_sample.csv`
- Bootstrap del VPS: `deploy/PRODUCTION_BOOTSTRAP_LOG.md`
