# TrashPanda

Pipeline local para limpieza masiva de listas de emails. Procesa CSV/XLSX por chunks, normaliza, valida, corrige typos, verifica DNS/MX, deduplica, puntua y exporta por buckets de confianza — sin validacion SMTP, sin costos por registro.

---

## Estado de implementacion

### Subfase 2: Discovery y normalizacion base
- Acepta `--input-dir` o `--input-file`
- Descubre archivos `.csv` y `.xlsx`; ignora extensiones no soportadas con warning
- Convierte `XLSX` a `CSV` temporal dentro de `temp/`
- Lee `CSV` por chunks con `pandas`
- Normaliza headers a nombres canonicos
- Valida presencia de columna minima `email`
- Normaliza valores string de forma conservadora
- Agrega metadata tecnica por fila

### Subfase 3: Validacion sintactica de email
- Valida sintaxis basica offline y deterministica
- Detecta: ausencia/multiples `@`, local/domain parts vacios, espacios, domain sin punto, puntos consecutivos, local part comenzando/terminando con punto, domain labels con guion inicial/final, caracteres invalidos
- Columnas agregadas: `syntax_valid`, `syntax_reason`, `has_single_at`, `local_part_present`, `domain_part_present`, `domain_has_dot`, `contains_spaces`

### Subfase 4: Extraccion de dominio y typo correction
- Extrae `local_part_from_email` y `domain_from_email` de emails validos
- Aplica typo correction conservadora con mapa cerrado (`configs/typo_map.csv`); sin fuzzy matching
- Columnas agregadas: `typo_corrected`, `typo_original_domain`, `corrected_domain`, `domain_matches_input_column`

### Subfase 5: Validacion DNS/MX del dominio corregido
- Consulta MX del `corrected_domain`; fallback a A/AAAA configurable
- Cache en memoria por `corrected_domain`, compartido entre todos los chunks y archivos del run
- Resolucion paralela de dominios nuevos via `ThreadPoolExecutor` (configurable con `max_workers`)
- Solo filas con `syntax_valid=True` y `corrected_domain` no nulo son consultadas; el resto recibe `pd.NA`
- Manejo explicito de: NXDOMAIN, NoAnswer, Timeout, NoNameservers, error generico
- Columnas agregadas: `dns_check_performed`, `domain_exists`, `has_mx_record`, `has_a_record`, `dns_error`
- Semantica de `domain_exists`: `True` solo si hay MX o A/AAAA util; NXDOMAIN y timeout producen `False`
- Metricas por chunk: `dns_new_queries`, `dns_cache_hits`, `mx_found`, `a_fallback`, `dns_failures`
- Metricas globales por run: `dns_total_queries`, `dns_total_cache_hits`
- **Sin SMTP. Sin verificacion de inbox individual. Sin scoring. Sin decisiones finales.**

### Subfase 6: Scoring y asignacion de bucket preliminar
- Consume senales de subfases 3, 4 y 5; no genera nuevas consultas de red
- Detecta **hard fails** (fuerzan score=0, bucket=invalid): sintaxis invalida, dominio ausente, NXDOMAIN confirmado
- Calcula **score 0-100** con pesos fijos: `syntax_valid` +25, MX presente +50, solo A/AAAA +20; penalizaciones: timeout/no_nameservers/error -15, no_mx/no_mx_no_a -10, typo corrected -3, domain mismatch -5
- Produce **score_reasons** como string separado por `|` en orden estable (sintaxis → mx/a → dns → typo → mismatch)
- Asigna **preliminary_bucket**: `high_confidence` (score≥70), `review` (score≥40), `invalid` (resto); umbrales configurables
- Columnas agregadas: `hard_fail`, `score`, `score_reasons`, `preliminary_bucket`
- Metricas por chunk: `hard_fails`, `high_confidence`, `review`, `invalid`, `avg_score`
- Metricas globales por run: `scoring_hard_fails`, `high_confidence`, `review`, `invalid`
- **Sin SMTP. Sin deduplicacion. Sin export. Las decisiones finales se difieren a Subfase 7 (dedupe).**

### Pendiente (futuras subfases)
- Deteccion de dominios desechables
- Deteccion de patrones sospechosos
- Deduplicacion global
- Export por buckets
- Reportes finales JSON/CSV

---

## Objetivo del proyecto

Limpiar bases masivas de contactos (aprox. 2.86M registros, ~25 listas de ~114k filas) sin pagar validacion externa por registro. El sistema reduce volumen basura, normaliza la data, detecta duplicados, corrige typos frecuentes, descarta dominios inexistentes, segmenta por riesgo y produce archivos de entrega profesional.

**Lo que NO promete este sistema:**
- Que un inbox individual existe
- Que un email no rebotara
- Que el email pertenece hoy a la misma persona
- Que el correo no sea catch-all

**Lo que SI entrega:**
- Base mucho mas limpia y normalizada
- Segmentacion clara: mantener / revisar / eliminar
- Transparencia total de reglas aplicadas
- Reproducibilidad completa
- Velocidad para millones de registros

---

## Arquitectura del proyecto

```
TrashPanda/
  app/
    __init__.py
    config.py          # Carga configuracion desde YAML / env vars
    logger.py          # Logging estructurado
    models.py          # Tipos y estructuras de datos internas
    pipeline.py        # Orquesta el proceso completo
    rules.py           # Reglas de negocio combinadas
    normalizers.py     # Normalizacion de strings, columnas y emails
    validators.py      # Validacion sintactica y reglas duras
    dns_utils.py       # Resolucion MX/A con cache y concurrencia limitada
    dedupe.py          # Deduplicacion exacta y futura logica de priorizacion
    scoring.py         # Score de calidad y asignacion de bucket
    reporting.py       # Construccion de resumenes y reportes finales
    io_utils.py        # Lectura/escritura CSV/XLSX, manejo de chunks
    cli.py             # Interfaz de linea de comandos
  configs/
    default.yaml              # Configuracion principal
    disposable_domains.txt    # Lista de dominios desechables conocidos
    typo_map.csv              # Mapa de correcciones de dominios
  input/
  output/
  logs/
  tests/
    test_normalizers.py
    test_validators.py
    test_dns_utils.py
    test_dedupe.py
    test_scoring.py
    test_pipeline_small.py
  requirements.txt
  README.md
```

---

## Archivos de salida

| Archivo | Descripcion |
|---|---|
| `clean_high_confidence.csv` | Registros con alta confianza estructural y de dominio |
| `review_medium_confidence.csv` | Registros que no deben eliminarse automaticamente pero requieren revision |
| `removed_invalid.csv` | Registros descartados por reglas duras |
| `processing_report.json` | Resumen tecnico con metricas por archivo y globales |
| `processing_report.csv` | Version tabular del resumen |
| `domain_summary.csv` | Resumen por dominio: volumen, DNS, MX, correcciones, aprobados |
| `typo_corrections.csv` | Registro auditado de correcciones automaticas aplicadas |
| `duplicate_summary.csv` | Resumen de duplicados eliminados |
| `logs/` | Logs detallados por ejecucion |

---

## Filosofia de clasificacion

Cada fila termina en uno de tres buckets:

**High confidence** — conservar
- Email no vacio, sintaxis valida, dominio valido con DNS resolvible, preferiblemente con MX, no duplicado, no temporal, sin patrones basura

**Medium confidence / review** — no eliminar automaticamente
- Dominio existe pero sin MX claro (solo A record), estructura rara pero potencialmente utilizable, campos de nombre vacios con email valido, correccion automatica aplicada con ambiguedad moderada

**Invalid / remove** — descartar
- Vacio, sin `@`, multiples `@`, dominio inexistente, TLD imposible, patrones basura extremos, duplicado descartado, dominio temporal

---

## Scoring de calidad

Cada registro recibe un puntaje numerico que determina su bucket:

| Criterio | Puntos |
|---|---|
| Sintaxis valida | +25 |
| Dominio resolvible | +20 |
| MX presente | +25 |
| Typo corregido (alta confianza) | +5 |
| No duplicado | +10 |
| No dominio temporal | +10 |
| Nombre presente | +3 |
| Apellido presente | +2 |
| Correccion dudosa | -10 |
| Mismatch domain columna vs email | -5 |
| Sin MX pero con A record | -10 |
| Timeout DNS | -15 |
| Patron sospechoso | -25 |

**Umbrales configurables:**
- `70+` → high confidence
- `40–69` → review
- `<40` → invalid

---

## Orden del pipeline

```
Etapa 1:  Carga — leer archivo, validar columnas obligatorias
Etapa 2:  Limpieza basica — trim, lowercase email/domain, normalizar vacios
Etapa 3:  Derivacion — extraer domain_from_email, comparar con columna domain
Etapa 4:  Validacion sintactica — marcar syntax_valid
Etapa 5:  Correccion de typos — recomputar domain si hubo cambio
Etapa 6:  Dominio unico — construir catalogo de dominios para DNS
Etapa 7:  DNS/MX — resolver dominios unicos, unir resultados al dataset
Etapa 8:  Reglas sospechosas — disposable, patrones basura
Etapa 9:  Scoring — score + reasons por fila
Etapa 10: Dedupe — deduplicar globalmente por email_normalized
Etapa 11: Decision final — high confidence / review / invalid
Etapa 12: Export — escribir por bucket
Etapa 13: Reporte — estadisticas por archivo y globales
```

---

## Columnas derivadas internas

Cada fila procesada incluye:

```
email_normalized
domain_from_email
syntax_valid
typo_corrected
typo_original_domain
domain_matches_input_column
dns_status
mx_present
a_present
disposable_domain
suspicious_pattern
duplicate_flag
score
decision
decision_reasons
```

Ejemplo de trazabilidad por fila:
```
decision = invalid
decision_reasons = missing_at_symbol|domain_nxdomain
score = 5
```

---

## Instalacion

```bash
pip install -r requirements.txt
```

---

## Uso de la CLI

```bash
python -m app.cli \
  --input-dir ./input \
  --output-dir ./output/run_001 \
  --chunk-size 50000 \
  --workers 20 \
  --config ./configs/default.yaml
```

**Opciones disponibles:**

| Flag | Descripcion |
|---|---|
| `--input-file` | Archivo individual a procesar |
| `--input-dir` | Directorio con multiples archivos |
| `--output-dir` | Carpeta de salida para esta corrida |
| `--chunk-size` | Filas por chunk (default: 50000) |
| `--workers` | Concurrencia para DNS (default: 20) |
| `--config` | Ruta al archivo YAML de configuracion |
| `--disable-dns` | Omitir lookups DNS/MX |
| `--dry-run` | Ejecutar sin escribir outputs |
| `--sample-size` | Procesar solo N filas de muestra |
| `--resume` | Continuar corrida previa interrumpida |

---

## Configuracion (default.yaml)

```yaml
chunk_size: 50000
max_workers: 20
high_confidence_threshold: 70
review_threshold: 40
fallback_to_a_record: true
invalid_if_disposable: true
dns_timeout_seconds: 4
retry_dns_times: 1
export_review_bucket: true
keep_original_columns: true
```

---

## Decisiones tecnicas clave

**Sin validacion SMTP de buzon individual**
No se construye sondeo SMTP en V1. Razones: alto riesgo de bloqueo, baja confiabilidad en escala, muchos servidores aceptan y rebotan despues, riesgo de reputacion de IP, complejidad innecesaria.

**Resolver por dominio unico, no por fila**
Con 2.8M filas y ~200k dominios unicos, el costo DNS baja dramaticamente. Nunca se resuelve el mismo dominio dos veces en una corrida.

**Caching obligatorio**
Resultado de resolucion almacenado por dominio durante la corrida, opcionalmente persistido entre corridas.

**Typo correction conservadora**
Solo dominios de error extremadamente conocidos y de alta confianza. Sin fuzzy matching, sin Levenshtein, sin heurísticas abiertas. Mapa cerrado y explicito en `configs/typo_map.csv`.

**Deduplicacion con prioridad por completitud**
Cuando un email aparece mas de una vez, se conserva la fila con mayor cantidad de campos no vacios. Si empatan, se conserva la primera ocurrencia.

---

## Metricas de reporte

Por archivo y globalmente:

```
total_rows
valid_syntax_count / invalid_syntax_count
typo_corrected_count
unique_domains
domains_with_mx / domains_without_mx
nxdomain_count
disposable_count
duplicate_count
high_confidence_count / review_count / invalid_count
```

Adicionalmente: top 100 dominios por volumen, top dominios invalidos, top dominios corregidos, tabla agregada de `decision_reasons`.

---

## Hoja de ruta estrategica

### V1 (actual) — Pre-validator fuerte y gratuito
- Ingest CSV/XLSX
- Normalizacion y validacion sintactica
- Typo correction
- MX/A lookup con cache
- Deteccion de dominios desechables
- Dedupe exacto por email normalizado
- Scoring y export por buckets
- Reportes completos

### V1.1 — Mejoras de calidad
- Cache persistente en SQLite o JSONL entre corridas
- Mejor scoring con mas heuristicas
- Mejores patrones sospechosos
- Resumen HTML o dashboard simple

### V2 — Verificacion avanzada
- Modulo SMTP experimental para muestras pequenas
- Pool de IPs limpias con rate limiting serio
- Deteccion de catch-all / accept-all
- Clasificacion probabilistica (no binaria)
- Retries inteligentes y listas de exclusion
- Telemetria y sistema de reputacion por proveedor
- UI basica

---

## Contexto y alcance

Este sistema es una **V1 de reduccion fuerte de basura**, no una fuente de verdad absoluta sobre entregabilidad. Su valor esta en bajar dramaticamente el volumen malo sin costo por validacion externa, dejando al cliente con una base mucho mas sana y una capa de transparencia profesional.

El sistema procesa el formato de lista estandar:

```
id, email, domain, fname, lname, state, address, county, city, zip, website, ip
```

Volumen objetivo: ~25 listas x ~114k filas = ~2.86M registros en total.

---

## Tests

```bash
pytest tests/
```

Cobertura incluida:
- Normalizacion de email
- Extraccion de dominio
- Validacion sintactica
- Typo correction
- Scoring
- Reglas de descarte
- Pipeline con mini-CSV de ejemplo
- Dedupe correcto
- Export por buckets
- Reportes con conteos esperados

---

## Principios de implementacion

- Procesar por chunks; no cargar archivos completos en memoria si se puede evitar
- Resolver DNS por dominio unico y cachear resultados
- Registrar `decision_reasons` detallados por cada fila
- Mantener columnas originales en outputs; agregar columnas derivadas sin destruirlas
- Manejar fallos parciales (fila corrupta, encoding raro, timeout DNS) sin abortar la corrida
- No enviar emails, no hacer conexiones SMTP a buzones, no modificar archivos originales
