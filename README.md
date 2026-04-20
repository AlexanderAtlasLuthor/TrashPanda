# Email Cleaner

Pipeline local en construccion para limpieza masiva de listas de emails. El proyecto implementa un flujo de validacion y normalizacion con arquitectura modular y cerrada por fases.

## Estado actual

La implementacion actual llega hasta **Subfase 3** con:

**Subfase 2: Discovery y normalizacion base**
- aceptar `--input-dir` o `--input-file`
- descubrir archivos soportados
- soportar `.csv` y `.xlsx`
- ignorar extensiones no soportadas dentro de un directorio con warning
- convertir `XLSX` a `CSV` temporal dentro de `temp/`
- leer `CSV` por chunks con `pandas`
- normalizar headers a nombres canonicos
- validar presencia de la columna minima `email`
- normalizar valores string de forma conservadora
- agregar metadata tecnica por fila

**Subfase 3: Validacion sintactica de email (NUEVO)**
- validar sintaxis basica real de la columna `email` (offline, sin DNS)
- detectar errores obvios y relevantes:
  - ausencia o multiples `@`
  - local part o domain part vacios
  - espacios en el email
  - domain sin punto
  - puntos consecutivos
  - local part comenzando o terminando con punto
  - domain labels comenzando o terminando con guion
  - caracteres invalidos en local o domain part
- agregar columnas de validacion por fila:
  - `syntax_valid`: booleano indicando validez
  - `syntax_reason`: razon principal de validez/invalidez
  - `has_single_at`: exactamente un `@`
  - `local_part_present`: local part no vacio
  - `domain_part_present`: domain part no vacio
  - `domain_has_dot`: al menos un punto en domain
  - `contains_spaces`: presencia de espacios
- contar validos e invalidos por chunk
- procesar chunks de forma consistente (offline, deterministica)

**Subfase 4: Extracción de dominio, typo correction y comparación (NUEVO)**
- extraer `local_part_from_email` y `domain_from_email` de emails sintácticamente válidos
- aplicar typo correction conservadora del dominio con mapa cerrado y explícito (`configs/typo_map.csv`)
  - ejemplos cubiertos: `gmial.com → gmail.com`, `hotnail.com → hotmail.com`, `yaho.com → yahoo.com`, etc.
  - sin fuzzy matching, sin Levenshtein, sin heurísticas abiertas
  - el local part nunca se modifica
- agregar columnas de trazabilidad por fila:
  - `local_part_from_email`: parte local del email (antes del `@`)
  - `domain_from_email`: dominio del email (después del `@`)
  - `typo_corrected`: booleano, `True` si se aplicó corrección
  - `typo_original_domain`: dominio antes de la corrección
  - `corrected_domain`: dominio final (corregido o igual al original)
  - `domain_matches_input_column`: si el dominio final coincide con la columna `domain` del input
- contar por chunk: dominios derivados, correcciones aplicadas, mismatches con columna `domain`
- sigue siendo completamente offline y determinista

## Lo que todavia NO existe

Todavia no esta implementado (futuras subfases):

- DNS o MX lookup
- disposable email detection
- suspicious pattern detection avanzado
- scoring de calidad
- dedupe
- SQLite real
- reporting final
- export final por buckets

## Instalacion

```bash
pip install -r requirements.txt
```

## Uso de la CLI

Ejemplo con directorio de input:

```bash
python -m app.cli --input-dir ./input
```

Ejemplo con archivo individual:

```bash
python -m app.cli --input-file ./examples/sample_contacts.csv --chunk-size 25000 --workers 10
```

## Notas tecnicas

### Subfase 2: Normalizacion base
- El formato interno de procesamiento es `CSV`.
- Si un input es `XLSX`, se convierte primero a `CSV` temporal dentro de la carpeta `temp/` de la corrida.
- La normalizacion de esta fase es solo estructural. No hay interpretacion semantica de calidad del email.
- Cada fila queda enriquecida con metadata tecnica para trazabilidad en fases posteriores.

### Subfase 3: Validacion sintactica
- Las reglas son **offline** y **deterministas**: no requieren acceso a la red.
- La validacion es **sintactica**: valida si el email tiene forma valida, NO si existe realmente.
- Reglas conservadoras y explícitas, sin intento de soportar corner cases extremos del RFC.
- La informacion de validacion se agrega como columnas al chunk, lista para futuras subfases.
- **NO se modifica** la columna `email` original; solo se agregan columnas derivadas.
- **NO se toman decisiones** sobre buckets, correcciones ni acciones; solo se recopila informacion de validez.
