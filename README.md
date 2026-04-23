# TrashPanda

TrashPanda is a data cleaning and email validation system designed to reduce dependency on expensive per-record validation tools. It combines structural data cleaning, deterministic scoring, validation signals, and probabilistic deliverability modeling into one explainable processing engine.

The project is built around a staged pipeline and a Validation Engine V2 that treats email quality as a decision problem, not a binary lookup. Instead of simply returning `valid` or `invalid`, TrashPanda produces a probability, confidence level, validation status, and action recommendation.

---

## Running Locally

**Prerequisites:** Python 3.10+, Node.js 18+, `.venv` with dependencies installed.

```bash
# First-time setup (if needed)
python -m venv .venv
.venv\Scripts\activate        # Windows
pip install -r requirements.txt

cd trashpanda-next && npm install && cd ..
```

If the backend fails during startup with a message like
`ModuleNotFoundError: No module named 'sqlalchemy'`, the active Python
environment is missing the new DB packages. Reinstall backend
dependencies in the same environment you use to start the app:

```bash
.venv\Scripts\python -m pip install -r requirements.txt
```

Or install just the DB packages:

```bash
python -m pip install sqlalchemy "psycopg[binary]"
```

**Start everything with one command:**

| Option | Command |
|---|---|
| Double-click | `start_trashpanda.bat` |
| PowerShell | `.\start_trashpanda.ps1` |
| Python (cross-platform) | `python scripts/dev_launcher.py` |

The launcher will:
1. Validate `.venv` and `node_modules` exist
2. Start FastAPI on **http://localhost:8000**
3. Start Next.js on **http://localhost:3000**
4. Open the browser automatically

**To stop:** close the two terminal windows that open, or press `Ctrl+C` in the Python launcher.

---

## Current Capabilities

### Pipeline Engine

TrashPanda includes a stage-based pipeline for processing contact datasets in a deterministic and auditable way.

- Modular stages for cleaning, normalization, validation, scoring, deduplication, and export
- Deterministic processing suitable for repeatable runs and test fixtures
- Chunk-friendly design for large CSV/XLSX inputs
- Clear separation between raw input fields and derived technical fields
- Structured reporting and test coverage across major stages

### Scoring System

TrashPanda includes both the original production baseline scoring system and a newer explainable scoring layer.

- **Scoring V1**: production baseline used for deterministic quality buckets
- **Scoring V2**: normalized, explainable, calibrated scoring model
- V1/V2 comparison layer for evaluating score drift, calibration quality, and readiness to replace or run alongside V1
- Reason-code based explanations for why records receive their scores

### Validation Engine V2

Validation Engine V2 is the newer validation architecture. It is modular, explainable, and designed to collect controlled validation signals before producing a probabilistic deliverability decision.

#### Domain Intelligence

- Provider detection
- Provider reputation scoring
- Suspicious domain pattern detection
- Exclusion rules
- In-memory domain and pattern caching
- Passive intelligence collection before active network behavior

#### Control Plane

- Structured telemetry events
- Per-domain and global rate limiting
- Network execution policy controls
- Decision traces for full explainability
- Safe behavior when telemetry or optional services fail

#### SMTP Controlled Probing

- Safe single-attempt SMTP probe
- Stops at the SMTP `RCPT TO` response
- Does not send email
- Does not perform aggressive mailbox validation
- Runs only when execution policy, rate limits, candidacy, exclusions, and SMTP policy allow it

#### Catch-all + Retry Logic

- Conservative catch-all / accept-all classification
- Uses existing SMTP signals, provider behavior, and historical cache signals
- No random mailbox brute forcing
- Bounded retry strategy for timeout, connection error, and 4xx temporary failures
- Maximum one retry per request

#### Probability Layer

- `deliverability_probability` from 0.0 to 1.0
- Non-binary `validation_status`
- Product-facing `action_recommendation`
- Weighted signal aggregation
- Confidence scoring
- Deterministic explanation with positive and negative contributing factors

---

## Example Output

- `deliverability_probability`: `0.78`
- `validation_status`: `likely_valid`
- `action_recommendation`: `send_with_monitoring`

---

## What This System Is

TrashPanda is not just an email validator. It is a probabilistic decision engine for contact-data quality.

It combines structural quality, domain intelligence, provider reputation, controlled SMTP signals, catch-all assessment, retry outcomes, and calibrated probability modeling into one explainable output. The goal is to support better business decisions: send, monitor, review, verify, or block.

---

## What This System Is Not

TrashPanda is intentionally honest about its limits.

- It does not guarantee that an inbox exists.
- It does not guarantee that an address will not bounce.
- It does not perform aggressive SMTP probing.
- It does not behave like a large-scale commercial sender yet.
- It does not promise perfect deliverability.
- It does not replace real-world calibration against bounce, engagement, and delivery outcomes.

---

## Roadmap

### Phase A - Scoring V2

- Finalize calibration against representative datasets
- Decide whether V2 replaces V1 or runs in hybrid mode
- Continue evaluating V1/V2 disagreement patterns
- Improve threshold tuning for operational buckets

### Phase B - Validation Engine V2

#### B1. Domain Intelligence

- Persistent cache improvements
- Provider reputation expansion
- More complete exclusion lists
- Pattern heuristic refinement
- Better historical domain behavior tracking

#### B2. SMTP Controlled Probing

- Sampling strategy improvements
- Smarter rate limiting
- Intelligent retries, already partially implemented
- Expanded telemetry
- IP hygiene and clean probing pool, not implemented yet

#### B3. Catch-all Detection

- Better classification accuracy
- Stronger domain-level heuristics
- Improved use of historical signals
- Probabilistic modeling improvements

#### B4. Deliverability Probability Layer

- Calibration with real outcome data
- Weight tuning
- Provider-specific adjustments
- Confidence model improvements
- Better mapping between probability, product action, and business risk

---

## Future

These are advanced capabilities and are not yet implemented.

- Large-scale SMTP handshake systems
- Infrastructure-level sender behavior simulation
- Deep provider-specific tuning
- High-volume validation systems
- Enterprise-grade deliverability modeling
- Long-term reputation-aware validation infrastructure

---

## Design Principles

- Safety over aggressiveness
- Explainability first
- Deterministic behavior
- Modular architecture
- Production-minded constraints
- Clear separation between passive signals, controlled network behavior, and final decisions
- Conservative defaults for anything that could affect infrastructure reputation

---

## Conclusion

TrashPanda is a functional foundation for a full data cleaning and email validation platform. The pipeline, scoring systems, Validation Engine V2, controlled SMTP sampler, catch-all logic, retry strategy, and probability layer are in place.

The next step is calibration against real-world outcomes so the probability layer can be tuned for production decision-making at scale.



---

## Input Format (for Operators)

This section documents what the pipeline accepts as input. It is
intentionally short and operational.

### Supported formats

- CSV (`.csv`)
- Excel workbooks (`.xlsx`) — the first visible sheet is converted to
  a temporary CSV automatically (the original file is never modified).

### Minimum required columns

- `email` (the only hard requirement)

Everything else is optional and carried through to the client outputs
when present.

### Recognized column aliases

Headers are normalized before the pipeline looks them up:
lower-cased, accents stripped (`é → e`, `ñ → n`, `ü → u`), and
whitespace/dashes converted to underscores. After that, the following
aliases map to their canonical internal name:

| Canonical | Recognized aliases |
|-----------|-------------------|
| `email`   | `email`, `e-mail`, `e_mail`, `mail`, `email_address`, `correo`, `correo electrónico`, `correo_electronico` |
| `fname`   | `first_name`, `firstname`, `given_name`, `nombre`, `nombres`, `primer_nombre` |
| `lname`   | `last_name`, `lastname`, `surname`, `family_name`, `apellido`, `apellidos`, `primer_apellido` |
| `phone`   | `phone`, `phone_number`, `mobile`, `cell`, `teléfono`, `telefono`, `tel`, `celular`, `móvil` |
| `company` | `company`, `organization`, `org`, `empresa`, `compañía`, `compania`, `razón_social` |
| `city`    | `city`, `ciudad` |
| `state`   | `state`, `estado`, `provincia`, `region` |
| `zip`     | `zip`, `zip_code`, `zipcode`, `postal_code`, `codigo_postal`, `cp` |

Whenever a header is remapped, an `INFO` log line is emitted:
`Mapped column 'correo electrónico' -> 'email'`.

### Valid header examples

```
email,fname,lname,phone,company,city,state
correo,nombre,apellido,teléfono,empresa,ciudad,estado
E-Mail,First Name,Last Name,Phone Number,Organization,City,State
```

### Encoding

The pipeline tries these encodings in order and uses the first one
that decodes the file:

1. `utf-8-sig` (strips a Byte Order Mark if present)
2. `utf-8`
3. `cp1252` (common Excel-exported CSVs on Windows)
4. `latin-1` (guaranteed final fallback)

The detected encoding is logged once per file
(`Detected input encoding: cp1252 | file=contacts.csv`). The input
file is never modified; the fallback happens only at read time.

**Recommendation for operators**: save CSVs as UTF-8 when possible to
avoid ambiguity with accented characters. If the source is Excel, use
"CSV UTF-8 (comma delimited)" when exporting.

### File size / chunking

CSV/XLSX files are streamed in chunks of `chunk_size` rows (default
50,000) defined in `configs/default.yaml`. There is no hard upper
bound — 100k+ row files are routine. For very large inputs, split the
file into multiple CSVs and place them in `input/` to use
`--input-dir` mode.

### Unicode / accented emails

The email syntax validator enforces **ASCII-only local parts and
domains**. Emails such as `maría@example.com` or `user@café.com`
are rejected with syntax reasons like `local_part_invalid_chars` or
`domain_invalid_chars`. This is the documented policy: most SMTP
infrastructure does not universally support SMTPUTF8/IDN, so
accented addresses are treated as syntactically invalid rather than
passed downstream where they would likely bounce.

Accented characters in **other columns** (names, company, city,
state, etc.) are preserved unchanged.

### MX vs A record policy

DNS enrichment produces two signals per domain: `has_mx_record` and
`has_a_record`. The scoring layer applies them asymmetrically so
that A-only domains never auto-promote to the `valid` bucket:

- Domains with a valid **MX** record contribute `+50` to the score
  (strong email signal).
- Domains with **only A/AAAA** records contribute `+20` (weak
  signal; the domain resolves but is not advertised as an email
  receiver).

With the default thresholds (`high_confidence_threshold: 70`,
`review_threshold: 40`), an A-only domain combined with valid
syntax tops out at a score of `45` → `review` bucket. It cannot
reach the `valid` bucket without an MX record. The flag
`fallback_to_a_record` in `configs/default.yaml` controls whether
the A-record lookup happens at all; disabling it makes A-only
domains look like `no_mx_no_a` and fall further toward `invalid`.

### Outputs

Every run produces, under the run directory:

- Technical CSVs: `clean_high_confidence.csv`,
  `review_medium_confidence.csv`, `removed_invalid.csv`,
  `processing_report.json`/`processing_report.csv`,
  `domain_summary.csv`, `typo_corrections.csv`,
  `duplicate_summary.csv`.
- Client XLSX deliverables: `valid_emails.xlsx`,
  `review_emails.xlsx`, `invalid_or_bounce_risk.xlsx`,
  `summary_report.xlsx`.

---

## Local HTTP API (Phase 7)

The FastAPI wrapper lives in `app/server.py` and delegates processing to
`app.api_boundary.run_cleaning_job(...)`.

Install HTTP dependencies if needed:

```bash
pip install -r requirements.txt
```

If `uvicorn app.server:app` exits immediately with a missing DB module,
run:

```bash
.venv\Scripts\python -m pip install -r requirements.txt
```

Run the local backend:

```bash
uvicorn app.server:app --reload --port 8000
```

Endpoints:

- `POST /jobs` with multipart field `file` (`.csv` or `.xlsx`)
- `GET /jobs/{job_id}`
- `GET /jobs/{job_id}/artifacts/{key}`

Jobs are tracked in memory and executed with FastAPI background tasks.
Runtime files are written under `runtime/uploads/{job_id}` and
`runtime/jobs/{job_id}`.

To connect the Next.js frontend, set:

```bash
TRASHPANDA_BACKEND_URL=http://localhost:8000
```
