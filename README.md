# TrashPanda

TrashPanda is a data cleaning and email validation system designed to reduce dependency on expensive per-record validation tools. It combines structural data cleaning, deterministic scoring, validation signals, and probabilistic deliverability modeling into one explainable processing engine.

The project is built around a staged pipeline and a Validation Engine V2 that treats email quality as a decision problem, not a binary lookup. Instead of simply returning `valid` or `invalid`, TrashPanda produces a probability, confidence level, validation status, and action recommendation.

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
