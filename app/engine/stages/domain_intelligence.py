"""V2.6 — Domain Intelligence stage.

Promotes domain-level reputation and cold-start handling into a
first-class row-level decision signal. Runs after
:class:`CatchAllDetectionStage` and before :class:`DecisionStage` so
the centralized policy (``apply_v2_decision_policy``) can read
canonical domain fields and apply safety caps.

Sources of evidence (offline only — no network at chunk time)
-------------------------------------------------------------

1. **Common consumer providers** — ``COMMON_PROVIDERS`` from
   :mod:`app.validation_v2.services.domain_intelligence`. Domains in
   this set (``gmail.com``, ``yahoo.com``, ``outlook.com``, …) classify
   as ``free_provider`` / ``low`` risk.
2. **Disposable domain list** — already loaded once per run by
   :class:`ScoringStage` into ``context.extras["disposable_domains"]``.
   Domains in this set classify as ``disposable`` / ``high`` risk.
3. **Suspicious-shape heuristics** — long domains, many hyphens,
   numeric-heavy. Surfaced as ``known_risky`` with ``high`` risk.
4. **Cold-start fallback** — anything else (no history, not in known
   sets, not suspicious) is ``cold_start`` with ``unknown`` risk.

The architectural seam for full historical-data integration stays
clean: callers can pre-populate ``context.extras["domain_intel_cache"]``
with classifications derived from the persistent history store before
the chunk engine starts. The stage will then prefer cached entries
over the heuristic.

What this stage does NOT do
---------------------------

  * No DNS lookups (DNSEnrichmentStage already handled DNS).
  * No SMTP probing (SMTPVerificationStage already handled SMTP).
  * No catch-all detection (CatchAllDetectionStage already handled it).
  * No history writes — the post-pass orchestrator in api_boundary
    continues to handle history writes after the run.

V2.6 priority chain (enforced in
:func:`app.v2_decision_policy.apply_v2_decision_policy`):

  1. duplicate / V1 hard-fail / SMTP-invalid remain terminal.
  2. ``domain_risk_level == "high"`` caps ``auto_approve`` → review.
  3. ``domain_cold_start AND smtp_status != "valid"`` caps approval.
  4. Otherwise the V2.4/V2.5 rules stand.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from ...validation_v2.services.domain_intelligence import (
    COMMON_PROVIDERS,
    PROVIDER_FAMILY_CORPORATE_UNKNOWN,
    SimpleDomainIntelligenceService,
    provider_family_for,
)
from ..context import PipelineContext
from ..payload import ChunkPayload
from ..stage import Stage


# --------------------------------------------------------------------------- #
# Canonical V2.6 vocabulary                                                   #
# --------------------------------------------------------------------------- #


# ``domain_intel_status``
INTEL_STATUS_AVAILABLE = "available"
INTEL_STATUS_UNAVAILABLE = "unavailable"
INTEL_STATUS_NOT_APPLICABLE = "not_applicable"
INTEL_STATUS_ERROR = "error"


# ``domain_risk_level``
RISK_LEVEL_LOW = "low"
RISK_LEVEL_MEDIUM = "medium"
RISK_LEVEL_HIGH = "high"
RISK_LEVEL_UNKNOWN = "unknown"


RISK_LEVELS: frozenset[str] = frozenset({
    RISK_LEVEL_LOW,
    RISK_LEVEL_MEDIUM,
    RISK_LEVEL_HIGH,
    RISK_LEVEL_UNKNOWN,
})


# ``domain_behavior_class``
BEHAVIOR_KNOWN_GOOD = "known_good"
BEHAVIOR_KNOWN_RISKY = "known_risky"
BEHAVIOR_COLD_START = "cold_start"
BEHAVIOR_FREE_PROVIDER = "free_provider"
BEHAVIOR_CORPORATE = "corporate"
BEHAVIOR_DISPOSABLE = "disposable"
BEHAVIOR_UNKNOWN = "unknown"


BEHAVIOR_CLASSES: frozenset[str] = frozenset({
    BEHAVIOR_KNOWN_GOOD,
    BEHAVIOR_KNOWN_RISKY,
    BEHAVIOR_COLD_START,
    BEHAVIOR_FREE_PROVIDER,
    BEHAVIOR_CORPORATE,
    BEHAVIOR_DISPOSABLE,
    BEHAVIOR_UNKNOWN,
})


# Risk levels that block ``auto_approve`` regardless of probability,
# SMTP, or catch-all signals. Read by ``apply_v2_decision_policy``.
DOMAIN_RISK_BLOCKING: frozenset[str] = frozenset({RISK_LEVEL_HIGH})


# Columns this stage appends.
DOMAIN_INTELLIGENCE_OUTPUT_COLUMNS: tuple[str, ...] = (
    "domain_intel_status",
    "domain_reputation_score",
    "domain_risk_level",
    "domain_behavior_class",
    "domain_observation_count",
    "domain_cold_start",
    "domain_intel_reason",
    # V2.10.11 — coarse provider family ("yahoo_family",
    # "google_family", "microsoft_family", "apple_family",
    # "proton_family", "corporate_unknown"). Read by the review
    # classifier and the decision/UI layer to route AOL / Verizon /
    # AT&T-Yahoo backbone consistently. Always populated; defaults
    # to ``corporate_unknown`` for any domain not in the table.
    "provider_family",
)


# --------------------------------------------------------------------------- #
# Coercion helpers                                                            #
# --------------------------------------------------------------------------- #


def _coerce_bool(val: Any) -> bool:
    if val is None:
        return False
    try:
        if pd.isna(val):
            return False
    except (TypeError, ValueError):
        pass
    try:
        return bool(val)
    except (TypeError, ValueError):
        return False


def _coerce_str(val: Any) -> str:
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except (TypeError, ValueError):
        pass
    s = str(val)
    return s if s.lower() not in {"nan", "none"} else ""


# --------------------------------------------------------------------------- #
# DomainIntelligenceClassification                                            #
# --------------------------------------------------------------------------- #


class DomainIntelligenceClassification:
    """The seven canonical V2.6 fields for one row.

    Implemented as a slotted class (not a dataclass) so the column
    order on the frame matches :data:`DOMAIN_INTELLIGENCE_OUTPUT_COLUMNS`
    exactly.
    """

    __slots__ = (
        "status",
        "reputation_score",
        "risk_level",
        "behavior_class",
        "observation_count",
        "cold_start",
        "reason",
        "provider_family",
    )

    def __init__(
        self,
        *,
        status: str,
        reputation_score: float,
        risk_level: str,
        behavior_class: str,
        observation_count: int,
        cold_start: bool,
        reason: str,
        provider_family: str = PROVIDER_FAMILY_CORPORATE_UNKNOWN,
    ) -> None:
        self.status = status
        # Clamp to [0, 1] so a buggy upstream cannot push the score
        # out of range and silently inflate downstream probability.
        self.reputation_score = max(0.0, min(1.0, float(reputation_score)))
        self.risk_level = risk_level
        self.behavior_class = behavior_class
        self.observation_count = max(0, int(observation_count))
        self.cold_start = bool(cold_start)
        self.reason = reason
        self.provider_family = provider_family

    @classmethod
    def not_applicable(cls, reason: str = "not_a_candidate") -> "DomainIntelligenceClassification":
        """Used for rows where domain intelligence is meaningless
        (V1 hard fail, no MX, missing email/domain, etc.).
        """
        return cls(
            status=INTEL_STATUS_NOT_APPLICABLE,
            reputation_score=0.0,
            risk_level=RISK_LEVEL_UNKNOWN,
            behavior_class=BEHAVIOR_UNKNOWN,
            observation_count=0,
            cold_start=False,
            reason=reason,
            provider_family=PROVIDER_FAMILY_CORPORATE_UNKNOWN,
        )

    @classmethod
    def error(cls, reason: str = "intel_error") -> "DomainIntelligenceClassification":
        return cls(
            status=INTEL_STATUS_ERROR,
            reputation_score=0.0,
            risk_level=RISK_LEVEL_UNKNOWN,
            behavior_class=BEHAVIOR_UNKNOWN,
            observation_count=0,
            cold_start=True,
            reason=reason,
            provider_family=PROVIDER_FAMILY_CORPORATE_UNKNOWN,
        )


# --------------------------------------------------------------------------- #
# Candidate selection + classification                                        #
# --------------------------------------------------------------------------- #


def is_domain_intel_candidate(row: dict[str, Any]) -> bool:
    """Return True iff domain intelligence should run for this row.

    Mirrors :func:`is_smtp_candidate`/`is_catch_all_candidate` so the
    three V2.2/V2.3/V2.6 stages address the same cohort. V1 hard-fail
    and missing-domain rows short-circuit to ``not_applicable``.
    """
    if _coerce_bool(row.get("hard_fail")):
        return False
    if not _coerce_bool(row.get("syntax_valid")):
        return False
    domain = _coerce_str(row.get("corrected_domain") or row.get("domain"))
    if not domain:
        return False
    return True


def classify_domain_heuristic(
    domain: str,
    *,
    disposable_domains: frozenset[str],
    intel_service: SimpleDomainIntelligenceService,
    min_observations_for_reputation: int = 3,
) -> DomainIntelligenceClassification:
    """Classify a domain using heuristics + the existing intel service.

    Priority (first match wins):

      1. Empty domain → not_applicable.
      2. Disposable domain → ``disposable`` / ``high`` risk.
      3. Common consumer provider (``gmail.com`` etc.) → ``free_provider``
         / ``low`` risk.
      4. Suspicious-shape pattern (long, hyphen-heavy, numeric-heavy) →
         ``known_risky`` / ``high`` risk.
      5. Anything else → ``cold_start`` / ``unknown`` risk.

    No history lookup at chunk time. ``observation_count`` is always 0
    for V2.6; the architectural seam for history-driven classification
    stays clean — callers can pre-populate
    ``context.extras["domain_intel_cache"]`` with overrides derived
    from persisted history if/when that integration is wired up.
    """
    normalized = domain.strip().lower()
    if not normalized:
        return DomainIntelligenceClassification.not_applicable("empty_domain")

    family = provider_family_for(normalized)

    # Rule 2 — disposable.
    if normalized in disposable_domains:
        return DomainIntelligenceClassification(
            status=INTEL_STATUS_AVAILABLE,
            reputation_score=0.05,
            risk_level=RISK_LEVEL_HIGH,
            behavior_class=BEHAVIOR_DISPOSABLE,
            observation_count=0,
            cold_start=False,
            reason="disposable_domain",
            provider_family=family,
        )

    # Rule 3 — common consumer provider.
    if normalized in COMMON_PROVIDERS:
        return DomainIntelligenceClassification(
            status=INTEL_STATUS_AVAILABLE,
            reputation_score=0.85,
            risk_level=RISK_LEVEL_LOW,
            behavior_class=BEHAVIOR_FREE_PROVIDER,
            observation_count=0,
            cold_start=False,
            reason="common_consumer_provider",
            provider_family=family,
        )

    # Rule 4 — suspicious-shape (use the existing intel service).
    intel = intel_service.analyze(normalized)
    if intel.get("is_suspicious_pattern"):
        suspicious_reasons = intel.get("suspicious_reasons") or []
        reason_token = (
            "suspicious_pattern:" + "|".join(suspicious_reasons)
            if suspicious_reasons
            else "suspicious_pattern"
        )
        return DomainIntelligenceClassification(
            status=INTEL_STATUS_AVAILABLE,
            reputation_score=0.10,
            risk_level=RISK_LEVEL_HIGH,
            behavior_class=BEHAVIOR_KNOWN_RISKY,
            observation_count=0,
            cold_start=False,
            reason=reason_token,
            provider_family=family,
        )

    # Rule 5 — cold start.
    return DomainIntelligenceClassification(
        status=INTEL_STATUS_AVAILABLE,
        reputation_score=0.50,
        risk_level=RISK_LEVEL_UNKNOWN,
        behavior_class=BEHAVIOR_COLD_START,
        observation_count=0,
        cold_start=True,
        reason="no_history_or_known_signal",
        provider_family=family,
    )


# --------------------------------------------------------------------------- #
# Per-domain cache                                                            #
# --------------------------------------------------------------------------- #


class DomainIntelCache:
    """Per-run cache keyed by lowercased domain.

    Mirrors :class:`SMTPCache` and :class:`CatchAllCache`. Stored on
    ``context.extras["domain_intel_cache"]`` for the duration of one
    run. Callers may seed the cache with history-derived classifications
    before the chunk engine starts; the stage will then prefer cached
    entries over heuristic ones.
    """

    def __init__(self) -> None:
        self._store: dict[str, DomainIntelligenceClassification] = {}
        self.classifications_computed: int = 0
        self.cache_hits: int = 0

    def get(self, domain: str) -> DomainIntelligenceClassification | None:
        return self._store.get(domain)

    def set(self, domain: str, value: DomainIntelligenceClassification) -> None:
        self._store[domain] = value
        self.classifications_computed += 1

    def __contains__(self, domain: str) -> bool:
        return domain in self._store

    def __len__(self) -> int:
        return len(self._store)


# --------------------------------------------------------------------------- #
# Stage                                                                       #
# --------------------------------------------------------------------------- #


def _intel_enabled(context: PipelineContext) -> bool:
    cfg = getattr(context, "config", None)
    cfg_block = (
        getattr(cfg, "domain_intelligence", None) if cfg is not None else None
    )
    if cfg_block is None:
        return True
    return bool(getattr(cfg_block, "enabled", True))


def _min_observations(context: PipelineContext) -> int:
    cfg = getattr(context, "config", None)
    cfg_block = (
        getattr(cfg, "domain_intelligence", None) if cfg is not None else None
    )
    if cfg_block is None:
        return 3
    try:
        return int(getattr(cfg_block, "min_observations_for_reputation", 3))
    except (TypeError, ValueError):
        return 3


def _disposable_set(context: PipelineContext) -> frozenset[str]:
    """Return the disposable-domain set, falling back to empty.

    ScoringStage loads this once into ``context.extras["disposable_domains"]``.
    If the stage runs before scoring (or no config wired), the empty
    set means we just don't fire the disposable rule for this run.
    """
    extras = getattr(context, "extras", {}) or {}
    val = extras.get("disposable_domains")
    if isinstance(val, frozenset):
        return val
    if isinstance(val, set):
        return frozenset(val)
    return frozenset()


class DomainIntelligenceStage(Stage):
    """Append canonical domain intelligence fields per row.

    Position in the chunk engine:

        CatchAllDetectionStage → **DomainIntelligenceStage** →
        DecisionStage

    The stage is offline-only — heuristics + the existing
    :class:`SimpleDomainIntelligenceService`. Future integration with
    persistent domain history is a single seam: pre-populate
    ``context.extras["domain_intel_cache"]`` and the stage will prefer
    those entries over the heuristic.
    """

    name = "domain_intelligence"
    requires = (
        "syntax_valid",
        "hard_fail",
        "corrected_domain",
    )
    produces = DOMAIN_INTELLIGENCE_OUTPUT_COLUMNS

    def __init__(
        self,
        intel_service: SimpleDomainIntelligenceService | None = None,
    ) -> None:
        # Constructor-injectable for tests; production runs share one
        # service instance for the life of the stage object.
        self._intel_service = intel_service or SimpleDomainIntelligenceService()

    def run(self, payload: ChunkPayload, context: PipelineContext) -> ChunkPayload:
        if not _intel_enabled(context):
            return payload.with_frame(_write_unavailable_all(payload.frame))

        cache: DomainIntelCache = context.extras.setdefault(
            "domain_intel_cache", DomainIntelCache()
        )
        disposable = _disposable_set(context)
        min_obs = _min_observations(context)

        frame = payload.frame
        rows = frame.to_dict(orient="records")

        out: dict[str, list[Any]] = {
            col: [] for col in DOMAIN_INTELLIGENCE_OUTPUT_COLUMNS
        }

        for row in rows:
            if not is_domain_intel_candidate(row):
                _emit(out, DomainIntelligenceClassification.not_applicable())
                continue

            domain = _coerce_str(
                row.get("corrected_domain") or row.get("domain")
            ).strip().lower()
            if not domain:
                _emit(out, DomainIntelligenceClassification.not_applicable("empty_domain"))
                continue

            cached = cache.get(domain)
            if cached is not None:
                cache.cache_hits += 1
                _emit(out, cached)
                continue

            try:
                classification = classify_domain_heuristic(
                    domain,
                    disposable_domains=disposable,
                    intel_service=self._intel_service,
                    min_observations_for_reputation=min_obs,
                )
            except Exception:  # pragma: no cover - defensive
                classification = DomainIntelligenceClassification.error(
                    reason="classification_exception"
                )

            cache.set(domain, classification)
            _emit(out, classification)

        new_frame = frame.copy()
        for col, values in out.items():
            new_frame[col] = values
        return payload.with_frame(new_frame)


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #


def _emit(
    out: dict[str, list[Any]],
    c: DomainIntelligenceClassification,
) -> None:
    out["domain_intel_status"].append(c.status)
    out["domain_reputation_score"].append(round(c.reputation_score, 3))
    out["domain_risk_level"].append(c.risk_level)
    out["domain_behavior_class"].append(c.behavior_class)
    out["domain_observation_count"].append(c.observation_count)
    out["domain_cold_start"].append(c.cold_start)
    out["domain_intel_reason"].append(c.reason)
    out["provider_family"].append(c.provider_family)


def _write_unavailable_all(frame: pd.DataFrame) -> pd.DataFrame:
    """Populate canonical columns with ``unavailable`` for every row.

    Used when ``domain_intelligence.enabled=false`` so DecisionStage
    and downstream consumers always see the column contract. The
    centralized policy treats ``unavailable`` the same as ``unknown``
    and never approves on it alone.
    """
    out = frame.copy()
    n = len(out)
    out["domain_intel_status"] = [INTEL_STATUS_UNAVAILABLE] * n
    out["domain_reputation_score"] = [0.0] * n
    out["domain_risk_level"] = [RISK_LEVEL_UNKNOWN] * n
    out["domain_behavior_class"] = [BEHAVIOR_UNKNOWN] * n
    out["domain_observation_count"] = [0] * n
    out["domain_cold_start"] = [True] * n
    out["domain_intel_reason"] = ["disabled_by_config"] * n
    out["provider_family"] = [PROVIDER_FAMILY_CORPORATE_UNKNOWN] * n
    return out


__all__ = [
    "BEHAVIOR_CLASSES",
    "BEHAVIOR_COLD_START",
    "BEHAVIOR_CORPORATE",
    "BEHAVIOR_DISPOSABLE",
    "BEHAVIOR_FREE_PROVIDER",
    "BEHAVIOR_KNOWN_GOOD",
    "BEHAVIOR_KNOWN_RISKY",
    "BEHAVIOR_UNKNOWN",
    "DOMAIN_INTELLIGENCE_OUTPUT_COLUMNS",
    "DOMAIN_RISK_BLOCKING",
    "DomainIntelCache",
    "DomainIntelligenceClassification",
    "DomainIntelligenceStage",
    "INTEL_STATUS_AVAILABLE",
    "INTEL_STATUS_ERROR",
    "INTEL_STATUS_NOT_APPLICABLE",
    "INTEL_STATUS_UNAVAILABLE",
    "RISK_LEVELS",
    "RISK_LEVEL_HIGH",
    "RISK_LEVEL_LOW",
    "RISK_LEVEL_MEDIUM",
    "RISK_LEVEL_UNKNOWN",
    "classify_domain_heuristic",
    "is_domain_intel_candidate",
]
