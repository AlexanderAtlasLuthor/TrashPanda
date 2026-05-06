"""V2.10.10.b — Action-oriented classifier for the manual_review bucket.

The V2.10.10 audit (WY_small) showed that ``review_emails.xlsx`` is a
heterogeneous bag — operationally, an operator looking at "793 require
review" needs to know *what to do with each row*, not *why the engine
classified it that way*. The decision-reason subdivision shipped in
V2.10.10 (``review_cold_start_b2b.xlsx`` etc.) answers the latter; this
module answers the former.

The classifier maps every review row onto one of five action
categories:

* ``low_risk``           — SMTP did not confirm but the row has good
                            signals (high probability, no risk flags).
                            Worth a second-pass / live SMTP retry from
                            a warmer egress, or careful inclusion in a
                            non-aggressive campaign.
* ``catch_all_consumer`` — Yahoo / AOL / Verizon-class addresses, or
                            any row where catch-all detection fired.
                            Cannot be confirmed automatically without
                            sending; treat as spam-trap risk for cold
                            campaigns.
* ``timeout_retry``      — Operational SMTP failures (blocked, timeout,
                            temp_fail, greylisted). MX is alive but the
                            probe didn't get a clean answer. Re-running
                            with a different egress or after a short
                            backoff usually resolves these.
* ``high_risk``          — Low probability or multiple negative signals.
                            Not enough confidence to send.
* ``do_not_send``        — Disposable / suspicious-shape / explicit
                            ``domain_high_risk`` decision. The
                            probability scoring would have approved
                            them but the domain itself is poison.

A *second pass candidate* is any row classified as ``low_risk`` or
``timeout_retry`` — these are the cohorts where another verification
round (live SMTP from a warmed sender, or a paid third-party probe) is
realistically expected to flip the row to confirmed-safe. Rows in the
other three categories should not be re-probed: catch-all consumer is
unverifiable by design, high_risk lacks evidence to begin with, and
do_not_send has positive evidence of risk.

The classifier is **pure** — no I/O, no side effects, no network. It
operates row-by-row on the technical-CSV columns surfaced in the
client-shaped review frame (``decision_reason``, ``smtp_status``,
``catch_all_flag``, ``deliverability_probability``, ``email``,
``client_reason``). Missing or unparseable fields collapse to the
conservative branch (``high_risk``) — the function never raises on
malformed input.
"""

from __future__ import annotations

from typing import Any, Mapping


# --------------------------------------------------------------------------- #
# Action vocabulary                                                           #
# --------------------------------------------------------------------------- #


REVIEW_ACTION_READY_PROBABLE: str = "ready_probable"
REVIEW_ACTION_LOW_RISK: str = "low_risk"
REVIEW_ACTION_CATCH_ALL_CONSUMER: str = "catch_all_consumer"
REVIEW_ACTION_TIMEOUT_RETRY: str = "timeout_retry"
REVIEW_ACTION_HIGH_RISK: str = "high_risk"
REVIEW_ACTION_DO_NOT_SEND: str = "do_not_send"

REVIEW_ACTIONS: tuple[str, ...] = (
    REVIEW_ACTION_READY_PROBABLE,
    REVIEW_ACTION_LOW_RISK,
    REVIEW_ACTION_CATCH_ALL_CONSUMER,
    REVIEW_ACTION_TIMEOUT_RETRY,
    REVIEW_ACTION_HIGH_RISK,
    REVIEW_ACTION_DO_NOT_SEND,
)


# Cohorts considered "second-pass candidates" — the rescatable rows
# where re-verification is expected to flip a meaningful share to
# confirmed-safe. Catch-all-consumer is excluded on purpose: even a
# perfect retry cannot disambiguate a Yahoo silent-accept.
# ``ready_probable`` is included because the rolled-up rescue file
# semantically means "rows where another pass should confirm"; an
# ``ready_probable`` row is one that's *almost* confirmed already.
SECOND_PASS_CANDIDATE_ACTIONS: frozenset[str] = frozenset({
    REVIEW_ACTION_READY_PROBABLE,
    REVIEW_ACTION_LOW_RISK,
    REVIEW_ACTION_TIMEOUT_RETRY,
})


# --------------------------------------------------------------------------- #
# Provider tables                                                             #
# --------------------------------------------------------------------------- #


# Domains where SMTP probing is fundamentally unreliable because the
# server accepts the handshake silently and rejects later, OR responds
# generically to any RCPT. The list is intentionally narrow — the
# catch_all_flag from the SMTP probe is the authoritative signal; this
# table is a fallback when the upstream probe didn't fire (e.g. the row
# wasn't an SMTP candidate, or SMTP was disabled). Adding a domain
# here is a one-line change but each entry should reflect documented
# operator experience, not speculation.
CONSUMER_CATCH_ALL_PROVIDERS: frozenset[str] = frozenset({
    # Yahoo family.
    "yahoo.com",
    "yahoo.co.uk",
    "yahoo.co.in",
    "yahoo.fr",
    "yahoo.de",
    "yahoo.es",
    "yahoo.it",
    "yahoo.ca",
    "yahoo.com.br",
    "yahoo.com.mx",
    "ymail.com",
    "rocketmail.com",
    # AOL.
    "aol.com",
    "aol.co.uk",
    "aim.com",
    # Verizon (now AOL/Yahoo backend but the user-facing domain stays).
    "verizon.net",
    # ATT / SBC (Yahoo Mail backend).
    "att.net",
    "sbcglobal.net",
    "bellsouth.net",
    "swbell.net",
    "pacbell.net",
    # Other historically catch-all-prone consumer hosts.
    "gmx.com",
    "gmx.net",
    "mail.com",
})


# SMTP statuses that map to operational (retryable) failures rather
# than semantic ones. ``error`` is intentionally NOT here — an
# unspecified error is more ambiguous and we route it to high_risk
# unless the probability rescues it via the low_risk branch.
_OPERATIONAL_SMTP_STATUSES: frozenset[str] = frozenset({
    "blocked",
    "timeout",
    "temp_fail",
})


# Decision reasons that map directly onto the operational/timeout
# bucket. Mirrors the SMTP-derived reasons emitted by
# :func:`apply_v2_decision_policy` (rule 5a).
_OPERATIONAL_DECISION_REASONS: frozenset[str] = frozenset({
    "smtp_blocked",
    "smtp_timeout",
    "smtp_temp_fail",
})


# Catch-all-shaped decision reasons. ``smtp_unconfirmed_for_candidate``
# is intentionally excluded — that reason is generic ("we tried but
# didn't get valid") and could mean either a Yahoo silent-accept OR a
# corporate-domain probe block; the catch_all_flag column tells us
# which one without ambiguity.
_CATCH_ALL_DECISION_REASONS: frozenset[str] = frozenset({
    "catch_all_possible",
    "catch_all_confirmed",
})


# Decision reasons that surface positive evidence the row is risky
# even though probability would otherwise approve it. ``domain_high_risk``
# fires for disposable / suspicious-shape domains.
_DO_NOT_SEND_DECISION_REASONS: frozenset[str] = frozenset({
    "domain_high_risk",
})


# Substrings that, when present in ``client_reason``, indicate the row
# carries a structural signal that defeats any retry strategy. Match
# is case-insensitive substring against the lowercased reason string.
_DO_NOT_SEND_REASON_KEYWORDS: tuple[str, ...] = (
    "disposable",
    "suspicious_pattern",
    "fake",
    "placeholder",
)


# --------------------------------------------------------------------------- #
# Probability threshold for low_risk
#
# Rows with ``deliverability_probability >= LOW_RISK_PROBABILITY_THRESHOLD``
# that fall through every other rule land in low_risk. The threshold is
# tuned slightly above the engine's review_threshold (0.50) so we don't
# rescue marginal rows by accident — only those that were one signal
# away from auto_approve. Adjust via :func:`classify_review_row` kwarg
# for tests / future per-tenant tuning.
# --------------------------------------------------------------------------- #


LOW_RISK_PROBABILITY_THRESHOLD: float = 0.55


# Probability threshold for the V2.10.11 Tier-2 ``ready_probable``
# split. A row that lands on the low_risk branch of the classifier
# AND has ``probability >= READY_PROBABLE_PROBABILITY_THRESHOLD`` is
# upgraded to ``ready_probable``. The cut is deliberately above the
# engine's auto_approve threshold's natural neighborhood so we don't
# over-promise: a row with probability=0.71 is a strong second-pass
# candidate, but not yet "confirmed safe-only".
READY_PROBABLE_PROBABILITY_THRESHOLD: float = 0.70


# --------------------------------------------------------------------------- #
# Coercion helpers                                                            #
# --------------------------------------------------------------------------- #


_TRUTHY_STRINGS: frozenset[str] = frozenset({"1", "true", "t", "yes", "y"})


def _coerce_bool(value: Any) -> bool:
    """Lenient bool coercion that accepts CSV-stringified truth values."""
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if not text or text in {"none", "nan"}:
        return False
    return text in _TRUTHY_STRINGS


def _coerce_str(value: Any) -> str:
    """Return a stripped string; treat ``None`` / ``"nan"`` as empty."""
    if value is None:
        return ""
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none"}:
        return ""
    return text


def _coerce_float(value: Any) -> float | None:
    """Best-effort float coercion. Returns ``None`` for unparseable input."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none"}:
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _domain_for(email: str) -> str:
    """Return the lowercased domain part of an email, or empty string."""
    if "@" not in email:
        return ""
    return email.rsplit("@", 1)[-1].strip().lower()


# --------------------------------------------------------------------------- #
# Classifier                                                                  #
# --------------------------------------------------------------------------- #


def classify_review_row(
    row: Mapping[str, Any],
    *,
    low_risk_probability_threshold: float = LOW_RISK_PROBABILITY_THRESHOLD,
    ready_probable_probability_threshold: float = (
        READY_PROBABLE_PROBABILITY_THRESHOLD
    ),
    consumer_catch_all_providers: frozenset[str] = CONSUMER_CATCH_ALL_PROVIDERS,
) -> str:
    """Map a single review row onto a review-action category.

    Priority chain (first match wins):

    1. ``do_not_send`` — explicit ``domain_high_risk`` decision_reason
       OR the human-readable ``client_reason`` carries a structural
       red-flag keyword (disposable / suspicious / fake / placeholder).
    2. ``catch_all_consumer`` — ``catch_all_flag=True`` OR
       ``catch_all_status`` indicates risk OR ``decision_reason``
       names a catch-all OR the email's domain matches the
       ``CONSUMER_CATCH_ALL_PROVIDERS`` table.
    3. ``timeout_retry`` — operational SMTP failure
       (``smtp_status`` in {blocked, timeout, temp_fail} OR
       ``decision_reason`` names one of those).
    4. ``low_risk`` — ``deliverability_probability`` is at or above
       ``low_risk_probability_threshold`` (default 0.55) AND none of
       the prior rules fired.
    5. ``high_risk`` — fallback. Probability is below the low-risk
       threshold and no clearer signal points to a specific action.

    The function operates on a single mapping (one ``pandas.Series``-
    like row or a plain ``dict``) and returns one of the constants in
    :data:`REVIEW_ACTIONS`.
    """
    decision_reason = _coerce_str(row.get("decision_reason"))
    client_reason = _coerce_str(row.get("client_reason")).lower()
    smtp_status = _coerce_str(row.get("smtp_status")).lower()
    catch_all_status = _coerce_str(row.get("catch_all_status")).lower()
    catch_all_flag = _coerce_bool(row.get("catch_all_flag"))
    probability = _coerce_float(row.get("deliverability_probability"))
    email = _coerce_str(row.get("email"))
    domain = _domain_for(email)
    # V2.10.11 — prefer the canonical ``provider_family`` column when
    # the DomainIntelligenceStage populated it. Falls back to the
    # local ``CONSUMER_CATCH_ALL_PROVIDERS`` table for runs where the
    # stage didn't fire.
    provider_family = _coerce_str(row.get("provider_family")).lower()

    # 1. Strong negative evidence — the row carries a domain-level or
    #    structural signal that no retry will fix.
    if decision_reason in _DO_NOT_SEND_DECISION_REASONS:
        return REVIEW_ACTION_DO_NOT_SEND
    for keyword in _DO_NOT_SEND_REASON_KEYWORDS:
        if keyword in client_reason:
            return REVIEW_ACTION_DO_NOT_SEND

    # 2. Catch-all consumer. The catch_all_flag from the SMTP probe is
    #    authoritative when present; the provider_family column from
    #    DomainIntelligenceStage is the next-most-reliable signal for
    #    Yahoo-backbone domains (AOL / Verizon / AT&T / SBCGlobal /
    #    Bellsouth / Pacbell). The local CONSUMER_CATCH_ALL_PROVIDERS
    #    table stays as the last-resort fallback.
    if catch_all_flag:
        return REVIEW_ACTION_CATCH_ALL_CONSUMER
    if catch_all_status in {"confirmed_catch_all", "possible_catch_all"}:
        return REVIEW_ACTION_CATCH_ALL_CONSUMER
    if decision_reason in _CATCH_ALL_DECISION_REASONS:
        return REVIEW_ACTION_CATCH_ALL_CONSUMER
    if smtp_status == "catch_all_possible":
        return REVIEW_ACTION_CATCH_ALL_CONSUMER
    if provider_family == "yahoo_family":
        return REVIEW_ACTION_CATCH_ALL_CONSUMER
    if domain and domain in consumer_catch_all_providers:
        return REVIEW_ACTION_CATCH_ALL_CONSUMER

    # 3. Operational SMTP failures — retry-eligible.
    if decision_reason in _OPERATIONAL_DECISION_REASONS:
        return REVIEW_ACTION_TIMEOUT_RETRY
    if smtp_status in _OPERATIONAL_SMTP_STATUSES:
        return REVIEW_ACTION_TIMEOUT_RETRY

    # 4. Probability rescue — high-confidence rows that SMTP simply
    #    didn't confirm (cold-start B2B is the dominant case here).
    #    V2.10.11 splits this branch into two tiers based on
    #    probability so the UI can render "almost-ready" separately
    #    from "rescatable":
    #
    #      * probability >= 0.70 → ``ready_probable`` (Tier 2)
    #      * 0.55 <= probability < 0.70 → ``low_risk`` (Tier-2
    #        secondary, still rescatable)
    if probability is not None:
        if probability >= ready_probable_probability_threshold:
            return REVIEW_ACTION_READY_PROBABLE
        if probability >= low_risk_probability_threshold:
            return REVIEW_ACTION_LOW_RISK

    # 5. Fallback — low probability or unparseable.
    return REVIEW_ACTION_HIGH_RISK


def is_second_pass_candidate(action: str) -> bool:
    """Return True iff ``action`` is in the second-pass-candidate set."""
    return action in SECOND_PASS_CANDIDATE_ACTIONS


__all__ = [
    "CONSUMER_CATCH_ALL_PROVIDERS",
    "LOW_RISK_PROBABILITY_THRESHOLD",
    "READY_PROBABLE_PROBABILITY_THRESHOLD",
    "REVIEW_ACTIONS",
    "REVIEW_ACTION_CATCH_ALL_CONSUMER",
    "REVIEW_ACTION_DO_NOT_SEND",
    "REVIEW_ACTION_HIGH_RISK",
    "REVIEW_ACTION_LOW_RISK",
    "REVIEW_ACTION_READY_PROBABLE",
    "REVIEW_ACTION_TIMEOUT_RETRY",
    "SECOND_PASS_CANDIDATE_ACTIONS",
    "classify_review_row",
    "is_second_pass_candidate",
]
