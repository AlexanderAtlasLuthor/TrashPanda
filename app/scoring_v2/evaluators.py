"""Concrete signal evaluators for Scoring V2.

Each evaluator reads a plain-dict row and emits zero, one, or more
``ScoringSignal`` instances. No scoring math is performed here — the
engine aggregates signals in a later phase. Evaluators are deterministic
and side-effect-free; they never mutate the input row.

Signal-design choices (provisional but internally consistent):

  * ``value`` is ``1.0`` for every signal these evaluators emit.
    Binary conditions either fire at full strength or do not fire.
    Gradient values are reserved for future evaluators (e.g., a
    probabilistic deliverability score).
  * ``weight`` approximately mirrors the V1 point budget so the two
    systems can be compared side-by-side and a future calibration
    pass can reuse the same scale.
  * ``direction`` is ``"positive"`` when the signal raises confidence
    in the row, ``"negative"`` when it lowers it. No evaluator in this
    batch emits ``"neutral"``.
  * ``confidence`` reflects how much the underlying evidence can be
    trusted. Deterministic checks (syntax, typo map, domain presence,
    domain comparison) use ``1.0``. DNS signals use lower confidence
    when the underlying error is transient (timeouts, generic errors)
    and higher confidence when the signal is definitive (NXDOMAIN,
    confirmed no-record responses).
  * ``explanation`` is a stable, human-readable sentence safe to
    surface in audit reports.
"""

from __future__ import annotations

from .evaluator import SignalEvaluator
from .signal import ScoringSignal


# ---------------------------------------------------------------------------
# Syntax
# ---------------------------------------------------------------------------


class SyntaxSignalEvaluator(SignalEvaluator):
    """Emit a positive signal on valid syntax, a negative signal otherwise.

    Missing / None / non-True values all collapse to the negative branch —
    absence of syntax validation is treated as a negative signal, not as
    silence. This is an intentional design choice: a scoring system that
    ignored unknown syntax would over-weight rows with missing data.
    """

    name = "syntax"

    def evaluate(self, row: dict) -> list[ScoringSignal]:
        if row.get("syntax_valid") is True:
            return [
                ScoringSignal(
                    name="syntax",
                    direction="positive",
                    value=1.0,
                    weight=25.0,
                    confidence=1.0,
                    reason_code="syntax_valid",
                    explanation="Email passes offline syntax validation.",
                )
            ]
        return [
            ScoringSignal(
                name="syntax",
                direction="negative",
                value=1.0,
                weight=50.0,
                confidence=1.0,
                reason_code="syntax_invalid",
                explanation="Email does not pass offline syntax validation.",
            )
        ]


# ---------------------------------------------------------------------------
# Domain presence
# ---------------------------------------------------------------------------


class DomainPresenceSignalEvaluator(SignalEvaluator):
    """Emit a positive signal when a corrected_domain is present.

    A non-empty string is treated as present. Missing / None / empty
    string → negative. The negative carries the same weight as other
    hard-stop-class signals so the future policy layer can treat
    "no_domain" as a hard stop without re-weighting.
    """

    name = "domain_presence"

    def evaluate(self, row: dict) -> list[ScoringSignal]:
        domain = row.get("corrected_domain")
        if isinstance(domain, str) and domain:
            return [
                ScoringSignal(
                    name="domain_presence",
                    direction="positive",
                    value=1.0,
                    weight=10.0,
                    confidence=1.0,
                    reason_code="domain_present",
                    explanation="A usable corrected_domain is present on the row.",
                )
            ]
        return [
            ScoringSignal(
                name="domain_presence",
                direction="negative",
                value=1.0,
                weight=50.0,
                confidence=1.0,
                reason_code="no_domain",
                explanation="No corrected_domain is present on the row.",
            )
        ]


# ---------------------------------------------------------------------------
# Typo correction
# ---------------------------------------------------------------------------


class TypoCorrectionSignalEvaluator(SignalEvaluator):
    """Emit a negative signal only when a typo correction was applied.

    No correction means "no applicable signal" — the absence of a typo
    is not evidence of quality, so silence is the correct behavior.
    """

    name = "typo_correction"

    def evaluate(self, row: dict) -> list[ScoringSignal]:
        if row.get("typo_corrected") is True:
            return [
                ScoringSignal(
                    name="typo_correction",
                    direction="negative",
                    value=1.0,
                    weight=3.0,
                    confidence=1.0,
                    reason_code="typo_corrected",
                    explanation="The domain required correction via the typo map.",
                )
            ]
        return []


# ---------------------------------------------------------------------------
# Domain match
# ---------------------------------------------------------------------------


class DomainMatchSignalEvaluator(SignalEvaluator):
    """Emit a positive/negative signal based on corrected vs. input domain.

    Missing comparison (None, missing key, any non-bool) is silent: we
    cannot infer anything from the absence of the comparison, so no
    signal is emitted in that case.
    """

    name = "domain_match"

    def evaluate(self, row: dict) -> list[ScoringSignal]:
        val = row.get("domain_matches_input_column")
        if val is True:
            return [
                ScoringSignal(
                    name="domain_match",
                    direction="positive",
                    value=1.0,
                    weight=5.0,
                    confidence=1.0,
                    reason_code="domain_match",
                    explanation="Corrected domain equals the input domain column.",
                )
            ]
        if val is False:
            return [
                ScoringSignal(
                    name="domain_match",
                    direction="negative",
                    value=1.0,
                    weight=5.0,
                    confidence=1.0,
                    reason_code="domain_mismatch",
                    explanation="Corrected domain differs from the input domain column.",
                )
            ]
        return []


# ---------------------------------------------------------------------------
# DNS
# ---------------------------------------------------------------------------


# (weight, confidence, explanation) per dns_error token.
_DNS_ERROR_META: dict[str, tuple[float, float, str]] = {
    "nxdomain": (
        50.0,
        0.95,
        "Domain does not exist in DNS (NXDOMAIN).",
    ),
    "timeout": (
        15.0,
        0.30,
        "DNS query timed out — result is uncertain.",
    ),
    "no_nameservers": (
        15.0,
        0.50,
        "No nameservers available for the domain.",
    ),
    "no_mx": (
        10.0,
        0.85,
        "Domain resolves but has no MX record; A-fallback disabled.",
    ),
    "no_mx_no_a": (
        10.0,
        0.90,
        "Domain has neither MX nor A/AAAA records.",
    ),
    "error": (
        15.0,
        0.30,
        "Generic DNS resolution error.",
    ),
}

# Map of dns_error → reason_code for the emitted signal.
_DNS_REASON_CODES: dict[str, str] = {
    "nxdomain": "nxdomain",
    "timeout": "dns_timeout",
    "no_nameservers": "dns_no_nameservers",
    "no_mx": "dns_no_mx",
    "no_mx_no_a": "dns_no_mx_no_a",
    "error": "dns_error",
}


class DnsSignalEvaluator(SignalEvaluator):
    """Emit DNS-derived positive and/or negative signals.

    Emission rules:
      * Positive (mutually exclusive): ``has_mx_record=True`` →
        ``mx_present``; else ``has_a_record=True`` → ``a_fallback``.
      * Negative: a single specific signal derived from ``dns_error``,
        if the error token is in the known set. Unknown error tokens
        are ignored (fall through to the generic branch below).
      * Fallback negative: when ``domain_exists is False`` and no
        specific DNS negative was emitted, emit ``domain_not_resolving``
        so the absence of resolution is still audibly represented.

    The evaluator may emit both a positive (MX/A) and a negative (error)
    signal when both are warranted by the row data — that combination is
    rare but possible (e.g., a partial resolution that logged a transient
    error while also yielding records). The design is intentionally
    honest about mixed evidence.
    """

    name = "dns"

    def evaluate(self, row: dict) -> list[ScoringSignal]:
        signals: list[ScoringSignal] = []

        # Positive branch: MX > A fallback, mutually exclusive.
        if row.get("has_mx_record") is True:
            signals.append(
                ScoringSignal(
                    name="dns",
                    direction="positive",
                    value=1.0,
                    weight=50.0,
                    confidence=0.95,
                    reason_code="mx_present",
                    explanation="Domain has a resolvable MX record.",
                )
            )
        elif row.get("has_a_record") is True:
            signals.append(
                ScoringSignal(
                    name="dns",
                    direction="positive",
                    value=1.0,
                    weight=20.0,
                    confidence=0.75,
                    reason_code="a_fallback",
                    explanation=(
                        "Domain has no MX record but resolves to an A/AAAA "
                        "address (weaker evidence of email hosting)."
                    ),
                )
            )

        # Negative branch: specific dns_error → specific signal.
        emitted_specific_dns_negative = False
        dns_error = row.get("dns_error")
        if isinstance(dns_error, str) and dns_error in _DNS_ERROR_META:
            weight, confidence, explanation = _DNS_ERROR_META[dns_error]
            signals.append(
                ScoringSignal(
                    name="dns",
                    direction="negative",
                    value=1.0,
                    weight=weight,
                    confidence=confidence,
                    reason_code=_DNS_REASON_CODES[dns_error],
                    explanation=explanation,
                )
            )
            emitted_specific_dns_negative = True

        # Fallback negative: domain_exists=False without a specific error.
        if (
            row.get("domain_exists") is False
            and not emitted_specific_dns_negative
        ):
            signals.append(
                ScoringSignal(
                    name="dns",
                    direction="negative",
                    value=1.0,
                    weight=10.0,
                    confidence=0.60,
                    reason_code="domain_not_resolving",
                    explanation=(
                        "Domain did not resolve to any usable record, but no "
                        "specific DNS error was recorded."
                    ),
                )
            )

        return signals
