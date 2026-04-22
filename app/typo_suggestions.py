"""Conservative, non-destructive domain typo *suggestion* engine.

Design principles (see README / redesign spec):

* **Suggest only** — this module never modifies an email or its domain. It
  returns a ``TypoSuggestion`` describing what a correction *would* look
  like if one were safe. The pipeline decides (based on mode + DNS
  evidence) whether to act on that suggestion.
* **Conservative** — candidates come from an explicit whitelist of
  high-confidence providers plus the closed legacy typo map. Free-form
  fuzzy matching against arbitrary domains is not allowed.
* **Bounded distance** — a candidate is only considered if it is within
  ``max_edit_distance`` of the original domain (default 2). This prevents
  large re-writes like ``acme.net`` → ``gmail.com``.
* **Explicit TLD-swap guardrails** — a suggestion that differs *only* in
  TLD (``gmail.net`` → ``gmail.com``) is labelled ``tld_typo`` and is
  only emitted when the whitelist whitelists the suggested domain; it is
  never applied silently in suggest-only mode.
* **Auditable** — every suggestion carries a ``typo_type`` classification
  and a numeric ``confidence`` in ``[0, 1]`` so downstream reports and
  reviewers can see *why* a correction was proposed.

This module does **no DNS work**. Validation against MX records is the
responsibility of the post-DNS ``TypoSuggestionValidationStage``; the
detector here only filters on structural similarity and a small set of
provider rules.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - import only for type hints
    import pandas as pd


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


# Default whitelist of canonical, high-confidence email providers. Only
# domains in this set can be proposed as a ``suggested_domain`` by the
# whitelist/distance path. The legacy typo-map path adds a second, equally
# explicit source of candidates.
DEFAULT_PROVIDER_WHITELIST: frozenset[str] = frozenset({
    "gmail.com",
    "yahoo.com",
    "outlook.com",
    "hotmail.com",
    "icloud.com",
})


# Keyboard-adjacency map used for the ``keyboard_typo`` heuristic. Each
# entry lists neighbouring keys on a standard QWERTY layout. Only used
# for classification; it never changes the candidate set.
_QWERTY_NEIGHBOURS: dict[str, frozenset[str]] = {
    "q": frozenset("wa"),
    "w": frozenset("qeas"),
    "e": frozenset("wrds"),
    "r": frozenset("etdf"),
    "t": frozenset("ryfg"),
    "y": frozenset("tugh"),
    "u": frozenset("yihj"),
    "i": frozenset("uojk"),
    "o": frozenset("ipkl"),
    "p": frozenset("ol"),
    "a": frozenset("qwsz"),
    "s": frozenset("awedxz"),
    "d": frozenset("serfcx"),
    "f": frozenset("drtgvc"),
    "g": frozenset("ftyhbv"),
    "h": frozenset("gyujnb"),
    "j": frozenset("huikmn"),
    "k": frozenset("jiolm"),
    "l": frozenset("kop"),
    "z": frozenset("asx"),
    "x": frozenset("zsdc"),
    "c": frozenset("xdfv"),
    "v": frozenset("cfgb"),
    "b": frozenset("vghn"),
    "n": frozenset("bhjm"),
    "m": frozenset("njk"),
}


# Recognised typo-type tokens (exported for tests / reporting).
TYPO_TYPE_COMMON_PROVIDER: str = "common_provider_typo"
TYPO_TYPE_TLD: str = "tld_typo"
TYPO_TYPE_KEYBOARD: str = "keyboard_typo"
TYPO_TYPE_UNKNOWN: str = "unknown"


@dataclass(slots=True)
class TypoSuggestion:
    """A non-destructive suggestion describing a possible domain typo.

    When ``detected`` is ``False`` the other fields are all ``None`` and
    the row must be treated as if no typo was observed. The detector
    never mutates the input domain and never produces a suggestion that
    equals the original domain.
    """

    detected: bool
    original_domain: str | None
    suggested_domain: str | None
    suggested_email: str | None
    typo_type: str | None
    confidence: float | None


@dataclass(slots=True)
class TypoDetectorConfig:
    """Runtime configuration for the typo *suggestion* engine."""

    mode: str = "suggest_only"
    max_edit_distance: int = 2
    whitelist: frozenset[str] = field(default_factory=lambda: DEFAULT_PROVIDER_WHITELIST)
    require_original_no_mx: bool = True

    def __post_init__(self) -> None:
        # Normalise whitelist entries defensively so callers can pass a
        # plain list / set from YAML without losing case-folding.
        self.whitelist = frozenset(d.strip().lower() for d in self.whitelist if d)
        if self.mode not in ("suggest_only", "auto_apply_safe"):
            self.mode = "suggest_only"
        if self.max_edit_distance < 1:
            self.max_edit_distance = 1


# ---------------------------------------------------------------------------
# Levenshtein distance (small, dependency-free)
# ---------------------------------------------------------------------------


def levenshtein(a: str, b: str) -> int:
    """Classic DP Levenshtein edit distance between two strings.

    Pure, dependency-free implementation. Returns ``0`` when the two
    strings are equal. Empty inputs are handled naturally by the
    initialisation row/column.
    """

    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)

    # Rolling two-row implementation keeps memory O(min(len)).
    if len(a) < len(b):
        a, b = b, a

    previous = list(range(len(b) + 1))
    for i, ca in enumerate(a, start=1):
        current = [i] + [0] * len(b)
        for j, cb in enumerate(b, start=1):
            cost = 0 if ca == cb else 1
            current[j] = min(
                current[j - 1] + 1,        # insertion
                previous[j] + 1,           # deletion
                previous[j - 1] + cost,    # substitution
            )
        previous = current
    return previous[-1]


# ---------------------------------------------------------------------------
# Typo-type classification
# ---------------------------------------------------------------------------


def _split_tld(domain: str) -> tuple[str, str]:
    """Return ``(stem, tld)`` for a domain, or ``(domain, "")`` if no dot."""
    if "." not in domain:
        return domain, ""
    stem, _, tld = domain.rpartition(".")
    return stem, tld


def classify_typo_type(original: str, suggested: str) -> str:
    """Classify the *kind* of typo between two domains.

    Heuristics, applied in order:

    1. ``tld_typo`` — same stem, different TLD (``gmail.net`` vs ``gmail.com``).
    2. ``keyboard_typo`` — a single substitution where the substituted
       character is adjacent to the intended one on a QWERTY layout
       (``gnail.com`` → ``gmail.com``, ``n`` is adjacent to ``m``).
    3. ``common_provider_typo`` — suggested domain is a well-known provider
       and the edit distance is small (the caller already filtered by
       whitelist, so this is the default bucket for provider matches).
    4. ``unknown`` — fallback when no heuristic fires.
    """

    if not original or not suggested or original == suggested:
        return TYPO_TYPE_UNKNOWN

    o_stem, o_tld = _split_tld(original)
    s_stem, s_tld = _split_tld(suggested)

    if o_stem and o_stem == s_stem and o_tld != s_tld:
        return TYPO_TYPE_TLD

    if len(original) == len(suggested):
        diffs = [(co, cs) for co, cs in zip(original, suggested) if co != cs]
        if len(diffs) == 1:
            co, cs = diffs[0]
            neighbours = _QWERTY_NEIGHBOURS.get(cs.lower())
            if neighbours is not None and co.lower() in neighbours:
                return TYPO_TYPE_KEYBOARD

    return TYPO_TYPE_COMMON_PROVIDER


def _confidence_for(
    original: str,
    suggested: str,
    *,
    from_typo_map: bool,
    distance: int,
    max_distance: int,
) -> float:
    """Compute a simple auditable confidence value in ``[0, 1]``.

    Explicit typo-map hits get the highest confidence (0.95) because
    they were curated by hand. Whitelist/distance hits decay linearly
    with the edit distance.
    """

    if from_typo_map:
        return 0.95

    if max_distance <= 0:
        return 0.0
    # distance=1 → 0.85, distance=2 → 0.65, clamped.
    base = 1.0 - (distance / (max_distance + 1)) * 0.5
    return max(0.0, min(1.0, round(base, 3)))


# ---------------------------------------------------------------------------
# Core detection entry point
# ---------------------------------------------------------------------------


def _empty_suggestion(original_domain: str | None) -> TypoSuggestion:
    return TypoSuggestion(
        detected=False,
        original_domain=original_domain,
        suggested_domain=None,
        suggested_email=None,
        typo_type=None,
        confidence=None,
    )


def _compose_suggested_email(local_part: str | None, suggested_domain: str) -> str | None:
    if not local_part:
        return None
    return f"{local_part}@{suggested_domain}"


def detect_typo_suggestion(
    *,
    local_part: str | None,
    domain: str | None,
    config: TypoDetectorConfig,
    typo_map: dict[str, str] | None = None,
) -> TypoSuggestion:
    """Propose a safe domain correction for ``domain``, without applying it.

    Candidate sources (evaluated in priority order):

    1. The closed, human-curated legacy typo map. Only used when the
       target maps to a domain in the configured whitelist — this
       prevents stale map entries from silently re-writing TLDs outside
       the trusted set.
    2. The whitelist + edit-distance path. For each whitelisted domain
       whose edit distance to ``domain`` is in
       ``[1, max_edit_distance]``, the closest candidate wins.

    No DNS lookup is performed here. The caller is expected to validate
    the suggestion against MX records *after* this function returns.
    """

    if domain is None:
        return _empty_suggestion(None)

    normalised = domain.strip().lower()
    if not normalised:
        return _empty_suggestion(domain)

    # If the input is already a trusted provider we never propose a change.
    if normalised in config.whitelist:
        return _empty_suggestion(normalised)

    # ---- Source 1: explicit typo map (only if target is whitelisted) ----
    if typo_map:
        mapped = typo_map.get(normalised)
        if mapped and mapped != normalised and mapped in config.whitelist:
            distance = levenshtein(normalised, mapped)
            if distance == 0 or distance > max(config.max_edit_distance, 2):
                # Guard against stale/absurd map entries. A distance of 0
                # shouldn't happen given the ``!=`` check but is defensive.
                pass
            else:
                typo_type = classify_typo_type(normalised, mapped)
                confidence = _confidence_for(
                    normalised,
                    mapped,
                    from_typo_map=True,
                    distance=distance,
                    max_distance=config.max_edit_distance,
                )
                return TypoSuggestion(
                    detected=True,
                    original_domain=normalised,
                    suggested_domain=mapped,
                    suggested_email=_compose_suggested_email(local_part, mapped),
                    typo_type=typo_type,
                    confidence=confidence,
                )

    # ---- Source 2: whitelist + bounded edit distance ----
    best_candidate: str | None = None
    best_distance: int = config.max_edit_distance + 1

    for candidate in config.whitelist:
        distance = levenshtein(normalised, candidate)
        if distance == 0:
            # Should be caught by the early-return above, but keep the
            # invariant "we never propose the original domain" explicit.
            return _empty_suggestion(normalised)
        if distance < best_distance:
            best_distance = distance
            best_candidate = candidate

    if best_candidate is None or best_distance > config.max_edit_distance:
        return _empty_suggestion(normalised)

    typo_type = classify_typo_type(normalised, best_candidate)

    # TLD-only swaps are only emitted when the suggested domain is in the
    # whitelist (which is enforced above by construction) *and* the stems
    # match. That's already the case for the TLD branch of
    # ``classify_typo_type``; no extra gate needed here, but we keep the
    # confidence calibrated so downstream reports can see it.
    confidence = _confidence_for(
        normalised,
        best_candidate,
        from_typo_map=False,
        distance=best_distance,
        max_distance=config.max_edit_distance,
    )

    return TypoSuggestion(
        detected=True,
        original_domain=normalised,
        suggested_domain=best_candidate,
        suggested_email=_compose_suggested_email(local_part, best_candidate),
        typo_type=typo_type,
        confidence=confidence,
    )


# ---------------------------------------------------------------------------
# Post-DNS safety pass (frame-level)
# ---------------------------------------------------------------------------


def clear_typo_suggestion_when_original_has_mx(frame: "pd.DataFrame") -> "pd.DataFrame":
    """Post-DNS safety pass: suppress suggestions for live domains.

    Implements the "si el dominio original tiene registros válidos → NO
    sugerir" rule. Any row whose original domain came back with a valid
    deliverability record has its suggestion cleared so downstream
    reporting does not second-guess a perfectly deliverable domain. All
    other rows pass through untouched.

    Expects the column produced by the enrichment stage
    (``has_mx_record``). It is safe to call even when that column is
    absent (it becomes a no-op). Lives in this module rather than in
    ``app.normalizers`` because the latter is deliberately kept free of
    DNS/deliverability concerns.
    """

    import pandas as pd  # Local import keeps module import cheap.

    if "typo_detected" not in frame.columns:
        return frame
    if "has_mx_record" not in frame.columns:
        return frame

    result = frame.copy()

    for idx in result.index:
        detected = result.loc[idx, "typo_detected"]
        if detected is None or (isinstance(detected, float) and pd.isna(detected)):
            continue
        try:
            if not bool(detected):
                continue
        except (TypeError, ValueError):
            continue

        has_mx = result.loc[idx, "has_mx_record"]
        try:
            if has_mx is not None and not (isinstance(has_mx, float) and pd.isna(has_mx)):
                if bool(has_mx):
                    # Original domain is live — drop the suggestion entirely.
                    result.loc[idx, "typo_detected"] = False
                    result.loc[idx, "typo_corrected"] = False
                    result.loc[idx, "suggested_domain"] = None
                    result.loc[idx, "suggested_email"] = None
                    result.loc[idx, "typo_type"] = None
                    result.loc[idx, "typo_confidence"] = pd.NA
        except (TypeError, ValueError):
            continue

    result["typo_detected"] = result["typo_detected"].astype("boolean")
    result["typo_corrected"] = result["typo_corrected"].astype("boolean")
    return result
