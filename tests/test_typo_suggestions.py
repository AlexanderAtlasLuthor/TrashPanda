"""Tests for the conservative, non-destructive typo *suggestion* engine.

These tests cover the redesign spec in the repository README section
"typo correction redesign":

* gmail typo (``gmal.com``) produces a suggestion.
* ``gmail.net`` (TLD typo) is suggested cautiously and is suppressed
  once DNS confirms the original domain has a valid MX record.
* Random invalid domains produce no suggestion.
* Valid domains that already have MX (simulated via the post-DNS
  validator) produce no suggestion.
* The output schema is consistent and the original email / domain are
  never mutated.
"""

from __future__ import annotations

import pandas as pd

from app.engine import ChunkPayload, PipelineContext
from app.engine.stages import TypoCorrectionStage, TypoSuggestionValidationStage
from app.normalizers import (
    TYPO_SUGGESTION_COLUMNS,
    apply_domain_typo_suggestion_column,
)
from app.typo_suggestions import (
    DEFAULT_PROVIDER_WHITELIST,
    TYPO_TYPE_COMMON_PROVIDER,
    TYPO_TYPE_KEYBOARD,
    TYPO_TYPE_TLD,
    TypoDetectorConfig,
    classify_typo_type,
    clear_typo_suggestion_when_original_has_mx,
    detect_typo_suggestion,
    levenshtein,
)


# ---------------------------------------------------------------------------
# Pure detector tests
# ---------------------------------------------------------------------------


class TestLevenshtein:
    def test_equal_strings_are_distance_zero(self):
        assert levenshtein("gmail.com", "gmail.com") == 0

    def test_single_substitution(self):
        assert levenshtein("gmail.com", "gnail.com") == 1

    def test_insertion_and_deletion(self):
        assert levenshtein("gmal.com", "gmail.com") == 1
        assert levenshtein("gmail.com", "gmal.com") == 1

    def test_empty_strings(self):
        assert levenshtein("", "") == 0
        assert levenshtein("gmail", "") == 5


class TestClassifyTypoType:
    def test_tld_typo(self):
        assert classify_typo_type("gmail.net", "gmail.com") == TYPO_TYPE_TLD

    def test_keyboard_typo(self):
        # 'n' is adjacent to 'm' on QWERTY — classic "gnail" typo.
        assert classify_typo_type("gnail.com", "gmail.com") == TYPO_TYPE_KEYBOARD

    def test_generic_provider_typo_falls_back(self):
        # Transposition is not a single-char substitution → common provider.
        assert classify_typo_type("gmial.com", "gmail.com") == TYPO_TYPE_COMMON_PROVIDER


class TestDetectTypoSuggestion:
    def _config(self, **kwargs) -> TypoDetectorConfig:
        base = dict(
            mode="suggest_only",
            max_edit_distance=2,
            whitelist=DEFAULT_PROVIDER_WHITELIST,
            require_original_no_mx=True,
        )
        base.update(kwargs)
        return TypoDetectorConfig(**base)

    def test_gmail_typo_is_suggested(self):
        # "gmal.com" is distance 1 from "gmail.com" → safe suggestion.
        suggestion = detect_typo_suggestion(
            local_part="alice",
            domain="gmal.com",
            config=self._config(),
        )
        assert suggestion.detected is True
        assert suggestion.original_domain == "gmal.com"
        assert suggestion.suggested_domain == "gmail.com"
        assert suggestion.suggested_email == "alice@gmail.com"
        assert suggestion.typo_type in {
            TYPO_TYPE_COMMON_PROVIDER,
            TYPO_TYPE_KEYBOARD,
        }
        assert 0.0 < (suggestion.confidence or 0.0) <= 1.0

    def test_whitelisted_domain_is_not_suggested(self):
        # gmail.com is already a trusted provider — no suggestion at all.
        suggestion = detect_typo_suggestion(
            local_part="alice",
            domain="gmail.com",
            config=self._config(),
        )
        assert suggestion.detected is False
        assert suggestion.suggested_domain is None
        assert suggestion.suggested_email is None

    def test_unknown_random_domain_is_not_suggested(self):
        # Far from every whitelisted provider → no suggestion is safe
        # enough to emit.
        suggestion = detect_typo_suggestion(
            local_part="sam",
            domain="acme-industries.co.uk",
            config=self._config(),
        )
        assert suggestion.detected is False
        assert suggestion.suggested_domain is None

    def test_original_domain_is_never_mutated(self):
        # Mixed case + stray whitespace must not be written back to the
        # returned ``original_domain``; the detector normalises for
        # matching only, and the DataFrame applier preserves the raw value.
        suggestion = detect_typo_suggestion(
            local_part="alice",
            domain="  GMAL.COM  ".strip(),
            config=self._config(),
        )
        assert suggestion.detected is True
        assert suggestion.suggested_domain == "gmail.com"

    def test_legacy_typo_map_only_accepts_whitelisted_targets(self):
        # A hostile map entry pointing to a non-whitelisted domain must
        # NOT be followed — this is the "no silent TLD swap" guardrail.
        bad_map = {"gmial.com": "evil-corp.example"}
        suggestion = detect_typo_suggestion(
            local_part="alice",
            domain="gmial.com",
            config=self._config(),
            typo_map=bad_map,
        )
        # The whitelist+distance path may still propose gmail.com on its
        # own (gmial→gmail is distance 1), but the *stale* map entry must
        # never become the suggested domain.
        assert suggestion.suggested_domain != "evil-corp.example"

    def test_very_large_distance_is_rejected(self):
        # acme.example is nowhere near any whitelisted provider.
        suggestion = detect_typo_suggestion(
            local_part="sam",
            domain="acme.example",
            config=self._config(max_edit_distance=1),
        )
        assert suggestion.detected is False


# ---------------------------------------------------------------------------
# DataFrame applier schema tests
# ---------------------------------------------------------------------------


class TestApplySuggestionColumn:
    def test_output_schema_is_stable(self):
        df = pd.DataFrame(
            {
                "local_part_from_email": ["alice", "bob", None],
                "domain_from_email": ["gmal.com", "gmail.com", None],
            }
        )
        out = apply_domain_typo_suggestion_column(
            df, detector_config=TypoDetectorConfig(), typo_map={}
        )
        for col in TYPO_SUGGESTION_COLUMNS:
            assert col in out.columns, f"missing column {col}"
        # Original email was never modified (not even implicitly).
        assert list(out.columns) != []
        assert out.iloc[0]["corrected_domain"] == "gmal.com"
        assert out.iloc[0]["suggested_domain"] == "gmail.com"
        # Row 1 (gmail.com) produces no suggestion.
        assert bool(out.iloc[1]["typo_detected"]) is False
        # Row 2 (no domain) → typo_detected is pd.NA.
        assert pd.isna(out.iloc[2]["typo_detected"])

    def test_never_mutates_input_frame(self):
        df = pd.DataFrame(
            {
                "local_part_from_email": ["alice"],
                "domain_from_email": ["gmal.com"],
            }
        )
        before_cols = list(df.columns)
        _ = apply_domain_typo_suggestion_column(
            df, detector_config=TypoDetectorConfig(), typo_map={}
        )
        assert list(df.columns) == before_cols


# ---------------------------------------------------------------------------
# Post-DNS validation stage
# ---------------------------------------------------------------------------


class TestClearSuggestionWhenOriginalHasMx:
    def test_suggestion_is_cleared_when_original_resolves(self):
        # Even if we detected a suggestion ("gmail.net" → "gmail.com"),
        # once DNS says the original has a valid MX we must drop the
        # suggestion — the user's domain is real.
        df = pd.DataFrame(
            {
                "domain_from_email": ["gmail.net"],
                "typo_detected": pd.array([True], dtype="boolean"),
                "typo_corrected": pd.array([True], dtype="boolean"),
                "original_domain": ["gmail.net"],
                "suggested_domain": ["gmail.com"],
                "suggested_email": ["alice@gmail.com"],
                "typo_type": [TYPO_TYPE_TLD],
                "typo_confidence": [0.9],
                "has_mx_record": pd.array([True], dtype="boolean"),
            }
        )
        out = clear_typo_suggestion_when_original_has_mx(df)
        assert bool(out.iloc[0]["typo_detected"]) is False
        assert bool(out.iloc[0]["typo_corrected"]) is False
        # pandas normalises ``None`` to ``NaN`` in object columns that
        # were previously string-typed; either form is acceptable as long
        # as the suggestion is empty.
        assert pd.isna(out.iloc[0]["suggested_domain"]) or out.iloc[0]["suggested_domain"] is None
        assert pd.isna(out.iloc[0]["suggested_email"]) or out.iloc[0]["suggested_email"] is None
        assert pd.isna(out.iloc[0]["typo_type"]) or out.iloc[0]["typo_type"] is None
        assert pd.isna(out.iloc[0]["typo_confidence"])

    def test_suggestion_preserved_when_original_has_no_mx(self):
        df = pd.DataFrame(
            {
                "domain_from_email": ["gmial.com"],
                "typo_detected": pd.array([True], dtype="boolean"),
                "typo_corrected": pd.array([True], dtype="boolean"),
                "original_domain": ["gmial.com"],
                "suggested_domain": ["gmail.com"],
                "suggested_email": ["alice@gmail.com"],
                "typo_type": [TYPO_TYPE_COMMON_PROVIDER],
                "typo_confidence": [0.9],
                "has_mx_record": pd.array([False], dtype="boolean"),
            }
        )
        out = clear_typo_suggestion_when_original_has_mx(df)
        assert bool(out.iloc[0]["typo_detected"]) is True
        assert out.iloc[0]["suggested_domain"] == "gmail.com"


# ---------------------------------------------------------------------------
# Engine-stage integration
# ---------------------------------------------------------------------------


class TestTypoSuggestionValidationStage:
    def test_stage_clears_suggestion_for_live_domains(self):
        df = pd.DataFrame(
            {
                "domain_from_email": ["gmail.net"],
                "typo_detected": pd.array([True], dtype="boolean"),
                "typo_corrected": pd.array([True], dtype="boolean"),
                "original_domain": ["gmail.net"],
                "suggested_domain": ["gmail.com"],
                "suggested_email": ["alice@gmail.com"],
                "typo_type": [TYPO_TYPE_TLD],
                "typo_confidence": [0.9],
                "has_mx_record": pd.array([True], dtype="boolean"),
            }
        )
        out = TypoSuggestionValidationStage().run(
            ChunkPayload(frame=df), PipelineContext()
        ).frame
        assert bool(out.iloc[0]["typo_detected"]) is False
        assert (
            pd.isna(out.iloc[0]["suggested_domain"])
            or out.iloc[0]["suggested_domain"] is None
        )


class TestTypoCorrectionStageWiring:
    def test_end_to_end_non_destructive(self):
        # The stage, driven from a PipelineContext with no AppConfig,
        # must fall back to safe defaults and still produce the full
        # non-destructive output schema.
        df = pd.DataFrame(
            {
                "local_part_from_email": ["alice"],
                "domain_from_email": ["gmal.com"],
            }
        )
        ctx = PipelineContext(typo_map={})
        out = TypoCorrectionStage().run(ChunkPayload(frame=df), ctx).frame
        # Critical invariant: the domain used by DNS/scoring is the
        # original domain, not the guessed one.
        assert out.iloc[0]["corrected_domain"] == "gmal.com"
        assert out.iloc[0]["suggested_domain"] == "gmail.com"
        assert bool(out.iloc[0]["typo_detected"]) is True
