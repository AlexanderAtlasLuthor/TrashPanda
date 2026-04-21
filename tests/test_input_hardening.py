"""Phase 4 — Input Hardening tests.

Locks in the documented policies for:
  - Spanish column aliases + accent-insensitive header normalization
  - CSV encoding fallback chain (utf-8-sig → utf-8 → cp1252 → latin-1)
  - MX vs A record scoring asymmetry (A-only cannot reach ``valid``)
  - ASCII-only email syntax policy (unicode local/domain parts rejected)

No existing tests are removed or altered.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import pytest

from app.email_rules import check_email_syntax
from app.io_utils import CSV_ENCODING_FALLBACKS, _detect_csv_encoding, read_csv_in_chunks
from app.normalizers import normalize_header_name, normalize_headers
from app.scoring import score_row


# ---------------------------------------------------------------------------
# A. Spanish aliases / accent-insensitive normalization
# ---------------------------------------------------------------------------

class TestColumnAliases:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("correo", "email"),
            ("Correo", "email"),
            ("correo_electronico", "email"),
            ("correo electrónico", "email"),  # space + accent
            ("Correo Electrónico", "email"),
            ("e-mail", "email"),
            ("E_Mail", "email"),
            ("mail", "email"),
            ("email_address", "email"),
        ],
    )
    def test_email_aliases(self, raw, expected):
        assert normalize_header_name(raw) == expected

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("nombre", "fname"),
            ("Nombre", "fname"),
            ("nombres", "fname"),
            ("first_name", "fname"),
            ("First Name", "fname"),
            ("given_name", "fname"),
        ],
    )
    def test_first_name_aliases(self, raw, expected):
        assert normalize_header_name(raw) == expected

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("apellido", "lname"),
            ("Apellidos", "lname"),
            ("last_name", "lname"),
            ("Last Name", "lname"),
            ("surname", "lname"),
            ("family_name", "lname"),
        ],
    )
    def test_last_name_aliases(self, raw, expected):
        assert normalize_header_name(raw) == expected

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("telefono", "phone"),
            ("teléfono", "phone"),
            ("Teléfono", "phone"),
            ("celular", "phone"),
            ("móvil", "phone"),
            ("mobile", "phone"),
            ("phone_number", "phone"),
            ("Phone Number", "phone"),
        ],
    )
    def test_phone_aliases(self, raw, expected):
        assert normalize_header_name(raw) == expected

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("empresa", "company"),
            ("compañía", "company"),
            ("compania", "company"),
            ("razón social", "company"),
            ("organization", "company"),
        ],
    )
    def test_company_aliases(self, raw, expected):
        assert normalize_header_name(raw) == expected

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("ciudad", "city"),
            ("Ciudad", "city"),
            ("estado", "state"),
            ("provincia", "state"),
        ],
    )
    def test_city_state_aliases(self, raw, expected):
        assert normalize_header_name(raw) == expected

    def test_unknown_header_passes_through_lowercased(self):
        # Unknown headers are still lowered/underscored/accent-stripped but not aliased.
        assert normalize_header_name("Notas Adicionales") == "notas_adicionales"

    def test_existing_canonical_names_are_stable(self):
        for col in ("email", "fname", "lname", "state", "city", "zip", "domain"):
            assert normalize_header_name(col) == col

    def test_normalize_headers_logs_remaps(self, caplog):
        df = pd.DataFrame(
            {"Correo Electrónico": ["a@b.com"], "Teléfono": ["1"], "email": ["x@y.com"]}
        )
        with caplog.at_level(logging.INFO, logger="app.normalizers.headers"):
            out = normalize_headers(df)
        assert list(out.columns) == ["email", "phone", "email"]  # two 'email' after rename
        # Only the remapped columns should log (not the one already canonical).
        remap_messages = [r.getMessage() for r in caplog.records if "Mapped column" in r.getMessage()]
        assert any("Correo Electrónico" in m and "'email'" in m for m in remap_messages)
        assert any("Teléfono" in m and "'phone'" in m for m in remap_messages)
        assert not any("'email' -> 'email'" in m for m in remap_messages)


# ---------------------------------------------------------------------------
# B. Encoding fallback chain
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_csv_factory(tmp_path):
    def _make(text: str, encoding: str, name: str = "input.csv") -> Path:
        p = tmp_path / name
        p.write_bytes(text.encode(encoding))
        return p
    return _make


class TestEncodingFallback:
    SAMPLE_TEXT = "email,nombre\nuser@example.com,José\notro@example.com,María\n"

    def test_fallback_chain_is_utf8_first(self):
        assert CSV_ENCODING_FALLBACKS[0] == "utf-8-sig"
        assert "utf-8" in CSV_ENCODING_FALLBACKS
        assert "cp1252" in CSV_ENCODING_FALLBACKS
        assert CSV_ENCODING_FALLBACKS[-1] == "latin-1"

    def test_utf8_file_is_detected(self, tmp_csv_factory):
        path = tmp_csv_factory(self.SAMPLE_TEXT, "utf-8")
        assert _detect_csv_encoding(path) in {"utf-8-sig", "utf-8"}

    def test_utf8_sig_file_is_detected(self, tmp_csv_factory):
        # Explicit BOM prefix
        path = tmp_csv_factory("\ufeff" + self.SAMPLE_TEXT, "utf-8")
        assert _detect_csv_encoding(path) == "utf-8-sig"

    def test_cp1252_file_is_detected_over_utf8(self, tmp_csv_factory):
        # cp1252-encoded accents are invalid as utf-8, so the probe should
        # walk past utf-8 variants.
        path = tmp_csv_factory(self.SAMPLE_TEXT, "cp1252")
        enc = _detect_csv_encoding(path)
        assert enc in {"cp1252", "latin-1"}
        # cp1252 should win before latin-1 in the fallback order.
        assert enc == "cp1252"

    def test_read_csv_in_chunks_reads_utf8(self, tmp_csv_factory, caplog):
        path = tmp_csv_factory(self.SAMPLE_TEXT, "utf-8")
        with caplog.at_level(logging.INFO, logger="app.io_utils"):
            chunks = list(read_csv_in_chunks(path, chunk_size=100))
        assert len(chunks) == 1
        frame, _ = chunks[0]
        assert list(frame["nombre"]) == ["José", "María"]
        assert any("Detected input encoding" in r.getMessage() for r in caplog.records)

    def test_read_csv_in_chunks_reads_cp1252(self, tmp_csv_factory, caplog):
        path = tmp_csv_factory(self.SAMPLE_TEXT, "cp1252")
        with caplog.at_level(logging.INFO, logger="app.io_utils"):
            chunks = list(read_csv_in_chunks(path, chunk_size=100))
        assert len(chunks) == 1
        frame, _ = chunks[0]
        # Values are decoded correctly back to their unicode form.
        assert list(frame["nombre"]) == ["José", "María"]
        logged = " ".join(r.getMessage() for r in caplog.records)
        assert "Detected input encoding: cp1252" in logged

    def test_read_csv_in_chunks_reads_latin1(self, tmp_csv_factory):
        # latin-1 is a subset/codepage compatible with cp1252 for these
        # characters; we still want the pipeline to read the file.
        path = tmp_csv_factory(self.SAMPLE_TEXT, "latin-1")
        chunks = list(read_csv_in_chunks(path, chunk_size=100))
        frame, _ = chunks[0]
        assert "José" in list(frame["nombre"])


# ---------------------------------------------------------------------------
# C. MX vs A record scoring policy
# ---------------------------------------------------------------------------

class TestMxVsAPolicy:
    """A-only domains must never auto-promote to high_confidence."""

    def _score(self, *, has_mx: bool, has_a: bool):
        return score_row(
            syntax_valid=True,
            corrected_domain="acme-corp.io",
            domain_exists=True,
            has_mx_record=has_mx,
            has_a_record=has_a,
            dns_error=None,
            typo_corrected=False,
            domain_matches_input_column=True,
            disposable_domains=frozenset(),
            invalid_if_disposable=True,
            local_part="alice",
            high_confidence_threshold=70,
            review_threshold=40,
        )

    def test_mx_present_reaches_high_confidence(self):
        result = self._score(has_mx=True, has_a=False)
        assert result.preliminary_bucket == "high_confidence"
        assert result.score >= 70

    def test_a_only_falls_to_review(self):
        result = self._score(has_mx=False, has_a=True)
        assert result.preliminary_bucket == "review"
        assert 40 <= result.score < 70
        assert "a_fallback" in result.score_reasons
        assert "mx_present" not in result.score_reasons

    def test_no_dns_records_falls_to_invalid_or_review(self):
        result = self._score(has_mx=False, has_a=False)
        # Without either record and no positive DNS signals, the score
        # floor is 25 (syntax only) → below review_threshold=40.
        assert result.preliminary_bucket == "invalid"
        assert result.score < 40


# ---------------------------------------------------------------------------
# D. Unicode / accented email policy
# ---------------------------------------------------------------------------

class TestUnicodeEmailPolicy:
    """Document current behavior: ASCII-only local/domain parts enforced."""

    def test_ascii_email_is_valid(self):
        result = check_email_syntax("user@example.com")
        assert result.syntax_valid is True
        assert result.syntax_reason == "valid"

    @pytest.mark.parametrize(
        "email",
        [
            "josé@example.com",
            "maría@example.com",
            "niño@example.com",
            "user+álias@example.com",
        ],
    )
    def test_accented_local_part_is_rejected(self, email):
        result = check_email_syntax(email)
        assert result.syntax_valid is False
        # Reason belongs to the documented local-part invalid family.
        assert "local" in result.syntax_reason or result.syntax_reason == "local_part_invalid_chars"

    @pytest.mark.parametrize(
        "email",
        [
            "user@café.com",
            "user@piñata.com",
            "user@münchen.de",
        ],
    )
    def test_accented_domain_part_is_rejected(self, email):
        result = check_email_syntax(email)
        assert result.syntax_valid is False
        # Reason belongs to the documented domain invalid family.
        assert "domain" in result.syntax_reason

    def test_ascii_values_in_other_columns_are_preserved(self):
        """Non-email columns should accept accented characters unchanged."""
        df = pd.DataFrame(
            {"email": ["user@example.com"], "nombre": ["José"], "ciudad": ["Córdoba"]}
        )
        out = normalize_headers(df)
        # Values are untouched; only headers were normalized.
        assert out.iloc[0]["fname"] == "José"
        assert out.iloc[0]["city"] == "Córdoba"
