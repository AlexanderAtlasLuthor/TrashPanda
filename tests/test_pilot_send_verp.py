"""V2.10.12 — VERP encoder/decoder tests."""

from __future__ import annotations

import pytest

from app.pilot_send.verp import (
    DEFAULT_VERP_LOCAL_PART,
    decode_verp_token,
    encode_verp_token,
    extract_token_from_envelope,
    new_verp_token,
)


class TestNewVerpToken:
    def test_token_is_hex_16_chars(self):
        token = new_verp_token()
        assert isinstance(token, str)
        assert len(token) == 16
        assert all(c in "0123456789abcdef" for c in token)

    def test_tokens_are_unique(self):
        # 100 tokens with 64 bits of entropy each — collision astronomically
        # unlikely.
        tokens = {new_verp_token() for _ in range(100)}
        assert len(tokens) == 100


class TestEncodeVerpToken:
    def test_canonical_encoding(self):
        out = encode_verp_token("abc123", return_path_domain="bounces.acme.com")
        assert out == "bounce+abc123@bounces.acme.com"

    def test_custom_local_part(self):
        out = encode_verp_token(
            "abc",
            return_path_domain="acme.com",
            local_part="dsn",
        )
        assert out == "dsn+abc@acme.com"

    def test_empty_token_raises(self):
        with pytest.raises(ValueError):
            encode_verp_token("", return_path_domain="acme.com")

    def test_whitespace_only_token_raises(self):
        with pytest.raises(ValueError):
            encode_verp_token("   ", return_path_domain="acme.com")

    def test_domain_with_at_raises(self):
        with pytest.raises(ValueError):
            encode_verp_token("abc", return_path_domain="user@acme.com")

    def test_local_part_with_plus_raises(self):
        with pytest.raises(ValueError):
            encode_verp_token(
                "abc", return_path_domain="acme.com", local_part="x+y",
            )


class TestDecodeVerpToken:
    def test_round_trip(self):
        encoded = encode_verp_token("xyz789", return_path_domain="acme.com")
        assert decode_verp_token(encoded) == "xyz789"

    def test_strips_angle_brackets(self):
        assert decode_verp_token("<bounce+abc@acme.com>") == "abc"

    def test_returns_none_on_no_plus(self):
        assert decode_verp_token("plain@acme.com") is None

    def test_returns_none_on_garbage(self):
        assert decode_verp_token("not an email") is None

    def test_returns_none_on_empty(self):
        assert decode_verp_token("") is None


class TestExtractTokenFromEnvelope:
    def test_finds_token_in_diagnostic_body(self):
        text = (
            "Diagnostic-Code: smtp; 550 5.1.1 The email account that you "
            "tried to reach does not exist; bounce+abc123@bounces.acme.com"
        )
        assert extract_token_from_envelope(text) == "abc123"

    def test_finds_first_token_when_multiple(self):
        text = "first: bounce+aaa@x.com second: bounce+bbb@y.com"
        assert extract_token_from_envelope(text) == "aaa"

    def test_returns_none_when_no_match(self):
        assert (
            extract_token_from_envelope("no verp here, just plain@acme.com")
            is None
        )

    def test_handles_empty_input(self):
        assert extract_token_from_envelope("") is None
        assert extract_token_from_envelope(None) is None  # type: ignore
