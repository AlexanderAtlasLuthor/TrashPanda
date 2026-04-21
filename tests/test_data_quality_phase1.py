"""Tests for Phase 1 "Data Quality Hardening".

Sections:
  1. TestTypoCorrection      — real-world typo → canonical domain lookups
  2. TestDisposableDomains   — disposable domains detected + INVALID + reason
  3. TestRoleAccounts        — role local parts → REVIEW + "Role-based email"
  4. TestPlaceholder         — placeholder local parts/domains → INVALID
  5. TestClientReason        — client_reason field contract
  6. TestRegressionGuard     — normal valid emails are not affected

Scope:
  These tests only add coverage for the Phase 1 additions (expanded typo
  map, full disposable list, role-account detection, placeholder hard
  fail, human-readable client_reason). No existing pipeline behavior is
  modified.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_VENDOR_SITE = _PROJECT_ROOT / "vendor_site"
if str(_VENDOR_SITE) not in sys.path:
    sys.path.insert(0, str(_VENDOR_SITE))

from app.scoring import (  # noqa: E402
    CLIENT_REASON_MAP,
    PLACEHOLDER_DOMAINS,
    PLACEHOLDER_LOCAL_PARTS,
    ROLE_ACCOUNT_LOCAL_PARTS,
    ScoringResult,
    score_row,
)
from app.typo_rules import apply_domain_typo_correction, build_typo_map  # noqa: E402


TYPO_MAP_PATH = _PROJECT_ROOT / "configs" / "typo_map.csv"
DISPOSABLE_PATH = _PROJECT_ROOT / "configs" / "disposable_domains.txt"


@pytest.fixture(scope="module")
def typo_map() -> dict[str, str]:
    return build_typo_map(TYPO_MAP_PATH)


@pytest.fixture(scope="module")
def disposable_domains() -> frozenset[str]:
    domains: set[str] = set()
    with DISPOSABLE_PATH.open(encoding="utf-8") as fh:
        for line in fh:
            d = line.strip().lower()
            if d and not d.startswith("#"):
                domains.add(d)
    return frozenset(domains)


def _score(**overrides) -> ScoringResult:
    """Call score_row with neutral, non-placeholder defaults."""
    defaults = dict(
        syntax_valid=True,
        corrected_domain="gmail.com",
        has_mx_record=True,
        has_a_record=False,
        domain_exists=True,
        dns_error=None,
        typo_corrected=False,
        domain_matches_input_column=None,
        local_part="john",
        disposable_domains=None,
        invalid_if_disposable=True,
    )
    defaults.update(overrides)
    return score_row(**defaults)


# ===========================================================================
# SECTION 1 — Typo correction
# ===========================================================================

class TestTypoCorrection:
    """Real-world typos must map to canonical domains via the closed map."""

    @pytest.mark.parametrize(
        "typo,correct",
        [
            ("gmial.com", "gmail.com"),
            ("gamil.com", "gmail.com"),
            ("yaho.com", "yahoo.com"),
            ("hotmial.com", "hotmail.com"),
            ("outlok.com", "outlook.com"),
        ],
    )
    def test_domain_corrected(self, typo_map, typo, correct):
        result = apply_domain_typo_correction(typo, typo_map)
        assert result.typo_corrected is True
        assert result.corrected_domain == correct
        assert result.typo_original_domain == typo

    @pytest.mark.parametrize(
        "typo,correct",
        [
            ("gmial.com", "gmail.com"),
            ("gamil.com", "gmail.com"),
            ("yaho.com", "yahoo.com"),
            ("hotmial.com", "hotmail.com"),
            ("outlok.com", "outlook.com"),
        ],
    )
    def test_corrected_email_is_usable(self, typo_map, typo, correct):
        """After correction, the resulting email string round-trips cleanly."""
        result = apply_domain_typo_correction(typo, typo_map)
        corrected_email = f"user@{result.corrected_domain}"
        assert corrected_email == f"user@{correct}"
        assert "@" in corrected_email and corrected_email.count("@") == 1

    def test_clean_domain_is_pass_through(self, typo_map):
        result = apply_domain_typo_correction("gmail.com", typo_map)
        assert result.typo_corrected is False
        assert result.corrected_domain == "gmail.com"


# ===========================================================================
# SECTION 2 — Disposable domains
# ===========================================================================

class TestDisposableDomains:
    """Emails on the disposable blocklist must hard-fail as INVALID."""

    @pytest.mark.parametrize(
        "domain",
        ["mailinator.com", "10minutemail.com", "guerrillamail.com"],
    )
    def test_domain_is_in_blocklist(self, disposable_domains, domain):
        assert domain in disposable_domains

    @pytest.mark.parametrize(
        "domain",
        ["mailinator.com", "10minutemail.com", "guerrillamail.com"],
    )
    def test_classified_invalid_when_flag_on(self, disposable_domains, domain):
        r = _score(
            corrected_domain=domain,
            local_part="john",
            disposable_domains=disposable_domains,
            invalid_if_disposable=True,
        )
        assert r.hard_fail is True
        assert r.preliminary_bucket == "invalid"
        assert r.score_reasons == "disposable"

    @pytest.mark.parametrize(
        "domain",
        ["mailinator.com", "10minutemail.com", "guerrillamail.com"],
    )
    def test_client_reason_disposable(self, disposable_domains, domain):
        r = _score(
            corrected_domain=domain,
            local_part="john",
            disposable_domains=disposable_domains,
        )
        assert r.client_reason == "Temporary/disposable email"

    def test_flag_off_disables_rule(self, disposable_domains):
        r = _score(
            corrected_domain="mailinator.com",
            local_part="john",
            disposable_domains=disposable_domains,
            invalid_if_disposable=False,
        )
        assert r.hard_fail is False
        assert "disposable" not in r.score_reasons

    def test_blocklist_has_expected_scale(self, disposable_domains):
        """Phase 1 replaced the stub list with a large upstream blocklist."""
        assert len(disposable_domains) >= 3000


# ===========================================================================
# SECTION 3 — Role accounts
# ===========================================================================

class TestRoleAccounts:
    """Role-based local parts must be REVIEW (not INVALID) with role reason."""

    @pytest.mark.parametrize("local", ["info", "support", "sales", "contact", "billing"])
    def test_role_local_is_in_set(self, local):
        assert local in ROLE_ACCOUNT_LOCAL_PARTS

    @pytest.mark.parametrize("local", ["info", "support", "sales"])
    def test_not_hard_fail(self, local):
        r = _score(local_part=local, corrected_domain="company.org")
        assert r.hard_fail is False

    @pytest.mark.parametrize("local", ["info", "support", "sales"])
    def test_bucket_is_review(self, local):
        r = _score(local_part=local, corrected_domain="company.org")
        assert r.preliminary_bucket == "review"

    @pytest.mark.parametrize("local", ["info", "support", "sales"])
    def test_role_reason_token_present(self, local):
        r = _score(local_part=local, corrected_domain="company.org")
        assert "role_account" in r.score_reasons.split("|")

    @pytest.mark.parametrize("local", ["info", "support", "sales"])
    def test_client_reason_role_based(self, local):
        r = _score(local_part=local, corrected_domain="company.org")
        # "Role-based email" may be overridden by a higher-priority token
        # like dns_no_records, but with our neutral defaults it should win.
        assert r.client_reason == "Role-based email"

    def test_role_case_insensitive(self):
        r = _score(local_part="INFO", corrected_domain="company.org")
        assert r.preliminary_bucket == "review"
        assert "role_account" in r.score_reasons.split("|")

    def test_role_with_good_mx_still_review(self):
        """Even with a perfect DNS signal, a role account stays in REVIEW."""
        r = _score(
            local_part="info",
            corrected_domain="company.org",
            has_mx_record=True,
            domain_exists=True,
        )
        assert r.preliminary_bucket == "review"


# ===========================================================================
# SECTION 4 — Placeholder / fake email detection
# ===========================================================================

class TestPlaceholder:
    """Placeholder emails must hard-fail as INVALID with 'placeholder' reason."""

    @pytest.mark.parametrize(
        "local,domain",
        [
            ("test", "test.com"),
            ("a", "a.com"),
            ("asdf", "example.com"),
            ("fake", "invalid.com"),
        ],
    )
    def test_classified_invalid(self, local, domain):
        r = _score(local_part=local, corrected_domain=domain)
        assert r.hard_fail is True
        assert r.preliminary_bucket == "invalid"

    @pytest.mark.parametrize(
        "local,domain",
        [
            ("test", "test.com"),
            ("a", "a.com"),
            ("asdf", "example.com"),
            ("fake", "invalid.com"),
        ],
    )
    def test_reason_is_placeholder(self, local, domain):
        r = _score(local_part=local, corrected_domain=domain)
        assert r.score_reasons == "placeholder"

    @pytest.mark.parametrize(
        "local,domain",
        [
            ("test", "test.com"),
            ("a", "a.com"),
            ("asdf", "example.com"),
            ("fake", "invalid.com"),
        ],
    )
    def test_client_reason_fake_placeholder(self, local, domain):
        r = _score(local_part=local, corrected_domain=domain)
        assert r.client_reason == "Fake or placeholder email"

    def test_placeholder_local_in_set(self):
        for local in ["test", "asdf", "a", "fake", "null"]:
            assert local in PLACEHOLDER_LOCAL_PARTS

    def test_placeholder_domain_in_set(self):
        for dom in ["test.com", "example.com", "invalid.com", "none.com"]:
            assert dom in PLACEHOLDER_DOMAINS

    def test_placeholder_beats_disposable(self, disposable_domains):
        """If both rules match, placeholder fires first (declared priority)."""
        r = _score(
            local_part="test",
            corrected_domain="mailinator.com",
            disposable_domains=disposable_domains,
        )
        assert r.score_reasons == "placeholder"


# ===========================================================================
# SECTION 5 — client_reason contract
# ===========================================================================

class TestClientReason:
    """client_reason must always be populated, human-readable, and stable."""

    def test_field_always_present(self):
        r = _score()
        assert hasattr(r, "client_reason")
        assert isinstance(r.client_reason, str)

    def test_empty_when_no_reasons_signal(self):
        """A clean, fully-positive row has no negative tokens → empty client_reason."""
        r = _score(typo_corrected=False, domain_matches_input_column=None)
        # With MX + syntax + no penalties, no priority token fires.
        assert r.client_reason == ""

    @pytest.mark.parametrize(
        "syntax_valid,expected",
        [(False, "Invalid email format"), (None, "Invalid email format")],
    )
    def test_syntax_invalid_maps_to_human_string(self, syntax_valid, expected):
        r = _score(syntax_valid=syntax_valid)
        assert r.client_reason == expected

    def test_no_domain_maps_to_human_string(self):
        r = _score(corrected_domain=None)
        assert r.client_reason == "Domain does not exist"

    def test_nxdomain_maps_to_human_string(self):
        r = _score(domain_exists=False, dns_error="nxdomain")
        assert r.client_reason == "Domain does not exist"

    def test_dns_no_records_maps_to_human_string(self):
        r = _score(
            has_mx_record=False,
            has_a_record=False,
            domain_exists=True,
            dns_error="no_mx",
        )
        assert r.client_reason == "No mail server (MX) found"

    def test_no_technical_tokens_leak(self):
        """client_reason must never contain raw internal tokens."""
        forbidden = {"syntax_invalid", "nxdomain", "no_mx", "dns_no_records",
                     "mx_present", "a_fallback", "role_account"}
        # Run through every mapped token
        for token, human in CLIENT_REASON_MAP.items():
            # human text must differ from the token
            assert human != token
            # and must not itself be one of the raw tokens
            assert human not in forbidden

    def test_priority_placeholder_over_disposable(self, disposable_domains):
        r = _score(
            local_part="test",
            corrected_domain="mailinator.com",
            disposable_domains=disposable_domains,
        )
        assert r.client_reason == "Fake or placeholder email"

    def test_priority_role_over_typo(self):
        """With neutral DNS signals, role_account wins over typo_corrected."""
        r = _score(
            local_part="info",
            corrected_domain="company.org",
            typo_corrected=True,
        )
        assert r.client_reason == "Role-based email"


# ===========================================================================
# SECTION 6 — Regression guard: normal emails are not affected
# ===========================================================================

class TestRegressionGuard:
    """Normal valid emails must remain valid (not invalidated by Phase 1 rules)."""

    @pytest.mark.parametrize(
        "local,domain",
        [
            ("john", "gmail.com"),
            ("alice", "yahoo.com"),
            ("bob", "outlook.com"),
            ("charlie", "empresa.com"),
        ],
    )
    def test_not_hard_fail(self, local, domain):
        r = _score(local_part=local, corrected_domain=domain)
        assert r.hard_fail is False

    @pytest.mark.parametrize(
        "local,domain",
        [
            ("john", "gmail.com"),
            ("alice", "yahoo.com"),
            ("bob", "outlook.com"),
        ],
    )
    def test_bucket_is_valid_or_review(self, local, domain):
        r = _score(local_part=local, corrected_domain=domain)
        assert r.preliminary_bucket in ("high_confidence", "review")

    def test_contact_local_is_role_review_not_invalid(self):
        """'contact@empresa.com' is a role account → review, NOT invalid."""
        r = _score(local_part="contact", corrected_domain="empresa.com")
        assert r.hard_fail is False
        assert r.preliminary_bucket == "review"

    def test_regular_user_not_flagged_role(self):
        r = _score(local_part="john", corrected_domain="gmail.com")
        assert "role_account" not in r.score_reasons.split("|")

    def test_regular_user_not_flagged_disposable(self, disposable_domains):
        r = _score(
            local_part="john",
            corrected_domain="gmail.com",
            disposable_domains=disposable_domains,
        )
        assert "disposable" not in r.score_reasons
        assert r.hard_fail is False

    def test_regular_user_not_flagged_placeholder(self):
        r = _score(local_part="john", corrected_domain="gmail.com")
        assert "placeholder" not in r.score_reasons

    @pytest.mark.parametrize(
        "local,domain",
        [
            ("john", "gmail.com"),
            ("alice", "yahoo.com"),
            ("bob", "outlook.com"),
        ],
    )
    def test_good_email_reasons_contain_positive_signals(self, local, domain):
        r = _score(local_part=local, corrected_domain=domain)
        tokens = r.score_reasons.split("|")
        assert "syntax_valid" in tokens
        assert "mx_present" in tokens
