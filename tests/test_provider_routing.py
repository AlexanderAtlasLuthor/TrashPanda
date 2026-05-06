"""V2.10.14 — provider_routing tests."""

from __future__ import annotations

from pathlib import Path

from app.validation_v2.network.provider_routing import (
    ACTION_DIRECT,
    ACTION_RELAY_PREFIX,
    ACTION_SKIP,
    PolicyTable,
    ProviderPolicy,
    load_provider_policy,
    policy_for_email,
)


def _write_policy(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "provider_policy.yaml"
    path.write_text(content, encoding="utf-8")
    return path


class TestLoaderBasics:
    def test_missing_file_returns_empty_default_policy(self, tmp_path: Path):
        # Path does not exist.
        table = load_provider_policy(tmp_path / "no-such.yaml")
        assert table.default.action == ACTION_DIRECT
        assert table.by_domain == {}

    def test_loads_yaml_groups_and_domains(self, tmp_path: Path):
        path = _write_policy(tmp_path, """
default:
  action: direct
providers:
  yahoo_family:
    domains: [yahoo.com, ymail.com]
    action: skip
    reason: "opaque"
  ms_family:
    domains: [outlook.com, hotmail.com]
    action: direct
    max_per_run: 50
""")
        table = load_provider_policy(path)
        assert table.for_domain("yahoo.com").action == ACTION_SKIP
        assert table.for_domain("yahoo.com").reason == "opaque"
        assert table.for_domain("outlook.com").action == ACTION_DIRECT
        assert table.for_domain("outlook.com").max_per_run == 50
        assert table.for_domain("unknown.com").action == ACTION_DIRECT

    def test_relay_action_is_treated_as_skip(self, tmp_path: Path):
        path = _write_policy(tmp_path, """
providers:
  warm_pool:
    domains: [bigcorp.com]
    action: relay:postmark_warm_a
""")
        table = load_provider_policy(path)
        policy = table.for_domain("bigcorp.com")
        assert policy.action.startswith(ACTION_RELAY_PREFIX)
        assert policy.skips_smtp is True
        assert "bigcorp.com" in table.skip_domains()

    def test_invalid_action_falls_back_to_default(self, tmp_path: Path):
        path = _write_policy(tmp_path, """
providers:
  weird:
    domains: [x.com]
    action: hyperdrive
""")
        table = load_provider_policy(path)
        # Unknown action → fallback to default (direct).
        assert table.for_domain("x.com").action == ACTION_DIRECT

    def test_malformed_yaml_returns_empty_table(self, tmp_path: Path):
        path = _write_policy(tmp_path, "{not: valid: yaml")
        table = load_provider_policy(path)
        assert table.by_domain == {}

    def test_duplicate_domain_first_group_wins(self, tmp_path: Path):
        path = _write_policy(tmp_path, """
providers:
  group_a:
    domains: [shared.com]
    action: skip
    reason: "from group A"
  group_b:
    domains: [shared.com]
    action: direct
    reason: "from group B"
""")
        table = load_provider_policy(path)
        assert table.for_domain("shared.com").reason == "from group A"


class TestSkipDomains:
    def test_skip_domains_collects_skip_and_relay(self, tmp_path: Path):
        path = _write_policy(tmp_path, """
providers:
  skipped:
    domains: [skip-me.com]
    action: skip
  relayed:
    domains: [relay-me.com]
    action: relay:postmark
  direct_probed:
    domains: [probe-me.com]
    action: direct
""")
        table = load_provider_policy(path)
        skips = table.skip_domains()
        assert skips == frozenset({"skip-me.com", "relay-me.com"})


class TestPolicyForEmail:
    def test_resolves_domain_from_email(self, tmp_path: Path):
        path = _write_policy(tmp_path, """
providers:
  yahoo:
    domains: [yahoo.com]
    action: skip
""")
        table = load_provider_policy(path)
        policy = policy_for_email(table, "Alice@Yahoo.COM")
        assert policy.action == ACTION_SKIP

    def test_no_at_returns_default(self, tmp_path: Path):
        path = _write_policy(tmp_path, "")
        table = load_provider_policy(path)
        policy = policy_for_email(table, "no-at-here")
        assert policy.action == ACTION_DIRECT


class TestSeedConfig:
    """Smoke test that the shipped configs/provider_policy.yaml parses
    and yields the expected behavior. This pins the seed file shape."""

    def test_shipped_seed_yaml_parses(self):
        from app.config import resolve_project_paths
        seed = resolve_project_paths().project_root / "configs" / "provider_policy.yaml"
        if not seed.is_file():
            return  # not shipped in this checkout
        table = load_provider_policy(seed)
        # Yahoo family should be skipped.
        assert table.for_domain("yahoo.com").action == ACTION_SKIP
        # Microsoft consumer should be direct with a cap.
        ms = table.for_domain("outlook.com")
        assert ms.action == ACTION_DIRECT
        assert ms.max_per_run is not None
