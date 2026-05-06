"""V2.10.14 — Config-driven per-provider SMTP routing policy.

The original skip-list (``DEFAULT_OPAQUE_PROVIDERS`` + the operator-
editable ``configs/skip_smtp_providers.txt``) is binary: a domain is
either probed or skipped. That worked when "skip" was the only
policy lever we needed. With the May 2026 sender-side classifier
work, we now want richer per-provider policy:

* ``direct``   — probe via direct SMTP from our own IP (default).
* ``skip``     — short-circuit to ``inconclusive`` without a network
                 call. Equivalent to the opaque-provider behavior.
* ``relay:<name>`` — placeholder for a future external relay route
                 (e.g. SendGrid / Postmark warm-pool). Today this is
                 treated as ``skip`` with a different reason so the
                 operator sees how many rows would benefit when the
                 relay lands.

Plus per-provider knobs:

* ``max_per_run`` — cap how many candidates we probe for this provider
                    per pilot run (e.g. cap Microsoft at 50 to avoid
                    triggering reputation cliffs).
* ``reason``     — human-readable explanation, surfaced in logs and
                    in the per-row ``smtp_reason`` for skip rows.

The policy lives at ``configs/provider_policy.yaml``. When the file
is missing or empty, the module synthesizes a policy that mirrors
the existing skip-list behavior — so adding this module is strictly
additive: nothing changes until the operator authors the YAML.

Wiring: ``smtp_probe.load_skip_providers_from_file`` extends its
skip-set with the ``skip`` and ``relay:*`` domains from the policy.
This keeps the existing tests green (DEFAULT_OPAQUE_PROVIDERS still
covers the canonical Yahoo/AOL set) while letting the operator add
Microsoft / GMail / arbitrary corporate domains to the skip-set with
explicit reasons.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml


_LOGGER = logging.getLogger(__name__)


PROVIDER_POLICY_FILENAME: str = "provider_policy.yaml"
_DEFAULT_POLICY_FILE: Path = Path("configs") / PROVIDER_POLICY_FILENAME

# Action values.
ACTION_DIRECT: str = "direct"
ACTION_SKIP: str = "skip"
ACTION_RELAY_PREFIX: str = "relay:"

ALL_ACTIONS: tuple[str, ...] = (ACTION_DIRECT, ACTION_SKIP)


@dataclass(frozen=True, slots=True)
class ProviderPolicy:
    """Policy for a single domain or default."""

    action: str = ACTION_DIRECT
    max_per_run: int | None = None
    reason: str = ""
    group: str = ""  # e.g. "yahoo_family", for log/metric grouping

    @property
    def skips_smtp(self) -> bool:
        """True iff this policy short-circuits the SMTP probe (skip
        or any relay action — relay is treated as skip until a relay
        backend is wired)."""
        return (
            self.action == ACTION_SKIP
            or self.action.startswith(ACTION_RELAY_PREFIX)
        )


@dataclass(frozen=True, slots=True)
class PolicyTable:
    """Resolved policy table: per-domain rules + a default."""

    default: ProviderPolicy
    by_domain: dict[str, ProviderPolicy] = field(default_factory=dict)

    def for_domain(self, domain: str) -> ProviderPolicy:
        d = (domain or "").strip().lower()
        return self.by_domain.get(d, self.default)

    def skip_domains(self) -> frozenset[str]:
        """Return the set of domains whose policy short-circuits SMTP."""
        return frozenset(
            d for d, p in self.by_domain.items() if p.skips_smtp
        )


def _coerce_policy(
    raw: dict | None,
    *,
    group: str = "",
    fallback: ProviderPolicy | None = None,
) -> ProviderPolicy:
    base = fallback or ProviderPolicy()
    if not isinstance(raw, dict):
        return ProviderPolicy(
            action=base.action,
            max_per_run=base.max_per_run,
            reason=base.reason,
            group=group or base.group,
        )
    action = str(raw.get("action") or base.action).strip().lower()
    if not (
        action == ACTION_DIRECT
        or action == ACTION_SKIP
        or action.startswith(ACTION_RELAY_PREFIX)
    ):
        _LOGGER.warning(
            "provider_policy: unknown action %r (group=%s); "
            "falling back to %r",
            action, group, base.action,
        )
        action = base.action

    max_per_run_raw = raw.get("max_per_run", base.max_per_run)
    max_per_run: int | None
    if max_per_run_raw is None:
        max_per_run = None
    else:
        try:
            max_per_run = int(max_per_run_raw)
            if max_per_run < 0:
                max_per_run = None
        except (TypeError, ValueError):
            max_per_run = base.max_per_run

    reason = str(raw.get("reason") or base.reason or "")
    return ProviderPolicy(
        action=action,
        max_per_run=max_per_run,
        reason=reason,
        group=group or base.group,
    )


def load_provider_policy(
    path: str | Path | None = None,
) -> PolicyTable:
    """Load the per-provider policy table.

    File format (``configs/provider_policy.yaml``)::

        default:
          action: direct
          max_per_run: null
          reason: ""

        providers:
          yahoo_family:
            domains: [yahoo.com, ymail.com, aol.com, ...]
            action: skip
            reason: "Yahoo family: opaque acceptance + reputation throttling"
          microsoft_family:
            domains: [outlook.com, hotmail.com, live.com, msn.com]
            action: direct
            max_per_run: 50
            reason: "Microsoft: probe directly but cap to avoid IP block escalation"

    Missing/empty file → an empty ``PolicyTable`` (default action
    ``direct``, no per-domain rules). Combined with the existing
    skip-list, behavior is unchanged.
    """
    env_override = os.environ.get("TRASHPANDA_PROVIDER_POLICY_PATH")
    candidate = (
        Path(path)
        if path is not None
        else (Path(env_override) if env_override else _DEFAULT_POLICY_FILE)
    )

    if not candidate.is_file():
        _LOGGER.debug(
            "provider_policy: file not found at %s — empty policy",
            candidate,
        )
        return PolicyTable(default=ProviderPolicy())

    try:
        text = candidate.read_text(encoding="utf-8")
    except OSError as exc:  # pragma: no cover - defensive
        _LOGGER.warning(
            "provider_policy: failed to read %s (%s) — empty policy",
            candidate, exc,
        )
        return PolicyTable(default=ProviderPolicy())

    try:
        payload = yaml.safe_load(text) or {}
    except yaml.YAMLError as exc:
        _LOGGER.warning(
            "provider_policy: invalid YAML at %s (%s) — empty policy",
            candidate, exc,
        )
        return PolicyTable(default=ProviderPolicy())

    if not isinstance(payload, dict):
        return PolicyTable(default=ProviderPolicy())

    default_policy = _coerce_policy(
        payload.get("default") if isinstance(payload.get("default"), dict)
        else None
    )

    by_domain: dict[str, ProviderPolicy] = {}
    providers = payload.get("providers") or {}
    if isinstance(providers, dict):
        for group_name, group_raw in providers.items():
            if not isinstance(group_raw, dict):
                continue
            group_policy = _coerce_policy(
                group_raw, group=str(group_name), fallback=default_policy,
            )
            domains = group_raw.get("domains") or []
            if not isinstance(domains, list):
                continue
            for raw_domain in domains:
                d = str(raw_domain or "").strip().lower()
                if not d:
                    continue
                if d in by_domain:
                    _LOGGER.warning(
                        "provider_policy: domain %r appears in multiple "
                        "groups; first wins (existing group=%s, new group=%s)",
                        d, by_domain[d].group, group_name,
                    )
                    continue
                by_domain[d] = group_policy

    return PolicyTable(default=default_policy, by_domain=by_domain)


def policy_for_email(
    table: PolicyTable, email: str,
) -> ProviderPolicy:
    """Convenience wrapper: resolve policy from an email address."""
    if not email or "@" not in email:
        return table.default
    return table.for_domain(email.rsplit("@", 1)[-1])


__all__ = [
    "ACTION_DIRECT",
    "ACTION_RELAY_PREFIX",
    "ACTION_SKIP",
    "ALL_ACTIONS",
    "PROVIDER_POLICY_FILENAME",
    "PolicyTable",
    "ProviderPolicy",
    "load_provider_policy",
    "policy_for_email",
]
