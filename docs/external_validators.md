# External email-validator integration (V2.10.11)

TrashPanda's pipeline runs an in-process SMTP probe, an in-process
retry of 4xx outcomes, and a deferred retry queue. Together they
classify most rows confidently. For the long tail — Yahoo / AOL /
Verizon-class catch-all consumers, opaque corporate MXes, addresses
where every signal is "maybe" — third-party email-validation APIs
can offer a meaningful **second opinion**.

This document describes how to plug one in.

## What the integration is — and isn't

* It is a **registry + protocol**: TrashPanda defines the contract
  and the consensus aggregator. The operator implements an adapter
  for their preferred vendor (ZeroBounce, Hunter, NeverBounce,
  Mailgun, etc.) and registers it at process start.
* It does **not** ship vendor SDKs or API keys. Each operator
  controls their own credentials and rate limits.
* External validators are a **second opinion**: a unanimous
  "invalid" can reject a row, but no number of "valid" verdicts
  can escalate a row to auto-approve. The policy is monotone.

## The contract

```python
# app/external_validators/registry.py
@runtime_checkable
class ExternalEmailValidator(Protocol):
    name: str

    def probe(
        self,
        email: str,
        *,
        timeout: float,
    ) -> ExternalValidationResult: ...


@dataclass(frozen=True, slots=True)
class ExternalValidationResult:
    validator_name: str
    verdict: str            # "valid"|"invalid"|"catch_all"|"unknown"|"risky"
    confidence: float = 0.0  # 0..1, vendor-supplied or estimated
    raw_response: dict = ...  # vendor SDK output, preserved verbatim
    error: str | None = None
```

Adapters must:

1. Return one of the five canonical verdicts.
2. Never raise on vendor errors — return ``verdict="unknown"`` with
   ``error=<short string>`` instead.
3. Be thread-safe (the SMTP stage may probe concurrently).
4. Be idempotent — re-registering the same name silently replaces
   the prior entry.

## Writing an adapter

```python
# configs/external_validators/zerobounce.py  (operator-supplied)
import os
import requests

from app.external_validators import (
    ExternalEmailValidator,
    ExternalValidationResult,
    register,
)


class ZeroBounceValidator:
    """Adapter for ZeroBounce's /v2/validate endpoint."""

    name = "zerobounce"

    def __init__(self, api_key: str | None = None):
        self._api_key = api_key or os.environ["ZEROBOUNCE_API_KEY"]

    def probe(
        self,
        email: str,
        *,
        timeout: float,
    ) -> ExternalValidationResult:
        try:
            r = requests.get(
                "https://api.zerobounce.net/v2/validate",
                params={"api_key": self._api_key, "email": email},
                timeout=timeout,
            )
            r.raise_for_status()
            data = r.json()
        except Exception as exc:
            return ExternalValidationResult(
                validator_name=self.name,
                verdict="unknown",
                error=str(exc)[:200],
            )

        # Map ZeroBounce's "status" field onto the canonical verdicts.
        status = (data.get("status") or "").lower()
        verdict = {
            "valid":         "valid",
            "invalid":       "invalid",
            "catch-all":     "catch_all",
            "unknown":       "unknown",
            "do_not_mail":   "risky",
            "spamtrap":      "invalid",
            "abuse":         "risky",
        }.get(status, "unknown")

        return ExternalValidationResult(
            validator_name=self.name,
            verdict=verdict,
            confidence=0.85 if verdict in ("valid", "invalid") else 0.5,
            raw_response=data,
        )


# Register at import time so the SMTP stage finds the adapter.
register(ZeroBounceValidator())
```

## Registering at process start

The operator decides which adapter modules to import:

```yaml
# configs/default.yaml (operator's site copy)
external_validators:
  enabled: true
  timeout_seconds: 5.0
  modules:
    - configs.external_validators.zerobounce
    - configs.external_validators.hunter
```

The TrashPanda backend imports each ``modules`` entry at startup
(`app/server.py` startup hook — wire the import once your config
reader reads the new block). Each adapter calls
``register(...)`` at module import time and joins the registry.

## The consensus aggregator

When the SMTP stage finishes a row, it queries every registered
validator (subject to `external_validators.timeout_seconds` and the
SMTP stage's existing rate limit) and aggregates the results via
``compute_consensus``. The rules:

| input | consensus |
|---|---|
| empty / no validators registered | `not_run` |
| any `invalid` | `invalid` |
| any `valid` AND no `invalid` AND no `risky` | `valid` |
| any `valid` AND any `risky` (no invalids) | `disputed` |
| every other case (all unknown / all catch_all / all risky) | `unconfirmed` |

The consensus is recorded as the ``external_consensus`` row column
and read by the V2 decision policy:

* Rule 5f (V2.10.11) — if ``external_consensus == "invalid"`` the
  row is rejected with reason ``external_validators_invalid``.
* `valid` / `disputed` / `unconfirmed` — informational only. The
  decision policy never *escalates* a row based on external
  opinions.

## Testing your adapter

The protocol is runtime-checkable:

```python
import pytest

from app.external_validators import (
    ExternalEmailValidator,
    clear_registry,
    register,
    registered_validators,
)

from configs.external_validators.zerobounce import ZeroBounceValidator


@pytest.fixture(autouse=True)
def _clean():
    clear_registry()
    yield
    clear_registry()


def test_zerobounce_adapter_satisfies_protocol():
    v = ZeroBounceValidator(api_key="fake")
    register(v)
    assert isinstance(v, ExternalEmailValidator)
    assert registered_validators() == (v,)
```

For unit tests that don't hit the network, write a fake adapter
returning a hard-coded verdict and register it before running the
SMTP stage. The same pattern lets staging environments swap the
production adapter for a deterministic stub.

## Operational considerations

* **Rate limits.** External APIs have strict per-minute caps.
  Either implement client-side throttling inside the adapter or
  set TrashPanda's ``smtp_probe.rate_limit_per_second`` to a
  conservative value — both probes share the per-row cadence.
* **Cost.** Vendors charge per probe. Filter the candidate set
  inside the adapter (e.g. only call the API for
  ``review_catch_all_consumer`` rows) instead of probing every
  row. The SMTP stage exposes the row dict to the adapter via
  the ``probe`` call site; the adapter can return
  ``verdict="not_run"`` early.
* **Caching.** Results are not cached across runs by default.
  Adapters can implement their own cache (the same cache pattern
  used by ``SimpleDomainIntelligenceService``); the
  ``raw_response`` field is preserved on the queue row so a future
  re-clean can audit the original answer.
* **Auth failures.** A vendor-side 401 / quota-exhaustion should
  surface as ``verdict="unknown"`` with a useful ``error`` string,
  not a Python exception. The aggregator treats unknowns as
  "no opinion" rather than escalating.

## When to skip external validators

* You only have B2B / corporate-domain lists. SMTP + the deferred
  retry queue + the cold-start cap toggle (V2.10.10 P0.2) handle
  these well; vendor APIs add little signal.
* Cost > value. Vendor pricing ($0.001 – $0.01 per probe) is
  meaningful at 100k+ rows; benchmark recovery rate before turning
  it on.
* Auditability matters more than recovery rate. Every external
  verdict is an opaque vendor decision; if your customer requires
  a paper trail, prefer the deferred SMTP retry queue (whose
  evidence is the actual MX response) over a vendor API.
