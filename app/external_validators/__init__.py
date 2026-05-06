"""V2.10.11 — external email-validator integration.

Defines the contract a third-party email-validation service must
satisfy to be used as a "second opinion" alongside the in-process
SMTP probe and the deferred retry queue. The package intentionally
ships **no built-in providers** (ZeroBounce / Hunter / NeverBounce
/ etc.) — the operator writes a small adapter that wraps their
preferred service and registers it. This keeps the repository
neutral between vendors and avoids embedding API keys / vendor
SDKs into the public codebase.

Public API
----------

* :class:`ExternalValidationResult` — uniform shape every adapter
  returns. Carries a coarse verdict and a confidence score; the
  raw vendor response is preserved as a free-form dict for audit
  / debugging.
* :class:`ExternalEmailValidator` — protocol the adapter
  implements. Single method ``probe(email, *, timeout)``.
* :func:`register` / :func:`registered_validators` —
  process-wide registry. Adapters call ``register`` at import
  time; downstream consumers (the SMTP stage's external-opinion
  step, tests, the operator-triggered re-clean) iterate
  ``registered_validators()``.
* :func:`compute_consensus` — aggregator that turns N
  per-validator results into one ``external_consensus`` value.
  Decision rule 5f reads only this consensus, never an individual
  validator, so the policy is monotone in the number of opinions.

Threading
---------

The registry is global module state. Adapters should be
idempotent — registering the same name twice replaces the prior
entry, which is the right behaviour for hot-reloading dev
environments. Production registers each adapter exactly once at
process start.
"""

from __future__ import annotations

from .consensus import (
    EXTERNAL_CONSENSUS_DISPUTED,
    EXTERNAL_CONSENSUS_INVALID,
    EXTERNAL_CONSENSUS_NOT_RUN,
    EXTERNAL_CONSENSUS_UNCONFIRMED,
    EXTERNAL_CONSENSUS_VALID,
    EXTERNAL_CONSENSUS_VALUES,
    compute_consensus,
)
from .registry import (
    ExternalEmailValidator,
    ExternalValidationResult,
    clear_registry,
    register,
    registered_validators,
)

__all__ = [
    "EXTERNAL_CONSENSUS_DISPUTED",
    "EXTERNAL_CONSENSUS_INVALID",
    "EXTERNAL_CONSENSUS_NOT_RUN",
    "EXTERNAL_CONSENSUS_UNCONFIRMED",
    "EXTERNAL_CONSENSUS_VALID",
    "EXTERNAL_CONSENSUS_VALUES",
    "ExternalEmailValidator",
    "ExternalValidationResult",
    "clear_registry",
    "compute_consensus",
    "register",
    "registered_validators",
]
