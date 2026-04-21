"""ProbeDecisionTrace: step-by-step audit trail for a validation.

Every decision ValidationEngineV2 makes during a single
``validate(request)`` call appends a step to a trace. The trace
is then attached to the result so downstream callers can
reconstruct exactly why the engine did (or did not) probe, in
what order, and with which inputs.

A trace step is a plain dict so it is trivially JSON-serializable:

    {
        "stage":    <str>,  # "domain_intelligence", "exclusion", ...
        "decision": <str>,  # "collected", "excluded", "allowed", ...
        "reason":   <str>,  # fine-grained reason code
        "inputs":   <dict>  # what the engine consulted to decide
    }

The trace does not carry timestamps — determinism is more
valuable than wall-clock ordering for the audit use case. Tests
and downstream consumers compare traces by value; a timestamp
would make every fixture flaky.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


# Stage tokens emitted by the engine. Keeping them as module-level
# constants makes `trace.steps[0]["stage"] == STAGE_DOMAIN_INTELLIGENCE`
# style assertions easy to read and refactor.
STAGE_DOMAIN_INTELLIGENCE = "domain_intelligence"
STAGE_PROVIDER_REPUTATION = "provider_reputation"
STAGE_EXCLUSION = "exclusion"
STAGE_CANDIDATE = "candidate_selector"
STAGE_RATE_LIMIT = "rate_limit"
STAGE_EXECUTION_POLICY = "execution_policy"


@dataclass
class ProbeDecisionTrace:
    """Mutable, ordered list of decision steps.

    The dataclass is intentionally *not* frozen: the trace is
    built up step-by-step during a single ``validate`` call. It
    is never mutated after being attached to a result — the
    engine calls :meth:`to_dict` and hands the snapshot over.
    """

    steps: list[dict[str, Any]] = field(default_factory=list)

    def add_step(
        self,
        stage: str,
        decision: str,
        reason: str,
        inputs: dict[str, Any] | None = None,
    ) -> None:
        """Append a step to the trace.

        Parameters:
            stage: Which stage produced the decision. Prefer one
                of the ``STAGE_*`` constants above; arbitrary
                strings are allowed so future stages can append
                without edits here.
            decision: Short machine token for the outcome
                (``"allowed"``, ``"blocked"``, ``"collected"``,
                ``"skipped"``, etc.). Free-form — the trace
                consumer decides how to interpret it.
            reason: Fine-grained reason code. Matches the
                reason-code vocabulary of the stage (exclusion
                reasons, candidate reasons, execution reasons).
            inputs: Snapshot of what the stage consulted. Copied
                shallowly so later caller mutations do not
                retroactively change the trace.
        """
        self.steps.append(
            {
                "stage": stage,
                "decision": decision,
                "reason": reason,
                "inputs": dict(inputs) if inputs else {},
            }
        )

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable snapshot of the trace.

        Each step is deep-enough-copied that callers cannot
        mutate the trace's internal state through the returned
        dict. The ``inputs`` dict is shallow-copied — nested
        structures inside it are assumed to already be treated
        as immutable by callers.
        """
        return {
            "steps": [
                {
                    "stage": step["stage"],
                    "decision": step["decision"],
                    "reason": step["reason"],
                    "inputs": dict(step.get("inputs") or {}),
                }
                for step in self.steps
            ]
        }

    def __len__(self) -> int:
        return len(self.steps)

    def stages(self) -> list[str]:
        """Ordered list of stage tokens. Handy for assertions."""
        return [step["stage"] for step in self.steps]

    def reasons(self) -> list[str]:
        """Ordered list of reason codes. Handy for assertions."""
        return [step["reason"] for step in self.steps]


__all__ = [
    "ProbeDecisionTrace",
    "STAGE_DOMAIN_INTELLIGENCE",
    "STAGE_PROVIDER_REPUTATION",
    "STAGE_EXCLUSION",
    "STAGE_CANDIDATE",
    "STAGE_RATE_LIMIT",
    "STAGE_EXECUTION_POLICY",
]
