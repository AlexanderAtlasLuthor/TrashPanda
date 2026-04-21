"""Shared run-wide state carried across every stage of a pipeline run."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class PipelineContext:
    """Run-wide state shared by every stage for the duration of one run.

    Fields are typed as ``Any`` on purpose so this module does not need to
    import concrete business types (``AppConfig``, ``RunContext``,
    ``DnsCache``, ``DedupeIndex``, ``StagingDB`` …). That keeps the engine
    package free of domain coupling; callers assign the concrete instances
    at construction time.

    Attributes:
        config: The ``AppConfig`` for this run.
        logger: The run-scoped logger.
        run_context: The ``RunContext`` with resolved paths and run id.
        typo_map: Loaded typo correction map.
        dns_cache: Shared DNS cache across chunks and files.
        dedupe_index: Shared global deduplication index.
        staging: Shared staging database handle.
        metrics: Placeholder bag for run-level aggregate counters.
        extras: Free-form extension point for future subsystems so new
            shared services can be attached without modifying this class.
    """

    config: Any = None
    logger: Any = None
    run_context: Any = None
    typo_map: Any = None
    dns_cache: Any = None
    dedupe_index: Any = None
    staging: Any = None
    metrics: dict[str, Any] = field(default_factory=dict)
    extras: dict[str, Any] = field(default_factory=dict)
