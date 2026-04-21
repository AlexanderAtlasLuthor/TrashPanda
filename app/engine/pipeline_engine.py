"""Generic stage executor for the pipeline.

The engine is deliberately domain-agnostic: it knows about stages,
payloads, and contexts, and nothing about emails, DNS, scoring, or any
other business concern. All domain logic belongs in Stage subclasses.
"""

from __future__ import annotations

import logging
import time
from typing import Iterable

from .context import PipelineContext
from .payload import ChunkPayload
from .stage import Stage


class PipelineEngine:
    """Execute an ordered list of stages against a payload.

    The engine owns only three responsibilities:
      1. Iterate the configured stages in order.
      2. Thread the returned payload from stage N into stage N+1.
      3. Emit basic lifecycle logs and propagate stage exceptions.
    """

    def __init__(
        self,
        stages: Iterable[Stage],
        logger: logging.Logger | None = None,
    ) -> None:
        self._stages: list[Stage] = list(stages)
        self._logger = logger or logging.getLogger(__name__)

    @property
    def stages(self) -> list[Stage]:
        """Return a copy of the configured stage list."""
        return list(self._stages)

    def run(self, payload: ChunkPayload, context: PipelineContext) -> ChunkPayload:
        """Run every stage in order, threading the payload through.

        Each stage may mutate ``payload.frame`` in place and return the
        same payload, or produce a fresh one via ``payload.with_frame``.
        The engine updates its local reference to whatever the stage
        returns and passes it to the next stage. Any exception is logged
        with the stage name and re-raised unchanged.
        """
        current = payload
        for stage in self._stages:
            stage_name = stage.name or stage.__class__.__name__
            self._logger.debug("Stage start: %s", stage_name)
            t0 = time.perf_counter()
            try:
                current = stage.run(current, context)
            except Exception:
                self._logger.exception("Stage failed: %s", stage_name)
                raise
            elapsed = time.perf_counter() - t0
            self._logger.info(
                "[TIMING] stage=%s chunk=%s rows=%s elapsed=%.3fs",
                stage_name,
                current.chunk_index,
                len(current.frame),
                elapsed,
            )
        return current
