"""Generic stage-based pipeline execution primitives.

This package provides foundational abstractions for a stage-based
pipeline engine:

  * ``Stage`` — an abstract processing unit.
  * ``ChunkPayload`` — the data envelope flowing between stages.
  * ``PipelineContext`` — shared state across one pipeline run.
  * ``PipelineEngine`` — the ordered stage executor.

The package has no business-domain coupling. Adding it to the project
does not change any existing behavior; it only exposes new building
blocks that future refactors can compose.
"""

from __future__ import annotations

from .context import PipelineContext
from .payload import ChunkPayload
from .pipeline_engine import PipelineEngine
from .stage import Stage

__all__ = [
    "ChunkPayload",
    "PipelineContext",
    "PipelineEngine",
    "Stage",
]
