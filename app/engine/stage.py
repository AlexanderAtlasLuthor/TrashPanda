"""Stage abstraction for the pipeline engine.

A Stage is a single, composable unit of processing. It receives a
ChunkPayload and a shared PipelineContext, performs its work, and returns
a (possibly new) ChunkPayload. Stages must not hold mutable state between
invocations — any such state belongs on the PipelineContext.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .context import PipelineContext
    from .payload import ChunkPayload


class Stage(ABC):
    """Base class for a pipeline stage.

    Subclasses override ``name`` (a stable string identifier) and implement
    ``run``. ``requires`` and ``produces`` are declared here as empty
    tuples so stages can start populating them now; the engine does not
    enforce them yet, but a future schema-validation pass will read these
    without any further API change.
    """

    name: str = ""
    requires: tuple[str, ...] = ()
    produces: tuple[str, ...] = ()

    @abstractmethod
    def run(
        self,
        payload: "ChunkPayload",
        context: "PipelineContext",
    ) -> "ChunkPayload":
        """Process a payload and return the transformed payload.

        Implementations may mutate ``payload.frame`` in place and return
        the same payload, or produce a new one via ``payload.with_frame``.
        """
        raise NotImplementedError
