"""ChunkPayload: the unit of data flowing through pipeline stages."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd


@dataclass
class ChunkPayload:
    """One chunk of rows moving through the pipeline.

    Attributes:
        frame: The active DataFrame for this chunk.
        chunk_index: Index of this chunk within its source file.
        source_file: Original filename the chunk came from.
        metadata: Free-form bag stages can read from and write to for
            transient per-chunk information (processing flags, partial
            summaries, per-stage timings, etc.). Using ``metadata`` avoids
            having to change this class's signature every time a stage
            needs to pass a new piece of information forward.
    """

    frame: pd.DataFrame
    chunk_index: int = 0
    source_file: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def with_frame(self, frame: pd.DataFrame) -> "ChunkPayload":
        """Return a new payload backed by ``frame`` but sharing provenance.

        Useful for stages that prefer immutable-style updates: the new
        payload carries the same ``chunk_index``, ``source_file``, and
        ``metadata`` dict reference, but points at the given frame.
        """
        return ChunkPayload(
            frame=frame,
            chunk_index=self.chunk_index,
            source_file=self.source_file,
            metadata=self.metadata,
        )
