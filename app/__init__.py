"""Runtime package for the email cleaner project."""

from __future__ import annotations

import sys
from pathlib import Path

# Add vendor_site to sys.path before any other imports
_project_root = Path(__file__).parent.parent
_vendor_site = _project_root / "vendor_site"
if str(_vendor_site) not in sys.path:
    sys.path.insert(0, str(_vendor_site))

from .config import AppConfig, ProjectPaths, load_config, resolve_project_paths
from .models import ChunkContext, FileIngestionMetrics, InputFile, PipelineResult, PreparedInputFile, RunContext
from .pipeline import EmailCleaningPipeline

__all__ = [
    "AppConfig",
    "ProjectPaths",
    "RunContext",
    "InputFile",
    "PreparedInputFile",
    "ChunkContext",
    "FileIngestionMetrics",
    "PipelineResult",
    "EmailCleaningPipeline",
    "load_config",
    "resolve_project_paths",
]
