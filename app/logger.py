"""Logging setup for pipeline runs."""

from __future__ import annotations

import logging
from pathlib import Path


def setup_run_logger(logs_dir: Path, log_level: str = "INFO") -> logging.Logger:
    """Create a logger with both file and console output for a single run."""

    logs_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("email_cleaner")
    logger.handlers.clear()
    logger.setLevel(_resolve_log_level(log_level))
    logger.propagate = False

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(logs_dir / "run.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger


def _resolve_log_level(log_level: str) -> int:
    """Resolve a log level name to a stdlib logging constant."""

    resolved = getattr(logging, log_level.upper(), None)
    if not isinstance(resolved, int):
        raise ValueError(f"Unsupported log level: {log_level}")
    return resolved
