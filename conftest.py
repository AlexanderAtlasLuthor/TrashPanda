"""Pytest bootstrap for local vendored dependencies."""

from __future__ import annotations

import sys
from pathlib import Path


_VENDOR_PATH = Path(__file__).resolve().parent / ".vendor_py"
if _VENDOR_PATH.exists():
    sys.path.insert(0, str(_VENDOR_PATH))
