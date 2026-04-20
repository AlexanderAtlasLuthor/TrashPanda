"""Entry point for running app.cli as a module."""

from __future__ import annotations

import sys
from pathlib import Path

# Add vendor_site to sys.path before any imports
project_root = Path(__file__).parent.parent
vendor_site = project_root / "vendor_site"
if str(vendor_site) not in sys.path:
    sys.path.insert(0, str(vendor_site))

from .cli import main

if __name__ == "__main__":
    main()
