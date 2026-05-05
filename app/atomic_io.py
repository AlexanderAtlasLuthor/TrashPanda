"""V2.9.9 — atomic file write helpers.

Small, dependency-free helpers used by report writers that need to
guarantee a target file is never observed in a half-written state.

The pattern is the standard tmp-then-rename:

* write to ``.<name>.tmp`` in the same directory as the target,
* call :func:`os.replace` (atomic on POSIX, atomic on Windows when
  src/dst share a filesystem),
* clean up the temporary file on failure.

Used by:

* :mod:`app.client_package_builder` for ``client_package_manifest.json``
* :mod:`app.operator_review_gate` for ``operator_review_summary.json``

Other report writers in the project intentionally keep their existing
direct-write semantics; expanding atomic write across the codebase is
out of scope for V2.9.9.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Mapping


def atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    """Write ``payload`` as JSON to ``path`` atomically.

    Steps:

    1. Ensure parent directory exists.
    2. Serialise ``payload`` to ``.<name>.tmp`` in the same directory.
    3. ``os.replace`` the temp file onto ``path``.
    4. If anything fails, remove the temp file before re-raising.

    Notes
    -----
    * JSON is written with ``indent=2, sort_keys=True`` and a trailing
      newline so produced files are deterministic.
    * The replace call is atomic on POSIX. On Windows it is atomic when
      source and target share a filesystem, which is always the case
      here because the temp file lives in the target's parent dir.
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(f".{target.name}.tmp")
    try:
        tmp.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(tmp, target)
    finally:
        # If os.replace succeeded, tmp no longer exists. If it failed,
        # we delete the leftover so subsequent runs start clean.
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:  # pragma: no cover - defensive
            pass


__all__ = ["atomic_write_json"]
