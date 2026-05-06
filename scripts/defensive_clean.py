"""Defensive-only clean: run the pipeline without SMTP probing and
emit the customer bundle.

Why this exists
---------------

For lists where the budget does not allow a controlled SMTP pilot
(or where a pilot already failed because the sending IP has
reputation issues), we still want to do a defensible cleanup
using only the layers that don't need a network handshake to a
recipient MX:

* syntax checks
* MX existence
* domain risk / known-bad
* disposable detection
* role-based detection
* typo correction / suggestion
* historical bounce memory
* domain intelligence

This script wires the existing pipeline with ``smtp_probe.enabled
= False`` and then calls ``emit_customer_bundle`` so the operator
gets the same four customer-language files
(``clean_deliverable``, ``review_provider_limited``,
``high_risk_removed``, ``smtp_evidence_report``) as a pilot run
would produce — but with the SMTP-evidence file containing only
the header, since no SMTP rows were generated.

Usage
-----

    python -m scripts.defensive_clean \\
        --input-file path/to/list.csv \\
        --output-dir runtime/jobs/defensive-001
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

if __name__ == "__main__" and __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import load_config, resolve_project_paths  # noqa: E402
from app.customer_bundle import emit_customer_bundle  # noqa: E402
from app.io_utils import build_run_context  # noqa: E402
from app.logger import setup_run_logger  # noqa: E402
from app.pipeline import EmailCleaningPipeline  # noqa: E402


def _run(args: argparse.Namespace) -> dict:
    if not args.input_file and not args.input_dir:
        raise SystemExit("Provide --input-file or --input-dir.")

    project_paths = resolve_project_paths()
    overrides: dict[str, object] = {
        "chunk_size": args.chunk_size,
        "max_workers": args.workers,
    }
    config = load_config(
        config_path=args.config,
        base_dir=project_paths.project_root,
        overrides=overrides,
    )
    # Force SMTP probing off — that's the whole point of "defensive
    # only". Any other config (syntax / MX / disposable / etc.) is
    # left as-is.
    if hasattr(config, "smtp_probe") and config.smtp_probe is not None:
        try:
            config.smtp_probe.enabled = False
        except Exception:  # pragma: no cover - dataclass frozen edge
            pass

    run_context = build_run_context(config, output_dir=args.output_dir)
    logger = setup_run_logger(run_context.logs_dir, log_level=config.log_level)
    pipeline = EmailCleaningPipeline(config=config, logger=logger)

    result = pipeline.run(
        input_dir=args.input_dir,
        input_file=args.input_file,
        output_dir=run_context.run_dir,
        run_context=run_context,
    )

    bundle = emit_customer_bundle(result.run_dir)

    return {
        "status": result.status,
        "run_id": result.run_id,
        "run_dir": str(result.run_dir),
        "total_files": result.total_files,
        "total_rows": result.total_rows,
        "smtp_probing": "disabled (defensive-only mode)",
        "customer_bundle": {
            "dir": str(bundle.bundle_dir),
            "counts": bundle.counts,
            "files": {k: str(v) for k, v in bundle.files_written.items()},
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run the email cleaning pipeline with SMTP probing OFF "
            "and emit the customer-language bundle."
        ),
    )
    parser.add_argument("--input-file", help="Single CSV/XLSX input file.")
    parser.add_argument("--input-dir", help="Directory containing inputs.")
    parser.add_argument("--output-dir", help="Run output directory.")
    parser.add_argument("--config", help="Path to a YAML config file.")
    parser.add_argument(
        "--chunk-size", type=int, help="Override chunk size from config.",
    )
    parser.add_argument(
        "--workers", type=int, help="Override max worker count from config.",
    )
    args = parser.parse_args(argv)

    payload = _run(args)
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
