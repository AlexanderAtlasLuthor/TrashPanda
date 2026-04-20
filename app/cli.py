"""Command-line entrypoint for the ingestion pipeline."""

from __future__ import annotations

import argparse
import json

from .config import load_config, resolve_project_paths
from .io_utils import build_run_context
from .logger import setup_run_logger
from .pipeline import EmailCleaningPipeline


def build_parser() -> argparse.ArgumentParser:
    """Create the CLI parser for the current project stage."""

    parser = argparse.ArgumentParser(description="Run the local email cleaning pipeline.")
    parser.add_argument("--input-dir", help="Directory containing CSV/XLSX inputs.")
    parser.add_argument("--input-file", help="Single CSV/XLSX input file.")
    parser.add_argument("--output-dir", help="Optional run output directory.")
    parser.add_argument("--chunk-size", type=int, help="Override chunk size from config.")
    parser.add_argument("--workers", type=int, help="Override max worker count from config.")
    parser.add_argument("--config", help="Path to a YAML config file.")
    return parser


def main() -> None:
    """Parse CLI arguments, initialize runtime dependencies, and execute the pipeline."""

    parser = build_parser()
    args = parser.parse_args()

    project_paths = resolve_project_paths()
    overrides = {
        "chunk_size": args.chunk_size,
        "max_workers": args.workers,
    }
    config = load_config(config_path=args.config, base_dir=project_paths.project_root, overrides=overrides)

    run_context = build_run_context(config, output_dir=args.output_dir)
    logger = setup_run_logger(run_context.logs_dir, log_level=config.log_level)
    pipeline = EmailCleaningPipeline(config=config, logger=logger)
    result = pipeline.run(
        input_dir=args.input_dir,
        input_file=args.input_file,
        output_dir=run_context.run_dir,
        run_context=run_context,
    )

    payload = {
        "status": result.status,
        "input_mode": result.input_mode,
        "run_id": result.run_id,
        "run_dir": str(result.run_dir),
        "logs_dir": str(result.logs_dir),
        "temp_dir": str(result.temp_dir),
        "total_files": result.total_files,
        "total_rows": result.total_rows,
        "total_chunks": result.total_chunks,
        "processed_files": result.processed_files,
        "ignored_files": result.ignored_files,
    }
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
