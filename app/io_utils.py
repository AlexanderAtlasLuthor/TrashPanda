"""File-system helpers, discovery, conversion and chunked reading for Subphase 2."""

from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import Iterator

import openpyxl
import pandas as pd

from .config import AppConfig
from .models import ChunkContext, InputFile, PreparedInputFile, RunContext
from .rules import SUPPORTED_FILE_TYPES


def build_run_context(config: AppConfig, output_dir: str | Path | None = None) -> RunContext:
    """Create the run directory structure and return a run context."""

    if config.paths is None:
        raise ValueError("Config paths are not initialized.")

    run_id = datetime.now().strftime("run_%Y%m%d_%H%M%S")
    run_dir = Path(output_dir).resolve() if output_dir else config.paths.output_dir / run_id
    logs_dir = run_dir / "logs"
    temp_dir = run_dir / config.temp_dir_name
    staging_db_path = run_dir / config.staging_db_name

    run_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    temp_dir.mkdir(parents=True, exist_ok=True)

    return RunContext(
        run_id=run_id,
        run_dir=run_dir,
        logs_dir=logs_dir,
        temp_dir=temp_dir,
        staging_db_path=staging_db_path,
        started_at=datetime.now(),
    )


def discover_input_files(
    input_dir: str | Path | None = None,
    input_file: str | Path | None = None,
) -> tuple[list[InputFile], list[str]]:
    """Discover supported input files from a directory or a single file."""

    if bool(input_dir) == bool(input_file):
        raise ValueError("Provide exactly one of --input-dir or --input-file.")

    discovered: list[InputFile] = []
    ignored: list[str] = []

    if input_file:
        candidate = Path(input_file).resolve()
        if not candidate.exists():
            raise FileNotFoundError(f"Input file does not exist: {candidate}")
        if not candidate.is_file():
            raise ValueError(f"Expected a file for --input-file: {candidate}")
        file_type = SUPPORTED_FILE_TYPES.get(candidate.suffix.lower())
        if file_type is None:
            raise ValueError(f"Unsupported input file extension: {candidate.suffix}")
        discovered.append(
            InputFile(
                absolute_path=candidate,
                original_name=candidate.name,
                file_type=file_type,
            )
        )
        return discovered, ignored

    directory = Path(input_dir).resolve()
    if not directory.exists():
        raise FileNotFoundError(f"Input directory does not exist: {directory}")
    if not directory.is_dir():
        raise ValueError(f"Expected a directory for --input-dir: {directory}")

    for candidate in sorted(directory.iterdir()):
        if not candidate.is_file():
            continue
        file_type = SUPPORTED_FILE_TYPES.get(candidate.suffix.lower())
        if file_type is None:
            ignored.append(candidate.name)
            continue
        discovered.append(
            InputFile(
                absolute_path=candidate.resolve(),
                original_name=candidate.name,
                file_type=file_type,
            )
        )

    if not discovered:
        raise FileNotFoundError(f"No supported CSV/XLSX files found in: {directory}")
    return discovered, ignored


def prepare_input_file(input_file: InputFile, run_context: RunContext) -> PreparedInputFile:
    """Prepare a discovered file for CSV-based internal processing."""

    if input_file.file_type == "csv":
        return PreparedInputFile(source=input_file, processing_csv_path=input_file.absolute_path)

    csv_path, sheet_name = convert_xlsx_to_temporary_csv(input_file, run_context.temp_dir)
    return PreparedInputFile(
        source=input_file,
        processing_csv_path=csv_path,
        sheet_name=sheet_name,
    )


def convert_xlsx_to_temporary_csv(input_file: InputFile, temp_dir: Path) -> tuple[Path, str]:
    """Convert an XLSX workbook to a temporary CSV using the first visible or active sheet."""

    workbook = openpyxl.load_workbook(input_file.absolute_path, read_only=True, data_only=True)
    visible_sheets = [sheet for sheet in workbook.worksheets if sheet.sheet_state == "visible"]
    worksheet = visible_sheets[0] if visible_sheets else workbook.active
    sheet_name = worksheet.title

    safe_sheet_name = "".join(char if char.isalnum() or char in {"_", "-"} else "_" for char in sheet_name)
    csv_path = temp_dir / f"{input_file.absolute_path.stem}__{safe_sheet_name}.csv"

    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        for row in worksheet.iter_rows(values_only=True):
            writer.writerow(["" if value is None else value for value in row])

    workbook.close()
    return csv_path, sheet_name


def read_csv_in_chunks(csv_path: Path, chunk_size: int) -> Iterator[tuple[pd.DataFrame, ChunkContext]]:
    """Yield CSV chunks with technical chunk context."""

    try:
        reader = pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype=str,
            keep_default_na=False,
            encoding="utf-8-sig",
        )
        start_row_number = 2
        for chunk_index, chunk in enumerate(reader):
            row_count = len(chunk.index)
            context = ChunkContext(
                chunk_index=chunk_index,
                row_count=row_count,
                start_row_number=start_row_number,
            )
            yield chunk, context
            start_row_number += row_count
    except UnicodeDecodeError as exc:
        raise ValueError(f"Unable to decode CSV file as UTF-8: {csv_path}") from exc
    except pd.errors.EmptyDataError:
        return
