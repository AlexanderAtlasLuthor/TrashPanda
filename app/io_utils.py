"""File-system helpers, discovery, conversion and chunked reading for Subphase 2."""

from __future__ import annotations

import csv
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import openpyxl
import pandas as pd

from .config import AppConfig
from .models import ChunkContext, InputFile, PreparedInputFile, RunContext
from .rules import SUPPORTED_FILE_TYPES


# Encoding fallback chain for CSV reads. Order matters: utf-8-sig first
# so byte-order marks on utf-8 files are transparently stripped; plain
# utf-8 second; cp1252 and latin-1 cover Windows/Excel exports that use
# Western European codepages (handles tildes, ñ, accented characters).
CSV_ENCODING_FALLBACKS: tuple[str, ...] = (
    "utf-8-sig",
    "utf-8",
    "cp1252",
    "latin-1",
)

_IO_LOGGER = logging.getLogger("app.io_utils")


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
        started_at=datetime.now(timezone.utc),
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


def _detect_csv_encoding(
    csv_path: Path,
    fallbacks: tuple[str, ...] = CSV_ENCODING_FALLBACKS,
    probe_bytes: int = 64 * 1024,
) -> str:
    """Pick the first encoding from ``fallbacks`` that can decode the file.

    Reads a probe from the start of the file for each candidate; if the
    probe alone can't disprove a codec we fall back to a full-file read
    for the last remaining candidate. ``latin-1`` can decode any byte
    stream, so the chain is guaranteed to terminate.
    """
    with csv_path.open("rb") as fh:
        head = fh.read(probe_bytes)
    for encoding in fallbacks:
        try:
            head.decode(encoding)
        except UnicodeDecodeError:
            continue
        # Probe passed. For very small files (probe covers the whole
        # file) we know the encoding is safe. For larger files we still
        # rely on the probe; if a later byte fails pandas will raise
        # a UnicodeDecodeError and the caller will retry with the next
        # encoding in ``read_csv_in_chunks`` below.
        return encoding
    # Unreachable because latin-1 never raises, but keep a defensive default.
    return fallbacks[-1]


def read_csv_in_chunks(
    csv_path: Path,
    chunk_size: int,
    logger: logging.Logger | None = None,
) -> Iterator[tuple[pd.DataFrame, ChunkContext]]:
    """Yield CSV chunks with technical chunk context.

    Encoding handling: tries ``utf-8-sig`` → ``utf-8`` → ``cp1252`` →
    ``latin-1`` in order. The detected encoding is logged once at INFO
    level. The input file is never modified; the fallback happens at
    read time only.
    """
    log = logger or _IO_LOGGER

    # Pre-flight probe: pick the best encoding candidate.
    preferred = _detect_csv_encoding(csv_path)
    encodings_to_try: list[str] = [preferred]
    for enc in CSV_ENCODING_FALLBACKS:
        if enc not in encodings_to_try:
            encodings_to_try.append(enc)

    last_error: UnicodeDecodeError | None = None
    for encoding in encodings_to_try:
        try:
            reader = pd.read_csv(
                csv_path,
                chunksize=chunk_size,
                dtype=str,
                keep_default_na=False,
                encoding=encoding,
            )
            start_row_number = 2
            emitted = False
            for chunk_index, chunk in enumerate(reader):
                if not emitted:
                    log.info(
                        "Detected input encoding: %s | file=%s",
                        encoding, csv_path.name,
                    )
                    emitted = True
                row_count = len(chunk.index)
                context = ChunkContext(
                    chunk_index=chunk_index,
                    row_count=row_count,
                    start_row_number=start_row_number,
                )
                yield chunk, context
                start_row_number += row_count
            if not emitted:
                # Empty file: still log the encoding decision.
                log.info(
                    "Detected input encoding: %s | file=%s (empty)",
                    encoding, csv_path.name,
                )
            return
        except UnicodeDecodeError as exc:
            last_error = exc
            log.warning(
                "Failed to decode %s as %s; trying next encoding.",
                csv_path.name, encoding,
            )
            continue
        except pd.errors.EmptyDataError:
            return

    raise ValueError(
        f"Unable to decode CSV file with any of {encodings_to_try}: {csv_path}"
    ) from last_error
