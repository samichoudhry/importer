"""
Orchestration logic for multi-format file parsing.

This module contains the core business logic for processing files in batch,
independent of CLI concerns.
"""

import json
import logging
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from multi_format_parser.csv_writer import CSVWriter
from multi_format_parser.models import ParsingStats
from multi_format_parser.parsers import parse_csv, parse_fixed_width, parse_json, parse_xml
from multi_format_parser.validators import validate_config

logger = logging.getLogger(__name__)


class FileProcessingError(Exception):
    """Exception raised when a file fails to process."""
    pass


def parse_files(
    config_path: Path,
    input_files: List[Path],
    output_dir: Path,
    dry_run: bool = False,
    fail_fast: bool = False
) -> Tuple[Dict[str, int], Dict[str, ParsingStats], Dict[str, str]]:
    """Parse files according to configuration.

    Args:
        config_path: Path to configuration JSON file
        input_files: List of input files to process
        output_dir: Output directory for results
        dry_run: If True, parse and validate but don't write outputs
        fail_fast: If True, stop on first file error (default: continue)

    Returns:
        Tuple: (stats dict, record_stats dict, file_errors dict)
    """
    start_time = time.time()
    file_errors: Dict[str, str] = {}

    if not config_path.exists():
        raise FileNotFoundError(f"Config not found: {config_path}")

    logger.info(f"Loading configuration from {config_path}")
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)

    # Validate configuration
    logger.info("Validating configuration...")
    config_errors = validate_config(config)
    if config_errors:
        for error in config_errors:
            logger.error(f"Config validation error: {error}")
        raise ValueError(f"Configuration validation failed with {len(config_errors)} error(s)")
    logger.info("Configuration valid")

    format_type = config.get("format_type", "").lower()
    if not format_type:
        raise ValueError("Config must specify 'format_type'")
    
    # Get max file size limit (in bytes, default: None = no limit)
    # Example: 1GB = 1073741824, 500MB = 524288000
    max_file_size = config.get("max_file_size")
    if max_file_size is not None and not isinstance(max_file_size, int):
        raise ValueError(f"max_file_size must be an integer (bytes), got {type(max_file_size).__name__}")

    # Filter files by regex pattern (file_mask)
    file_mask = config.get("file_mask")
    if file_mask is not None:
        if not isinstance(file_mask, str):
            raise ValueError(f"file_mask must be a string regex pattern, got {type(file_mask).__name__}")
        try:
            pattern = re.compile(file_mask)
            original_count = len(input_files)
            input_files = [f for f in input_files if pattern.search(str(f.name))]
            filtered_count = original_count - len(input_files)
            if filtered_count > 0:
                logger.info(f"File mask '{file_mask}' filtered out {filtered_count} file(s), {len(input_files)} remaining")
            if len(input_files) == 0:
                raise ValueError(f"File mask '{file_mask}' filtered out all input files. No files to process.")
        except re.error as e:
            raise ValueError(f"Invalid file_mask regex pattern '{file_mask}': {e}")

    # Get max files limit (default: None = no limit)
    max_files = config.get("max_files")
    if max_files is not None:
        if not isinstance(max_files, int):
            raise ValueError(f"max_files must be an integer, got {type(max_files).__name__}")
        if max_files <= 0:
            raise ValueError(f"max_files must be greater than 0, got {max_files}")
        if len(input_files) > max_files:
            logger.warning(f"Limiting processing to first {max_files} of {len(input_files)} files")
            input_files = input_files[:max_files]

    if dry_run:
        logger.info("🧪 DRY RUN MODE - Files will be parsed but no outputs will be written")

    logger.info(f"Format: {format_type}, Files: {len(input_files)}, Fail-fast: {fail_fast}")

    # Initialize stats dict for parsers (currently unused but required by signature)
    stats = {}
    record_stats = {}
    successful_files = 0
    failed_files = 0

    # Get flush_every setting from config (default: 1000 for production performance)
    # None = flush every row, 0 = flush only on close, N = flush every N rows
    flush_every = config.get("output", {}).get("flush_every", 1000)

    if not dry_run:
        if flush_every == 0:
            logger.info("CSV flushing: only on close (maximum performance, higher crash risk)")
        elif flush_every is None:
            logger.info("CSV flushing: every row (maximum safety, lower performance)")
        else:
            logger.info(f"CSV flushing: every {flush_every} rows (balanced performance/safety)")

    # Use context manager to ensure files are always closed, even on exceptions
    # In dry-run mode, writer will be None
    writer_or_none: Optional[CSVWriter] = None if dry_run else CSVWriter(output_dir, flush_every=flush_every)

    try:
        if writer_or_none:
            writer_or_none.__enter__()

        for file_idx, input_file in enumerate(input_files, 1):
            file_start = time.time()

            # Validate file exists before processing
            if not input_file.exists():
                error_msg = f"Input file not found: {input_file}"
                logger.error(f"[{file_idx}/{len(input_files)}] {error_msg}")
                file_errors[str(input_file)] = error_msg
                failed_files += 1
                if fail_fast:
                    raise FileProcessingError(error_msg)
                continue

            # Get file size safely, handling special files (pipes, symlinks, etc.)
            try:
                file_stat = input_file.stat()
                file_size = file_stat.st_size

                # Check if it's a special file type
                import stat as stat_module
                if stat_module.S_ISFIFO(file_stat.st_mode):
                    logger.info(f"[{file_idx}/{len(input_files)}] Processing: {input_file.name} (named pipe)")
                elif stat_module.S_ISSOCK(file_stat.st_mode):
                    logger.warning(f"[{file_idx}/{len(input_files)}] Warning: {input_file.name} is a socket, processing may fail")
                elif stat_module.S_ISBLK(file_stat.st_mode) or stat_module.S_ISCHR(file_stat.st_mode):
                    logger.warning(f"[{file_idx}/{len(input_files)}] Warning: {input_file.name} is a device file")
                elif input_file.is_symlink():
                    # Resolve symlink and get target size
                    try:
                        resolved = input_file.resolve()
                        resolved_size = resolved.stat().st_size
                        
                        # Check file size limit
                        if max_file_size is not None and resolved_size > max_file_size:
                            size_mb = resolved_size / (1024 * 1024)
                            limit_mb = max_file_size / (1024 * 1024)
                            error_msg = f"File size {size_mb:.2f} MB exceeds maximum allowed size {limit_mb:.2f} MB"
                            logger.error(f"[{file_idx}/{len(input_files)}] {error_msg}")
                            file_errors[str(input_file)] = error_msg
                            failed_files += 1
                            if fail_fast:
                                raise FileProcessingError(error_msg)
                            continue
                        
                        size_mb = resolved_size / (1024 * 1024)
                        logger.info(f"[{file_idx}/{len(input_files)}] Processing: {input_file.name} -> {resolved.name} ({size_mb:.2f} MB)")
                    except (OSError, RuntimeError):
                        logger.info(f"[{file_idx}/{len(input_files)}] Processing: {input_file.name} (symlink, size unknown)")
                elif file_size == 0:
                    logger.warning(f"[{file_idx}/{len(input_files)}] Processing: {input_file.name} (0 bytes - empty or special file)")
                else:
                    # Check file size limit
                    if max_file_size is not None and file_size > max_file_size:
                        size_mb = file_size / (1024 * 1024)
                        limit_mb = max_file_size / (1024 * 1024)
                        error_msg = f"File size {size_mb:.2f} MB exceeds maximum allowed size {limit_mb:.2f} MB"
                        logger.error(f"[{file_idx}/{len(input_files)}] {error_msg}")
                        file_errors[str(input_file)] = error_msg
                        failed_files += 1
                        if fail_fast:
                            raise FileProcessingError(error_msg)
                        continue
                    
                    size_mb = file_size / (1024 * 1024)
                    logger.info(f"[{file_idx}/{len(input_files)}] Processing: {input_file.name} ({size_mb:.2f} MB)")
            except (OSError, PermissionError, AttributeError) as e:
                # stat() failed - log warning but continue processing
                logger.warning(f"[{file_idx}/{len(input_files)}] Processing: {input_file.name} (size unavailable: {e})")

            # Wrap individual file processing in try/except for continue-on-error
            try:
                if format_type == "xml":
                    # parse_xml returns (ok: bool, error: Optional[str])
                    ok, error = parse_xml(input_file, config, writer_or_none, stats, record_stats)
                    if not ok:
                        # File-level parse failure handled by ignoreBrokenFiles flag
                        raise FileProcessingError(error or "XML parse failure")
                elif format_type == "csv":
                    # parse_csv returns (ok: bool, error: Optional[str])
                    ok, error = parse_csv(input_file, config, writer_or_none, stats, record_stats)
                    if not ok:
                        raise FileProcessingError(error or "CSV parse failure")
                elif format_type == "fixed_width":
                    # parse_fixed_width returns (ok: bool, error: Optional[str])
                    ok, error = parse_fixed_width(input_file, config, writer_or_none, stats, record_stats)
                    if not ok:
                        raise FileProcessingError(error or "Fixed-width parse failure")
                elif format_type == "json":
                    # parse_json returns (ok: bool, error: Optional[str])
                    ok, error = parse_json(input_file, config, writer_or_none, stats, record_stats)
                    if not ok:
                        raise FileProcessingError(error or "JSON parse failure")
                else:
                    raise ValueError(f"Unsupported format: {format_type}")

                file_duration = time.time() - file_start
                logger.info(f"✅ Completed {input_file.name} in {file_duration:.2f}s")
                successful_files += 1

            except Exception as e:
                file_duration = time.time() - file_start
                error_msg = f"{type(e).__name__}: {str(e)}"
                logger.error(f"❌ Failed {input_file.name} after {file_duration:.2f}s: {error_msg}")
                file_errors[str(input_file)] = error_msg
                failed_files += 1

                if fail_fast:
                    raise FileProcessingError(f"File processing failed: {error_msg}") from e
                # Otherwise continue to next file

    finally:
        # Ensure writer is properly closed
        if writer_or_none:
            try:
                writer_or_none.__exit__(None, None, None)
            except Exception as e:
                logger.error(f"Error closing writer: {e}")

    # Update end times after processing completes
    for stat in record_stats.values():
        if stat.end_time is None:
            stat.end_time = time.time()

    total_duration = time.time() - start_time
    logger.info(f"Total processing time: {total_duration:.2f}s")
    logger.info(f"Files: {successful_files} succeeded, {failed_files} failed")

    # Populate stats dictionary with summary
    stats = {
        "processed": successful_files + failed_files,
        "succeeded": successful_files,
        "failed": failed_files,
        "duration": total_duration
    }

    return stats, record_stats, file_errors
