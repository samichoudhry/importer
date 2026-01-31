"""
Command-line interface for multi-format parser.

This module handles CLI argument parsing, logging configuration,
and user interaction.
"""

import argparse
import logging
import sys
from pathlib import Path

from multi_format_parser.orchestrator import FileProcessingError, parse_files

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def main() -> int:
    """CLI entry point.

    Returns:
        Exit code: 0 = success, 1 = all files failed, 2 = partial failure
    """
    parser = argparse.ArgumentParser(
        description="Production-ready multi-format data parser",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Parse XML files
  multiformat-parser --config config.json --out ./output file1.xml file2.xml

  # Parse CSV files
  multiformat-parser --config config.json --out ./output file1.csv file2.csv

  # Parse JSON files
  multiformat-parser --config config.json --out ./output file1.json file2.json

  # Dry run (validate only, no output)
  multiformat-parser --config config.json --out ./output --dry-run file1.xml
        """
    )
    parser.add_argument("--config", required=True, type=Path, help="Config JSON file")
    parser.add_argument("--out", required=True, type=Path, help="Output directory")
    parser.add_argument("input_files", nargs="+", type=Path, help="Input files")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                       help="Logging level")
    parser.add_argument("--dry-run", action="store_true",
                       help="Parse and validate without writing outputs")
    parser.add_argument("--fail-fast", action="store_true",
                       help="Stop processing on first file error (default: continue)")

    args = parser.parse_args()
    logging.getLogger().setLevel(args.log_level)

    try:
        stats, record_stats, file_errors = parse_files(
            args.config,
            args.input_files,
            args.out,
            dry_run=args.dry_run,
            fail_fast=args.fail_fast
        )

        # Log summary using structured logging
        logger.info("="*80)
        logger.info("PARSING COMPLETE")
        logger.info("="*80)

        if stats:
            logger.info("Row Counts:")
            for table in sorted(stats.keys()):
                logger.info(f"  {table}: {stats[table]:,}")

        if record_stats:
            logger.info("Performance Metrics:")
            for table, pstats in sorted(record_stats.items()):
                success_rate = (pstats.success_rows / pstats.total_rows * 100) if pstats.total_rows > 0 else 0
                logger.info(f"  {table}:")
                logger.info(f"    Total processed: {pstats.total_rows:,}")
                logger.info(f"    Successful: {pstats.success_rows:,} ({success_rate:.1f}%)")
                logger.info(f"    Failed: {pstats.failed_rows:,}")
                if pstats.skipped_rows > 0:
                    logger.info(f"    Skipped: {pstats.skipped_rows:,}")
                logger.info(f"    Validation errors: {pstats.validation_errors:,}")
                if pstats.file_parse_failures > 0:
                    logger.info(f"    File parse failures: {pstats.file_parse_failures:,}")
                logger.info(f"    Duration: {pstats.duration:.2f}s")
                logger.info(f"    Throughput: {pstats.rows_per_second:.0f} rows/sec")

        total_success = sum(s.success_rows for s in record_stats.values())
        total_failed = sum(s.failed_rows for s in record_stats.values())
        total_errors = sum(s.validation_errors for s in record_stats.values())

        logger.info("="*80)
        logger.info(f"Total Successful: {total_success:,} rows")
        if total_failed > 0:
            logger.warning(f"Total Failed: {total_failed:,} rows (see *_rejected.csv files)")
        if total_errors > 0:
            logger.warning(f"Total Validation Errors: {total_errors:,}")

        if not args.dry_run:
            logger.info(f"Output Location: {args.out.resolve()}")
        else:
            logger.info("DRY RUN - No outputs written")

        # Log file-level errors if any
        if file_errors:
            logger.error("="*80)
            logger.error(f"FILE PROCESSING ERRORS ({len(file_errors)} files failed):")
            for file_path, error_msg in file_errors.items():
                logger.error(f"  {file_path}: {error_msg}")
            logger.error("="*80)

        logger.info("="*80)

        # Determine exit code
        total_files = len(args.input_files)
        failed_file_count = len(file_errors)
        successful_file_count = total_files - failed_file_count

        if failed_file_count == 0:
            # All files succeeded
            return 0
        elif successful_file_count == 0:
            # All files failed
            return 1
        else:
            # Partial failure - some files succeeded, some failed
            return 2

    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        return 1
    except ValueError as e:
        logger.error(f"Validation error: {e}")
        return 1
    except FileProcessingError as e:
        logger.error(f"File processing error: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
