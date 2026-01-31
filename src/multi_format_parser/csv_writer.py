"""
CSV Writer with resource management and performance optimization.
"""

import csv
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional


class CSVWriter:
    """Manages CSV output files with proper resource management.

    Args:
        out_dir: Output directory for CSV files
        flush_every: Flush to disk every N rows (0 = flush on close only, None = flush every row).
                     Default: 1000 for production performance.
    """

    def __init__(self, out_dir: Path, flush_every: Optional[int] = 1000):
        self.out_dir = out_dir
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self._writers = {}
        self._rejected_writers = {}
        self._row_counts = {}
        self._write_counts = {}  # Track writes per file for periodic flushing
        self._rejected_write_counts = {}  # Track writes per rejected file
        self._closed = False
        self.flush_every = flush_every  # None=every row, 0=on close only, N=every N rows

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures files are closed."""
        self.close()
        return False  # Don't suppress exceptions

    def write_row(self, table: str, row: Dict[str, Any], columns: List[str]):
        """Write a row to the CSV file."""
        if self._closed:
            raise RuntimeError("CSVWriter is closed")

        if table not in self._writers:
            fp = None
            try:
                fp = (self.out_dir / f"{table}.csv").open("w", newline="", encoding="utf-8")
                writer = csv.DictWriter(fp, fieldnames=columns, extrasaction="ignore")
                writer.writeheader()
                fp.flush()
                self._writers[table] = (writer, fp, columns)
                fp = None
            except Exception:
                if fp is not None:
                    try:
                        fp.close()
                    except Exception:
                        pass  # Ignore errors during cleanup
                raise

        writer, fp, cols = self._writers[table]
        if cols != columns:
            raise RuntimeError(f"Schema mismatch for table '{table}'")

        clean = {}
        for col in columns:
            v = row.get(col)
            clean[col] = format(v, "f") if isinstance(v, Decimal) else v
        writer.writerow(clean)

        self._row_counts[table] = self._row_counts.get(table, 0) + 1
        write_count = self._write_counts.get(table, 0) + 1
        self._write_counts[table] = write_count

        should_flush = (
            self.flush_every is None or
            (self.flush_every > 0 and write_count % self.flush_every == 0)
        )
        if should_flush:
            fp.flush()

    def write_rejected_row(self, table: str, row: Dict[str, Any], error: str, columns: List[str]):
        """Write a rejected row to a separate file with error reason."""
        if self._closed:
            raise RuntimeError("CSVWriter is closed")

        reject_table = f"{table}_rejected"
        reject_cols = columns + ["_error_reason"]

        if reject_table not in self._rejected_writers:
            fp = None
            try:
                fp = (self.out_dir / f"{reject_table}.csv").open("w", newline="", encoding="utf-8")
                writer = csv.DictWriter(fp, fieldnames=reject_cols, extrasaction="ignore")
                writer.writeheader()
                fp.flush()
                self._rejected_writers[reject_table] = (writer, fp, reject_cols)
                fp = None
            except Exception:
                if fp is not None:
                    try:
                        fp.close()
                    except Exception:
                        pass  # Ignore errors during cleanup
                raise

        writer, fp, cols = self._rejected_writers[reject_table]

        clean = {}
        for col in columns:
            v = row.get(col)
            clean[col] = format(v, "f") if isinstance(v, Decimal) else v
        clean["_error_reason"] = error
        writer.writerow(clean)

        write_count = self._rejected_write_counts.get(reject_table, 0) + 1
        self._rejected_write_counts[reject_table] = write_count

        should_flush = (
            self.flush_every is None or
            (self.flush_every > 0 and write_count % self.flush_every == 0)
        )
        if should_flush:
            fp.flush()

    def close(self):
        """Close all open files with error handling."""
        if self._closed:
            return

        errors = []

        for table, (writer, fp, _) in list(self._writers.items()):
            try:
                if fp and not fp.closed:
                    fp.flush()
                    fp.close()
            except Exception as e:
                errors.append(f"Error closing {table}.csv: {e}")

        for table, (writer, fp, _) in list(self._rejected_writers.items()):
            try:
                if fp and not fp.closed:
                    fp.flush()
                    fp.close()
            except Exception as e:
                errors.append(f"Error closing {table}.csv: {e}")

        self._closed = True
        self._writers.clear()
        self._rejected_writers.clear()

        if errors:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Errors during CSVWriter.close(): {'; '.join(errors)}")

    def get_row_count(self, table: str) -> int:
        """Get row count for a table."""
        return self._row_counts.get(table, 0)
