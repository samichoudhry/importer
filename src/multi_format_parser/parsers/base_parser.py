"""
Base parser class with common functionality.

This module provides a base class that extracts common logic shared across
all parser implementations (XML, CSV, JSON, Fixed-Width).
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from multi_format_parser.casting import cast_value
from multi_format_parser.csv_writer import CSVWriter
from multi_format_parser.formula_utils import format_formula
from multi_format_parser.models import FieldDef, ParsingStats
from multi_format_parser.validators import validate_field_value

logger = logging.getLogger(__name__)


class BaseParser:
    """Base class for all file format parsers.
    
    Provides common functionality including:
    - Configuration flag extraction
    - Stats initialization
    - Field definition building
    - Computed field processing
    - Row validation and writing
    - Progress logging
    """

    def __init__(self, file_path: Path, config: dict, writer: Optional[CSVWriter],
                 stats: dict, record_stats: Dict[str, ParsingStats]):
        """Initialize parser with common configuration.
        
        Args:
            file_path: Path to input file
            config: Parser configuration dict
            writer: Optional CSV writer for output
            stats: Row count statistics dict
            record_stats: Per-record parsing statistics
        """
        self.file_path = file_path
        self.config = config
        self.writer = writer
        self.stats = stats
        self.record_stats = record_stats

        # Extract common configuration flags
        self.ignore_broken = self._get_config_flag("ignoreBrokenFiles", False)
        self.continue_on_error = self._get_config_flag("continueOnError", False)
        self.progress_interval = config.get("progress_interval", 10000)
        self.safe_mode = config.get("normalization", {}).get("cast_mode", "safe") == "safe"

        # Pre-build computed fields dictionary
        self.computed_fields = {c["name"]: c for c in config.get("computed_fields", [])}

        # Initialize stats for all records upfront
        self._initialize_record_stats()

    def _get_config_flag(self, flag_name: str, default: bool) -> bool:
        """Get configuration flag from top-level or parser sub-config.
        
        Args:
            flag_name: Name of configuration flag
            default: Default value if not found
            
        Returns:
            Configuration flag value
        """
        return (self.config.get(flag_name, default) or
                self.config.get("parser", {}).get(flag_name, default))

    def _initialize_record_stats(self) -> None:
        """Initialize parsing statistics for all records."""
        for record in self.config["records"]:
            record_name = record["name"]
            if record_name not in self.record_stats:
                self.record_stats[record_name] = ParsingStats()

    def build_field_defs(self, record: dict) -> List[FieldDef]:
        """Build FieldDef objects for validation (excluding computed fields).
        
        Args:
            record: Record configuration dict
            
        Returns:
            List of FieldDef objects for non-computed fields
        """
        field_defs = []
        for fld in record.get("fields", []):
            # Skip computed fields - they are generated and shouldn't be validated
            if fld.get("type") == "computed":
                continue
            field_defs.append(FieldDef(
                name=fld["name"],
                type=fld.get("type", "string"),
                nullable=fld.get("nullable", True),
                regex=fld.get("regex"),
                min_value=fld.get("min_value"),
                max_value=fld.get("max_value")
            ))
        return field_defs

    def get_columns(self, record: dict) -> List[str]:
        """Extract column names from record configuration.
        
        Args:
            record: Record configuration dict
            
        Returns:
            Ordered list of column names (context + fields)
        """
        context_cols = [c["name"] for c in record.get("context", [])]
        field_cols = [f["name"] for f in record.get("fields", [])]
        return list(dict.fromkeys(context_cols + field_cols))

    def extract_context(self, record: dict, node=None) -> Dict[str, any]:
        """Extract context variables from record configuration.
        
        Args:
            record: Record configuration dict
            node: Optional data node for extraction (format-specific)
            
        Returns:
            Dictionary of context variable names to values
        """
        context_data = {}
        for ctx in record.get("context", []):
            if "value" in ctx:
                # Static value
                context_data[ctx["name"]] = ctx["value"]
            elif ctx.get("from") and node is not None:
                # Dynamic value - to be implemented by subclass
                pass
        return context_data

    def process_computed_fields(self, row: Dict[str, any]) -> Dict[str, any]:
        """Process computed fields by evaluating formulas.
        
        Args:
            row: Row data dict
            
        Returns:
            Updated row with computed field values
        """
        for field_name, field_config in self.computed_fields.items():
            if field_name in row:
                formula = field_config.get("formula", "")
                if formula:
                    computed_value = format_formula(formula, row)
                    field_type = field_config.get("type", "string")
                    row[field_name] = cast_value(computed_value, field_type, self.safe_mode)
        return row

    def validate_and_write_row(self, record_name: str, row: Dict[str, any],
                               columns: List[str], field_defs: List[FieldDef],
                               row_num: Optional[int] = None) -> bool:
        """Validate row data and write to output or rejected file.
        
        Args:
            record_name: Name of the record type
            row: Row data dict
            columns: Column names
            field_defs: Field definitions for validation
            row_num: Optional row number for logging
            
        Returns:
            True if row was valid and written, False if rejected
        """
        # Validate fields
        validation_errors = []
        for field_def in field_defs:
            value = row.get(field_def.name)
            is_valid, error_msg = validate_field_value(value, field_def)
            if not is_valid:
                validation_errors.append(error_msg)
                self.record_stats[record_name].validation_errors += 1

        # Write row or reject it
        if validation_errors:
            self.record_stats[record_name].failed_rows += 1
            if self.writer:
                error_summary = "; ".join(validation_errors)
                self.writer.write_rejected_row(record_name, row, error_summary, columns)
            return False
        else:
            self.record_stats[record_name].success_rows += 1
            if self.writer:
                self.writer.write_row(record_name, row, columns)

            # Update stats counter
            self.stats[record_name] = self.stats.get(record_name, 0) + 1
            return True

    def log_progress(self, record_name: str, row_num: int, total_processed: int) -> None:
        """Log parsing progress at intervals.
        
        Args:
            record_name: Name of the record being processed
            row_num: Current row number
            total_processed: Total rows processed so far
        """
        if self.progress_interval > 0 and row_num % self.progress_interval == 0:
            logger.info(f"[{record_name}] Processed {row_num:,} rows ({total_processed:,} total)")

    def handle_row_error(self, record_name: str, error: Exception,
                        row_num: Optional[int] = None) -> None:
        """Handle row-level parsing errors based on continueOnError setting.
        
        Args:
            record_name: Name of the record being processed
            error: Exception that occurred
            row_num: Optional row number for logging
            
        Raises:
            Exception: Re-raises error if continueOnError is False
        """
        row_info = f" at row {row_num}" if row_num else ""
        logger.error(f"Error processing {record_name}{row_info}: {error}")
        self.record_stats[record_name].skipped_rows += 1

        if not self.continue_on_error:
            raise

    def handle_file_error(self, error: Exception) -> Tuple[bool, Optional[str]]:
        """Handle file-level parsing errors based on ignoreBrokenFiles setting.
        
        Args:
            error: Exception that occurred
            
        Returns:
            Tuple of (success, error_message)
            - If ignoreBrokenFiles=True: (False, error_message)
            - If ignoreBrokenFiles=False: raises the error
            
        Raises:
            Exception: Re-raises error if ignoreBrokenFiles is False
        """
        error_msg = str(error)

        if self.ignore_broken:
            logger.error(f"File parsing failed: {error_msg} (continuing due to ignoreBrokenFiles)")
            # Track file-level failures in stats
            for record_name in self.record_stats:
                self.record_stats[record_name].file_parse_failures += 1
            return False, error_msg
        else:
            # Re-raise the error
            raise

    def finalize_stats(self) -> None:
        """Finalize parsing statistics with end time."""
        import time
        for record_name, pstats in self.record_stats.items():
            if pstats.end_time is None:
                pstats.end_time = time.time()
