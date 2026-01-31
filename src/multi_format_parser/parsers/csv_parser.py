"""
CSV parser module.
"""

import csv
import logging
from pathlib import Path
from typing import Dict, Optional, Tuple

from multi_format_parser.casting import cast_value
from multi_format_parser.csv_writer import CSVWriter
from multi_format_parser.formula_utils import format_formula
from multi_format_parser.models import ParsingStats
from multi_format_parser.parsers.base_parser import BaseParser

logger = logging.getLogger(__name__)


def parse_csv(csv_path: Path, config: dict, writer: Optional[CSVWriter], stats: dict, record_stats: Dict[str, ParsingStats]) -> Tuple[bool, Optional[str]]:
    """Parse CSV file.
    
    Returns:
        Tuple[bool, Optional[str]]: (success, error_message)
    """
    parser_obj = BaseParser(csv_path, config, writer, stats, record_stats)
    
    try:
        delimiter = config.get("csv_delimiter", ",")
        quotechar = config.get("csv_quotechar", '"')
        escapechar = config.get("csv_escapechar")
        doublequote = config.get("csv_doublequote", True)
        has_header = config.get("csv_has_header", True)
        skip_rows = config.get("csv_skip_rows", 0)
        encoding = config.get("csv_encoding", "utf-8")

        # Pre-build field mappings for all records
        record_field_maps = []

        with open(csv_path, 'r', encoding=encoding, newline='') as f:
            for _ in range(skip_rows):
                next(f, None)

            reader_args = {'delimiter': delimiter, 'quotechar': quotechar, 'doublequote': doublequote}
            if escapechar is not None:
                reader_args['escapechar'] = escapechar
                reader_args['doublequote'] = False

            reader = csv.reader(f, **reader_args)

            header = None
            header_idx = {}
            if has_header:
                header = next(reader, None)
                if header:
                    header_idx = {name: i for i, name in enumerate(header)}

            for record in config["records"]:
                field_map = {}
                for fld in record.get("fields", []):
                    if fld.get("type") == "computed" or not fld.get("path"):
                        if not fld.get("path") and fld.get("type") != "computed":
                            logger.debug(f"Field '{fld['name']}' in record '{record['name']}' has no path configured")
                        continue

                    if header:
                        field_map[fld["name"]] = header_idx.get(fld["path"])
                    else:
                        try:
                            field_map[fld["name"]] = int(fld["path"])
                        except ValueError:
                            field_map[fld["name"]] = None

                columns = parser_obj.get_columns(record)
                field_defs = parser_obj.build_field_defs(record)

                record_field_maps.append({
                    "record": record,
                    "field_map": field_map,
                    "columns": columns,
                    "field_defs": field_defs
                })

            row_num = 0
            for csv_row in reader:
                row_num += 1
                
                # Log progress periodically
                parser_obj.log_progress("CSV", row_num, row_num)

                # Skip completely empty rows
                if not csv_row:
                    logger.debug(f"Skipping empty row at line {row_num}")
                    continue

                # Skip rows where all cells are empty/whitespace
                if all(not cell or cell.strip() == '' for cell in csv_row):
                    logger.debug(f"Skipping blank row at line {row_num}")
                    continue

                # Process row with first matching record type only
                # (prevents duplicate processing when multiple records are configured)
                row_processed = False
                for record_info in record_field_maps:
                    record = record_info["record"]
                    field_map = record_info["field_map"]
                    columns = record_info["columns"]
                    field_defs = record_info["field_defs"]

                    # Wrap row processing in try-except if continueOnError is enabled
                    try:
                        row = {}

                        # Extract context
                        for ctx in record.get("context", []):
                            if ctx.get("value") is not None:
                                row[ctx["name"]] = ctx["value"]
                            elif ctx.get("from") or ctx.get("from_expr"):
                                # Context extraction from CSV columns
                                path_key = ctx.get("from") or ctx.get("from_expr")
                                if header:
                                    col_idx = header_idx.get(path_key)
                                else:
                                    try:
                                        col_idx = int(path_key)
                                    except ValueError:
                                        col_idx = None
                                
                                if col_idx is not None and 0 <= col_idx < len(csv_row):
                                    cell = csv_row[col_idx].strip()
                                    row[ctx["name"]] = cast_value(cell, "string", parser_obj.safe_mode)
                                else:
                                    row[ctx["name"]] = None
                            else:
                                row[ctx["name"]] = None

                        # Extract fields
                        for fld in record.get("fields", []):
                            if fld.get("type") == "computed":
                                row[fld["name"]] = None
                                continue

                            col_idx = field_map.get(fld["name"])
                            if col_idx is not None and 0 <= col_idx < len(csv_row):
                                cell = csv_row[col_idx].strip()
                                row[fld["name"]] = cast_value(cell, fld.get("type", "string"), parser_obj.safe_mode)
                            else:
                                row[fld["name"]] = None

                        for fld in record.get("fields", []):
                            if fld.get("type") == "computed" and fld.get("computed_field"):
                                comp = parser_obj.computed_fields.get(fld["computed_field"])
                                if comp:
                                    formula = comp.get("formula", "")
                                    row[fld["name"]] = format_formula(formula, row) if formula else None
                                else:
                                    logger.warning(f"Computed field '{fld['computed_field']}' referenced but not defined in computed_fields")
                                    row[fld["name"]] = None

                        record_name = record["name"]
                        record_stats[record_name].total_rows += 1
                        
                        # Validate and write row
                        parser_obj.validate_and_write_row(record_name, row, columns, field_defs, row_num)

                        # Mark row as processed and break to prevent duplicate processing
                        # If you need ALL records to process each row, remove this break
                        row_processed = True
                        break
                    
                    except Exception as row_error:
                        # Handle row-level errors using base parser
                        parser_obj.handle_row_error(record["name"], row_error, row_num)
                        break  # Skip to next row

        parser_obj.finalize_stats()
        return (True, None)
    
    except (FileNotFoundError, PermissionError, UnicodeDecodeError, Exception) as e:
        return parser_obj.handle_file_error(e)
