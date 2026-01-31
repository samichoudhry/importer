"""
Fixed-width file parser module.
"""

import logging
from pathlib import Path
from typing import Dict, Optional, Tuple

from multi_format_parser.casting import cast_value
from multi_format_parser.csv_writer import CSVWriter
from multi_format_parser.formula_utils import format_formula
from multi_format_parser.models import ParsingStats
from multi_format_parser.parsers.base_parser import BaseParser

logger = logging.getLogger(__name__)


def parse_fixed_width(file_path: Path, config: dict, writer: Optional[CSVWriter], stats: dict, record_stats: Dict[str, ParsingStats]) -> Tuple[bool, Optional[str]]:
    """Parse fixed-width file.
    
    Returns:
        Tuple[bool, Optional[str]]: (success, error_message)
    """
    parser_obj = BaseParser(file_path, config, writer, stats, record_stats)
    
    try:
        encoding = config.get("fixed_width_encoding", "utf-8")
        skip_rows = config.get("fixed_width_skip_rows", 0)

        record_specs = []
        for record in config["records"]:
            # Build record type identifier if present
            record_type_field = record.get("record_type_field")
            record_type_value = record.get("record_type_value")

            field_specs = {}
            for fld in record.get("fields", []):
                if fld.get("type") == "computed":
                    continue
                start = fld.get("start")
                end = fld.get("end")
                width = fld.get("width")

                if start is None:
                    logger.warning(f"Field '{fld['name']}' in record '{record['name']}' has no start position defined")
                    continue

                # Calculate end from width if not explicitly provided
                if end is None and width is not None:
                    end = start + width

                # Validation: end must be set and greater than start
                if end is None:
                    logger.warning(f"Field '{fld['name']}' in record '{record['name']}' has no end or width defined")
                    continue

                if end <= start:
                    logger.warning(f"Field '{fld['name']}' in record '{record['name']}' has end ({end}) <= start ({start})")
                    continue

                field_specs[fld["name"]] = (start, end)

            columns = parser_obj.get_columns(record)
            field_defs = parser_obj.build_field_defs(record)

            record_specs.append({
                "record": record,
                "field_specs": field_specs,
                "columns": columns,
                "field_defs": field_defs,
                "record_type_field": record_type_field,
                "record_type_value": record_type_value
            })

        with open(file_path, 'r', encoding=encoding) as f:
            for _ in range(skip_rows):
                next(f, None)

            for line_num, line in enumerate(f, start=1):
                # Log progress periodically
                parser_obj.log_progress("Fixed-Width", line_num, line_num)
                
                line = line.rstrip('\n\r')
                if not line.strip():
                    continue

                # Process line - check which record type(s) match
                matched_records = []
                for record_info in record_specs:
                    record = record_info["record"]
                    record_type_field = record_info.get("record_type_field")
                    record_type_value = record_info.get("record_type_value")

                    # If record type identification is configured, check if line matches
                    if record_type_field is not None and record_type_value is not None:
                        type_spec = record_info["field_specs"].get(record_type_field)
                        if type_spec:
                            start, end = type_spec
                            # Validate line is long enough for record type field
                            if start >= len(line):
                                logger.debug(f"Line {line_num}: Too short ({len(line)} chars) for record type field at position {start}")
                                continue  # Line too short for this record type

                            # Extract record type value with safe boundary
                            actual_end = min(end, len(line))
                            actual_value = line[start:actual_end].strip()

                            # Check if record type matches
                            if actual_value != str(record_type_value):
                                continue  # Skip this record type
                        else:
                            logger.warning(f"Record type field '{record_type_field}' not found in field specs for record '{record['name']}'")
                            continue

                    matched_records.append(record_info)

                # If no record type identification is configured, process all records (backward compatibility)
                if not matched_records:
                    if any(r.get("record_type_field") is not None for r in record_specs):
                        # Record type identification is configured but no match found
                        logger.debug(f"Line {line_num}: No matching record type found")
                        continue
                    else:
                        # No record type identification configured - use all records
                        matched_records = record_specs

                for record_info in matched_records:
                    record = record_info["record"]
                    field_specs = record_info["field_specs"]
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
                                # Context extraction from fixed-width positions
                                # Use field_specs to find the position for the context field
                                path_key = ctx.get("from") or ctx.get("from_expr")
                                spec = field_specs.get(path_key)
                                if spec:
                                    start, end = spec
                                    if start < len(line):
                                        actual_end = min(end, len(line))
                                        val = line[start:actual_end].strip()
                                        row[ctx["name"]] = cast_value(val, "string", parser_obj.safe_mode)
                                    else:
                                        row[ctx["name"]] = None
                                else:
                                    row[ctx["name"]] = None
                            else:
                                row[ctx["name"]] = None

                        # Extract fields
                        for fld in record.get("fields", []):
                            if fld.get("type") == "computed":
                                row[fld["name"]] = None
                                continue

                            spec = field_specs.get(fld["name"])
                            if spec:
                                start, end = spec

                                # Check if start position is within line bounds
                                if start >= len(line):
                                    # Line is too short to contain this field
                                    if not fld.get("nullable", True):
                                        logger.warning(f"Line {line_num}: Field '{fld['name']}' start position {start} exceeds line length {len(line)} (non-nullable)")
                                    val = None
                                else:
                                    # Clamp end to line length to prevent over-reading
                                    actual_end = min(end, len(line))

                                    # Warn if field is truncated
                                    if actual_end < end:
                                        logger.debug(f"Line {line_num}: Field '{fld['name']}' truncated (expected end {end}, line length {len(line)})")

                                    # Extract field value
                                    val = line[start:actual_end].strip()

                                    # Treat empty strings as None if field is nullable
                                    if not val and fld.get("nullable", True):
                                        val = None

                                row[fld["name"]] = cast_value(val, fld.get("type", "string"), parser_obj.safe_mode)
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
                        parser_obj.validate_and_write_row(record_name, row, columns, field_defs, line_num)
                    
                    except Exception as row_error:
                        parser_obj.handle_row_error(record["name"], row_error, line_num)
                        break  # Skip to next line
        parser_obj.finalize_stats()
        return (True, None)
    
    except (FileNotFoundError, PermissionError, UnicodeDecodeError, Exception) as e:
        return parser_obj.handle_file_error(e)
