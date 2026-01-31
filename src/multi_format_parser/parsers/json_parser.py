"""
JSON parser module.
"""

import json
import logging
from pathlib import Path
from typing import Dict, Optional, Tuple

from multi_format_parser.casting import cast_value
from multi_format_parser.csv_writer import CSVWriter
from multi_format_parser.formula_utils import format_formula
from multi_format_parser.json_utils import extract_json_path, select_json_records
from multi_format_parser.models import ParsingStats
from multi_format_parser.parsers.base_parser import BaseParser

# Optional JSON Schema validation support
try:
    import jsonschema
    from jsonschema import Draft7Validator, ValidationError
    HAS_JSONSCHEMA = True
except ImportError:
    HAS_JSONSCHEMA = False

logger = logging.getLogger(__name__)


def validate_json_schema(data: dict, schema: dict) -> Tuple[bool, Optional[str]]:
    """Validate JSON data against a JSON Schema.
    
    Args:
        data: JSON data to validate
        schema: JSON Schema definition
        
    Returns:
        Tuple of (is_valid, error_message)
        
    Raises:
        ImportError: If jsonschema library is not installed
    """
    if not HAS_JSONSCHEMA:
        raise ImportError(
            "jsonschema library is required for JSON schema validation. "
            "Install with: pip install jsonschema"
        )
    
    try:
        # Use Draft7Validator for better error messages
        validator = Draft7Validator(schema)
        errors = list(validator.iter_errors(data))
        
        if errors:
            # Format validation errors
            error_messages = []
            for error in errors[:5]:  # Limit to first 5 errors
                path = ".".join(str(p) for p in error.path) if error.path else "root"
                error_messages.append(f"{path}: {error.message}")
            
            error_summary = "; ".join(error_messages)
            if len(errors) > 5:
                error_summary += f" (and {len(errors) - 5} more errors)"
                
            return False, error_summary
        
        return True, None
        
    except jsonschema.SchemaError as e:
        return False, f"Invalid JSON schema: {e.message}"
    except Exception as e:
        return False, f"Unexpected validation error: {str(e)}"



def parse_json(json_path: Path, config: dict, writer: Optional[CSVWriter], stats: dict, record_stats: Dict[str, ParsingStats]) -> Tuple[bool, Optional[str]]:
    """Parse JSON file.
    
    Supports optional JSON Schema validation via config.json_schema or config.json_schema_path.
    
    Args:
        json_path: Path to JSON file
        config: Parser configuration (may include json_schema or json_schema_path)
        writer: Optional CSV writer
        stats: Statistics dictionary
        record_stats: Per-record statistics
    
    Returns:
        Tuple[bool, Optional[str]]: (success, error_message)
    """
    parser_obj = BaseParser(json_path, config, writer, stats, record_stats)
    
    try:
        encoding = config.get("json_encoding", "utf-8")

        try:
            with open(json_path, 'r', encoding=encoding) as f:
                try:
                    root_data = json.load(f)
                except json.JSONDecodeError as e:
                    raise ValueError(f"Invalid JSON in file {json_path}: {e}")
        except FileNotFoundError:
            raise FileNotFoundError(f"JSON file not found: {json_path}")
        except PermissionError:
            raise PermissionError(f"Permission denied reading JSON file: {json_path}")

        # Optional JSON Schema validation
        json_schema = None
        if "json_schema" in config:
            json_schema = config["json_schema"]
        elif "json_schema_path" in config:
            schema_path = Path(config["json_schema_path"])
            if not schema_path.is_absolute():
                # Resolve relative to config file location
                config_dir = json_path.parent
                schema_path = config_dir / schema_path
            
            try:
                with open(schema_path, 'r', encoding='utf-8') as f:
                    json_schema = json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load JSON schema from {schema_path}: {e}")
        
        if json_schema:
            logger.info(f"Validating JSON against schema")
            is_valid, error_msg = validate_json_schema(root_data, json_schema)
            
            if not is_valid:
                raise ValueError(f"JSON schema validation failed: {error_msg}")
            
            logger.info("JSON schema validation passed")


        for record in config["records"]:
            select_expr = record.get("select", "")

            # Validate root data type before selection
            if not isinstance(root_data, (dict, list, str, int, float, bool, type(None))):
                logger.error(f"Unexpected root data type {type(root_data).__name__} for record '{record['name']}'")
                continue

            try:
                records = select_json_records(root_data, select_expr)
            except Exception as e:
                logger.error(f"Error selecting records for '{record['name']}' with selector '{select_expr}': {e}")
                continue

            if not records:
                logger.warning(f"No records found for '{record['name']}' with selector '{select_expr}'")
                continue

            # Validate records is actually a list
            if not isinstance(records, list):
                logger.error(f"select_json_records returned {type(records).__name__} instead of list for '{record['name']}'")
                continue

            logger.debug(f"Found {len(records)} record(s) for '{record['name']}' with selector '{select_expr}'")

            columns = parser_obj.get_columns(record)
            field_defs = parser_obj.build_field_defs(record)

            record_idx = 0
            for record_data in records:
                record_idx += 1
                
                # Log progress periodically
                parser_obj.log_progress(record["name"], record_idx, record_idx)
                
                if record_data is None:
                    continue

                # Wrap row processing in try-except if continueOnError is enabled
                try:
                    row = {}

                    # Extract context
                    for ctx in record.get("context", []):
                        if ctx.get("value") is not None:
                            row[ctx["name"]] = ctx["value"]
                        elif ctx.get("from") or ctx.get("from_expr"):
                            expr_raw = ctx.get("from") or ctx.get("from_expr")
                            if expr_raw.startswith("$"):
                                # Root-relative path
                                path = expr_raw[1:].lstrip('.')  # Remove $ and leading dot
                                val = extract_json_path(root_data, path) if path else root_data
                            else:
                                # Record-relative path
                                val = extract_json_path(record_data, expr_raw)
                            row[ctx["name"]] = cast_value(val, "string", parser_obj.safe_mode)
                        else:
                            row[ctx["name"]] = None

                    # Extract fields
                    for fld in record.get("fields", []):
                        if fld.get("type") == "computed":
                            row[fld["name"]] = None
                            continue

                        if not fld.get("path"):
                            logger.debug(f"Field '{fld['name']}' in record '{record['name']}' has no path configured")
                            row[fld["name"]] = None
                            continue

                        path = fld["path"]
                        if path.startswith("$"):
                            # Root-relative path
                            clean_path = path[1:].lstrip('.')  # Remove $ and leading dot
                            val = extract_json_path(root_data, clean_path) if clean_path else root_data
                        else:
                            # Record-relative path
                            val = extract_json_path(record_data, path)

                        # Log when path extraction fails for non-nullable fields
                        if val is None and not fld.get("nullable", True):
                            logger.debug(f"Field '{fld['name']}' (non-nullable) extracted None from path '{path}' in record '{record['name']}'")

                        if fld.get("type", "").lower() == "json" and val is not None:
                            row[fld["name"]] = json.dumps(val)
                        else:
                            row[fld["name"]] = cast_value(val, fld.get("type", "string"), parser_obj.safe_mode)

                    # Compute fields
                    for fld in record.get("fields", []):
                        if fld.get("type") == "computed" and fld.get("computed_field"):
                            comp = parser_obj.computed_fields.get(fld["computed_field"])
                            if comp:
                                formula = comp.get("formula", "")
                                row[fld["name"]] = format_formula(formula, row) if formula else None
                            else:
                                logger.warning(f"Computed field '{fld['computed_field']}' referenced but not defined in computed_fields")
                                row[fld["name"]] = None

                    # Validate row
                    record_name = record["name"]
                    record_stats[record_name].total_rows += 1
                    parser_obj.validate_and_write_row(record_name, row, columns, field_defs, record_idx)
                
                except Exception as row_error:
                    # Handle row-level errors if continueOnError is enabled
                    if parser_obj.continue_on_error:
                        logger.warning(f"Row processing error in {record['name']} (continuing): {type(row_error).__name__}: {row_error}")
                        record_name = record["name"]
                        if record_name not in record_stats:
                            record_stats[record_name] = ParsingStats()
                        record_stats[record_name].skipped_rows += 1
                        continue
                    else:
                        # Re-raise if continueOnError is not enabled
                        raise

        parser_obj.finalize_stats()
        return (True, None)
    
    except (FileNotFoundError, PermissionError, UnicodeDecodeError, Exception) as e:
        return parser_obj.handle_file_error(e)
