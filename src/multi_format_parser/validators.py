"""
Validation functions for configuration and field values.
"""

import re
from collections import Counter
from typing import Any, List, Optional, Tuple

from multi_format_parser.models import FieldDef


def validate_field_value(value: Any, field: FieldDef) -> Tuple[bool, Optional[str]]:
    """Validate field value against field definition.

    Returns:
        Tuple of (is_valid, error_message)
    """
    # Handle None/null values
    if value is None or value == '':
        if not field.nullable:
            return False, f"Field '{field.name}' cannot be null"
        return True, None

    # Regex validation (only for string values)
    if field.regex and isinstance(value, str):
        if not re.fullmatch(field.regex, value):
            return False, f"Field '{field.name}' failed regex validation: {field.regex}"

    # Numeric range validation
    if field.min_value is not None or field.max_value is not None:
        try:
            num_val = float(value)
            if field.min_value is not None and num_val < field.min_value:
                return False, f"Field '{field.name}' value {num_val} below minimum {field.min_value}"
            if field.max_value is not None and num_val > field.max_value:
                return False, f"Field '{field.name}' value {num_val} above maximum {field.max_value}"
        except (ValueError, TypeError):
            return False, f"Field '{field.name}' cannot be converted to number for range validation"

    return True, None


def validate_config(config: dict) -> List[str]:
    """Validate configuration structure.

    Returns:
        List of validation errors (empty if valid)
    """
    errors = []

    if "format_type" not in config:
        errors.append("Missing required field: 'format_type'")
    elif config["format_type"] not in ["xml", "csv", "fixed_width", "json"]:
        errors.append(f"Invalid format_type: {config['format_type']}")

    format_type = config.get("format_type")

    if "records" not in config:
        errors.append("Missing required field: 'records'")
    elif not isinstance(config["records"], list) or not config["records"]:
        errors.append("'records' must be a non-empty list")
    else:
        computed_field_names = set()
        for comp in config.get("computed_fields", []):
            if "name" in comp:
                computed_field_names.add(comp["name"])

        # Validate each record
        for idx, record in enumerate(config["records"]):
            record_name = record.get("name", f"<unnamed-{idx}>")

            if "name" not in record:
                errors.append(f"Record {idx}: missing 'name' field")

            if format_type == "xml":
                if "select" not in record or not record["select"]:
                    errors.append(f"Record '{record_name}': XML records must have a non-empty 'select' field")
            elif format_type == "json":
                if "select" not in record or not record["select"]:
                    errors.append(f"Record '{record_name}': JSON records must have a non-empty 'select' field")

            if "fields" not in record:
                errors.append(f"Record {idx}: missing 'fields' field")
            elif not isinstance(record["fields"], list):
                errors.append(f"Record {idx}: 'fields' must be a list")
            else:
                field_names = []
                for fld in record["fields"]:
                    if "name" in fld:
                        field_names.append(fld["name"])

                counts = Counter(field_names)
                duplicates = [name for name, count in counts.items() if count > 1]
                if duplicates:
                    errors.append(f"Record '{record_name}': duplicate field names: {', '.join(sorted(duplicates))}")

                for field_idx, fld in enumerate(record["fields"]):
                    field_name = fld.get("name", f"<unnamed-{field_idx}>")
                    field_type = fld.get("type", "string")
                    is_computed = field_type == "computed"

                    if is_computed:
                        computed_ref = fld.get("computed_field")
                        if not computed_ref:
                            errors.append(f"Record '{record_name}', field '{field_name}': computed field missing 'computed_field' reference")
                        elif computed_ref not in computed_field_names:
                            errors.append(f"Record '{record_name}', field '{field_name}': computed_field '{computed_ref}' not defined in 'computed_fields'")
                    else:
                        if format_type == "fixed_width":
                            has_start = "start" in fld
                            has_width = "width" in fld
                            has_end = "end" in fld

                            if not has_start:
                                errors.append(f"Record '{record_name}', field '{field_name}': fixed-width field missing 'start'")
                            elif not isinstance(fld["start"], int) or fld["start"] < 0:
                                errors.append(f"Record '{record_name}', field '{field_name}': 'start' must be a non-negative integer")

                            if has_width and has_end:
                                errors.append(f"Record '{record_name}', field '{field_name}': cannot specify both 'width' and 'end'")
                            elif not has_width and not has_end:
                                errors.append(f"Record '{record_name}', field '{field_name}': must specify either 'width' or 'end'")

                            if has_width:
                                if not isinstance(fld["width"], int) or fld["width"] <= 0:
                                    errors.append(f"Record '{record_name}', field '{field_name}': 'width' must be a positive integer")

                            if has_end:
                                if not isinstance(fld["end"], int) or fld["end"] < 0:
                                    errors.append(f"Record '{record_name}', field '{field_name}': 'end' must be a non-negative integer")
                                if has_start and isinstance(fld["start"], int) and isinstance(fld["end"], int):
                                    if fld["end"] <= fld["start"]:
                                        errors.append(f"Record '{record_name}', field '{field_name}': 'end' must be greater than 'start'")
                        else:
                            if "path" not in fld or not fld["path"]:
                                errors.append(f"Record '{record_name}', field '{field_name}': non-computed field missing 'path'")

                            if format_type == "csv" and not config.get("csv_has_header", True):
                                path_val = fld.get("path")
                                if path_val is not None:
                                    if not isinstance(path_val, int):
                                        try:
                                            int(str(path_val))
                                        except (ValueError, TypeError):
                                            errors.append(f"Record '{record_name}', field '{field_name}': CSV without headers requires 'path' to be an integer index, got '{path_val}'")

                    if "regex" in fld and fld["regex"]:
                        try:
                            re.compile(fld["regex"])
                        except re.error as e:
                            errors.append(f"Record '{record_name}', field '{field_name}': invalid regex pattern '{fld['regex']}': {e}")

    return errors
