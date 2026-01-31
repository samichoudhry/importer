"""
Formula interpolation utilities.

This module provides functions for interpolating formula expressions
with values from data rows.
"""

import hashlib
import re
from decimal import Decimal


def format_formula(formula: str, row: dict) -> str:
    """Interpolate formula placeholders with row values.
    
    Supports:
    - Simple interpolation: "{field_name}" -> value from row
    - MD5 hashing: "hash_md5({field1}{field2})" -> MD5 hash of concatenated values
    
    Args:
        formula: Formula expression with placeholders
        row: Dictionary of field values
        
    Returns:
        Interpolated formula result
    """
    if not formula:
        return ""

    hash_match = re.match(r'^hash_md5\((.+)\)$', formula.strip())

    def replace_placeholder(match):
        key = match.group(1)
        value = row.get(key)
        if value is None:
            return ""
        if isinstance(value, Decimal):
            return format(value, "f")
        return str(value)

    if hash_match:
        inner = hash_match.group(1)
        interpolated = re.sub(r"\{([^}]+)\}", replace_placeholder, inner)
        return hashlib.md5(interpolated.encode('utf-8')).hexdigest()
    else:
        return re.sub(r"\{([^}]+)\}", replace_placeholder, formula)
