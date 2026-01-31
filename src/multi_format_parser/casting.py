"""
Type casting utilities for the multi-format parser.

This module provides functions for safely converting values between different types,
with support for common data types used in data parsing.
"""

import logging
import re
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

try:
    from lxml import etree
    HAS_LXML = True
except ImportError:
    HAS_LXML = False

logger = logging.getLogger(__name__)


def safe_text(value: Any) -> Optional[str]:
    """Convert value to string safely.
    
    Args:
        value: Any value to convert to string
        
    Returns:
        String value or None if value is None/empty
    """
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        s = str(value).strip()
        return s if s else None
    if HAS_LXML:
        try:
            if isinstance(value, etree._Element):
                text = value.text
                return text.strip() if text else None
        except Exception:
            pass
    return str(value).strip() or None


def cast_value(value: Any, typ: str, safe_mode: bool = True) -> Any:
    """Cast value to specified type.
    
    Supported types:
    - string: String value
    - int: Integer value
    - decimal/number: Decimal value
    - float: Python float (for scientific notation, less precision)
    - boolean/bool: Boolean value (true/false, yes/no, 1/0)
    - date: ISO date string (YYYY-MM-DD)
    - datetime: ISO datetime string
    
    Args:
        value: Value to cast
        typ: Target type name
        safe_mode: If True, return None on error; if False, raise exception
        
    Returns:
        Casted value or None (in safe mode)
        
    Raises:
        ValueError: When casting fails and safe_mode is False
    """
    if value is None:
        return None

    if isinstance(value, list):
        value = value[0] if value else None

    s = safe_text(value)
    if s is None:
        return None

    # Handle empty type or default to string
    if not typ:
        typ = "string"

    try:
        t = typ.lower()
        if t == "string":
            return s
        if t == "int":
            return int(Decimal(s))
        if t in ("decimal", "number"):
            return Decimal(s)
        if t == "float":
            return float(s)
        if t in ("boolean", "bool"):
            # Handle common boolean representations
            lower_s = s.lower()
            if lower_s in ("true", "yes", "1", "t", "y"):
                return True
            elif lower_s in ("false", "no", "0", "f", "n"):
                return False
            else:
                raise ValueError(f"Cannot convert '{s}' to boolean")
        if t == "date":
            # Basic ISO date validation (YYYY-MM-DD)
            if not re.match(r'^\d{4}-\d{2}-\d{2}$', s):
                raise ValueError(f"Invalid date format '{s}', expected YYYY-MM-DD")
            return s
        if t == "datetime":
            # Basic ISO datetime validation
            if not re.match(r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}', s):
                raise ValueError(f"Invalid datetime format '{s}', expected ISO format")
            return s
        # Unknown type - log warning and return as string
        logger.warning(f"Unknown type '{typ}', treating as string")
        return s
    except (ValueError, InvalidOperation) as e:
        if safe_mode:
            return None
        raise ValueError(f"Failed to cast '{s}' to {typ}: {e}")  # noqa: B904
