"""
Utility functions for the multi-format parser.

DEPRECATED: This module is maintained for backward compatibility only.
All internal code has migrated to specialized modules.

External code should import directly from:
- casting.py: safe_text, cast_value
- xpath_utils.py: normalize_xpath
- json_utils.py: extract_json_path, select_json_records
- formula_utils.py: format_formula

This module will be removed in version 2.0.0.
"""

# Re-export functions from specialized modules for backward compatibility
from multi_format_parser.casting import cast_value, safe_text
from multi_format_parser.formula_utils import format_formula
from multi_format_parser.json_utils import extract_json_path, select_json_records
from multi_format_parser.xpath_utils import normalize_xpath

__all__ = [
    'safe_text',
    'cast_value',
    'format_formula',
    'normalize_xpath',
    'extract_json_path',
    'select_json_records',
]
