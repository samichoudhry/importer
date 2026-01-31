"""
JSON path extraction utilities.

This module provides functions for extracting values from JSON data structures
using dot notation paths and selector expressions.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


def extract_json_path(data: Any, path: str) -> Any:
    """Extract value from JSON data using dot notation path.

    Supports:
    - Dot notation: "user.name"
    - Array indexing: "items[0]", "items[0].price"
    - Nested paths: "user.address.city"

    Args:
        data: JSON data (dict, list, or primitive)
        path: Dot-notation path string

    Returns:
        Extracted value or None if path doesn't exist
    """
    if not path:
        return data

    if data is None:
        return None

    # Validate data is a container type before attempting path traversal
    if not isinstance(data, (dict, list)):
        # Primitive value - can't traverse path
        logger.debug(f"Cannot extract path '{path}' from primitive type {type(data).__name__}")
        return None

    try:
        parts = path.split('.')
        current = data

        for part_idx, part in enumerate(parts):
            if '[' in part and part.endswith(']'):
                # Handle array indexing: "items[0]"
                try:
                    bracket_pos = part.index('[')
                    key = part[:bracket_pos]
                    index_str = part[bracket_pos + 1:-1]
                except ValueError:
                    return None

                if key:
                    if not isinstance(current, dict):
                        return None
                    current = current.get(key)
                    if current is None:
                        return None

                # Validate current is a list before indexing
                if not isinstance(current, list):
                    logger.debug(f"Expected list for array indexing at '{part}' but got {type(current).__name__}")
                    return None

                try:
                    index = int(index_str)
                    # Handle negative indices safely
                    if index < 0:
                        logger.debug(f"Negative array index {index} not supported in path '{path}'")
                        return None
                    if index < len(current):
                        current = current[index]
                    else:
                        return None
                except (ValueError, TypeError):
                    return None
            else:
                if isinstance(current, dict):
                    current = current.get(part)
                elif isinstance(current, list):
                    try:
                        idx = int(part)
                        if idx < 0:
                            logger.debug(f"Negative array index {idx} not supported")
                            return None
                        if idx < len(current):
                            current = current[idx]
                        else:
                            return None
                    except (ValueError, TypeError):
                        return None
                else:
                    # Trying to traverse non-container type
                    remaining_path = '.'.join(parts[part_idx:])
                    logger.debug(f"Cannot traverse path '{remaining_path}' through {type(current).__name__}")
                    return None

            if current is None:
                return None

        return current
    except (AttributeError, KeyError, IndexError, TypeError) as e:
        logger.debug(f"Error extracting JSON path '{path}': {e}")
        return None


def select_json_records(data: Any, selector: str) -> list:
    """Select records from JSON data using a selector expression.

    Supports:
    - Root: "$" or "" (returns root as list)
    - Root array: "$" (if root is array, returns as-is)
    - Array element: "$[0]", "$[1:3]" (slice notation)
    - Object key: "$.key"
    - Nested path: "$.users", "$.users.items"
    - Direct path: "users" (without $, relative to root)

    Args:
        data: Root JSON data
        selector: Selector expression

    Returns:
        List of selected records (always returns a list for consistency)
    """
    # Handle None data
    if data is None:
        logger.warning("Cannot select records from None/null JSON data")
        return []

    # Handle root selector
    if not selector or selector == "$" or selector == "":
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            return [data]
        else:
            # Primitive value at root
            logger.warning(f"Root selector on primitive type {type(data).__name__}, returning as single-element list")
            return [data]

    selector = selector.lstrip('$').lstrip('.')

    if not selector:
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            return [data]
        else:
            return [data]

    # Handle direct array indexing on root
    if selector.startswith('['):
        if ']' not in selector:
            logger.warning(f"Malformed array selector: missing closing bracket in '{selector}'")
            return []

        if not isinstance(data, list):
            logger.warning(f"Array selector '{selector}' used on non-array type {type(data).__name__}")
            return []

        try:
            index_part = selector[1:selector.index(']')]
        except ValueError:
            logger.warning(f"Malformed array selector: '{selector}'")
            return []

        if ':' in index_part:
            # Slice notation: [1:3]
            try:
                parts = index_part.split(':')
                start = int(parts[0]) if parts[0] else 0
                end = int(parts[1]) if len(parts) > 1 and parts[1] else len(data)

                # Validate slice bounds
                if start < 0 or end < 0:
                    logger.warning(f"Negative indices not supported in slice '{index_part}'")
                    return []

                return data[start:end]
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid slice syntax in '{index_part}': {e}")
                return []
        else:
            try:
                idx = int(index_part)
                if idx < 0:
                    logger.warning(f"Negative index {idx} not supported")
                    return []
                if idx < len(data):
                    return [data[idx]]
                logger.warning(f"Array index {idx} out of bounds (length {len(data)})")
                return []
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid array index '{index_part}': {e}")
                return []

    # Use extract_json_path for complex paths
    result = extract_json_path(data, selector)

    if result is None:
        return []

    if isinstance(result, list):
        return result

    # Wrap non-list results in a list for consistency
    return [result]
