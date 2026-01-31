"""
XPath utilities for XML parsing.

This module provides helper functions for working with XPath expressions
in the multi-format parser, including XML to JSON conversion for variant fields.
"""

import json
import logging
import re
from typing import Any, List, Optional, Union

try:
    from lxml import etree
    HAS_LXML = True
except ImportError:
    HAS_LXML = False

try:
    import xmltodict
    HAS_XMLTODICT = True
except ImportError:
    HAS_XMLTODICT = False

logger = logging.getLogger(__name__)

# Configuration for JSON field conversion
MAX_JSON_FIELD_SIZE = 50000  # 50KB limit per JSON field (configurable)


def normalize_xpath(expr: str) -> str:
    """Fix common XPath typos and formatting.
    
    Common fixes:
    - *[1]/local-name() -> local-name(*[1])
    
    Args:
        expr: XPath expression to normalize
        
    Returns:
        Normalized XPath expression
    """
    if expr:
        expr = expr.strip()
        expr = re.sub(r"\*\[1\]/local-name\(\)", "local-name(*[1])", expr)
    return expr


def xml_element_to_json(
    element: Any,
    clean_namespaces: bool = True,
    force_list: bool = False
) -> str:
    """Convert XML element(s) to JSON string for variant fields.
    
    This function handles the conversion of XML elements to JSON format,
    suitable for storage in CSV/database JSON columns. It handles:
    - Single elements: Returns JSON object
    - Multiple elements: Returns JSON array
    - Namespace cleanup: Removes @xmlns:* attributes
    - Size limits: Warns if JSON exceeds threshold
    
    Args:
        element: lxml Element, list of Elements, or None
        clean_namespaces: Remove @xmlns:* attributes from output (default: True)
        force_list: Always return array even for single element (default: False)
        
    Returns:
        JSON string representation or None if element is None/empty
        
    Raises:
        ImportError: If xmltodict is not installed
        ValueError: If element type is unsupported
        
    Example:
        >>> elem = etree.fromstring('<Item><ID>123</ID><Name>Test</Name></Item>')
        >>> xml_element_to_json(elem)
        '{\"Item\": {\"ID\": \"123\", \"Name\": \"Test\"}}'
    """
    if not HAS_LXML:
        raise ImportError("lxml is required for XML to JSON conversion")
    
    if not HAS_XMLTODICT:
        raise ImportError(
            "xmltodict is required for JSON field type. Install: pip install xmltodict"
        )
    
    # Handle None/empty input
    if element is None:
        return None
    
    # Normalize to list
    if not isinstance(element, list):
        elements = [element]
    else:
        elements = element
    
    # Filter out non-Element types
    elements = [e for e in elements if isinstance(e, etree._Element)]
    
    if not elements:
        return None
    
    # Convert each element to dict
    result_dicts = []
    for elem in elements:
        try:
            # Convert XML to string
            xml_str = etree.tostring(elem, encoding='unicode')
            
            # Parse to dict using xmltodict
            parsed = xmltodict.parse(xml_str)
            
            # Clean namespaces if requested
            if clean_namespaces:
                parsed = _clean_namespaces_from_dict(parsed)
            
            result_dicts.append(parsed)
            
        except Exception as e:
            logger.warning(f"Failed to convert XML element to JSON: {e}")
            # Fallback: store element tag name
            result_dicts.append({elem.tag: "[conversion error]"})
    
    # Determine output format
    if len(result_dicts) == 1 and not force_list:
        result = result_dicts[0]
    else:
        result = result_dicts
    
    # Serialize to JSON
    try:
        json_str = json.dumps(result, ensure_ascii=False)
        
        # Check size and warn if large
        size = len(json_str)
        if size > MAX_JSON_FIELD_SIZE:
            logger.warning(
                f"JSON field size ({size} bytes) exceeds recommended limit "
                f"({MAX_JSON_FIELD_SIZE} bytes). This may cause issues with "
                f"CSV tools and databases. Consider restructuring data."
            )
        
        return json_str
        
    except (TypeError, ValueError) as e:
        logger.error(f"Failed to serialize to JSON: {e}")
        return None


def _clean_namespaces_from_dict(data: Any) -> Any:
    """Recursively remove @xmlns:* attributes from parsed XML dict.
    
    Args:
        data: Dictionary or list from xmltodict.parse()
        
    Returns:
        Cleaned dictionary/list with namespace attributes removed
    """
    if isinstance(data, dict):
        # Remove @xmlns:* keys
        cleaned = {}
        for key, value in data.items():
            if not (key.startswith('@xmlns') or key == '@xmlns'):
                cleaned[key] = _clean_namespaces_from_dict(value)
        return cleaned
    elif isinstance(data, list):
        return [_clean_namespaces_from_dict(item) for item in data]
    else:
        return data
