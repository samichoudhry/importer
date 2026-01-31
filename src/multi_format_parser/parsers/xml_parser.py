"""
XML parser module.
"""

import logging
from functools import lru_cache
from pathlib import Path
from typing import Dict, Optional, Tuple

try:
    from lxml import etree
    HAS_LXML = True
except ImportError:
    HAS_LXML = False

from multi_format_parser.casting import cast_value
from multi_format_parser.csv_writer import CSVWriter
from multi_format_parser.formula_utils import format_formula
from multi_format_parser.models import ParsingStats
from multi_format_parser.parsers.base_parser import BaseParser
from multi_format_parser.xpath_utils import normalize_xpath, xml_element_to_json

logger = logging.getLogger(__name__)


@lru_cache(maxsize=256)
def compile_xpath(expr: str, namespaces_tuple: tuple = ()) -> etree.XPath:
    """Compile and cache XPath expression for better performance.
    
    Uses LRU cache to store compiled expressions, providing significant
    speedup when the same XPath is used multiple times (e.g., for each
    record in a large XML file).
    
    Args:
        expr: XPath expression string
        namespaces_tuple: Tuple of (prefix, uri) tuples for namespace resolution.
                         Must be a tuple since dicts aren't hashable for caching.
        
    Returns:
        Compiled XPath object
        
    Raises:
        etree.XPathSyntaxError: If expression is invalid
    """
    try:
        # Convert tuple back to dict for lxml
        ns_dict = dict(namespaces_tuple) if namespaces_tuple else None
        return etree.XPath(expr, namespaces=ns_dict)
    except etree.XPathSyntaxError as e:
        logger.error(f"Invalid XPath expression '{expr}': {e}")
        raise


def clear_xpath_cache():
    """Clear the XPath compilation cache.
    
    Useful for testing or when processing many different XML schemas
    to prevent cache bloat.
    """
    compile_xpath.cache_clear()



def parse_xml(
    xml_path: Path,
    config: dict,
    writer: Optional[CSVWriter],
    stats: dict,
    record_stats: Dict[str, ParsingStats]
) -> Tuple[bool, Optional[str]]:
    """Parse XML file.

    Args:
        xml_path: Path to XML file
        config: Parser configuration
        writer: Optional CSV writer for output
        stats: Row count statistics dict
        record_stats: Per-record parsing statistics

    Returns:
        Tuple[bool, Optional[str]]: (success, error_message)
            - When ignoreBrokenFiles is False: always returns (True, None) or raises
            - When ignoreBrokenFiles is True: returns (False, error_msg) on parse failure

    Raises:
        ImportError: Always raised if lxml is not available (regardless of ignoreBrokenFiles)
        ValueError/XMLSyntaxError: Raised only when ignoreBrokenFiles is False
    """
    # lxml missing should ALWAYS raise - this is a config/environment issue
    if not HAS_LXML:
        raise ImportError("lxml is required for XML parsing. Install: pip install lxml")

    parser_obj = BaseParser(xml_path, config, writer, stats, record_stats)
    total_processed = 0

    # Wrap XML parsing logic to catch file-level failures
    try:
        parser = etree.XMLParser(recover=True, huge_tree=True, remove_blank_text=True)
        tree = etree.parse(str(xml_path), parser)

        # Check for fatal parsing errors
        if parser.error_log:
            fatal_errors = [e for e in parser.error_log if 'FATAL' in str(e)]
            if fatal_errors:
                error_details = '; '.join(str(e) for e in fatal_errors[:3])  # First 3 errors
                raise ValueError(f"XML parsing errors: {error_details}")

        root = tree.getroot()

        ns = {}
        default_ns_uri = root.nsmap.get(None)

        if not default_ns_uri:
            for elem in root.iter():
                uri = elem.nsmap.get(None)
                if uri:
                    default_ns_uri = uri
                    break

        for elem in root.iter():
            for prefix, uri in elem.nsmap.items():
                if prefix:
                    ns[prefix] = uri

        config_namespaces = config.get("namespaces", {})
        ns.update(config_namespaces)

        if default_ns_uri:
            if default_ns_uri not in ns.values():
                if 'ns0' not in ns:
                    ns['ns0'] = default_ns_uri
                    logger.info(f"Detected default XML namespace URI={default_ns_uri}; auto-mapped to prefix 'ns0'. "
                               f"Use 'ns0:' in XPath or provide namespaces in config to override.")
                else:
                    logger.warning(f"Default namespace detected (URI={default_ns_uri}), but 'ns0' is already mapped "
                                  f"in config to a different URI. Provide an explicit prefix mapping for the default "
                                  f"namespace in config (namespaces: {{\"yourprefix\": \"{default_ns_uri}\"}}) to use it in XPath.")

        for record in config["records"]:
            select_expr = normalize_xpath(record["select"])
            
            # Convert namespaces dict to tuple for caching
            ns_tuple = tuple(sorted(ns.items())) if ns else ()
            
            # Use cached compiled XPath for better performance
            try:
                compiled_select = compile_xpath(select_expr, ns_tuple)
                nodes = compiled_select(root)
            except etree.XPathSyntaxError:
                # Fallback to direct xpath if compilation fails
                logger.warning(f"Failed to compile XPath '{select_expr}', using fallback")
                nodes = root.xpath(select_expr, namespaces=ns)
                
            if not isinstance(nodes, list):
                nodes = [nodes] if nodes else []

            columns = parser_obj.get_columns(record)
            field_defs = parser_obj.build_field_defs(record)

            for node in nodes:
                if not isinstance(node, etree._Element):
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
                            expr = normalize_xpath(expr_raw)
                            
                            # Use cached compiled XPath
                            try:
                                compiled_expr = compile_xpath(expr, ns_tuple)
                                if expr.startswith("/"):
                                    val = compiled_expr(root)
                                else:
                                    val = compiled_expr(node)
                            except etree.XPathSyntaxError:
                                # Fallback for dynamic/invalid expressions
                                if expr.startswith("/"):
                                    val = root.xpath(expr, namespaces=ns)
                                else:
                                    val = node.xpath(expr, namespaces=ns)
                                    
                            val = val[0] if isinstance(val, list) and val else val
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

                        expr = normalize_xpath(fld["path"])
                        
                        # Use cached compiled XPath
                        try:
                            compiled_expr = compile_xpath(expr, ns_tuple)
                            val = compiled_expr(node)
                        except etree.XPathSyntaxError:
                            # Fallback for dynamic/invalid expressions
                            val = node.xpath(expr, namespaces=ns)
                        
                        field_type = fld.get("type", "string").lower()
                        
                        # Handle JSON field type (variant/complex fields)
                        if field_type == "json":
                            try:
                                # val is already a list or single element from xpath
                                row[fld["name"]] = xml_element_to_json(val)
                            except ImportError as e:
                                logger.error(f"Cannot use JSON field type: {e}")
                                row[fld["name"]] = None
                            except Exception as e:
                                logger.warning(f"Failed to convert field '{fld['name']}' to JSON: {e}")
                                row[fld["name"]] = None
                        # Handle XML field type (stores raw XML string)
                        elif field_type == "xml":
                            val = val[0] if isinstance(val, list) and val else val
                            if isinstance(val, etree._Element):
                                row[fld["name"]] = etree.tostring(val, encoding="unicode", with_tail=False)
                            else:
                                row[fld["name"]] = cast_value(val, "string", parser_obj.safe_mode)
                        # Handle all other field types
                        else:
                            val = val[0] if isinstance(val, list) and val else val
                            row[fld["name"]] = cast_value(val, field_type, parser_obj.safe_mode)

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

                    # Validate and write row
                    record_name = record["name"]
                    record_stats[record_name].total_rows += 1
                    parser_obj.validate_and_write_row(record_name, row, columns, field_defs)

                except Exception as row_error:
                    parser_obj.handle_row_error(record["name"], row_error)
                    continue

            # Log progress periodically
            total_processed += len(nodes)
            parser_obj.log_progress(record["name"], total_processed, total_processed)
        # Success - return status tuple
        parser_obj.finalize_stats()
        return (True, None)

    except (etree.XMLSyntaxError, ValueError, UnicodeDecodeError, Exception) as e:
        return parser_obj.handle_file_error(e)
