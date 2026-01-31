"""
Streaming parser for processing huge files without loading them entirely into memory.

This module provides streaming parsers that process files incrementally,
making it possible to handle files larger than available RAM.
"""

import csv
import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Tuple

from multi_format_parser.casting import cast_value
from multi_format_parser.csv_writer import CSVWriter
from multi_format_parser.models import ParsingStats
from multi_format_parser.parsers.base_parser import BaseParser

try:
    from lxml import etree
    HAS_LXML = True
except ImportError:
    HAS_LXML = False

logger = logging.getLogger(__name__)


def stream_csv_records(
    file_path: Path,
    config: dict,
    chunk_size: int = 10000
) -> Iterator[Dict[str, Any]]:
    """Stream CSV records in chunks without loading entire file.
    
    Args:
        file_path: Path to CSV file
        config: Parser configuration
        chunk_size: Number of records to yield per chunk
        
    Yields:
        Dictionary containing parsed records
    """
    encoding = config.get("encoding", "utf-8")
    delimiter = config.get("delimiter", ",")
    has_header = config.get("has_header", True)
    
    with open(file_path, 'r', encoding=encoding) as f:
        reader = csv.DictReader(f, delimiter=delimiter) if has_header else csv.reader(f, delimiter=delimiter)
        
        chunk = []
        for row in reader:
            if isinstance(row, dict):
                chunk.append(row)
            else:
                # Handle case where no header
                chunk.append({f"col_{i}": val for i, val in enumerate(row)})
            
            if len(chunk) >= chunk_size:
                yield {"records": chunk}
                chunk = []
        
        # Yield remaining records
        if chunk:
            yield {"records": chunk}


def stream_json_records(
    file_path: Path,
    config: dict,
    selector: str = "$"
) -> Iterator[Dict[str, Any]]:
    """Stream JSON records using incremental parsing.
    
    This uses ijson for true streaming JSON parsing, which is useful
    for very large JSON files.
    
    Args:
        file_path: Path to JSON file
        config: Parser configuration
        selector: JSON path selector
        
    Yields:
        Individual records from the JSON file
        
    Requires:
        pip install ijson
    """
    try:
        import ijson
    except ImportError:
        raise ImportError(
            "ijson is required for streaming JSON parsing. "
            "Install with: pip install ijson"
        )
    
    encoding = config.get("json_encoding", "utf-8")
    
    # Convert selector to ijson path format
    # Simple conversions: $.users -> users.item, $.data.items -> data.items.item
    ijson_path = selector.lstrip('$').lstrip('.')
    
    if not ijson_path:
        ijson_path = "item"
    else:
        ijson_path = f"{ijson_path}.item"
    
    with open(file_path, 'r', encoding=encoding) as f:
        parser = ijson.items(f, ijson_path)
        
        for record in parser:
            yield record


def stream_xml_records(
    file_path: Path,
    config: dict,
    record_tag: str
) -> Iterator[Any]:
    """Stream XML records using iterparse for memory efficiency.
    
    This uses lxml's iterparse which processes the XML file incrementally,
    only keeping the current element in memory.
    
    Args:
        file_path: Path to XML file
        config: Parser configuration
        record_tag: XML tag name to treat as record boundary
        
    Yields:
        XML element objects
        
    Example:
        >>> for record in stream_xml_records(Path("huge.xml"), config, "Record"):
        ...     # Process record
        ...     pass
    """
    if not HAS_LXML:
        raise ImportError("lxml is required for XML streaming. Install: pip install lxml")
    
    # Parse XML incrementally
    context = etree.iterparse(str(file_path), events=('end',), tag=record_tag, huge_tree=True)
    
    for event, elem in context:
        # Yield the element
        yield elem
        
        # Clear the element and its ancestors to free memory
        elem.clear()
        
        # Also eliminate now-empty references from the root node
        while elem.getprevious() is not None:
            del elem.getparent()[0]
    
    # Clean up
    del context


def parse_csv_streaming(
    csv_path: Path,
    config: dict,
    writer: Optional[CSVWriter],
    stats: dict,
    record_stats: Dict[str, ParsingStats],
    chunk_size: int = 10000
) -> Tuple[bool, Optional[str]]:
    """Parse CSV file in streaming mode.
    
    Processes the CSV file in chunks to minimize memory usage.
    Ideal for files > 1GB.
    
    Args:
        csv_path: Path to CSV file
        config: Parser configuration
        writer: CSV writer for output
        stats: Statistics dictionary
        record_stats: Per-record statistics
        chunk_size: Records per chunk
        
    Returns:
        Tuple[bool, Optional[str]]: (success, error_message)
    """
    parser_obj = BaseParser(csv_path, config, writer, stats, record_stats)
    
    try:
        encoding = config.get("encoding", "utf-8")
        delimiter = config.get("delimiter", ",")
        has_header = config.get("has_header", True)
        
        with open(csv_path, 'r', encoding=encoding) as f:
            reader = csv.DictReader(f, delimiter=delimiter) if has_header else csv.reader(f, delimiter=delimiter)
            
            for record_config in config["records"]:
                record_name = record_config["name"]
                columns = parser_obj.get_columns(record_config)
                field_defs = parser_obj.build_field_defs(record_config)
                
                row_count = 0
                
                for row in reader:
                    row_count += 1
                    
                    # Convert list to dict if no header
                    if isinstance(row, list):
                        row_dict = {f"col_{i}": val for i, val in enumerate(row)}
                    else:
                        row_dict = row
                    
                    # Process row
                    try:
                        processed_row = {}
                        
                        for fld in record_config.get("fields", []):
                            field_name = fld["name"]
                            source_name = fld.get("path", field_name)
                            
                            value = row_dict.get(source_name)
                            processed_row[field_name] = cast_value(
                                value,
                                fld.get("type", "string"),
                                parser_obj.safe_mode
                            )
                        
                        # Validate and write
                        record_stats[record_name].total_rows += 1
                        parser_obj.validate_and_write_row(
                            record_name,
                            processed_row,
                            columns,
                            field_defs
                        )
                        
                    except Exception as e:
                        parser_obj.handle_row_error(record_name, e)
                    
                    # Log progress periodically
                    if row_count % 10000 == 0:
                        parser_obj.log_progress(record_name, row_count, row_count)
                
                logger.info(f"Streaming parse complete: {row_count} records processed")
        
        parser_obj.finalize_stats()
        return (True, None)
        
    except Exception as e:
        return parser_obj.handle_file_error(e)


def parse_xml_streaming(
    xml_path: Path,
    config: dict,
    writer: Optional[CSVWriter],
    stats: dict,
    record_stats: Dict[str, ParsingStats]
) -> Tuple[bool, Optional[str]]:
    """Parse XML file in streaming mode.
    
    Uses iterparse to process XML incrementally, clearing processed
    elements from memory. Ideal for files > 1GB.
    
    Args:
        xml_path: Path to XML file
        config: Parser configuration
        writer: CSV writer for output
        stats: Statistics dictionary
        record_stats: Per-record statistics
        
    Returns:
        Tuple[bool, Optional[str]]: (success, error_message)
    """
    if not HAS_LXML:
        raise ImportError("lxml is required for XML parsing. Install: pip install lxml")
    
    parser_obj = BaseParser(xml_path, config, writer, stats, record_stats)
    
    try:
        for record_config in config["records"]:
            record_name = record_config["name"]
            columns = parser_obj.get_columns(record_config)
            field_defs = parser_obj.build_field_defs(record_config)
            
            # Extract tag name from select XPath (simplified)
            select_expr = record_config["select"]
            # Handle simple cases like "//Record" or ".//Item"
            tag_name = select_expr.split('/')[-1].split('[')[0].split(':')[-1]
            
            if not tag_name or tag_name == '*':
                logger.error(f"Cannot determine tag name from selector '{select_expr}' for streaming")
                continue
            
            logger.info(f"Streaming XML records with tag '{tag_name}'")
            
            row_count = 0
            
            # Stream records
            for elem in stream_xml_records(xml_path, config, tag_name):
                row_count += 1
                
                try:
                    row = {}
                    
                    # Extract fields
                    for fld in record_config.get("fields", []):
                        if fld.get("type") == "computed":
                            row[fld["name"]] = None
                            continue
                        
                        path = fld.get("path", "")
                        if path:
                            # Simplified path extraction for streaming
                            val = elem.findtext(path.lstrip('./'))
                            row[fld["name"]] = cast_value(
                                val,
                                fld.get("type", "string"),
                                parser_obj.safe_mode
                            )
                        else:
                            row[fld["name"]] = None
                    
                    # Validate and write
                    record_stats[record_name].total_rows += 1
                    parser_obj.validate_and_write_row(
                        record_name,
                        row,
                        columns,
                        field_defs
                    )
                    
                except Exception as e:
                    parser_obj.handle_row_error(record_name, e)
                
                # Log progress
                if row_count % 10000 == 0:
                    parser_obj.log_progress(record_name, row_count, row_count)
            
            logger.info(f"Streaming parse complete: {row_count} records processed")
        
        parser_obj.finalize_stats()
        return (True, None)
        
    except Exception as e:
        return parser_obj.handle_file_error(e)


def parse_json_streaming(
    json_path: Path,
    config: dict,
    writer: Optional[CSVWriter],
    stats: dict,
    record_stats: Dict[str, ParsingStats]
) -> Tuple[bool, Optional[str]]:
    """Parse JSON file in streaming mode.
    
    Uses ijson for incremental parsing. Ideal for files > 1GB.
    
    Args:
        json_path: Path to JSON file
        config: Parser configuration
        writer: CSV writer for output
        stats: Statistics dictionary
        record_stats: Per-record statistics
        
    Returns:
        Tuple[bool, Optional[str]]: (success, error_message)
        
    Requires:
        pip install ijson
    """
    try:
        import ijson
    except ImportError:
        raise ImportError(
            "ijson is required for streaming JSON parsing. "
            "Install with: pip install ijson"
        )
    
    parser_obj = BaseParser(json_path, config, writer, stats, record_stats)
    
    try:
        for record_config in config["records"]:
            record_name = record_config["name"]
            selector = record_config.get("select", "$")
            
            columns = parser_obj.get_columns(record_config)
            field_defs = parser_obj.build_field_defs(record_config)
            
            row_count = 0
            
            # Stream records
            for record_data in stream_json_records(json_path, config, selector):
                row_count += 1
                
                try:
                    row = {}
                    
                    # Extract fields
                    for fld in record_config.get("fields", []):
                        if fld.get("type") == "computed":
                            row[fld["name"]] = None
                            continue
                        
                        path = fld.get("path", "")
                        if path:
                            # Simple path extraction
                            val = record_data.get(path) if isinstance(record_data, dict) else None
                            row[fld["name"]] = cast_value(
                                val,
                                fld.get("type", "string"),
                                parser_obj.safe_mode
                            )
                        else:
                            row[fld["name"]] = None
                    
                    # Validate and write
                    record_stats[record_name].total_rows += 1
                    parser_obj.validate_and_write_row(
                        record_name,
                        row,
                        columns,
                        field_defs
                    )
                    
                except Exception as e:
                    parser_obj.handle_row_error(record_name, e)
                
                # Log progress
                if row_count % 10000 == 0:
                    parser_obj.log_progress(record_name, row_count, row_count)
            
            logger.info(f"Streaming parse complete: {row_count} records processed")
        
        parser_obj.finalize_stats()
        return (True, None)
        
    except Exception as e:
        return parser_obj.handle_file_error(e)


# Convenience function to auto-select streaming or regular parsing
def parse_file_auto_stream(
    file_path: Path,
    config: dict,
    writer: Optional[CSVWriter],
    stats: dict,
    record_stats: Dict[str, ParsingStats],
    size_threshold_mb: int = 500
) -> Tuple[bool, Optional[str]]:
    """Automatically choose streaming or regular parsing based on file size.
    
    Args:
        file_path: Path to input file
        config: Parser configuration
        writer: CSV writer for output
        stats: Statistics dictionary
        record_stats: Per-record statistics
        size_threshold_mb: File size threshold in MB for streaming mode
        
    Returns:
        Tuple[bool, Optional[str]]: (success, error_message)
    """
    file_size_mb = file_path.stat().st_size / (1024 * 1024)
    use_streaming = file_size_mb > size_threshold_mb
    
    if use_streaming:
        logger.info(f"File size {file_size_mb:.1f} MB exceeds threshold {size_threshold_mb} MB, using streaming mode")
    
    format_type = config.get("format", "").lower()
    
    if use_streaming:
        if format_type == "csv":
            return parse_csv_streaming(file_path, config, writer, stats, record_stats)
        elif format_type == "xml":
            return parse_xml_streaming(file_path, config, writer, stats, record_stats)
        elif format_type == "json":
            return parse_json_streaming(file_path, config, writer, stats, record_stats)
    
    # Fall back to regular parsing
    from multi_format_parser.parsers import parse_csv, parse_json, parse_xml
    
    if format_type == "csv":
        return parse_csv(file_path, config, writer, stats, record_stats)
    elif format_type == "xml":
        return parse_xml(file_path, config, writer, stats, record_stats)
    elif format_type == "json":
        return parse_json(file_path, config, writer, stats, record_stats)
    else:
        raise ValueError(f"Unsupported format: {format_type}")
