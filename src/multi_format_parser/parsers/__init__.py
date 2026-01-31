"""
Parser modules for different file formats.
"""

from multi_format_parser.parsers.csv_parser import parse_csv
from multi_format_parser.parsers.fixed_width_parser import parse_fixed_width
from multi_format_parser.parsers.json_parser import parse_json
from multi_format_parser.parsers.xml_parser import parse_xml

__all__ = [
    "parse_xml",
    "parse_csv",
    "parse_json",
    "parse_fixed_width",
]
