"""
Multi-Format Parser: XML, CSV, Fixed-Width, and JSON file parser.

Backwards-compatibility wrapper for existing scripts.

Recommended usage:
  - Command line: multiformat-parser --config config.json --out ./output file.xml
  - Python module: python -m multi_format_parser.cli
  - Programmatic: from multi_format_parser.orchestrator import parse_files
"""

import sys

from multi_format_parser.cli import main
from multi_format_parser.orchestrator import FileProcessingError, parse_files

__all__ = ['main', 'parse_files', 'FileProcessingError']

if __name__ == "__main__":
    sys.exit(main())
