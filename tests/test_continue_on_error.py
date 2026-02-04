"""Tests for file-level error handling and continue-on-error behavior.

This is a CRITICAL test suite that locks in the behavior that the parser
must continue processing files even when one file fails.
"""

import json
from pathlib import Path

import pytest

from multi_format_parser.orchestrator import parse_files


def test_continue_on_file_error_basic(tmp_path, temp_output_dir):
    """Test that parser continues processing after one file fails."""
    # Create a good CSV file
    good_csv = tmp_path / "good.csv"
    good_csv.write_text("id,value\n1,100\n2,200\n")

    # Create a non-existent file path
    bad_file = tmp_path / "nonexistent.csv"

    # Create another good CSV file
    good_csv2 = tmp_path / "good2.csv"
    good_csv2.write_text("id,value\n3,300\n4,400\n")

    # Create config
    config = {
        "format_type": "csv",
        "csv_has_header": True,
        "records": [{
            "name": "Data",
            "fields": [
                {"name": "ID", "path": "id", "type": "string"},
                {"name": "Value", "path": "value", "type": "int"}
            ]
        }]
    }
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps(config))

    # Parse all three files (one will fail)
    stats, record_stats, file_errors = parse_files(
        config_file,
        [good_csv, bad_file, good_csv2],
        temp_output_dir,
        fail_fast=False  # Explicit: continue on error
    )

    # CRITICAL ASSERTIONS
    # 1. Exactly one file should have failed
    assert len(file_errors) == 1
    assert str(bad_file) in file_errors

    # 2. Both good files should have been processed
    assert record_stats["Data"].total_rows == 4  # 2 from each good file
    assert record_stats["Data"].success_rows == 4

    # 3. File-level stats should reflect 3 files processed, 2 succeeded, 1 failed
    assert stats["processed"] == 3
    assert stats["succeeded"] == 2
    assert stats["failed"] == 1


def test_fail_fast_mode(tmp_path, temp_output_dir):
    """Test that --fail-fast stops on first error."""
    # Create files
    good_csv = tmp_path / "good.csv"
    good_csv.write_text("id,value\n1,100\n")

    bad_file = tmp_path / "nonexistent.csv"

    good_csv2 = tmp_path / "good2.csv"
    good_csv2.write_text("id,value\n2,200\n")

    config = {
        "format_type": "csv",
        "csv_has_header": True,
        "records": [{
            "name": "Data",
            "fields": [
                {"name": "ID", "path": "id", "type": "string"},
                {"name": "Value", "path": "value", "type": "int"}
            ]
        }]
    }
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps(config))

    # Parse with fail_fast=True
    # First file succeeds, second fails, third shouldn't be processed
    # fail_fast raises an exception on first error
    from multi_format_parser.orchestrator import FileProcessingError

    with pytest.raises(FileProcessingError):
        stats, record_stats, file_errors = parse_files(
            config_file,
            [good_csv, bad_file, good_csv2],
            temp_output_dir,
            fail_fast=True
        )


def test_all_files_fail(tmp_path, temp_output_dir):
    """Test behavior when all files fail."""
    bad_file1 = tmp_path / "bad1.csv"
    bad_file2 = tmp_path / "bad2.csv"
    # Don't create these files

    config = {
        "format_type": "csv",
        "records": [{"name": "Data", "fields": [{"name": "ID", "path": "id"}]}]
    }
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps(config))

    stats, record_stats, file_errors = parse_files(
        config_file,
        [bad_file1, bad_file2],
        temp_output_dir
    )

    # Both files should fail
    assert len(file_errors) == 2
    assert str(bad_file1) in file_errors
    assert str(bad_file2) in file_errors


def test_malformed_file_continues(tmp_path, temp_output_dir):
    """Test that parser continues after encountering malformed file content."""
    # Good XML
    good_xml = tmp_path / "good.xml"
    good_xml.write_text("""<?xml version="1.0"?>
<Root>
    <Item><ID>1</ID></Item>
</Root>
""")

    # Malformed XML
    bad_xml = tmp_path / "bad.xml"
    bad_xml.write_text("""<?xml version="1.0"?>
<Root>
    <Item><ID>2</ID>
</Root>
""")  # Missing closing Item tag

    # Another good XML
    good_xml2 = tmp_path / "good2.xml"
    good_xml2.write_text("""<?xml version="1.0"?>
<Root>
    <Item><ID>3</ID></Item>
</Root>
""")

    config = {
        "format_type": "xml",
        "records": [{
            "name": "Items",
            "select": "/Root/Item",
            "fields": [{"name": "ID", "path": "ID", "type": "string"}]
        }]
    }
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps(config))

    stats, record_stats, file_errors = parse_files(
        config_file,
        [good_xml, bad_xml, good_xml2],
        temp_output_dir
    )

    # One file should fail (malformed XML)
    assert len(file_errors) == 1
    assert str(bad_xml) in file_errors

    # Two good files should process successfully
    assert record_stats["Items"].total_rows == 2
    assert record_stats["Items"].success_rows == 2


def test_permission_error_continues(tmp_path, temp_output_dir):
    """Test behavior with permission-denied file (Unix-like systems only)."""
    import sys
    if sys.platform == "win32":
        pytest.skip("Permission test not reliable on Windows")

    # Create files
    good_csv = tmp_path / "good.csv"
    good_csv.write_text("id\n1\n")

    restricted_file = tmp_path / "restricted.csv"
    restricted_file.write_text("id\n2\n")
    restricted_file.chmod(0o000)  # Remove all permissions

    good_csv2 = tmp_path / "good2.csv"
    good_csv2.write_text("id\n3\n")

    config = {
        "format_type": "csv",
        "csv_has_header": True,
        "records": [{
            "name": "Data",
            "fields": [{"name": "ID", "path": "id", "type": "string"}]
        }]
    }
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps(config))

    try:
        stats, record_stats, file_errors = parse_files(
            config_file,
            [good_csv, restricted_file, good_csv2],
            temp_output_dir
        )

        # Should have one error (permission denied)
        assert len(file_errors) >= 1  # May fail on file existence check or read

        # Should process the accessible files
        assert record_stats["Data"].success_rows >= 2
    finally:
        # Restore permissions for cleanup
        restricted_file.chmod(0o644)
