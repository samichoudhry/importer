"""Tests for XML parser functionality."""

import csv
from pathlib import Path

import pytest

from multi_format_parser.orchestrator import parse_files


def test_xml_parser_basic(sample_xml_file, sample_xml_config, temp_output_dir):
    """Test basic XML parsing with namespace."""
    stats, record_stats, file_errors = parse_files(
        sample_xml_config,
        [sample_xml_file],
        temp_output_dir
    )

    # Verify no file-level errors
    assert len(file_errors) == 0, f"Unexpected file errors: {file_errors}"

    # Verify file-level stats
    assert stats["processed"] == 1
    assert stats["succeeded"] == 1
    assert stats["failed"] == 0

    # Verify record stats
    assert "Transactions" in record_stats
    assert record_stats["Transactions"].total_rows == 2
    assert record_stats["Transactions"].success_rows == 2
    assert record_stats["Transactions"].failed_rows == 0

    # Verify output file exists
    output_file = temp_output_dir / "Transactions.csv"
    assert output_file.exists()

    # Verify output content
    with open(output_file) as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        assert len(rows) == 2
        assert rows[0]["ID"] == "TX001"
        assert rows[0]["Amount"] == "100.50"
        assert rows[1]["ID"] == "TX002"


def test_xml_parser_dry_run(sample_xml_file, sample_xml_config, temp_output_dir):
    """Test XML parsing in dry-run mode."""
    stats, record_stats, file_errors = parse_files(
        sample_xml_config,
        [sample_xml_file],
        temp_output_dir,
        dry_run=True
    )

    # Verify no file-level errors
    assert len(file_errors) == 0

    # Verify parsing happened
    assert record_stats["Transactions"].total_rows == 2
    assert record_stats["Transactions"].success_rows == 2

    # Verify NO output files were created
    output_file = temp_output_dir / "Transactions.csv"
    assert not output_file.exists()


def test_xml_parser_malformed_file(malformed_xml_file, sample_xml_config, temp_output_dir):
    """Test that malformed XML file is caught and continues processing."""
    stats, record_stats, file_errors = parse_files(
        sample_xml_config,
        [malformed_xml_file],
        temp_output_dir
    )

    # Verify file error was captured
    assert len(file_errors) == 1
    assert str(malformed_xml_file) in file_errors
    error_msg = file_errors[str(malformed_xml_file)]
    assert any(err_type in error_msg for err_type in ["XMLSyntaxError", "ParseError", "ValueError", "XML parsing errors"])


def test_xml_parser_with_validation(tmp_path, temp_output_dir):
    """Test XML parsing with field validation."""
    # Create XML with invalid data
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<Root xmlns="http://example.com">
    <Transaction>
        <ID>TX001</ID>
        <Amount>100.50</Amount>
    </Transaction>
    <Transaction>
        <ID>TX002</ID>
        <Amount>-50</Amount>
    </Transaction>
</Root>
"""
    xml_file = tmp_path / "validation_test.xml"
    xml_file.write_text(xml_content)

    # Create config with validation rules
    import json
    config = {
        "format_type": "xml",
        "namespaces": {"ns": "http://example.com"},
        "records": [{
            "name": "Transactions",
            "select": "/ns:Root/ns:Transaction",
            "fields": [
                {"name": "ID", "path": "ns:ID", "type": "string"},
                {"name": "Amount", "path": "ns:Amount", "type": "decimal",
                 "min_value": 0, "nullable": False}  # Must be >= 0
            ]
        }]
    }
    config_file = tmp_path / "validation_config.json"
    config_file.write_text(json.dumps(config))

    stats, record_stats, file_errors = parse_files(
        config_file,
        [xml_file],
        temp_output_dir
    )

    # One record should succeed, one should fail validation
    assert record_stats["Transactions"].total_rows == 2
    assert record_stats["Transactions"].success_rows == 1
    assert record_stats["Transactions"].failed_rows == 1
    assert record_stats["Transactions"].validation_errors == 1

    # Check rejected file exists
    rejected_file = temp_output_dir / "Transactions_rejected.csv"
    assert rejected_file.exists()


def test_xml_namespace_missing_prefix_in_select(tmp_path, temp_output_dir):
    """Test that missing namespace prefix in select path results in no records found."""
    # Create XML with namespace
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<Root xmlns="http://example.com">
    <Transaction>
        <ID>TX001</ID>
        <Amount>100.50</Amount>
    </Transaction>
    <Transaction>
        <ID>TX002</ID>
        <Amount>250.75</Amount>
    </Transaction>
</Root>
"""
    xml_file = tmp_path / "namespace_test.xml"
    xml_file.write_text(xml_content)

    # Create config with namespace defined but NOT used in select path
    import json
    config = {
        "format_type": "xml",
        "namespaces": {"ns": "http://example.com"},
        "records": [{
            "name": "Transactions",
            "select": "/Root/Transaction",  # Missing ns: prefix!
            "fields": [
                {"name": "ID", "path": "ns:ID", "type": "string"},
                {"name": "Amount", "path": "ns:Amount", "type": "decimal"}
            ]
        }]
    }
    config_file = tmp_path / "namespace_config.json"
    config_file.write_text(json.dumps(config))

    stats, record_stats, file_errors = parse_files(
        config_file,
        [xml_file],
        temp_output_dir
    )

    # Should parse file successfully but find no records
    assert len(file_errors) == 0
    assert stats["processed"] == 1
    assert stats["succeeded"] == 1

    # No records should be found due to namespace mismatch
    assert record_stats["Transactions"].total_rows == 0
    assert record_stats["Transactions"].success_rows == 0


def test_xml_namespace_wrong_prefix_in_fields(tmp_path, temp_output_dir):
    """Test that wrong namespace prefix in field paths results in extraction failures."""
    # Create XML with namespace
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<Root xmlns="http://example.com">
    <Transaction>
        <ID>TX001</ID>
        <Amount>100.50</Amount>
    </Transaction>
    <Transaction>
        <ID>TX002</ID>
        <Amount>250.75</Amount>
    </Transaction>
</Root>
"""
    xml_file = tmp_path / "wrong_prefix_test.xml"
    xml_file.write_text(xml_content)

    # Create config with correct select but wrong prefix in field paths
    import json
    config = {
        "format_type": "xml",
        "namespaces": {"ns": "http://example.com"},
        "records": [{
            "name": "Transactions",
            "select": "/ns:Root/ns:Transaction",
            "fields": [
                {"name": "ID", "path": "wrong:ID", "type": "string"},  # Wrong prefix!
                {"name": "Amount", "path": "ns:Amount", "type": "decimal"}
            ]
        }]
    }
    config_file = tmp_path / "wrong_prefix_config.json"
    config_file.write_text(json.dumps(config))

    stats, record_stats, file_errors = parse_files(
        config_file,
        [xml_file],
        temp_output_dir
    )

    # Should have a file-level error due to undefined namespace prefix 'wrong'
    assert len(file_errors) == 1
    error_msg = list(file_errors.values())[0].lower()
    assert any(keyword in error_msg for keyword in 
               ["xpath", "namespace", "prefix", "undefined"])
    
    # File should have failed to process
    assert stats["failed"] == 1


def test_xml_namespace_mismatched_uri(tmp_path, temp_output_dir):
    """Test that mismatched namespace URI results in no records found."""
    # Create XML with one namespace
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<Root xmlns="http://example.com">
    <Transaction>
        <ID>TX001</ID>
        <Amount>100.50</Amount>
    </Transaction>
</Root>
"""
    xml_file = tmp_path / "mismatched_uri_test.xml"
    xml_file.write_text(xml_content)

    # Create config with different namespace URI
    import json
    config = {
        "format_type": "xml",
        "namespaces": {"ns": "http://wrong-namespace.com"},  # Wrong URI!
        "records": [{
            "name": "Transactions",
            "select": "/ns:Root/ns:Transaction",
            "fields": [
                {"name": "ID", "path": "ns:ID", "type": "string"},
                {"name": "Amount", "path": "ns:Amount", "type": "decimal"}
            ]
        }]
    }
    config_file = tmp_path / "mismatched_uri_config.json"
    config_file.write_text(json.dumps(config))

    stats, record_stats, file_errors = parse_files(
        config_file,
        [xml_file],
        temp_output_dir
    )

    # Should parse file successfully but find no records due to URI mismatch
    assert len(file_errors) == 0
    assert stats["processed"] == 1
    assert stats["succeeded"] == 1

    # No records should be found
    assert record_stats["Transactions"].total_rows == 0
    assert record_stats["Transactions"].success_rows == 0


def test_xml_namespace_mixed_usage(tmp_path, temp_output_dir):
    """Test mixed namespace usage where some fields use prefix and some don't."""
    # Create XML with namespace
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<Root xmlns="http://example.com">
    <Transaction>
        <ID>TX001</ID>
        <Amount>100.50</Amount>
        <Status>Active</Status>
    </Transaction>
</Root>
"""
    xml_file = tmp_path / "mixed_usage_test.xml"
    xml_file.write_text(xml_content)

    # Create config with mixed namespace usage in fields
    import json
    config = {
        "format_type": "xml",
        "namespaces": {"ns": "http://example.com"},
        "records": [{
            "name": "Transactions",
            "select": "/ns:Root/ns:Transaction",
            "fields": [
                {"name": "ID", "path": "ns:ID", "type": "string"},  # Correct
                {"name": "Amount", "path": "Amount", "type": "decimal"},  # Missing prefix!
                {"name": "Status", "path": "ns:Status", "type": "string"}  # Correct
            ]
        }]
    }
    config_file = tmp_path / "mixed_usage_config.json"
    config_file.write_text(json.dumps(config))

    stats, record_stats, file_errors = parse_files(
        config_file,
        [xml_file],
        temp_output_dir
    )

    # Should parse and find records
    assert len(file_errors) == 0
    assert record_stats["Transactions"].total_rows == 1
    assert record_stats["Transactions"].success_rows == 1

    # Check output to verify which fields extracted correctly
    output_file = temp_output_dir / "Transactions.csv"
    assert output_file.exists()

    with open(output_file) as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        assert len(rows) == 1
        # ID and Status should extract correctly (with prefix)
        assert rows[0]["ID"] == "TX001"
        assert rows[0]["Status"] == "Active"
        # Amount should be empty (without prefix, won't match namespace)
        assert rows[0]["Amount"] == ""


def test_xml_namespace_undefined_prefix(tmp_path, temp_output_dir):
    """Test that using an undefined namespace prefix raises an error."""
    # Create XML with namespace
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<Root xmlns="http://example.com">
    <Transaction>
        <ID>TX001</ID>
    </Transaction>
</Root>
"""
    xml_file = tmp_path / "undefined_prefix_test.xml"
    xml_file.write_text(xml_content)

    # Create config that uses undefined prefix
    import json
    config = {
        "format_type": "xml",
        "namespaces": {"ns": "http://example.com"},
        "records": [{
            "name": "Transactions",
            "select": "/undefined:Root/undefined:Transaction",  # Undefined prefix!
            "fields": [
                {"name": "ID", "path": "ns:ID", "type": "string"}
            ]
        }]
    }
    config_file = tmp_path / "undefined_prefix_config.json"
    config_file.write_text(json.dumps(config))

    stats, record_stats, file_errors = parse_files(
        config_file,
        [xml_file],
        temp_output_dir
    )

    # Should have a file-level error due to undefined namespace prefix
    assert len(file_errors) == 1
    error_msg = list(file_errors.values())[0].lower()
    assert any(keyword in error_msg for keyword in 
               ["xpath", "namespace", "prefix", "undefined"])
    
    # File should have failed to process
    assert stats["failed"] == 1
