"""Tests for CSV parser functionality."""

import csv
from multi_format_parser.orchestrator import parse_files
from pathlib import Path

import pytest


def test_csv_parser_basic(sample_csv_file, sample_csv_config, temp_output_dir):
    """Test basic CSV parsing."""
    stats, record_stats, file_errors = parse_files(
        sample_csv_config,
        [sample_csv_file],
        temp_output_dir
    )

    # Verify no file-level errors
    assert len(file_errors) == 0

    # Verify file-level stats
    assert stats["processed"] == 1
    assert stats["succeeded"] == 1
    assert stats["failed"] == 0

    # Verify record stats
    assert "Orders" in record_stats
    assert record_stats["Orders"].total_rows == 3
    assert record_stats["Orders"].success_rows == 3
    assert record_stats["Orders"].failed_rows == 0

    # Verify output file exists
    output_file = temp_output_dir / "Orders.csv"
    assert output_file.exists()

    # Verify output content
    with open(output_file, 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        assert len(rows) == 3
        assert rows[0]["OrderID"] == "ORD001"
        assert rows[0]["Customer"] == "John Doe"
        assert rows[1]["OrderID"] == "ORD002"


def test_csv_parser_dry_run(sample_csv_file, sample_csv_config, temp_output_dir):
    """Test CSV parsing in dry-run mode."""
    stats, record_stats, file_errors = parse_files(
        sample_csv_config,
        [sample_csv_file],
        temp_output_dir,
        dry_run=True
    )

    # Verify no file-level errors
    assert len(file_errors) == 0

    # Verify parsing happened
    assert record_stats["Orders"].total_rows == 3

    # Verify NO output files were created
    output_file = temp_output_dir / "Orders.csv"
    assert not output_file.exists()


def test_csv_parser_empty_rows(tmp_path, temp_output_dir):
    """Test CSV parser skips empty rows."""
    csv_content = """order_id,customer,total
ORD001,John,100.50

ORD002,Jane,200.75

"""
    csv_file = tmp_path / "with_empty_rows.csv"
    csv_file.write_text(csv_content)

    import json
    config = {
        "format_type": "csv",
        "csv_delimiter": ",",
        "csv_has_header": True,
        "records": [{
            "name": "Orders",
            "fields": [
                {"name": "OrderID", "path": "order_id", "type": "string"},
                {"name": "Customer", "path": "customer", "type": "string"},
                {"name": "Total", "path": "total", "type": "decimal"}
            ]
        }]
    }
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps(config))

    stats, record_stats, file_errors = parse_files(
        config_file,
        [csv_file],
        temp_output_dir
    )

    # Should only process 2 non-empty rows
    assert record_stats["Orders"].total_rows == 2
    assert record_stats["Orders"].success_rows == 2


def test_csv_parser_with_validation(tmp_path, temp_output_dir):
    """Test CSV parsing with validation rules."""
    csv_content = """email,age
valid@email.com,25
invalid-email,30
another@valid.com,15
"""
    csv_file = tmp_path / "validation.csv"
    csv_file.write_text(csv_content)

    import json
    config = {
        "format_type": "csv",
        "csv_has_header": True,
        "records": [{
            "name": "Users",
            "fields": [
                {"name": "Email", "path": "email", "type": "string",
                 "regex": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"},
                {"name": "Age", "path": "age", "type": "int", "min_value": 18}
            ]
        }]
    }
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps(config))

    stats, record_stats, file_errors = parse_files(
        config_file,
        [csv_file],
        temp_output_dir
    )

    # One valid record, two with validation errors
    assert record_stats["Users"].total_rows == 3
    assert record_stats["Users"].success_rows == 1
    assert record_stats["Users"].failed_rows == 2
