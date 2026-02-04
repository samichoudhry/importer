"""Tests for JSON parser functionality."""

import csv
import json
from pathlib import Path

import pytest

from multi_format_parser.orchestrator import parse_files


def test_json_parser_basic(sample_json_file, sample_json_config, temp_output_dir):
    """Test basic JSON parsing."""
    stats, record_stats, file_errors = parse_files(
        sample_json_config,
        [sample_json_file],
        temp_output_dir
    )

    # Verify no file-level errors
    assert len(file_errors) == 0

    # Verify file-level stats
    assert stats["processed"] == 1
    assert stats["succeeded"] == 1
    assert stats["failed"] == 0

    # Verify record stats
    assert "JsonOrders" in record_stats
    assert record_stats["JsonOrders"].total_rows == 3
    assert record_stats["JsonOrders"].success_rows == 3
    assert record_stats["JsonOrders"].failed_rows == 0

    # Verify output file exists
    output_file = temp_output_dir / "JsonOrders.csv"
    assert output_file.exists()

    # Verify output content
    with open(output_file) as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        assert len(rows) == 3
        assert rows[0]["OrderID"] == "J001"
        assert rows[0]["Status"] == "completed"


def test_json_parser_dry_run(sample_json_file, sample_json_config, temp_output_dir):
    """Test JSON parsing in dry-run mode."""
    stats, record_stats, file_errors = parse_files(
        sample_json_config,
        [sample_json_file],
        temp_output_dir,
        dry_run=True
    )

    # Verify no file-level errors
    assert len(file_errors) == 0

    # Verify parsing happened
    assert record_stats["JsonOrders"].total_rows == 3

    # Verify NO output files were created
    output_file = temp_output_dir / "JsonOrders.csv"
    assert not output_file.exists()


def test_json_parser_nested_paths(tmp_path, temp_output_dir):
    """Test JSON parser with nested path extraction."""
    json_content = {
        "data": {
            "users": [
                {"profile": {"name": "Alice", "age": 30}, "id": "U001"},
                {"profile": {"name": "Bob", "age": 25}, "id": "U002"}
            ]
        }
    }
    json_file = tmp_path / "nested.json"
    json_file.write_text(json.dumps(json_content))

    config = {
        "format_type": "json",
        "records": [{
            "name": "Users",
            "select": "data.users",
            "fields": [
                {"name": "ID", "path": "id", "type": "string"},
                {"name": "Name", "path": "profile.name", "type": "string"},
                {"name": "Age", "path": "profile.age", "type": "int"}
            ]
        }]
    }
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps(config))

    stats, record_stats, file_errors = parse_files(
        config_file,
        [json_file],
        temp_output_dir
    )

    assert len(file_errors) == 0
    assert record_stats["Users"].total_rows == 2
    assert record_stats["Users"].success_rows == 2

    # Verify nested field extraction
    output_file = temp_output_dir / "Users.csv"
    with open(output_file) as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        assert rows[0]["Name"] == "Alice"
        assert rows[0]["Age"] == "30"


def test_json_parser_malformed_file(tmp_path, temp_output_dir, sample_json_config):
    """Test that malformed JSON is caught and error is recorded."""
    malformed_json = tmp_path / "malformed.json"
    malformed_json.write_text("{invalid json content")

    stats, record_stats, file_errors = parse_files(
        sample_json_config,
        [malformed_json],
        temp_output_dir
    )

    # Verify file error was captured
    assert len(file_errors) == 1
    assert str(malformed_json) in file_errors
