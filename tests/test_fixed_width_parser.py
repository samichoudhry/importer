"""Tests for fixed-width file parser functionality."""

import csv
import json
from multi_format_parser.orchestrator import parse_files
from pathlib import Path

import pytest


def test_fixed_width_parser_basic(sample_fixed_width_file, sample_fixed_width_config, temp_output_dir):
    """Test basic fixed-width file parsing."""
    stats, record_stats, file_errors = parse_files(
        sample_fixed_width_config,
        [sample_fixed_width_file],
        temp_output_dir
    )

    # Verify no file-level errors
    assert len(file_errors) == 0

    # Verify file-level stats
    assert stats["processed"] == 1
    assert stats["succeeded"] == 1
    assert stats["failed"] == 0

    # Verify record stats
    assert "FixedWidthRecords" in record_stats
    assert record_stats["FixedWidthRecords"].total_rows == 3
    assert record_stats["FixedWidthRecords"].success_rows == 3
    assert record_stats["FixedWidthRecords"].failed_rows == 0

    # Verify output file exists
    output_file = temp_output_dir / "FixedWidthRecords.csv"
    assert output_file.exists()

    # Verify output content
    with open(output_file, 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        assert len(rows) == 3
        assert rows[0]["ID"].strip() == "ID001"
        assert rows[0]["Item"].strip() == "ITEM01"


def test_fixed_width_parser_dry_run(sample_fixed_width_file, sample_fixed_width_config, temp_output_dir):
    """Test fixed-width parsing in dry-run mode."""
    stats, record_stats, file_errors = parse_files(
        sample_fixed_width_config,
        [sample_fixed_width_file],
        temp_output_dir,
        dry_run=True
    )

    # Verify no file-level errors
    assert len(file_errors) == 0

    # Verify parsing happened
    assert record_stats["FixedWidthRecords"].total_rows == 3

    # Verify NO output files were created
    output_file = temp_output_dir / "FixedWidthRecords.csv"
    assert not output_file.exists()


def test_fixed_width_parser_using_end(tmp_path, temp_output_dir):
    """Test fixed-width parser using 'end' instead of 'width'."""
    content = """ABC   12345  XYZ
DEF   67890  PQR
"""
    fw_file = tmp_path / "test_end.txt"
    fw_file.write_text(content)

    config = {
        "format_type": "fixed_width",
        "records": [{
            "name": "Records",
            "fields": [
                {"name": "Code", "start": 0, "end": 6, "type": "string"},
                {"name": "Number", "start": 6, "end": 13, "type": "string"},
                {"name": "Category", "start": 13, "end": 16, "type": "string"}
            ]
        }]
    }
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps(config))

    stats, record_stats, file_errors = parse_files(
        config_file,
        [fw_file],
        temp_output_dir
    )

    assert len(file_errors) == 0
    assert record_stats["Records"].total_rows == 2
    assert record_stats["Records"].success_rows == 2
