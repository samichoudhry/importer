"""
Tests for async orchestrator functionality.
"""
import asyncio
import json
from pathlib import Path
import pytest
from multi_format_parser.async_orchestrator import (
    parse_files_async,
    parse_file_batches_async,
    run_async_parse,
)


@pytest.fixture
def sample_csv_config(tmp_path):
    """Create a sample CSV config."""
    config = {
        "format_type": "csv",
        "csv_has_header": True,
        "records": [
            {
                "name": "TestRecord",
                "fields": [
                    {"name": "id", "path": "id", "type": "string"},
                    {"name": "value", "path": "value", "type": "int"}
                ]
            }
        ]
    }
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))
    return config_path


@pytest.fixture
def sample_csv_files(tmp_path):
    """Create sample CSV files."""
    files = []
    for i in range(3):
        csv_file = tmp_path / f"test_{i}.csv"
        csv_file.write_text("id,value\n1,100\n2,200\n")
        files.append(csv_file)
    return files


@pytest.mark.asyncio
async def test_parse_files_async(sample_csv_config, sample_csv_files, tmp_path):
    """Test basic async file parsing."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    
    stats, record_stats, errors = await parse_files_async(
        config_path=sample_csv_config,
        input_files=sample_csv_files,
        output_dir=output_dir,
        max_concurrent=2
    )
    
    assert stats["processed"] == 3
    assert stats["succeeded"] == 3
    assert stats["failed"] == 0
    assert len(errors) == 0


@pytest.mark.asyncio
async def test_parse_files_async_with_fail_fast(sample_csv_config, tmp_path):
    """Test async parsing with fail_fast mode."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    
    # Create files with one invalid file
    valid_file = tmp_path / "valid.csv"
    valid_file.write_text("id,value\n1,100\n")
    
    invalid_file = tmp_path / "invalid.csv"
    invalid_file.write_text("invalid csv content")
    
    files = [valid_file, invalid_file]
    
    stats, record_stats, errors = await parse_files_async(
        config_path=sample_csv_config,
        input_files=files,
        output_dir=output_dir,
        fail_fast=True,
        max_concurrent=2
    )
    
    # Should process at least one file before encountering error
    assert stats["processed"] >= 1


@pytest.mark.asyncio
async def test_parse_file_batches_async(sample_csv_config, tmp_path):
    """Test batch async file parsing."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    
    # Create multiple files
    files = []
    for i in range(10):
        csv_file = tmp_path / f"test_{i}.csv"
        csv_file.write_text("id,value\n1,100\n2,200\n")
        files.append(csv_file)
    
    stats, record_stats, errors = await parse_file_batches_async(
        config_path=sample_csv_config,
        input_files=files,
        output_dir=output_dir,
        batch_size=3,
        max_concurrent=2
    )
    
    assert stats["processed"] == 10
    assert stats["succeeded"] == 10
    assert stats["failed"] == 0


def test_run_async_parse_sync_wrapper(sample_csv_config, sample_csv_files, tmp_path):
    """Test synchronous wrapper for async parsing."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    
    stats, record_stats, errors = run_async_parse(
        config_path=sample_csv_config,
        input_files=sample_csv_files,
        output_dir=output_dir,
        max_concurrent=2
    )
    
    assert stats["processed"] == 3
    assert stats["succeeded"] == 3
    assert stats["failed"] == 0


@pytest.mark.asyncio
async def test_async_with_dry_run(sample_csv_config, sample_csv_files, tmp_path):
    """Test async parsing with dry_run mode."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    
    stats, record_stats, errors = await parse_files_async(
        config_path=sample_csv_config,
        input_files=sample_csv_files,
        output_dir=output_dir,
        dry_run=True,
        max_concurrent=2
    )
    
    assert stats["processed"] == 3
    # In dry run, files are validated but not written
    output_files = list(output_dir.glob("*.csv"))
    assert len(output_files) == 0  # No output in dry run


@pytest.mark.asyncio
async def test_async_with_missing_file(sample_csv_config, tmp_path):
    """Test async parsing with non-existent file."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    
    missing_file = tmp_path / "missing.csv"
    files = [missing_file]
    
    stats, record_stats, errors = await parse_files_async(
        config_path=sample_csv_config,
        input_files=files,
        output_dir=output_dir,
        max_concurrent=2
    )
    
    assert stats["processed"] == 1
    assert stats["failed"] == 1
    assert len(errors) == 1
