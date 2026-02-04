"""
Integration tests for batch processing multiple files.

Tests scenarios:
- Multiple files in a single run
- Mixed success/failure scenarios
- Progress reporting accuracy
- Dry-run mode

Note: These tests validate the actual batch processing behavior where
multiple input files are combined into output files per record type.
"""

import json
import tempfile
from pathlib import Path

import pytest

from multi_format_parser.orchestrator import parse_files


class TestBatchProcessing:
    """Integration tests for batch file processing."""

    @pytest.fixture
    def temp_workspace(self):
        """Create temporary workspace with config and input files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)

            # Create directories
            input_dir = base / "input"
            output_dir = base / "output"
            config_dir = base / "configs"

            input_dir.mkdir()
            output_dir.mkdir()
            config_dir.mkdir()

            yield {
                "base": base,
                "input": input_dir,
                "output": output_dir,
                "config": config_dir
            }

    def create_csv_config(self, config_path: Path, delimiter: str = ","):
        """Helper to create CSV configuration."""
        config = {
            "format_type": "csv",
            "version": "1.0",
            "normalization": {
                "trim_strings": True,
                "cast_mode": "safe"
            },
            "csv_delimiter": delimiter,
            "csv_quotechar": "\"",
            "csv_has_header": True,
            "csv_skip_rows": 0,
            "computed_fields": [],
            "records": [
                {
                    "name": "main",
                    "fields": [
                        {"name": "id", "path": "id", "type": "int", "nullable": False},
                        {"name": "name", "path": "name", "type": "string", "nullable": False},
                        {"name": "price", "path": "price", "type": "decimal", "nullable": False}
                    ]
                }
            ]
        }
        config_path.write_text(json.dumps(config, indent=2))
        return config_path

    def create_csv_file(self, file_path: Path, content: str):
        """Helper to create CSV file."""
        file_path.write_text(content)
        return file_path

    def test_multiple_files_success(self, temp_workspace):
        """Test processing multiple valid files in a single run."""
        # Setup config
        config_path = self.create_csv_config(
            temp_workspace["config"] / "test.json"
        )

        # Create multiple valid CSV files
        files = []
        for i in range(3):
            content = f"id,name,price\n1,Product{i},10.{i}0\n2,Item{i},20.{i}0\n"
            file_path = self.create_csv_file(
                temp_workspace["input"] / f"file{i}.csv",
                content
            )
            files.append(file_path)

        # Process all files
        stats, record_stats, errors = parse_files(
            config_path=config_path,
            input_files=files,
            output_dir=temp_workspace["output"],
            dry_run=False,
            fail_fast=False
        )

        # Verify all files processed successfully
        assert len(errors) == 0, f"Expected no errors, got: {errors}"
        assert stats["processed"] == 3
        assert stats["succeeded"] == 3
        assert stats["failed"] == 0

        # Verify combined output file created (all records go to main.csv)
        output_file = temp_workspace["output"] / "main.csv"
        assert output_file.exists(), f"Output file {output_file} not created"

        # Verify content has records from all files
        content = output_file.read_text()
        assert "Product0" in content or "Product1" in content or "Product2" in content

    def test_mixed_success_failure(self, temp_workspace):
        """Test processing with some files with different outcomes."""
        # Setup config
        config_path = self.create_csv_config(
            temp_workspace["config"] / "test.json"
        )

        # Create mix of valid and edge case files
        valid_file = self.create_csv_file(
            temp_workspace["input"] / "valid.csv",
            "id,name,price\n1,Product,10.00\n2,Item,20.00\n"
        )

        # File with wrong delimiter - will parse but likely get wrong data
        wrong_delim_file = self.create_csv_file(
            temp_workspace["input"] / "wrong_delim.csv",
            "id|name|price\n1|Product|10.00\n"
        )

        # Empty file - will process but produce no records
        empty_file = temp_workspace["input"] / "empty.csv"
        empty_file.write_text("")

        files = [valid_file, wrong_delim_file, empty_file]

        # Process with continue on error (fail_fast=False)
        stats, record_stats, errors = parse_files(
            config_path=config_path,
            input_files=files,
            output_dir=temp_workspace["output"],
            dry_run=False,
            fail_fast=False
        )

        # Verify all files were processed (no exceptions)
        assert stats["processed"] == 3
        assert stats["succeeded"] >= 1  # At least valid file succeeded

        # Verify valid file produced records
        assert len(record_stats) > 0

        # At least one record type should have processed records
        total_records = sum(s.total_rows for s in record_stats.values())
        assert total_records >= 2  # At least from valid file

    def test_progress_reporting_accuracy(self, temp_workspace):
        """Test that progress statistics are accurate."""
        # Setup config
        config_path = self.create_csv_config(
            temp_workspace["config"] / "test.json"
        )

        # Create files with known record counts
        files_data = [
            ("file1.csv", "id,name,price\n1,A,1.00\n2,B,2.00\n3,C,3.00\n", 3),
            ("file2.csv", "id,name,price\n4,D,4.00\n5,E,5.00\n", 2),
            ("file3.csv", "id,name,price\n6,F,6.00\n", 1),
        ]

        files = []
        expected_total_records = 0

        for filename, content, count in files_data:
            file_path = self.create_csv_file(
                temp_workspace["input"] / filename,
                content
            )
            files.append(file_path)
            expected_total_records += count

        # Process files
        stats, record_stats, errors = parse_files(
            config_path=config_path,
            input_files=files,
            output_dir=temp_workspace["output"],
            dry_run=False,
            fail_fast=False
        )

        # Verify file-level statistics
        assert stats["processed"] == 3
        assert stats["succeeded"] == 3
        assert stats["failed"] == 0

        # Verify record-level statistics exist
        assert len(record_stats) > 0

        # Check that total records processed matches expected
        total_records = sum(s.total_rows for s in record_stats.values())
        assert total_records == expected_total_records, \
            f"Expected {expected_total_records} records, got {total_records}"

    def test_dry_run_no_output(self, temp_workspace):
        """Test that dry_run doesn't create output files."""
        # Setup config
        config_path = self.create_csv_config(
            temp_workspace["config"] / "test.json"
        )

        # Create input file
        input_file = self.create_csv_file(
            temp_workspace["input"] / "test.csv",
            "id,name,price\n1,Product,10.00\n"
        )

        # Run in dry-run mode
        stats, record_stats, errors = parse_files(
            config_path=config_path,
            input_files=[input_file],
            output_dir=temp_workspace["output"],
            dry_run=True,
            fail_fast=False
        )

        # Verify processing succeeded
        assert stats["succeeded"] == 1
        assert len(errors) == 0

        # Verify no output files created (dry run)
        output_files = list(temp_workspace["output"].glob("*.csv"))
        assert len(output_files) == 0, "Dry run should not create output files"

        # Verify record stats still tracked
        assert len(record_stats) >= 1

    def test_large_batch_processing(self, temp_workspace):
        """Test processing a larger batch of files."""
        # Setup config
        config_path = self.create_csv_config(
            temp_workspace["config"] / "test.json"
        )

        # Create multiple small files
        num_files = 10
        files = []

        for i in range(num_files):
            content = f"id,name,price\n{i},Product{i},{i}.00\n"
            file_path = self.create_csv_file(
                temp_workspace["input"] / f"file_{i:03d}.csv",
                content
            )
            files.append(file_path)

        # Process all files
        stats, record_stats, errors = parse_files(
            config_path=config_path,
            input_files=files,
            output_dir=temp_workspace["output"],
            dry_run=False,
            fail_fast=False
        )

        # Verify all files processed
        assert stats["processed"] == num_files
        assert stats["succeeded"] == num_files
        assert stats["failed"] == 0
        assert len(errors) == 0

        # Verify output was created
        output_files = list(temp_workspace["output"].glob("*.csv"))
        assert len(output_files) >= 1  # At least main output file

        # Verify record stats complete
        assert len(record_stats) >= 1
        total_records = sum(s.total_rows for s in record_stats.values())
        assert total_records == num_files  # One record per file
