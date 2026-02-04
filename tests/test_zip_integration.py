"""
Integration tests for zip file processing.
"""

import json
import zipfile
from pathlib import Path

import pytest

from multi_format_parser.orchestrator import parse_files


class TestZipIntegration:
    """Integration tests for processing zip files."""

    def test_parse_csv_from_zip(self, tmp_path):
        """Test parsing CSV file from a zip archive."""
        # Create CSV content
        csv_content = """Name,Age,City
John,30,NYC
Jane,25,LA"""
        
        # Create zip file with CSV
        zip_path = tmp_path / "input" / "data.zip"
        zip_path.parent.mkdir(exist_ok=True)
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("people.csv", csv_content)
        
        # Create config
        config = {
            "format_type": "csv",
            "csv_delimiter": ",",
            "csv_has_header": True,
            "records": [{
                "name": "People",
                "fields": [
                    {"name": "Name", "path": "Name", "type": "string"},
                    {"name": "Age", "path": "Age", "type": "integer"},
                    {"name": "City", "path": "City", "type": "string"}
                ]
            }]
        }
        
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(config))
        
        output_dir = tmp_path / "output"
        output_dir.mkdir(exist_ok=True)
        
        # Parse
        stats, record_stats, errors = parse_files(
            config_path=config_path,
            input_files=[zip_path],
            output_dir=output_dir,
            dry_run=False
        )
        
        # Verify
        assert stats["succeeded"] == 1
        assert stats["failed"] == 0
        assert len(errors) == 0
        
        # Check output file was created
        output_file = output_dir / "People.csv"
        assert output_file.exists()
        
        # Verify content
        output_content = output_file.read_text()
        assert "John" in output_content
        assert "Jane" in output_content

    def test_parse_xml_from_zip(self, tmp_path):
        """Test parsing XML file from a zip archive."""
        # Create XML content
        xml_content = """<?xml version="1.0"?>
<root>
    <person>
        <name>Alice</name>
        <age>28</age>
    </person>
    <person>
        <name>Bob</name>
        <age>35</age>
    </person>
</root>"""
        
        # Create zip file with XML
        zip_path = tmp_path / "input" / "data.zip"
        zip_path.parent.mkdir(exist_ok=True)
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("people.xml", xml_content)
        
        # Create config
        config = {
            "format_type": "xml",
            "records": [{
                "name": "People",
                "select": "/root/person",
                "fields": [
                    {"name": "Name", "path": "name", "type": "string"},
                    {"name": "Age", "path": "age", "type": "integer"}
                ]
            }]
        }
        
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(config))
        
        output_dir = tmp_path / "output"
        output_dir.mkdir(exist_ok=True)
        
        # Parse
        stats, record_stats, errors = parse_files(
            config_path=config_path,
            input_files=[zip_path],
            output_dir=output_dir,
            dry_run=False
        )
        
        # Verify
        assert stats["succeeded"] == 1
        assert stats["failed"] == 0
        assert len(errors) == 0
        
        # Check output file was created
        output_file = output_dir / "People.csv"
        assert output_file.exists()
        
        # Verify content
        output_content = output_file.read_text()
        assert "Alice" in output_content
        assert "Bob" in output_content

    def test_parse_multiple_files_from_zip(self, tmp_path):
        """Test parsing multiple files from a single zip archive."""
        # Create multiple CSV files
        csv1 = "Name,Score\nAlice,95"
        csv2 = "Name,Score\nBob,87"
        csv3 = "Name,Score\nCharlie,92"
        
        # Create zip with multiple files
        zip_path = tmp_path / "input" / "data.zip"
        zip_path.parent.mkdir(exist_ok=True)
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("scores1.csv", csv1)
            zf.writestr("scores2.csv", csv2)
            zf.writestr("scores3.csv", csv3)
        
        # Create config
        config = {
            "format_type": "csv",
            "csv_delimiter": ",",
            "csv_has_header": True,
            "records": [{
                "name": "Scores",
                "fields": [
                    {"name": "Name", "path": "Name", "type": "string"},
                    {"name": "Score", "path": "Score", "type": "integer"}
                ]
            }]
        }
        
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(config))
        
        output_dir = tmp_path / "output"
        output_dir.mkdir(exist_ok=True)
        
        # Parse
        stats, record_stats, errors = parse_files(
            config_path=config_path,
            input_files=[zip_path],
            output_dir=output_dir,
            dry_run=False
        )
        
        # Verify - should have processed 3 files
        assert stats["succeeded"] == 3
        assert stats["failed"] == 0
        assert len(errors) == 0

    def test_parse_mixed_zip_and_regular_files(self, tmp_path):
        """Test processing both zip archives and regular files together."""
        # Create a zip file with one CSV
        csv_in_zip = "Name,Value\nFromZip,100"
        zip_path = tmp_path / "input" / "archive.zip"
        zip_path.parent.mkdir(exist_ok=True)
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("data.csv", csv_in_zip)
        
        # Create a regular CSV file
        regular_csv = tmp_path / "input" / "regular.csv"
        regular_csv.write_text("Name,Value\nRegular,200")
        
        # Create config
        config = {
            "format_type": "csv",
            "csv_delimiter": ",",
            "csv_has_header": True,
            "records": [{
                "name": "Data",
                "fields": [
                    {"name": "Name", "path": "Name", "type": "string"},
                    {"name": "Value", "path": "Value", "type": "integer"}
                ]
            }]
        }
        
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(config))
        
        output_dir = tmp_path / "output"
        output_dir.mkdir(exist_ok=True)
        
        # Parse both files
        stats, record_stats, errors = parse_files(
            config_path=config_path,
            input_files=[zip_path, regular_csv],
            output_dir=output_dir,
            dry_run=False
        )
        
        # Verify - should have processed 2 files total
        assert stats["succeeded"] == 2
        assert stats["failed"] == 0
        assert len(errors) == 0

    def test_parse_zip_with_dry_run(self, tmp_path):
        """Test that dry run mode works with zip files."""
        # Create CSV in zip
        csv_content = "Name,Age\nTest,30"
        zip_path = tmp_path / "input" / "data.zip"
        zip_path.parent.mkdir(exist_ok=True)
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("test.csv", csv_content)
        
        # Create config
        config = {
            "format_type": "csv",
            "csv_delimiter": ",",
            "csv_has_header": True,
            "records": [{
                "name": "Test",
                "fields": [
                    {"name": "Name", "path": "Name", "type": "string"},
                    {"name": "Age", "path": "Age", "type": "integer"}
                ]
            }]
        }
        
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(config))
        
        output_dir = tmp_path / "output"
        output_dir.mkdir(exist_ok=True)
        
        # Parse with dry run
        stats, record_stats, errors = parse_files(
            config_path=config_path,
            input_files=[zip_path],
            output_dir=output_dir,
            dry_run=True
        )
        
        # Verify parsing succeeded
        assert stats["succeeded"] == 1
        assert stats["failed"] == 0
        
        # Verify no output files were created
        output_files = list(output_dir.glob("*.csv"))
        assert len(output_files) == 0

    def test_parse_invalid_zip_file(self, tmp_path):
        """Test handling of files with .zip extension that aren't valid zips.
        
        Files with .zip extension that aren't valid zip archives will be
        treated as regular files and will likely fail during parsing.
        """
        # Create a fake zip file (not a valid zip)
        fake_zip = tmp_path / "input" / "fake.zip"
        fake_zip.parent.mkdir(exist_ok=True)
        fake_zip.write_text("col\nvalue1")  # Make it valid CSV content
        
        # Create config
        config = {
            "format_type": "csv",
            "csv_delimiter": ",",
            "csv_has_header": True,
            "records": [{
                "name": "Test",
                "fields": [{"name": "col", "path": "col", "type": "string"}]
            }]
        }
        
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(config))
        
        output_dir = tmp_path / "output"
        output_dir.mkdir(exist_ok=True)
        
        # Parse - since it's not a valid zip, it will be parsed as CSV
        stats, record_stats, errors = parse_files(
            config_path=config_path,
            input_files=[fake_zip],
            output_dir=output_dir,
            dry_run=False,
            fail_fast=False
        )
        
        # Verify it was processed as a regular CSV file
        assert stats["succeeded"] == 1
        assert stats["failed"] == 0
