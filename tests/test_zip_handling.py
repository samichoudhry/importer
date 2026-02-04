"""
Tests for zip file handling functionality.
"""

import tempfile
import zipfile
from pathlib import Path

import pytest

from multi_format_parser.zip_utils import extract_zip_file, is_compressed_file, is_zip_file


class TestZipUtils:
    """Test suite for zip utility functions."""

    def test_is_zip_file_valid_zip(self, tmp_path):
        """Test is_zip_file returns True for valid zip files."""
        zip_path = tmp_path / "test.zip"
        
        # Create a valid zip file
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("test.txt", "test content")
        
        assert is_zip_file(zip_path) is True

    def test_is_zip_file_not_zip(self, tmp_path):
        """Test is_zip_file returns False for non-zip files."""
        text_file = tmp_path / "test.txt"
        text_file.write_text("not a zip file")
        
        assert is_zip_file(text_file) is False

    def test_is_zip_file_nonexistent(self, tmp_path):
        """Test is_zip_file returns False for nonexistent files."""
        nonexistent = tmp_path / "nonexistent.zip"
        assert is_zip_file(nonexistent) is False

    def test_extract_zip_file_single_file(self, tmp_path):
        """Test extracting a zip file with a single file."""
        zip_path = tmp_path / "test.zip"
        
        # Create zip with one file
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("data.csv", "col1,col2\nval1,val2")
        
        # Extract
        extracted = extract_zip_file(zip_path)
        
        assert len(extracted) == 1
        assert extracted[0].name == "data.csv"
        assert extracted[0].exists()
        assert "col1,col2" in extracted[0].read_text()

    def test_extract_zip_file_multiple_files(self, tmp_path):
        """Test extracting a zip file with multiple files."""
        zip_path = tmp_path / "test.zip"
        
        # Create zip with multiple files
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("file1.xml", "<root>test1</root>")
            zf.writestr("file2.csv", "a,b,c")
            zf.writestr("file3.json", '{"key": "value"}')
        
        # Extract
        extracted = extract_zip_file(zip_path)
        
        assert len(extracted) == 3
        file_names = {f.name for f in extracted}
        assert file_names == {"file1.xml", "file2.csv", "file3.json"}

    def test_extract_zip_file_nested_structure(self, tmp_path):
        """Test extracting a zip file with nested directory structure."""
        zip_path = tmp_path / "test.zip"
        
        # Create zip with nested structure
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("folder1/file1.txt", "content1")
            zf.writestr("folder1/subfolder/file2.txt", "content2")
            zf.writestr("folder2/file3.txt", "content3")
        
        # Extract
        extracted = extract_zip_file(zip_path)
        
        assert len(extracted) == 3
        # Verify all files exist and have correct content
        for file_path in extracted:
            assert file_path.exists()
            assert file_path.read_text().startswith("content")

    def test_extract_zip_file_empty_zip(self, tmp_path):
        """Test extracting an empty zip file."""
        zip_path = tmp_path / "empty.zip"
        
        # Create empty zip
        with zipfile.ZipFile(zip_path, 'w') as zf:
            pass
        
        # Extract
        extracted = extract_zip_file(zip_path)
        
        assert len(extracted) == 0

    def test_extract_zip_file_with_directories_only(self, tmp_path):
        """Test extracting a zip with only directories (no files)."""
        zip_path = tmp_path / "dirs_only.zip"
        
        # Create zip with only directories
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("folder1/", "")
            zf.writestr("folder2/subfolder/", "")
        
        # Extract
        extracted = extract_zip_file(zip_path)
        
        assert len(extracted) == 0

    def test_extract_zip_file_to_specific_directory(self, tmp_path):
        """Test extracting zip to a specific directory."""
        zip_path = tmp_path / "test.zip"
        extract_dir = tmp_path / "extracted"
        
        # Create zip
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("test.txt", "content")
        
        # Extract to specific directory
        extracted = extract_zip_file(zip_path, extract_dir)
        
        assert len(extracted) == 1
        assert extracted[0].parent == extract_dir
        assert extract_dir.exists()

    def test_extract_zip_file_invalid_zip(self, tmp_path):
        """Test that extract_zip_file raises error for invalid zip."""
        not_a_zip = tmp_path / "fake.zip"
        not_a_zip.write_text("This is not a zip file")
        
        with pytest.raises(zipfile.BadZipFile):
            extract_zip_file(not_a_zip)

    def test_is_compressed_file(self, tmp_path):
        """Test is_compressed_file detects compressed files by content."""
        # Create actual compressed files
        zip_file = tmp_path / "test.zip"
        with zipfile.ZipFile(zip_file, 'w') as zf:
            zf.writestr("test.txt", "content")
        
        gz_file = tmp_path / "test.gz"
        with zipfile.ZipFile(gz_file, 'w') as zf:
            zf.writestr("test.txt", "content")
        
        tar_file = tmp_path / "test.tar"
        with zipfile.ZipFile(tar_file, 'w') as zf:
            zf.writestr("test.txt", "content")
        
        # Test detection on real files
        assert is_compressed_file(zip_file) is True
        assert is_compressed_file(gz_file) is True  # Also a zip
        assert is_compressed_file(tar_file) is True  # Also a zip
        
        # Non-compressed files
        txt_file = tmp_path / "test.txt"
        txt_file.write_text("not compressed")
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("col1,col2")
        
        assert is_compressed_file(txt_file) is False
        assert is_compressed_file(csv_file) is False
