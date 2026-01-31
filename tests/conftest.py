"""Pytest configuration and shared fixtures."""

import json
import shutil
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def fixtures_dir() -> Path:
    """Return path to test fixtures directory."""
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def temp_output_dir():
    """Create temporary output directory for tests."""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    # Cleanup
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def sample_xml_file(fixtures_dir, tmp_path) -> Path:
    """Create a sample XML file for testing."""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<Root xmlns="http://example.com">
    <Transaction>
        <ID>TX001</ID>
        <Amount>100.50</Amount>
        <Date>2024-01-15</Date>
    </Transaction>
    <Transaction>
        <ID>TX002</ID>
        <Amount>250.75</Amount>
        <Date>2024-01-16</Date>
    </Transaction>
</Root>
"""
    xml_file = tmp_path / "sample.xml"
    xml_file.write_text(xml_content)
    return xml_file


@pytest.fixture
def sample_xml_config(tmp_path) -> Path:
    """Create a sample XML configuration."""
    config = {
        "format_type": "xml",
        "namespaces": {"ns": "http://example.com"},
        "records": [{
            "name": "Transactions",
            "select": "/ns:Root/ns:Transaction",
            "fields": [
                {"name": "ID", "path": "ns:ID", "type": "string"},
                {"name": "Amount", "path": "ns:Amount", "type": "decimal"},
                {"name": "Date", "path": "ns:Date", "type": "string"}
            ]
        }]
    }
    config_file = tmp_path / "xml_config.json"
    config_file.write_text(json.dumps(config, indent=2))
    return config_file


@pytest.fixture
def sample_csv_file(tmp_path) -> Path:
    """Create a sample CSV file for testing."""
    csv_content = """order_id,customer,total,date
ORD001,John Doe,150.25,2024-01-15
ORD002,Jane Smith,275.50,2024-01-16
ORD003,Bob Johnson,99.99,2024-01-17
"""
    csv_file = tmp_path / "sample.csv"
    csv_file.write_text(csv_content)
    return csv_file


@pytest.fixture
def sample_csv_config(tmp_path) -> Path:
    """Create a sample CSV configuration."""
    config = {
        "format_type": "csv",
        "csv_delimiter": ",",
        "csv_has_header": True,
        "records": [{
            "name": "Orders",
            "fields": [
                {"name": "OrderID", "path": "order_id", "type": "string"},
                {"name": "Customer", "path": "customer", "type": "string"},
                {"name": "Total", "path": "total", "type": "decimal"},
                {"name": "Date", "path": "date", "type": "string"}
            ]
        }]
    }
    config_file = tmp_path / "csv_config.json"
    config_file.write_text(json.dumps(config, indent=2))
    return config_file


@pytest.fixture
def sample_json_file(tmp_path) -> Path:
    """Create a sample JSON file for testing."""
    json_content = {
        "orders": [
            {"id": "J001", "amount": 125.50, "status": "completed"},
            {"id": "J002", "amount": 89.99, "status": "pending"},
            {"id": "J003", "amount": 310.00, "status": "completed"}
        ]
    }
    json_file = tmp_path / "sample.json"
    json_file.write_text(json.dumps(json_content, indent=2))
    return json_file


@pytest.fixture
def sample_json_config(tmp_path) -> Path:
    """Create a sample JSON configuration."""
    config = {
        "format_type": "json",
        "records": [{
            "name": "JsonOrders",
            "select": "orders",
            "fields": [
                {"name": "OrderID", "path": "id", "type": "string"},
                {"name": "Amount", "path": "amount", "type": "decimal"},
                {"name": "Status", "path": "status", "type": "string"}
            ]
        }]
    }
    config_file = tmp_path / "json_config.json"
    config_file.write_text(json.dumps(config, indent=2))
    return config_file


@pytest.fixture
def sample_fixed_width_file(tmp_path) -> Path:
    """Create a sample fixed-width file for testing."""
    content = """ID001  ITEM0120240115000100.50
ID002  ITEM0220240116000250.75
ID003  ITEM0320240117000099.99
"""
    fw_file = tmp_path / "sample_fixed.txt"
    fw_file.write_text(content)
    return fw_file


@pytest.fixture
def sample_fixed_width_config(tmp_path) -> Path:
    """Create a sample fixed-width configuration."""
    config = {
        "format_type": "fixed_width",
        "records": [{
            "name": "FixedWidthRecords",
            "fields": [
                {"name": "ID", "start": 0, "width": 7, "type": "string"},
                {"name": "Item", "start": 7, "width": 6, "type": "string"},
                {"name": "Date", "start": 13, "width": 8, "type": "string"},
                {"name": "Amount", "start": 21, "width": 10, "type": "decimal"}
            ]
        }]
    }
    config_file = tmp_path / "fixed_width_config.json"
    config_file.write_text(json.dumps(config, indent=2))
    return config_file


@pytest.fixture
def malformed_xml_file(tmp_path) -> Path:
    """Create a malformed XML file for error testing."""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<Root>
    <Transaction>
        <ID>TX001</ID>
        <Amount>100.50
    </Transaction
"""  # Missing closing bracket on Transaction tag
    xml_file = tmp_path / "malformed.xml"
    xml_file.write_text(xml_content)
    return xml_file
