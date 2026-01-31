"""Tests for configuration validation."""

import json
from pathlib import Path

import pytest

from multi_format_parser.validators import validate_config


def test_valid_xml_config():
    """Test that valid XML config passes validation."""
    config = {
        "format_type": "xml",
        "namespaces": {"ns": "http://example.com"},
        "records": [{
            "name": "Items",
            "select": "/ns:Root/ns:Item",
            "fields": [
                {"name": "ID", "path": "ns:ID", "type": "string"}
            ]
        }]
    }
    errors = validate_config(config)
    assert len(errors) == 0


def test_missing_format_type():
    """Test that missing format_type is caught."""
    config = {
        "records": [{
            "name": "Items",
            "fields": [{"name": "ID", "path": "ID"}]
        }]
    }
    errors = validate_config(config)
    assert len(errors) > 0
    assert any("format_type" in err.lower() for err in errors)


def test_invalid_format_type():
    """Test that invalid format_type is caught."""
    config = {
        "format_type": "invalid_format",
        "records": [{
            "name": "Items",
            "fields": [{"name": "ID", "path": "ID"}]
        }]
    }
    errors = validate_config(config)
    assert len(errors) > 0
    assert any("invalid" in err.lower() for err in errors)


def test_missing_records():
    """Test that missing records is caught."""
    config = {
        "format_type": "csv"
    }
    errors = validate_config(config)
    assert len(errors) > 0
    assert any("records" in err.lower() for err in errors)


def test_duplicate_field_names():
    """Test that duplicate field names are caught."""
    config = {
        "format_type": "csv",
        "records": [{
            "name": "Items",
            "fields": [
                {"name": "ID", "path": "id"},
                {"name": "ID", "path": "id2"}  # Duplicate!
            ]
        }]
    }
    errors = validate_config(config)
    assert len(errors) > 0
    assert any("duplicate" in err.lower() for err in errors)


def test_fixed_width_missing_start():
    """Test that fixed-width fields must have start position."""
    config = {
        "format_type": "fixed_width",
        "records": [{
            "name": "Records",
            "fields": [
                {"name": "Field1", "width": 10}  # Missing start!
            ]
        }]
    }
    errors = validate_config(config)
    assert len(errors) > 0
    assert any("start" in err.lower() for err in errors)


def test_fixed_width_missing_width_or_end():
    """Test that fixed-width fields must have width OR end."""
    config = {
        "format_type": "fixed_width",
        "records": [{
            "name": "Records",
            "fields": [
                {"name": "Field1", "start": 0}  # Missing both width and end!
            ]
        }]
    }
    errors = validate_config(config)
    assert len(errors) > 0
    assert any("width" in err.lower() or "end" in err.lower() for err in errors)


def test_fixed_width_both_width_and_end():
    """Test that fixed-width fields cannot have both width AND end."""
    config = {
        "format_type": "fixed_width",
        "records": [{
            "name": "Records",
            "fields": [
                {"name": "Field1", "start": 0, "width": 10, "end": 10}  # Both!
            ]
        }]
    }
    errors = validate_config(config)
    assert len(errors) > 0
    assert any("both" in err.lower() for err in errors)


def test_xml_missing_select():
    """Test that XML records must have select expression."""
    config = {
        "format_type": "xml",
        "records": [{
            "name": "Items",
            # Missing select!
            "fields": [{"name": "ID", "path": "ID"}]
        }]
    }
    errors = validate_config(config)
    assert len(errors) > 0
    assert any("select" in err.lower() for err in errors)


def test_invalid_regex():
    """Test that invalid regex patterns are caught."""
    config = {
        "format_type": "csv",
        "records": [{
            "name": "Items",
            "fields": [
                {"name": "Email", "path": "email", "regex": "[invalid(regex"}  # Bad regex
            ]
        }]
    }
    errors = validate_config(config)
    assert len(errors) > 0
    assert any("regex" in err.lower() for err in errors)


def test_computed_field_reference():
    """Test that computed field references are validated."""
    config = {
        "format_type": "csv",
        "computed_fields": [
            {"name": "FullName", "formula": "{FirstName} {LastName}"}
        ],
        "records": [{
            "name": "People",
            "fields": [
                {"name": "First", "path": "first", "type": "string"},
                {"name": "Full", "type": "computed", "computed_field": "NonExistent"}  # Bad ref
            ]
        }]
    }
    errors = validate_config(config)
    assert len(errors) > 0
    assert any("computed" in err.lower() for err in errors)
