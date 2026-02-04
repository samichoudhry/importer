"""
Tests for Pydantic-based configuration models.
"""

import pytest
from pydantic import ValidationError

from multi_format_parser.config_models import (
    FieldConfig,
    FormatType,
    ParserConfig,
    RecordConfig,
)


class TestConfigValidation:
    """Test configuration validation."""

    def test_missing_format_type(self):
        """Test that missing format_type raises validation error."""
        with pytest.raises(ValidationError) as exc_info:
            ParserConfig.from_dict({"records": []})
        assert exc_info.value.error_count() >= 1

    def test_invalid_format_type(self):
        """Test that invalid format_type raises validation error."""
        with pytest.raises(ValidationError):
            ParserConfig.from_dict({"format_type": "invalid", "records": []})

    def test_duplicate_field_names(self):
        """Test that duplicate field names in a record raise validation error."""
        with pytest.raises(ValidationError) as exc_info:
            ParserConfig.from_dict({
                "format_type": "csv",
                "records": [{
                    "name": "test",
                    "fields": [
                        {"name": "field1", "path": "a"},
                        {"name": "field1", "path": "b"}
                    ]
                }]
            })
        errors = str(exc_info.value)
        assert "duplicate" in errors.lower() or "field" in errors.lower()

    def test_xml_requires_select(self):
        """Test that XML records require select expression."""
        with pytest.raises(ValidationError) as exc_info:
            ParserConfig.from_dict({
                "format_type": "xml",
                "records": [{
                    "name": "test",
                    "fields": [{"name": "field1", "path": "//item"}]
                }]
            })
        errors = str(exc_info.value)
        assert "select" in errors.lower()

    def test_json_requires_select(self):
        """Test that JSON records require select expression."""
        with pytest.raises(ValidationError) as exc_info:
            ParserConfig.from_dict({
                "format_type": "json",
                "records": [{
                    "name": "test",
                    "fields": [{"name": "field1", "path": "$.item"}]
                }]
            })
        errors = str(exc_info.value)
        assert "select" in errors.lower()

    def test_valid_csv_config(self):
        """Test that valid CSV config validates successfully."""
        config = ParserConfig.from_dict({
            "format_type": "csv",
            "records": [{
                "name": "test_table",
                "fields": [
                    {"name": "field1", "path": "column1"},
                    {"name": "field2", "path": "column2", "type": "int"}
                ]
            }]
        })
        assert config.format_type == FormatType.CSV
        assert len(config.records) == 1
        assert config.records[0].name == "test_table"
        assert len(config.records[0].fields) == 2

    def test_valid_xml_config(self):
        """Test that valid XML config validates successfully."""
        config = ParserConfig.from_dict({
            "format_type": "xml",
            "records": [{
                "name": "items",
                "select": "//item",
                "fields": [
                    {"name": "id", "path": "./id"},
                    {"name": "name", "path": "./name"}
                ]
            }]
        })
        assert config.format_type == FormatType.XML
        assert config.records[0].select == "//item"

    def test_fixed_width_with_start_and_width(self):
        """Test fixed-width fields with start and width."""
        config = ParserConfig.from_dict({
            "format_type": "fixed_width",
            "records": [{
                "name": "fixed_record",
                "fields": [
                    {"name": "field1", "start": 0, "width": 10},
                    {"name": "field2", "start": 10, "width": 20}
                ]
            }]
        })
        assert config.format_type == FormatType.FIXED_WIDTH
        assert config.records[0].fields[0].start == 0
        assert config.records[0].fields[0].width == 10

    def test_fixed_width_with_start_and_end(self):
        """Test fixed-width fields with start and end."""
        config = ParserConfig.from_dict({
            "format_type": "fixed_width",
            "records": [{
                "name": "fixed_record",
                "fields": [
                    {"name": "field1", "start": 0, "end": 10},
                    {"name": "field2", "start": 10, "end": 30}
                ]
            }]
        })
        assert config.records[0].fields[0].start == 0
        assert config.records[0].fields[0].end == 10

    def test_field_validation_constraints(self):
        """Test field validation constraints."""
        config = ParserConfig.from_dict({
            "format_type": "csv",
            "records": [{
                "name": "test",
                "fields": [{
                    "name": "age",
                    "path": "age",
                    "type": "int",
                    "nullable": False,
                    "min_value": 0,
                    "max_value": 150,
                    "regex": None
                }]
            }]
        })
        field = config.records[0].fields[0]
        assert field.nullable is False
        assert field.min_value == 0
        assert field.max_value == 150

    def test_computed_fields(self):
        """Test computed fields configuration."""
        config = ParserConfig.from_dict({
            "format_type": "csv",
            "computed_fields": [
                {"name": "full_name", "formula": "{first_name} {last_name}", "type": "string"}
            ],
            "records": [{
                "name": "test",
                "fields": [
                    {"name": "first_name", "path": "fname"},
                    {"name": "last_name", "path": "lname"},
                    {"name": "full_name", "type": "computed"}
                ]
            }]
        })
        assert len(config.computed_fields) == 1
        assert config.computed_fields[0].name == "full_name"
        assert config.computed_fields[0].formula == "{first_name} {last_name}"

    def test_config_to_legacy_dict(self):
        """Test converting config back to legacy dict format."""
        original = {
            "format_type": "csv",
            "continueOnError": True,
            "records": [{
                "name": "test",
                "fields": [{"name": "field1", "path": "col1"}]
            }]
        }
        config = ParserConfig.from_dict(original)
        legacy_dict = config.to_legacy_dict()

        assert legacy_dict["format_type"] == "csv"
        assert legacy_dict["continueOnError"] is True
        assert len(legacy_dict["records"]) == 1


class TestProductionConfigs:
    """Test that production config files validate."""

    def test_xml_config(self, tmp_path):
        """Test XML production config."""
        config = ParserConfig.from_json_file("prod_configs/xml.json")
        assert config.format_type == FormatType.XML
        assert len(config.records) > 0

    def test_csv_config(self):
        """Test CSV production config."""
        config = ParserConfig.from_json_file("prod_configs/csv.json")
        assert config.format_type == FormatType.CSV

    def test_json_config(self):
        """Test JSON production config."""
        config = ParserConfig.from_json_file("prod_configs/json.json")
        assert config.format_type == FormatType.JSON

    def test_fixed_config(self):
        """Test fixed-width production config."""
        config = ParserConfig.from_json_file("prod_configs/fixed.json")
        assert config.format_type == FormatType.FIXED_WIDTH
