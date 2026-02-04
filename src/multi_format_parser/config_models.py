"""
Pydantic models for strongly-typed configuration validation.

This module replaces dict-based config with validated, type-safe models.
"""

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class FormatType(str, Enum):
    """Supported file format types."""
    XML = "xml"
    CSV = "csv"
    JSON = "json"
    FIXED_WIDTH = "fixed_width"


class FieldType(str, Enum):
    """Supported field data types."""
    STRING = "string"
    INT = "int"
    DECIMAL = "decimal"
    NUMBER = "number"
    FLOAT = "float"
    BOOLEAN = "boolean"
    BOOL = "bool"
    DATE = "date"
    DATETIME = "datetime"
    COMPUTED = "computed"
    JSON = "json"  # XML/JSON structure serialized as JSON string


class CastMode(str, Enum):
    """Type casting modes."""
    SAFE = "safe"  # Return None on cast failure
    STRICT = "strict"  # Raise error on cast failure


class ContextConfig(BaseModel):
    """Context field configuration."""
    name: str = Field(..., description="Context variable name")
    from_expr: Optional[str] = Field(None, alias="from", description="XPath/JSONPath expression to extract value")
    value: Optional[Any] = Field(None, description="Static value for context variable")

    model_config = {"populate_by_name": True}

    @model_validator(mode='after')
    def validate_source(self):
        """Ensure either from_expr or value is provided."""
        if self.from_expr is None and self.value is None:
            raise ValueError(f"Context '{self.name}' must have either 'from' or 'value'")
        return self


class FieldConfig(BaseModel):
    """Field definition configuration."""
    name: str = Field(..., description="Field name in output")
    path: Optional[str] = Field(None, description="XPath/JSONPath/column reference")
    type: FieldType = Field(FieldType.STRING, description="Field data type")
    nullable: bool = Field(True, description="Whether field can be null")
    computed_field: Optional[str] = Field(None, description="Reference to computed field formula")

    # Fixed-width specific
    start: Optional[int] = Field(None, description="Start position for fixed-width (0-indexed)", ge=0)
    end: Optional[int] = Field(None, description="End position for fixed-width (0-indexed)", ge=0)
    width: Optional[int] = Field(None, description="Field width for fixed-width", gt=0)

    # Validation constraints
    regex: Optional[str] = Field(None, description="Regex pattern for validation")
    min_value: Optional[float] = Field(None, description="Minimum numeric value")
    max_value: Optional[float] = Field(None, description="Maximum numeric value")
    default: Optional[Any] = Field(None, description="Default value if field is missing/null")

    @model_validator(mode='after')
    def validate_fixed_width_constraints(self):
        """Validate fixed-width field definitions."""
        if self.start is not None or self.end is not None or self.width is not None:
            # If any fixed-width field is set, validate the combinations
            if self.start is not None and self.end is not None:
                if self.end <= self.start:
                    raise ValueError(f"Field '{self.name}': end position must be > start position")
            elif self.start is not None and self.width is not None:
                # Both start and width are valid (allow start=0)
                pass
            elif self.start is None:
                raise ValueError(
                    f"Field '{self.name}': fixed-width fields must have a start position"
                )
            else:
                raise ValueError(
                    f"Field '{self.name}': fixed-width fields must have either "
                    "(start + end) or (start + width)"
                )
        return self


class RecordConfig(BaseModel):
    """Record definition configuration."""
    name: str = Field(..., description="Record/table name")
    select: Optional[str] = Field(None, description="XPath/JSONPath selector for records")
    context: List[ContextConfig] = Field(default_factory=list, description="Context variables")
    fields: List[FieldConfig] = Field(..., description="Field definitions")

    @field_validator('fields')
    @classmethod
    def validate_unique_field_names(cls, fields):
        """Ensure field names are unique within a record."""
        field_names = [f.name for f in fields]
        duplicates = [name for name in set(field_names) if field_names.count(name) > 1]
        if duplicates:
            raise ValueError(f"Duplicate field names: {', '.join(sorted(duplicates))}")
        return fields

    @model_validator(mode='after')
    def validate_format_specific(self):
        """Placeholder for format-specific validation (will be checked in ParserConfig)."""
        return self


class ComputedFieldConfig(BaseModel):
    """Computed field formula configuration."""
    name: str = Field(..., description="Computed field name")
    formula: str = Field(..., description="Python expression formula")
    type: FieldType = Field(FieldType.STRING, description="Result data type")


class NormalizationConfig(BaseModel):
    """Data normalization settings."""
    cast_mode: CastMode = Field(CastMode.SAFE, description="Type casting mode")
    strip_whitespace: bool = Field(True, description="Strip leading/trailing whitespace")
    empty_string_as_null: bool = Field(True, description="Treat empty strings as NULL")


class OutputConfig(BaseModel):
    """Output file settings."""
    flush_every: Optional[int] = Field(
        1000,
        description="Flush CSV to disk every N rows (None=every row, 0=on close only)",
        ge=0
    )
    include_rejected: bool = Field(True, description="Write rejected rows to separate files")
    csv_encoding: str = Field("utf-8", description="Output CSV encoding")


class ParserConfig(BaseModel):
    """Main parser configuration."""
    format_type: FormatType = Field(..., description="Input file format type")
    records: List[RecordConfig] = Field(..., min_length=1, description="Record definitions")

    # Common options
    continueOnError: bool = Field(
        False,
        description="Continue processing on row-level validation errors"
    )
    ignoreBrokenFiles: bool = Field(
        False,
        description="Continue batch processing if individual files fail to parse"
    )
    max_file_size: Optional[int] = Field(
        None,
        description="Maximum file size in bytes (None = no limit)",
        gt=0
    )
    max_files: Optional[int] = Field(
        None,
        description="Maximum number of files to process (None = no limit)",
        gt=0
    )
    file_mask: Optional[str] = Field(
        None,
        description="Regex pattern to filter input files by filename (None = accept all files)"
    )
    progress_interval: int = Field(
        10000,
        description="Log progress every N rows",
        gt=0
    )

    # Namespaces (XML/JSON)
    namespaces: Dict[str, str] = Field(
        default_factory=dict,
        description="XML namespace prefix mappings or JSON path prefixes"
    )

    # Computed fields
    computed_fields: List[ComputedFieldConfig] = Field(
        default_factory=list,
        description="Computed field formulas"
    )

    # Normalization settings
    normalization: NormalizationConfig = Field(
        default_factory=NormalizationConfig,
        description="Data normalization rules"
    )

    # Output settings
    output: OutputConfig = Field(
        default_factory=OutputConfig,
        description="Output file configuration"
    )

    # CSV-specific options
    csv_delimiter: str = Field(",", description="CSV delimiter character")
    csv_quotechar: str = Field('"', description="CSV quote character")
    csv_escapechar: Optional[str] = Field(None, description="CSV escape character")
    csv_doublequote: bool = Field(True, description="CSV double quote handling")
    csv_has_header: bool = Field(True, description="CSV has header row")
    csv_skip_rows: int = Field(0, description="Number of rows to skip at start", ge=0)
    csv_encoding: str = Field("utf-8", description="Input CSV encoding")

    # Fixed-width specific options
    fixed_width_encoding: str = Field("utf-8", description="Fixed-width file encoding")

    # JSON-specific options
    json_encoding: str = Field("utf-8", description="JSON file encoding")

    # Legacy parser sub-config support (for backward compatibility)
    parser: Optional[Dict[str, Any]] = Field(
        None,
        description="Legacy parser sub-config (deprecated, use top-level fields)"
    )

    @field_validator('records')
    @classmethod
    def validate_unique_record_names(cls, records):
        """Ensure record names are unique."""
        record_names = [r.name for r in records]
        duplicates = [name for name in set(record_names) if record_names.count(name) > 1]
        if duplicates:
            raise ValueError(f"Duplicate record names: {', '.join(sorted(duplicates))}")
        return records

    @field_validator('computed_fields')
    @classmethod
    def validate_unique_computed_names(cls, fields):
        """Ensure computed field names are unique."""
        field_names = [f.name for f in fields]
        duplicates = [name for name in set(field_names) if field_names.count(name) > 1]
        if duplicates:
            raise ValueError(f"Duplicate computed field names: {', '.join(sorted(duplicates))}")
        return fields

    @model_validator(mode='after')
    def validate_format_specific_requirements(self):
        """Validate format-specific requirements."""
        if self.format_type in (FormatType.XML, FormatType.JSON):
            # XML and JSON require select expressions
            for record in self.records:
                if not record.select:
                    raise ValueError(
                        f"Record '{record.name}': {self.format_type.value.upper()} records "
                        f"must have a non-empty 'select' field"
                    )

        if self.format_type == FormatType.FIXED_WIDTH:
            # Fixed-width requires position/width info
            for record in self.records:
                for field in record.fields:
                    if field.type != FieldType.COMPUTED:
                        if field.start is None or (field.end is None and field.width is None):
                            raise ValueError(
                                f"Record '{record.name}', Field '{field.name}': "
                                f"Fixed-width fields must have start position and either end or width"
                            )

        return self

    def to_legacy_dict(self) -> dict:
        """
        Convert Pydantic model back to legacy dict format for backward compatibility.
        This allows gradual migration of parsers to use the new config models.
        """
        return self.model_dump(by_alias=True, exclude_none=False, mode='python')

    @classmethod
    def from_dict(cls, config_dict: dict) -> "ParserConfig":
        """
        Create ParserConfig from dictionary with comprehensive validation.
        
        Args:
            config_dict: Configuration dictionary (loaded from JSON)
            
        Returns:
            Validated ParserConfig instance
            
        Raises:
            ValidationError: If configuration is invalid
        """
        return cls.model_validate(config_dict)

    @classmethod
    def from_json_file(cls, config_path: str) -> "ParserConfig":
        """
        Load and validate configuration from JSON file.
        
        Args:
            config_path: Path to JSON configuration file
            
        Returns:
            Validated ParserConfig instance
            
        Raises:
            ValidationError: If configuration is invalid
            FileNotFoundError: If file doesn't exist
        """
        import json
        from pathlib import Path

        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(path, encoding='utf-8') as f:
            config_dict = json.load(f)

        return cls.from_dict(config_dict)


# Type alias for backward compatibility
ConfigDict = Dict[str, Any]  # Will be replaced by ParserConfig throughout codebase
