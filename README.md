# Multi-Format Parser

Enterprise-grade, configuration-driven data parser supporting XML, CSV, Fixed-Width, and JSON formats.

**Status:** Production | **Version:** 1.0.0 | **Python:** 3.8+

---

## Quick Start

```bash
# Install with Poetry (recommended)
git clone git@github.com:think-lp/parser-xml.git
cd multi_format_parser
poetry install

# Or install with pip
python -m venv venv
source venv/bin/activate
pip install -e .

# Basic usage
poetry run python parser.py --config config/example.json --out ./output input_file.xml

# Or with activated virtualenv
python parser.py --config config/example.json --out ./output input_file.xml

# Batch processing (continues on errors by default)
poetry run python parser.py --config config/batch.json --out ./output file1.xml file2.csv

# Fail-fast mode
poetry run python parser.py --config config/prod.json --out ./output --fail-fast *.xml

# Dry run (validate without writing files)
poetry run python parser.py --config config/test.json --out ./output --dry-run test.csv
```

---

## Running Prod Files (Temporary Quick Reference)

**XML:**
```bash
python parser.py --config prod_configs/xml.json --out ./output input/prod_xml.xml
```

**CSV:**
```bash
python parser.py --config prod_configs/csv.json --out ./output input/prod_csv.csv
```

**JSON:**
```bash
python parser.py --config prod_configs/json.json --out ./output input/prod_json.json
```

**Fixed-Width:**
```bash
python parser.py --config prod_configs/fixed.json --out ./output input/prod_fixed.txt
```
---

## Features

- **Multi-Format Support**: Parse XML (with namespaces), CSV, JSON, and Fixed-Width files
- **Declarative Configuration**: JSON-based parsing rules, no code changes needed
- **Production Resilient**: Continue-on-error default, comprehensive error tracking
- **Data Validation**: Field-level validation with regex patterns, numeric ranges, nullable constraints
- **Type Safety**: Pydantic configuration models with compile-time validation
- **Battle-Tested**: Comprehensive test suite with >90% coverage

---

## Configuration Examples

### XML

```json
{
  "format_type": "xml",
  "namespaces": {"ns": "http://example.com/namespace"},
  "records": [{
    "name": "Transactions",
    "select": "/ns:Root/ns:Transaction",
    "fields": [
      {"name": "ID", "path": "ns:ID", "type": "string", "nullable": false},
      {"name": "Amount", "path": "ns:Amount", "type": "decimal", "min_value": 0}
    ]
  }]
}
```

### CSV

```json
{
  "format_type": "csv",
  "csv_delimiter": ",",
  "csv_has_header": true,
  "records": [{
    "name": "Orders",
    "fields": [
      {"name": "OrderID", "path": "order_id", "type": "string", "regex": "^ORD[0-9]{6}$"},
      {"name": "Total", "path": "total_amount", "type": "decimal", "min_value": 0}
    ]
  }]
}
```

### JSON

```json
{
  "format_type": "json",
  "records": [{
    "name": "Users",
    "select": "data.users",
    "fields": [
      {"name": "UserID", "path": "id", "type": "string"},
      {"name": "Email", "path": "profile.email", "type": "string"}
    ]
  }]
}
```

### Fixed-Width

```json
{
  "format_type": "fixed_width",
  "records": [{
    "name": "Accounts",
    "fields": [
      {"name": "AccountNumber", "start": 0, "width": 10, "type": "string"},
      {"name": "Balance", "start": 10, "width": 15, "type": "decimal"}
    ]
  }]
}
```

See `config_examples/` for complete configuration templates.

---

## Command-Line Options

```
parser.py --config CONFIG --out OUTPUT_DIR [options] input_files...

Required:
  --config CONFIG       JSON configuration file
  --out OUTPUT_DIR      Output directory for CSV files
  input_files          One or more input files

Options:
  --log-level LEVEL    DEBUG, INFO, WARNING, ERROR, CRITICAL (default: INFO)
  --dry-run            Parse without writing output files
  --fail-fast          Stop on first file error (default: continue-on-error)
```

---

## Output Structure

```
output/
├── RecordName.csv              # Successfully validated records
└── RecordName_rejected.csv     # Failed records with _error_reason column
```

### Exit Codes

| Code | Status | Description |
|------|--------|-------------|
| 0 | Success | All files processed without errors |
| 1 | Complete Failure | All files failed |
| 2 | Partial Failure | Some files succeeded, others failed |

---

## Configuration Reference

### Global Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `format_type` | string | *required* | `xml`, `csv`, `json`, or `fixed_width` |
| `ignoreBrokenFiles` | boolean | `false` | Continue when file-level parse errors occur |
| `normalization.trim_strings` | boolean | `true` | Remove leading/trailing whitespace |
| `normalization.cast_mode` | string | `"safe"` | `"safe"` (null on error) or `"strict"` (exception) |

### Field Validation

| Rule | Types | Description | Example |
|------|-------|-------------|---------|
| `nullable` | All | Allow null/empty values | `"nullable": false` |
| `regex` | string | Pattern match | `"regex": "^[A-Z]{2}\\d{6}$"` |
| `min_value` | int, decimal | Minimum value (inclusive) | `"min_value": 0` |
| `max_value` | int, decimal | Maximum value (inclusive) | `"max_value": 999999.99` |

### Field Types

| Type | Description | Output Format | Use Case |
|------|-------------|---------------|----------|
| `string` | Text value | Plain text | Names, IDs, codes |
| `int` | Integer number | Numeric | Counts, quantities |
| `decimal` | Decimal number | Numeric with precision | Prices, amounts |
| `boolean` | True/false value | true/false | Flags, indicators |
| `date` | Date value | ISO date string | Dates without time |
| `datetime` | Date and time | ISO datetime string | Timestamps |
| `computed` | Formula result | Varies | Calculated fields |
| `json` | Complex/nested structure | JSON string | Variant/complex fields |

### JSON Field Type (Variant Fields)

The `json` field type allows capturing complex or repeating XML/JSON structures as a single JSON string in the output CSV. This is useful for:
- Nested structures with many optional fields
- Repeating elements that would require multiple rows or wide tables
- Preserving full data structure for later analysis

**Example:**
```json
{
  "name": "Transactions",
  "select": "//Transaction",
  "fields": [
    {"name": "ID", "path": "ID", "type": "string"},
    {"name": "TransactionLines", "path": "TransactionLine", "type": "json"}
  ]
}
```

**Output:**
```csv
ID,TransactionLines
T001,"[{""TransactionLine"":{""@status"":""normal"",""ItemLine"":{""ItemCode"":""12345"",""Price"":""9.99""}}}]"
```

**Features:**
- Automatically converts XML elements to JSON
- Handles single elements (returns object) or multiple elements (returns array)
- Removes XML namespace attributes for cleaner output
- Size warnings for large JSON fields (>50KB)
- Compatible with modern databases (Snowflake VARIANT, PostgreSQL JSONB, BigQuery JSON)

### Format-Specific Options

**XML:**
- `namespaces`: Namespace prefix-to-URI mappings
- `select`: XPath expression to locate records

**CSV:**
- `csv_delimiter`: Field separator (default: `","`)
- `csv_has_header`: First row contains column names (default: `true`)
- `csv_encoding`: Character encoding (default: `"utf-8"`)

**JSON:**
- `select`: Dot-notation path to array of records

**Fixed-Width:**
- `start`: Starting position (0-indexed)
- `width` or `end`: Field width or ending position

---

## Development

### Setup

```bash
# Install development dependencies with Poetry
poetry install

# Or with pip
pip install -e ".[dev]"

# Setup pre-commit hooks
poetry run pre-commit install
# or: pre-commit install (if using pip/venv)

# Run tests
poetry run pytest
# or: pytest (if using pip/venv)

# Run tests with coverage
pytest --cov=src --cov=parser --cov-report=html
```

### Code Quality

```bash
# Format code
black .

# Lint
ruff check .

# Type check
mypy src/ parser.py
```

---

## Advanced Features

### Computed Fields

Define calculated fields using formulas:

```json
{
  "computed_fields": [
    {"name": "FullKey", "formula": "{StoreID}-{Date}-{TxID}"},
    {"name": "Hash", "formula": "hash_md5({StoreID}-{TxID})"}
  ],
  "records": [{
    "fields": [
      {"name": "StoreID", "path": "store_id", "type": "string"},
      {"name": "ComputedKey", "type": "computed", "computed_field": "FullKey"}
    ]
  }]
}
```

### Context Fields

Add static values to all records:

```json
{
  "records": [{
    "context": [
      {"name": "FileSource", "value": "ProductionSystem"},
      {"name": "ProcessDate", "value": "2026-01-29"}
    ]
  }]
}
```

### Performance Tuning

Control CSV flush frequency:

```json
{
  "output": {
    "flush_every": 10000
  }
}
```

| Setting | Behavior |
|---------|----------|
| `0` | Flush on file close (fastest) |
| `N` | Flush every N rows (balanced) |
| `null` | Flush every row (safest) |

See [PERFORMANCE.md](PERFORMANCE.md) for detailed tuning guidance.

---

## Error Handling

### Fault-Tolerant Processing

By default, the parser continues processing all files even when individual files fail:

```bash
$ python parser.py --config config.json --out ./output file1.xml bad.xml file3.xml

[1/3] Processing file1.xml... ✅ Completed (1,000 rows)
[2/3] Processing bad.xml... ❌ Failed (XMLSyntaxError)
[3/3] Processing file3.xml... ✅ Completed (1,500 rows)

FILES: 2 succeeded, 1 failed
Exit code: 2
```

### Row-Level Validation

Invalid rows are written to rejected files with error details:

```csv
OrderID,Customer,Total,_error_reason
ORD123,John,-50,Field 'Total' value -50.0 below minimum 0
,Jane,100,Field 'OrderID' cannot be null
INVALID,Bob,100,Field 'OrderID' failed regex validation
```

---

## Architecture

```
CLI → Config Validation (Pydantic) → Orchestrator → Format Parser → Validator → CSV Writer
                                                          ↓
                                                    BaseParser
                                              (shared functionality)
```

### Components

- **config_models.py**: Pydantic configuration validation
- **orchestrator.py**: Batch file processing and error handling
- **parsers/base_parser.py**: Shared parser functionality
- **parsers/[format]_parser.py**: Format-specific parsing logic
- **validators.py**: Field-level validation
- **csv_writer.py**: Output file management

---

## Performance Considerations

| Format | Memory Usage | Notes |
|--------|--------------|-------|
| XML | O(1) per record | Streaming parser (iterparse) |
| CSV | O(1) per row | Line-by-line reading |
| JSON | O(n) entire file | Full file loaded into memory |
| Fixed-Width | O(1) per line | Line-by-line reading |

**For large JSON files (>1GB)**: Consider splitting into smaller files or using streaming format (NDJSON).

---

## Troubleshooting

### Common Issues

**"Config validation failed"**
- Check JSON syntax (no trailing commas)
- Verify all required fields present
- Use example configs as templates

**"No records found"**
- **XML**: Verify XPath expressions and namespace prefixes match source
- **JSON**: Check `select` path exists in data
- **CSV**: Ensure `csv_has_header` setting correct

**"lxml is required for XML parsing"**
```bash
pip install lxml
```

### Debugging

```bash
# Enable verbose logging
python parser.py --config config.json --out ./output --log-level DEBUG file.xml

# Test without writing files
python parser.py --config config.json --out ./output --dry-run test.xml

# Validate JSON config
python -m json.tool config.json
```

---

## Support

- **GitHub Issues**: Bug reports and feature requests
- **Slack**: `#data-platform` channel
- **Email**: data-platform@thinklp.com
- **Documentation**: See `config_examples/` for templates

---

**Multi-Format Parser** • Version 1.0.0 • Think LP Data Platform
