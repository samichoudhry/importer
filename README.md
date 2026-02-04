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

# Process zip archives (automatically extracts and parses contents)
poetry run python parser.py --config config/example.json --out ./output archive.zip

# Process gzip compressed files
poetry run python parser.py --config config/example.json --out ./output data.csv.gz

# Process tar.gz archives
poetry run python parser.py --config config/example.json --out ./output archive.tar.gz

# Mix compressed and regular files
poetry run python parser.py --config config/example.json --out ./output file1.xml archive.zip data.csv.gz file2.csv
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
- **Compression Support**: Automatically extracts and processes files from zip, gzip (.gz), bzip2 (.bz2), tar, tar.gz (.tgz), and tar.bz2 archives
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

The `json` field type allows capturing complex or repeating XML/JSON structures as a single JSON string in the output CSV. This is ideal for avoiding column proliferation when dealing with complex nested data.

**Use Cases:**
- Nested structures with many optional fields (reduces column explosion)
- Repeating elements that would require multiple rows or wide tables
- Preserving full data structure for later analysis in data warehouses
- Handling variant structures (e.g., different transaction line types in one field)

**Example Configuration:**
```json
{
  "format_type": "xml",
  "namespaces": {"nax": "http://www.naxml.org/POSBO/Vocabulary/2003-10-16"},
  "records": [{
    "name": "SaleEvents",
    "select": "//nax:SaleEvent",
    "fields": [
      {"name": "EventID", "path": "nax:EventSequenceID", "type": "string"},
      {"name": "TotalAmount", "path": "nax:TransactionSummary/nax:TransactionTotalGrandAmount", "type": "decimal"},
      {"name": "TransactionLines_json", "path": "nax:TransactionDetailGroup/nax:TransactionLine", "type": "json"}
    ]
  }]
}
```

**Output CSV:**
```csv
EventID,TotalAmount,TransactionLines_json
51,11.13,"[{""nax:TransactionLine"":{""@status"":""normal"",""nax:ItemLine"":{""nax:ItemCode"":{""nax:POSCode"":""00072250030625""},""nax:Description"":""MRS. FRESHLEY MINI CHOC."",""nax:ActualSalesPrice"":""2.49""}}}]"
```

**Features:**
- Automatically converts XML elements to JSON format
- Handles single elements (returns JSON object) or multiple elements (returns JSON array)
- Removes XML namespace attributes (`@xmlns`) for cleaner output
- Size warnings for large JSON fields (>50KB threshold)
- Proper CSV escaping to prevent data corruption
- UTF-8 support for Unicode and emoji
- Production-ready error handling and logging

**Database Compatibility:**

The JSON output can be directly loaded into modern data warehouse JSON/VARIANT columns:

| Database | Column Type | Query Example |
|----------|-------------|---------------|
| **Snowflake** | `VARIANT` | `SELECT lines[0]:ItemLine.Description::VARCHAR FROM sales;` |
| **PostgreSQL** | `JSONB` | `SELECT lines->0->'ItemLine'->'Description' FROM sales;` |
| **BigQuery** | `JSON` | `SELECT JSON_EXTRACT(lines, '$[0].ItemLine.Description') FROM sales;` |

**Example Database Usage:**
```sql
-- Snowflake
CREATE TABLE sales (
    event_id VARCHAR,
    total_amount DECIMAL(10,2),
    lines VARIANT  -- Load JSON string here
);

-- Query nested data
SELECT 
    event_id,
    total_amount,
    lines[0]:ItemLine.Description::VARCHAR as first_item,
    lines[0]:ItemLine.ActualSalesPrice::DECIMAL as first_price
FROM sales;
```

See [example_xml_json_variant.json](config_examples/example_xml_json_variant.json) for a complete configuration example.

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

### File Filtering Options

**`file_mask`** - Regex pattern to filter which files get processed

Apply a regular expression pattern to filter files by name. This is useful when:
- Processing directories with mixed file types
- Extracting specific files from zip archives
- Filtering files by naming convention

**Example config:**
```json
{
  "format_type": "csv",
  "file_mask": ".*\\.csv$",
  "records": [...]
}
```

**Usage examples:**
```bash
# Only process CSV files from a directory
python parser.py --config config.json --out ./output /data/*.* 

# Extract and process only XML files from zip archive
# Config: "file_mask": ".*\\.xml$"
python parser.py --config config.json --out ./output archive.zip

# Process files matching specific pattern
# Config: "file_mask": "^invoice_.*\\.json$"
python parser.py --config config.json --out ./output invoices/
```

**Important notes:**
- Pattern is applied to **file names**, not full paths
- For zip archives: pattern filters **extracted files**, not the zip file name itself
- Uses Python regex syntax (escape special characters: `\\.` for literal dot)
- If pattern matches no files, an error is raised

**`max_files`** - Limit number of files to process (integer)

Process only the first N files. Useful for:
- Testing configurations on large datasets
- Sampling data
- Rate limiting

**Example:** `"max_files": 10` processes first 10 files

**`max_file_size`** - Maximum file size in bytes (integer)

Skip files larger than the specified size. Useful for:
- Avoiding memory issues
- Filtering out unexpectedly large files
- Production safety limits

**Example:** `"max_file_size": 524288000` (500 MB limit)

See [example_with_file_mask.json](config_examples/example_with_file_mask.json) for a complete example.

---

## Compression Support

The parser automatically detects and extracts compressed/archived files before processing. This feature simplifies working with compressed data files.

### Supported Formats

| Format | Extensions | Type | Description |
|--------|-----------|------|-------------|
| **Zip** | `.zip` | Archive | Multiple files in one archive |
| **Gzip** | `.gz` | Compression | Single file compression (e.g., `data.csv.gz`) |
| **Bzip2** | `.bz2` | Compression | Single file compression (alternative to gzip) |
| **Tar** | `.tar` | Archive | Multiple files, no compression |
| **Tar+Gzip** | `.tar.gz`, `.tgz` | Archive+Compression | Multiple files with gzip compression |
| **Tar+Bzip2** | `.tar.bz2` | Archive+Compression | Multiple files with bzip2 compression |

### How It Works

1. **Content-Based Detection**: Files are identified by reading their **magic bytes** (file signature), not by file extension
   - **Zip**: `PK` signature (`50 4B`)
   - **Gzip**: `1f 8b` magic bytes
   - **Bzip2**: `BZh` signature
   - **Tar**: POSIX tar format detection
   - Works even if file has wrong extension (e.g., `data.xyz` that's actually gzip)
   - Fake compressed files are correctly identified and treated as regular files
2. **Extraction**: Contents are extracted to a temporary directory
3. **Processing**: Each file in the archive is processed according to the configuration
4. **Cleanup**: Temporary files are automatically removed after processing

### Usage Examples

**Process single compressed file:**
```bash
python parser.py --config config/csv.json --out ./output data.csv.gz
```

**Process archive with multiple files:**
```bash
python parser.py --config config/xml.json --out ./output archive.tar.gz
```

**Mix compressed and regular files:**
```bash
python parser.py --config config/csv.json --out ./output file1.csv data.zip file2.csv.gz file3.csv
```

**Process multiple archives:**
```bash
python parser.py --config config/json.json --out ./output *.tar.gz *.zip
```

**Extract and filter with file_mask:**
```bash
# Config: "file_mask": ".*\\.csv$"
python parser.py --config config.json --out ./output archive.tar.gz
# Only CSV files from the archive will be processed
```

### Features

- **Multi-file archives**: Processes all compatible files within archives (zip, tar, tar.gz, tar.bz2)
- **Single-file compression**: Automatically decompresses .gz and .bz2 files
- **Nested directories**: Handles archives with nested folder structures
- **Mixed processing**: Can process compressed and non-compressed files in the same batch
- **Error handling**: Invalid or corrupted archives are logged and skipped (with `--fail-fast` disabled)
- **Dry run support**: Works with `--dry-run` mode for validation

### Important Notes

**Detection Method:**
- **Content-based, not extension-based**: A file named `archive.xyz` will be detected correctly if it has valid compression structure
- **Reliable**: Uses standard Python libraries (`zipfile`, `tarfile`, `gzip`, `bz2`) with magic byte detection
- **Safe**: Won't try to extract files that aren't actually compressed

**Processing:**
- File filtering (via `file_mask`) and limits (via `max_files`) apply to **extracted files**, not archive names
- All extracted files must match the format specified in the configuration
- Temporary extraction directories are automatically cleaned up (even on errors)
- For `.gz` and `.bz2`: Output filename is derived by removing the compression extension

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
