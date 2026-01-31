# Performance Tuning Guide

This guide provides recommendations for optimizing the multi-format parser for different use cases and file sizes.

## Table of Contents

1. [Memory Management](#memory-management)
2. [File Size Recommendations](#file-size-recommendations)
3. [Batch Processing](#batch-processing)
4. [XML Performance](#xml-performance)
5. [CSV Performance](#csv-performance)
6. [JSON Performance](#json-performance)
7. [Monitoring and Profiling](#monitoring-and-profiling)

---

## Memory Management

### flush_every Setting

The `flush_every` configuration controls how often data is written to disk during processing. This is crucial for managing memory usage with large files.

**Default:** `flush_every: 1000`

#### Recommendations by File Size

| File Size | Records Count | Recommended flush_every | Memory Impact |
|-----------|---------------|------------------------|---------------|
| < 10 MB | < 10,000 | 5000 - 10000 | Low |
| 10-100 MB | 10,000 - 100,000 | 1000 - 5000 | Medium |
| 100-500 MB | 100,000 - 500,000 | 500 - 1000 | High |
| 500 MB - 1 GB | 500,000 - 1M | 100 - 500 | Very High |
| > 1 GB | > 1M | 50 - 100 | Critical |

#### Example Configuration

```json
{
  "format": "xml",
  "flush_every": 500,
  "records": [...]
}
```

#### Memory Usage Formula (Approximate)

```
Memory Usage ≈ (Average Row Size × flush_every × Number of Record Types) + Parser Overhead
```

**Example:**
- Average row size: 1 KB
- flush_every: 1000
- Record types: 2
- Parser overhead: ~50 MB

**Total:** `(1 KB × 1000 × 2) + 50 MB ≈ 52 MB`

### Tips for Memory-Constrained Environments

1. **Lower flush_every**: Set to 100-500 for files > 500 MB
2. **Process files individually**: Avoid batch processing many large files
3. **Use dry_run first**: Test configuration without writing output
4. **Monitor memory**: Use `ps aux | grep python` or system monitor

---

## File Size Recommendations

### Small Files (< 10 MB)

**Best Settings:**
```json
{
  "flush_every": 10000,
  "continueOnError": false
}
```

**Benefits:**
- Faster processing (less I/O)
- Full error reporting
- Complete in-memory processing

### Medium Files (10-100 MB)

**Best Settings:**
```json
{
  "flush_every": 1000,
  "continueOnError": true
}
```

**Benefits:**
- Balanced memory usage
- Resilient to individual record errors
- Good throughput

### Large Files (100 MB - 1 GB)

**Best Settings:**
```json
{
  "flush_every": 500,
  "continueOnError": true,
  "ignoreBrokenFiles": false
}
```

**Benefits:**
- Controlled memory usage
- Progress visibility
- Fail fast on file corruption

### Very Large Files (> 1 GB)

**Best Settings:**
```json
{
  "flush_every": 100,
  "continueOnError": true,
  "ignoreBrokenFiles": true
}
```

**Additional Recommendations:**
- Consider splitting files before processing
- Use streaming mode (when available)
- Process during off-peak hours
- Monitor disk I/O

---

## Batch Processing

### Processing Multiple Files

When processing multiple files in a single run, consider:

#### Sequential Processing

**Best for:**
- Large files (> 100 MB each)
- Memory-constrained environments
- Guaranteed order of processing

**Command:**
```bash
multiparse config.json file1.csv file2.csv file3.csv
```

#### Parallel Processing (External)

**Best for:**
- Many small-medium files
- Multi-core systems
- Time-critical processing

**Example using GNU Parallel:**
```bash
ls input/*.csv | parallel -j 4 'multiparse config.json {} -o output/{/.}.out.csv'
```

**Example using Python multiprocessing:**
```python
from multiprocessing import Pool
from pathlib import Path
from multi_format_parser.orchestrator import parse_files

def process_file(file_path):
    parse_files(
        config_path=Path("config.json"),
        input_files=[file_path],
        output_dir=Path("output"),
        fail_fast=False
    )

if __name__ == "__main__":
    files = list(Path("input").glob("*.csv"))
    with Pool(4) as pool:
        pool.map(process_file, files)
```

---

## XML Performance

### XPath Compilation Caching

The parser automatically caches compiled XPath expressions using LRU cache (max 256 expressions).

**Performance Impact:**
- First record: ~10ms per XPath compilation
- Cached records: ~0.1ms per XPath evaluation
- **Speedup: 100x for repeated XPath patterns**

#### When Caching Helps Most

1. **Large XML files** with repeated structure (1000+ records)
2. **Complex XPath expressions** (nested predicates, functions)
3. **Multiple fields per record** (each field path is cached)

#### Monitoring Cache Performance

```python
from multi_format_parser.parsers.xml_parser import compile_xpath

# Check cache statistics
cache_info = compile_xpath.cache_info()
print(f"Hits: {cache_info.hits}, Misses: {cache_info.misses}")
print(f"Hit rate: {cache_info.hits / (cache_info.hits + cache_info.misses) * 100:.1f}%")
```

#### Clear Cache (if needed)

```python
from multi_format_parser.parsers.xml_parser import clear_xpath_cache

# Clear cache between processing different XML schemas
clear_xpath_cache()
```

### XML Namespace Performance

**Recommendation:** Explicitly define namespaces in config

```json
{
  "format": "xml",
  "namespaces": {
    "ns": "http://example.com/schema",
    "soap": "http://schemas.xmlsoap.org/soap/envelope/"
  }
}
```

**Benefits:**
- Faster namespace resolution
- Explicit control over prefixes
- Avoids auto-detection overhead

---

## CSV Performance

### Large CSV Files

#### Optimal Settings

```json
{
  "format": "csv",
  "delimiter": ",",
  "has_header": true,
  "flush_every": 5000,
  "encoding": "utf-8"
}
```

#### Performance Tips

1. **Use correct encoding**: Avoid auto-detection overhead
2. **Set has_header correctly**: Prevents header row processing
3. **Higher flush_every**: CSV rows are typically smaller than XML/JSON
4. **Avoid complex type casting**: Use "string" type when possible

#### Benchmarks (Approximate)

| Records | File Size | flush_every=1000 | flush_every=5000 | Memory |
|---------|-----------|------------------|------------------|--------|
| 10,000 | 5 MB | 2s | 1.8s | 20 MB |
| 100,000 | 50 MB | 20s | 18s | 50 MB |
| 1,000,000 | 500 MB | 200s | 180s | 150 MB |

---

## JSON Performance

### JSON Schema Validation

**Performance Impact:**
- Validation adds ~10-30% processing time
- Scales linearly with data size
- Provides comprehensive error reporting

#### When to Use Schema Validation

**Enable when:**
- Data quality is critical
- Validating external/untrusted sources
- Development/testing phase
- First-time processing of new data sources

**Disable when:**
- Processing trusted internal data
- Production batch jobs (after validation)
- Performance is critical
- Very large files (> 500 MB)

#### Example Configuration

```json
{
  "format": "json",
  "json_schema_path": "schema/users.schema.json",
  "flush_every": 1000
}
```

### Nested JSON Structures

**Performance Tip:** Use specific selectors to reduce traversal

**Slower (traverses entire tree):**
```json
{
  "select": "$"
}
```

**Faster (direct path):**
```json
{
  "select": "$.data.users"
}
```

---

## Monitoring and Profiling

### Progress Logging

Enable INFO logging to monitor progress:

```bash
multiparse config.json input.xml -v
```

**Output:**
```
INFO - Processing 'orders' record 1000/5000
INFO - Processing 'orders' record 2000/5000
...
```

### Memory Profiling

Use `memory_profiler` to track memory usage:

```bash
pip install memory-profiler
python -m memory_profiler your_script.py
```

### Time Profiling

Use Python's `cProfile`:

```python
import cProfile
import pstats
from pathlib import Path
from multi_format_parser.orchestrator import parse_files

profiler = cProfile.Profile()
profiler.enable()

parse_files(
    config_path=Path("config.json"),
    input_files=[Path("large_file.xml")],
    output_dir=Path("output")
)

profiler.disable()
stats = pstats.Stats(profiler)
stats.sort_stats('cumulative')
stats.print_stats(20)  # Top 20 functions
```

### System Resource Monitoring

**Linux/Mac:**
```bash
# Monitor CPU and memory in real-time
top -p $(pgrep -f multiparse)

# Check I/O statistics
iostat -x 5
```

**Python script monitoring:**
```python
import psutil
import os

process = psutil.Process(os.getpid())

# Memory usage
mem_info = process.memory_info()
print(f"RSS: {mem_info.rss / 1024 / 1024:.1f} MB")

# CPU usage
cpu_percent = process.cpu_percent(interval=1.0)
print(f"CPU: {cpu_percent:.1f}%")
```

---

## Performance Tuning Checklist

### Before Processing

- [ ] Estimate file size and record count
- [ ] Set appropriate `flush_every` value
- [ ] Choose correct encoding
- [ ] Enable/disable schema validation
- [ ] Test with dry_run first

### During Processing

- [ ] Monitor memory usage
- [ ] Check log output for errors
- [ ] Verify output file growth
- [ ] Watch for I/O bottlenecks

### After Processing

- [ ] Validate output completeness
- [ ] Check error logs
- [ ] Review performance statistics
- [ ] Adjust settings if needed

---

## Common Performance Issues

### Issue: Out of Memory Errors

**Symptoms:**
- Process killed by OS
- `MemoryError` exceptions
- System becomes unresponsive

**Solutions:**
1. Reduce `flush_every` to 50-100
2. Process files individually
3. Split large files
4. Increase system swap space

### Issue: Slow Processing

**Symptoms:**
- Processing takes much longer than expected
- Low CPU usage
- High I/O wait

**Solutions:**
1. Check disk I/O (use SSD if possible)
2. Increase `flush_every` (if memory allows)
3. Simplify XPath/JSON path expressions
4. Disable unnecessary type casting
5. Use faster encoding (utf-8 vs auto-detect)

### Issue: High CPU Usage

**Symptoms:**
- CPU at 100%
- System responsive but slow progress

**Solutions:**
1. Simplify complex XPath expressions
2. Disable JSON schema validation (if enabled)
3. Use simpler type casting
4. Process during off-peak hours

---

## Advanced Topics

### Custom Flush Strategy

For very large files, consider implementing a custom flushing strategy:

```python
# Calculate optimal flush_every based on available memory
import psutil

available_memory_mb = psutil.virtual_memory().available / 1024 / 1024
avg_row_size_kb = 1  # Adjust based on your data
optimal_flush = int((available_memory_mb * 0.2) / avg_row_size_kb)

print(f"Recommended flush_every: {optimal_flush}")
```

### File Splitting

For files > 1 GB, consider splitting before processing:

**Split CSV:**
```bash
split -l 100000 large_file.csv chunk_ --additional-suffix=.csv
```

**Split XML (by record):**
```python
# Use xmlstarlet or custom script
xmlstarlet sel -t -c "//record[position() <= 10000]" large.xml > chunk1.xml
```

---

## Summary

**Quick Reference Table:**

| Scenario | flush_every | continueOnError | Other Settings |
|----------|-------------|-----------------|----------------|
| Small files (< 10 MB) | 5000-10000 | false | Default |
| Medium files (10-100 MB) | 1000-5000 | true | Default |
| Large files (100 MB - 1 GB) | 100-500 | true | Monitor memory |
| Very large files (> 1 GB) | 50-100 | true | Consider splitting |
| Batch processing | 1000 | true | Process sequentially |
| Memory-constrained | 50-100 | true | Lower priority |
| Performance-critical | 5000-10000 | false | SSD, more memory |

For questions or issues, consult the main README or file an issue on GitHub.
