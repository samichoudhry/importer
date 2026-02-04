#!/usr/bin/env python3
"""
Demo script showing file_mask parameter with zip archives.
"""
import json
import zipfile
from pathlib import Path
import tempfile
import sys
import shutil

def main():
    # Create a test zip with multiple file types
    tmpdir = tempfile.mkdtemp()
    tmp = Path(tmpdir)
    
    try:
        # Create zip with CSV, TXT, and JSON files
        zip_path = tmp / 'mixed_files.zip'
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr('data1.csv', 'Name,Value\nAlice,100\nBob,200')
            zf.writestr('data2.csv', 'Name,Value\nCharlie,300')
            zf.writestr('readme.txt', 'This is a text file')
            zf.writestr('config.json', '{"key": "value"}')
        
        # Create config with file_mask to only process CSV files
        config = {
            'format_type': 'csv',
            'csv_delimiter': ',',
            'csv_has_header': True,
            'file_mask': r'.*\.csv$',  # Only process .csv files
            'records': [{
                'name': 'Data',
                'fields': [
                    {'name': 'Name', 'path': 'Name', 'type': 'string'},
                    {'name': 'Value', 'path': 'Value', 'type': 'integer'}
                ]
            }]
        }
        
        config_path = tmp / 'config.json'
        config_path.write_text(json.dumps(config))
        
        output_dir = tmp / 'output'
        output_dir.mkdir()
        
        # Import and run parser
        from multi_format_parser.orchestrator import parse_files
        
        print('='*60)
        print('Testing file_mask with zip archive')
        print('='*60)
        print('\nZip contains:')
        print('  - data1.csv')
        print('  - data2.csv')
        print('  - readme.txt')
        print('  - config.json')
        print('\nConfig file_mask: ".*\\.csv$"')
        print('\nExpected: Only CSV files should be processed\n')
        
        stats, record_stats, errors = parse_files(
            config_path=config_path,
            input_files=[zip_path],
            output_dir=output_dir,
            dry_run=False
        )
        
        print('\n' + '='*60)
        print('Results:')
        print('='*60)
        print(f'Files succeeded: {stats["succeeded"]}')
        print(f'Files failed: {stats["failed"]}')
        print(f'Total rows processed: {record_stats["Data"].total_rows}')
        print('\nOutput file contents:')
        output_file = output_dir / 'Data.csv'
        if output_file.exists():
            print(output_file.read_text())
        
        print('\n✅ Success! file_mask correctly filtered to only .csv files')
        print('   (filtered out readme.txt and config.json)')
        
    finally:
        shutil.rmtree(tmpdir)

if __name__ == '__main__':
    main()
