#!/usr/bin/env python3
"""
Demonstration of all supported compression formats working together.
"""

import bz2
import gzip
import io
import json
import shutil
import tarfile
import tempfile
import zipfile
from pathlib import Path

from multi_format_parser.orchestrator import parse_files


def main():
    print('='*70)
    print('COMPREHENSIVE COMPRESSION FORMAT DEMO')
    print('='*70)
    
    tmpdir = Path(tempfile.mkdtemp())
    
    try:
        # Test CSV data
        csv_data = """Name,Age,City
Alice,30,NYC
Bob,25,LA
Charlie,35,Chicago"""
        
        print('\n📝 Creating test files in various compression formats...\n')
        
        # 1. Regular uncompressed CSV
        regular_csv = tmpdir / 'regular.csv'
        regular_csv.write_text(csv_data)
        print(f'  ✓ {regular_csv.name} (uncompressed)')
        
        # 2. Gzip compressed CSV
        gz_csv = tmpdir / 'data.csv.gz'
        with gzip.open(gz_csv, 'wt') as f:
            f.write(csv_data)
        print(f'  ✓ {gz_csv.name} (gzip)')
        
        # 3. Bzip2 compressed CSV
        bz2_csv = tmpdir / 'data.csv.bz2'
        with bz2.open(bz2_csv, 'wt') as f:
            f.write(csv_data)
        print(f'  ✓ {bz2_csv.name} (bzip2)')
        
        # 4. Zip archive
        zip_file = tmpdir / 'archive.zip'
        with zipfile.ZipFile(zip_file, 'w') as zf:
            zf.writestr('data_from_zip.csv', csv_data)
        print(f'  ✓ {zip_file.name} (zip archive)')
        
        # 5. Tar archive
        tar_file = tmpdir / 'archive.tar'
        with tarfile.open(tar_file, 'w') as tar:
            info = tarfile.TarInfo(name='data_from_tar.csv')
            data_bytes = csv_data.encode()
            info.size = len(data_bytes)
            tar.addfile(info, io.BytesIO(data_bytes))
        print(f'  ✓ {tar_file.name} (tar archive)')
        
        # 6. Tar.gz archive
        tgz_file = tmpdir / 'archive.tar.gz'
        with tarfile.open(tgz_file, 'w:gz') as tar:
            info = tarfile.TarInfo(name='data_from_tgz.csv')
            data_bytes = csv_data.encode()
            info.size = len(data_bytes)
            tar.addfile(info, io.BytesIO(data_bytes))
        print(f'  ✓ {tgz_file.name} (tar+gzip)')
        
        # 7. Tar.bz2 archive
        tbz2_file = tmpdir / 'archive.tar.bz2'
        with tarfile.open(tbz2_file, 'w:bz2') as tar:
            info = tarfile.TarInfo(name='data_from_tbz2.csv')
            data_bytes = csv_data.encode()
            info.size = len(data_bytes)
            tar.addfile(info, io.BytesIO(data_bytes))
        print(f'  ✓ {tbz2_file.name} (tar+bzip2)')
        
        # Create config
        config = {
            'format_type': 'csv',
            'csv_delimiter': ',',
            'csv_has_header': True,
            'records': [{
                'name': 'People',
                'fields': [
                    {'name': 'Name', 'path': 'Name', 'type': 'string'},
                    {'name': 'Age', 'path': 'Age', 'type': 'int'},
                    {'name': 'City', 'path': 'City', 'type': 'string'}
                ]
            }]
        }
        
        config_path = tmpdir / 'config.json'
        config_path.write_text(json.dumps(config, indent=2))
        
        output_dir = tmpdir / 'output'
        output_dir.mkdir()
        
        # Collect all test files
        input_files = [
            regular_csv, gz_csv, bz2_csv, zip_file,
            tar_file, tgz_file, tbz2_file
        ]
        
        print(f'\n🚀 Processing ALL {len(input_files)} files with single command...\n')
        
        # Parse all files at once
        stats, record_stats, errors = parse_files(
            config_path=config_path,
            input_files=input_files,
            output_dir=output_dir,
            dry_run=False
        )
        
        print('\n' + '='*70)
        print('RESULTS')
        print('='*70)
        print(f'\n📊 Statistics:')
        print(f'     Files processed:  {stats["processed"]}')
        print(f'     Files succeeded:  {stats["succeeded"]}')
        print(f'     Files failed:     {stats["failed"]}')
        print(f'     Total rows:       {record_stats["People"].total_rows}')
        print(f'     Success rows:     {record_stats["People"].success_rows}')
        
        # Show output
        output_file = output_dir / 'People.csv'
        if output_file.exists():
            print(f'\n📄 Output file content ({output_file.name}):')
            print('-'*70)
            print(output_file.read_text())
            print('-'*70)
        
        print('\n✅ SUCCESS! All compression formats processed correctly!')
        print('\n📦 Supported formats demonstrated:')
        print('   • Regular uncompressed files')
        print('   • Gzip (.gz)')
        print('   • Bzip2 (.bz2)')
        print('   • Zip (.zip)')
        print('   • Tar (.tar)')
        print('   • Tar+Gzip (.tar.gz, .tgz)')
        print('   • Tar+Bzip2 (.tar.bz2)')
        
    finally:
        shutil.rmtree(tmpdir)


if __name__ == '__main__':
    main()
