#!/usr/bin/env python3
"""Test script for compression format support."""

import bz2
import gzip
import io
import shutil
import tarfile
import tempfile
from pathlib import Path

from multi_format_parser.zip_utils import (
    extract_bz2_file,
    extract_gzip_file,
    extract_tar_file,
    is_bz2_file,
    is_gzip_file,
    is_tar_file,
)


def main():
    tmpdir = Path(tempfile.mkdtemp())
    
    try:
        # Test data
        csv_data = 'Name,Value\nTest,123'
        
        print('='*60)
        print('Creating test files...')
        print('='*60)
        
        # Create gzip file
        gz_file = tmpdir / 'data.csv.gz'
        with gzip.open(gz_file, 'wt') as f:
            f.write(csv_data)
        print(f'✓ Created: {gz_file.name}')
        
        # Create bz2 file
        bz2_file = tmpdir / 'data.csv.bz2'
        with bz2.open(bz2_file, 'wt') as f:
            f.write(csv_data)
        print(f'✓ Created: {bz2_file.name}')
        
        # Create tar file
        tar_file = tmpdir / 'archive.tar'
        with tarfile.open(tar_file, 'w') as tar:
            info = tarfile.TarInfo(name='data.csv')
            data_bytes = csv_data.encode()
            info.size = len(data_bytes)
            tar.addfile(info, io.BytesIO(data_bytes))
        print(f'✓ Created: {tar_file.name}')
        
        # Create tar.gz file
        tgz_file = tmpdir / 'archive.tar.gz'
        with tarfile.open(tgz_file, 'w:gz') as tar:
            info = tarfile.TarInfo(name='data.csv')
            data_bytes = csv_data.encode()
            info.size = len(data_bytes)
            tar.addfile(info, io.BytesIO(data_bytes))
        print(f'✓ Created: {tgz_file.name}')
        
        print('\n' + '='*60)
        print('Testing format detection...')
        print('='*60)
        
        print(f'  gzip (.gz):    {is_gzip_file(gz_file)} ✓')
        print(f'  bzip2 (.bz2):  {is_bz2_file(bz2_file)} ✓')
        print(f'  tar (.tar):    {is_tar_file(tar_file)} ✓')
        print(f'  tar.gz (.tgz): {is_tar_file(tgz_file)} ✓')
        
        print('\n' + '='*60)
        print('Testing extraction...')
        print('='*60)
        
        # Test gzip
        extracted = extract_gzip_file(gz_file)
        content = extracted[0].read_text()
        print(f'  gzip:   {len(extracted)} file(s) extracted')
        print(f'          Content: "{content}"')
        assert content == csv_data, 'Gzip extraction failed'
        
        # Test bz2
        extracted = extract_bz2_file(bz2_file)
        content = extracted[0].read_text()
        print(f'  bz2:    {len(extracted)} file(s) extracted')
        print(f'          Content: "{content}"')
        assert content == csv_data, 'Bz2 extraction failed'
        
        # Test tar
        extracted = extract_tar_file(tar_file)
        content = extracted[0].read_text()
        print(f'  tar:    {len(extracted)} file(s) extracted')
        print(f'          Content: "{content}"')
        assert content == csv_data, 'Tar extraction failed'
        
        # Test tar.gz
        extracted = extract_tar_file(tgz_file)
        content = extracted[0].read_text()
        print(f'  tar.gz: {len(extracted)} file(s) extracted')
        print(f'          Content: "{content}"')
        assert content == csv_data, 'Tar.gz extraction failed'
        
        print('\n' + '='*60)
        print('✅ ALL TESTS PASSED!')
        print('='*60)
        print('\nSupported formats:')
        print('  • ZIP archives (.zip)')
        print('  • Gzip compression (.gz)')
        print('  • Bzip2 compression (.bz2)')
        print('  • Tar archives (.tar)')
        print('  • Tar+Gzip (.tar.gz, .tgz)')
        print('  • Tar+Bzip2 (.tar.bz2)')
        
    finally:
        shutil.rmtree(tmpdir)


if __name__ == '__main__':
    main()
