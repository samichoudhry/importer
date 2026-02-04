"""
Utilities for handling compressed/archived files.

This module provides functionality to detect and extract various compressed
file formats (zip, gzip, tar.gz, bz2) before processing with the parser.
"""

import bz2
import gzip
import logging
import shutil
import tarfile
import tempfile
import zipfile
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)


def is_zip_file(file_path: Path) -> bool:
    """Check if a file is a valid zip archive.
    
    Args:
        file_path: Path to the file to check
        
    Returns:
        True if file is a valid zip archive, False otherwise
    """
    try:
        return zipfile.is_zipfile(file_path)
    except Exception:
        return False


def is_gzip_file(file_path: Path) -> bool:
    """Check if a file is a valid gzip compressed file.
    
    Uses magic bytes detection (1f 8b).
    
    Args:
        file_path: Path to the file to check
        
    Returns:
        True if file is a valid gzip file, False otherwise
    """
    try:
        with open(file_path, 'rb') as f:
            magic = f.read(2)
            return magic == b'\x1f\x8b'
    except Exception:
        return False


def is_bz2_file(file_path: Path) -> bool:
    """Check if a file is a valid bzip2 compressed file.
    
    Uses magic bytes detection (42 5a 68).
    
    Args:
        file_path: Path to the file to check
        
    Returns:
        True if file is a valid bzip2 file, False otherwise
    """
    try:
        with open(file_path, 'rb') as f:
            magic = f.read(3)
            return magic == b'BZh'
    except Exception:
        return False


def is_tar_file(file_path: Path) -> bool:
    """Check if a file is a valid tar archive (including tar.gz, tar.bz2).
    
    Args:
        file_path: Path to the file to check
        
    Returns:
        True if file is a valid tar archive, False otherwise
    """
    try:
        return tarfile.is_tarfile(file_path)
    except Exception:
        return False


def is_compressed_file(file_path: Path) -> bool:
    """Check if a file is any supported compressed/archived format.
    
    Checks for: zip, gzip, bzip2, tar (including tar.gz, tar.bz2)
    
    Args:
        file_path: Path to check
        
    Returns:
        True if file is compressed/archived, False otherwise
    """
    return (is_zip_file(file_path) or 
            is_gzip_file(file_path) or 
            is_bz2_file(file_path) or 
            is_tar_file(file_path))


def extract_gzip_file(gz_path: Path, extract_to: Optional[Path] = None) -> List[Path]:
    """Extract a gzip compressed file and return the extracted file path.
    
    Gzip compresses a single file. The extracted filename is derived from
    the gzip filename by removing the .gz extension.
    
    Args:
        gz_path: Path to the gzip file to extract
        extract_to: Directory to extract to. If None, creates temp directory.
        
    Returns:
        List containing single Path to extracted file
        
    Raises:
        OSError: If the file cannot be read or decompressed
    """
    if not is_gzip_file(gz_path):
        raise ValueError(f"Not a valid gzip file: {gz_path}")
    
    # Use provided directory or create temp directory
    if extract_to is None:
        extract_to = Path(tempfile.mkdtemp(prefix="parser_gz_"))
    else:
        extract_to.mkdir(parents=True, exist_ok=True)
    
    # Determine output filename (remove .gz extension)
    if gz_path.stem:
        output_name = gz_path.stem
    else:
        output_name = gz_path.name.replace('.gz', '')
    
    output_path = extract_to / output_name
    
    logger.info(f"Extracting gzip file: {gz_path.name}")
    
    # Extract the gzip file
    with gzip.open(gz_path, 'rb') as f_in:
        with open(output_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    
    logger.info(f"Successfully extracted to {output_path}")
    
    return [output_path]


def extract_bz2_file(bz2_path: Path, extract_to: Optional[Path] = None) -> List[Path]:
    """Extract a bzip2 compressed file and return the extracted file path.
    
    Bzip2 compresses a single file. The extracted filename is derived from
    the bz2 filename by removing the .bz2 extension.
    
    Args:
        bz2_path: Path to the bzip2 file to extract
        extract_to: Directory to extract to. If None, creates temp directory.
        
    Returns:
        List containing single Path to extracted file
        
    Raises:
        OSError: If the file cannot be read or decompressed
    """
    if not is_bz2_file(bz2_path):
        raise ValueError(f"Not a valid bzip2 file: {bz2_path}")
    
    # Use provided directory or create temp directory
    if extract_to is None:
        extract_to = Path(tempfile.mkdtemp(prefix="parser_bz2_"))
    else:
        extract_to.mkdir(parents=True, exist_ok=True)
    
    # Determine output filename (remove .bz2 extension)
    if bz2_path.stem:
        output_name = bz2_path.stem
    else:
        output_name = bz2_path.name.replace('.bz2', '')
    
    output_path = extract_to / output_name
    
    logger.info(f"Extracting bzip2 file: {bz2_path.name}")
    
    # Extract the bz2 file
    with bz2.open(bz2_path, 'rb') as f_in:
        with open(output_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    
    logger.info(f"Successfully extracted to {output_path}")
    
    return [output_path]


def extract_tar_file(tar_path: Path, extract_to: Optional[Path] = None) -> List[Path]:
    """Extract a tar archive and return list of extracted file paths.
    
    Supports tar, tar.gz (.tgz), and tar.bz2 archives.
    
    Args:
        tar_path: Path to the tar file to extract
        extract_to: Directory to extract to. If None, creates temp directory.
        
    Returns:
        List of Paths to extracted files
        
    Raises:
        tarfile.TarError: If the file is not a valid tar archive
        OSError: If extraction fails
    """
    if not is_tar_file(tar_path):
        raise tarfile.TarError(f"Not a valid tar file: {tar_path}")
    
    # Use provided directory or create temp directory
    if extract_to is None:
        extract_to = Path(tempfile.mkdtemp(prefix="parser_tar_"))
    else:
        extract_to.mkdir(parents=True, exist_ok=True)
    
    extracted_files: List[Path] = []
    
    logger.info(f"Extracting tar archive: {tar_path.name}")
    
    with tarfile.open(tar_path, 'r:*') as tar:
        # Get list of files (not directories)
        members = [m for m in tar.getmembers() if m.isfile()]
        
        if not members:
            logger.warning(f"Tar archive {tar_path.name} contains no files")
            return extracted_files
        
        logger.info(f"Found {len(members)} file(s) in archive")
        
        # Extract all files
        tar.extractall(extract_to)
        
        # Build list of extracted file paths
        for member in members:
            extracted_path = extract_to / member.name
            if extracted_path.exists() and extracted_path.is_file():
                extracted_files.append(extracted_path)
                logger.debug(f"Extracted: {member.name}")
            else:
                logger.warning(f"Failed to extract or locate: {member.name}")
    
    logger.info(f"Successfully extracted {len(extracted_files)} file(s) to {extract_to}")
    
    return extracted_files
    """Extract a zip archive and return list of extracted file paths.
    
    Args:
        zip_path: Path to the zip file to extract
        extract_to: Directory to extract to. If None, creates temp directory.
        
    Returns:
        List of Paths to extracted files
        
    Raises:
        zipfile.BadZipFile: If the file is not a valid zip archive
        PermissionError: If lacking permissions to extract
        OSError: If extraction fails
    """
    if not is_zip_file(zip_path):
        raise zipfile.BadZipFile(f"Not a valid zip file: {zip_path}")
    
    # Use provided directory or create temp directory
    if extract_to is None:
        # Create temp directory that will be cleaned up by caller
        extract_to = Path(tempfile.mkdtemp(prefix="parser_zip_"))
    else:
        extract_to.mkdir(parents=True, exist_ok=True)
    
    extracted_files: List[Path] = []
    
    logger.info(f"Extracting zip archive: {zip_path.name}")
    
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        # Get list of files in the archive (excluding directories)
        zip_members = [m for m in zip_ref.namelist() if not m.endswith('/')]
        
        if not zip_members:
            logger.warning(f"Zip archive {zip_path.name} contains no files")
            return extracted_files
        
        logger.info(f"Found {len(zip_members)} file(s) in archive")
        
        # Extract all files
        zip_ref.extractall(extract_to)
        
        # Build list of extracted file paths
        for member in zip_members:
            extracted_path = extract_to / member
            if extracted_path.exists() and extracted_path.is_file():
                extracted_files.append(extracted_path)
                logger.debug(f"Extracted: {member}")
            else:
                logger.warning(f"Failed to extract or locate: {member}")
    
    logger.info(f"Successfully extracted {len(extracted_files)} file(s) to {extract_to}")
    
    return extracted_files


def extract_zip_file(zip_path: Path, extract_to: Optional[Path] = None) -> List[Path]:
    """Extract a zip archive and return list of extracted file paths.
    
    Args:
        zip_path: Path to the zip file to extract
        extract_to: Directory to extract to. If None, creates temp directory.
        
    Returns:
        List of Paths to extracted files
        
    Raises:
        zipfile.BadZipFile: If the file is not a valid zip archive
        PermissionError: If lacking permissions to extract
        OSError: If extraction fails
    """
    if not is_zip_file(zip_path):
        raise zipfile.BadZipFile(f"Not a valid zip file: {zip_path}")
    
    # Use provided directory or create temp directory
    if extract_to is None:
        # Create temp directory that will be cleaned up by caller
        extract_to = Path(tempfile.mkdtemp(prefix="parser_zip_"))
    else:
        extract_to.mkdir(parents=True, exist_ok=True)
    
    extracted_files: List[Path] = []
    
    logger.info(f"Extracting zip archive: {zip_path.name}")
    
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        # Get list of files in the archive (excluding directories)
        zip_members = [m for m in zip_ref.namelist() if not m.endswith('/')]
        
        if not zip_members:
            logger.warning(f"Zip archive {zip_path.name} contains no files")
            return extracted_files
        
        logger.info(f"Found {len(zip_members)} file(s) in archive")
        
        # Extract all files
        zip_ref.extractall(extract_to)
        
        # Build list of extracted file paths
        for member in zip_members:
            extracted_path = extract_to / member
            if extracted_path.exists() and extracted_path.is_file():
                extracted_files.append(extracted_path)
                logger.debug(f"Extracted: {member}")
            else:
                logger.warning(f"Failed to extract or locate: {member}")
    
    logger.info(f"Successfully extracted {len(extracted_files)} file(s) to {extract_to}")
    
    return extracted_files


def extract_compressed_file(file_path: Path, extract_to: Optional[Path] = None) -> List[Path]:
    """Automatically detect compression format and extract.
    
    Supports: zip, gzip (.gz), bzip2 (.bz2), tar, tar.gz (.tgz), tar.bz2
    
    Args:
        file_path: Path to the compressed file
        extract_to: Directory to extract to. If None, creates temp directory.
        
    Returns:
        List of Paths to extracted files
        
    Raises:
        ValueError: If file format is not supported or not compressed
    """
    if is_tar_file(file_path):
        # Check tar first because tar.gz is both gzip and tar
        return extract_tar_file(file_path, extract_to)
    elif is_zip_file(file_path):
        return extract_zip_file(file_path, extract_to)
    elif is_gzip_file(file_path):
        return extract_gzip_file(file_path, extract_to)
    elif is_bz2_file(file_path):
        return extract_bz2_file(file_path, extract_to)
    else:
        raise ValueError(f"File is not a supported compressed format: {file_path}")


def get_supported_extensions() -> List[str]:
    """Get list of supported compressed file extensions.
    
    Returns:
        List of file extensions (with dot prefix)
    """
    return ['.zip', '.gz', '.bz2', '.tar', '.tgz', '.tar.gz', '.tar.bz2']
