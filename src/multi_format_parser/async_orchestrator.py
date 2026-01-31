"""
Async orchestration for parallel file processing.

This module provides async/await support for processing multiple files
concurrently, improving throughput for batch operations.
"""

import asyncio
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from multi_format_parser.models import ParsingStats
from multi_format_parser.orchestrator import parse_files

logger = logging.getLogger(__name__)


async def parse_files_async(
    config_path: Path,
    input_files: List[Path],
    output_dir: Path,
    dry_run: bool = False,
    fail_fast: bool = False,
    max_concurrent: int = 4
) -> Tuple[Dict[str, int], Dict[str, ParsingStats], Dict[str, str]]:
    """Parse files asynchronously with controlled concurrency.
    
    This function processes multiple files concurrently using asyncio,
    which can significantly improve throughput for I/O-bound operations.
    
    Args:
        config_path: Path to configuration JSON file
        input_files: List of input files to process
        output_dir: Output directory for results
        dry_run: If True, parse and validate but don't write outputs
        fail_fast: If True, stop on first file error
        max_concurrent: Maximum number of files to process concurrently
        
    Returns:
        Tuple: (stats dict, record_stats dict, file_errors dict)
        
    Example:
        >>> import asyncio
        >>> from pathlib import Path
        >>>
        >>> config = Path("config.json")
        >>> files = [Path(f"input/file{i}.csv") for i in range(10)]
        >>> output = Path("output")
        >>>
        >>> stats, record_stats, errors = asyncio.run(
        ...     parse_files_async(config, files, output, max_concurrent=4)
        ... )
    """
    # Initialize aggregated results
    total_stats = {"processed": 0, "succeeded": 0, "failed": 0}
    all_record_stats: Dict[str, ParsingStats] = {}
    all_errors: Dict[str, str] = {}
    
    # Create semaphore to limit concurrency
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def process_one_file(file_path: Path) -> Tuple[bool, Optional[str]]:
        """Process a single file with semaphore control."""
        async with semaphore:
            # Run synchronous parse_files in executor to avoid blocking
            loop = asyncio.get_event_loop()
            
            try:
                stats, record_stats, errors = await loop.run_in_executor(
                    None,
                    parse_files,
                    config_path,
                    [file_path],
                    output_dir,
                    dry_run,
                    fail_fast
                )
                
                # Merge results
                total_stats["processed"] += stats["processed"]
                total_stats["succeeded"] += stats["succeeded"]
                total_stats["failed"] += stats["failed"]
                
                all_record_stats.update(record_stats)
                all_errors.update(errors)
                
                success = stats["succeeded"] > 0
                error_msg = errors.get(str(file_path)) if errors else None
                
                return success, error_msg
                
            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}")
                total_stats["processed"] += 1
                total_stats["failed"] += 1
                all_errors[str(file_path)] = str(e)
                return False, str(e)
    
    # Process files concurrently
    tasks = [process_one_file(file_path) for file_path in input_files]
    
    if fail_fast:
        # Stop on first error
        for coro in asyncio.as_completed(tasks):
            success, error = await coro
            if not success and fail_fast:
                # Cancel remaining tasks
                for task in tasks:
                    if not task.done():
                        task.cancel()
                break
    else:
        # Process all files regardless of errors
        await asyncio.gather(*tasks, return_exceptions=True)
    
    return total_stats, all_record_stats, all_errors


async def parse_file_batches_async(
    config_path: Path,
    input_files: List[Path],
    output_dir: Path,
    dry_run: bool = False,
    fail_fast: bool = False,
    batch_size: int = 10,
    max_concurrent: int = 4
) -> Tuple[Dict[str, int], Dict[str, ParsingStats], Dict[str, str]]:
    """Parse files in batches asynchronously.
    
    Useful for processing a very large number of files by breaking them
    into manageable batches.
    
    Args:
        config_path: Path to configuration JSON file
        input_files: List of input files to process
        output_dir: Output directory for results
        dry_run: If True, parse and validate but don't write outputs
        fail_fast: If True, stop on first file error
        batch_size: Number of files per batch
        max_concurrent: Maximum concurrent files per batch
        
    Returns:
        Tuple: (stats dict, record_stats dict, file_errors dict)
    """
    total_stats = {"processed": 0, "succeeded": 0, "failed": 0}
    all_record_stats: Dict[str, ParsingStats] = {}
    all_errors: Dict[str, str] = {}
    
    # Process in batches
    for i in range(0, len(input_files), batch_size):
        batch = input_files[i:i + batch_size]
        
        logger.info(f"Processing batch {i // batch_size + 1} "
                   f"({len(batch)} files, {i + 1}-{i + len(batch)} of {len(input_files)})")
        
        stats, record_stats, errors = await parse_files_async(
            config_path=config_path,
            input_files=batch,
            output_dir=output_dir,
            dry_run=dry_run,
            fail_fast=fail_fast,
            max_concurrent=max_concurrent
        )
        
        # Merge results
        total_stats["processed"] += stats["processed"]
        total_stats["succeeded"] += stats["succeeded"]
        total_stats["failed"] += stats["failed"]
        
        all_record_stats.update(record_stats)
        all_errors.update(errors)
        
        if fail_fast and errors:
            logger.warning("Stopping batch processing due to fail_fast")
            break
    
    return total_stats, all_record_stats, all_errors


# Convenience function for simple async processing
def run_async_parse(
    config_path: Path,
    input_files: List[Path],
    output_dir: Path,
    dry_run: bool = False,
    fail_fast: bool = False,
    max_concurrent: int = 4
) -> Tuple[Dict[str, int], Dict[str, ParsingStats], Dict[str, str]]:
    """Synchronous wrapper for async file parsing.
    
    This is a convenience function that handles the asyncio event loop
    for you. Use this when calling from synchronous code.
    
    Args:
        config_path: Path to configuration JSON file
        input_files: List of input files to process
        output_dir: Output directory for results
        dry_run: If True, parse and validate but don't write outputs
        fail_fast: If True, stop on first file error
        max_concurrent: Maximum number of files to process concurrently
        
    Returns:
        Tuple: (stats dict, record_stats dict, file_errors dict)
        
    Example:
        >>> from pathlib import Path
        >>> from multi_format_parser.async_orchestrator import run_async_parse
        >>>
        >>> config = Path("config.json")
        >>> files = list(Path("input").glob("*.csv"))
        >>> output = Path("output")
        >>>
        >>> stats, record_stats, errors = run_async_parse(
        ...     config, files, output, max_concurrent=4
        ... )
    """
    return asyncio.run(
        parse_files_async(
            config_path=config_path,
            input_files=input_files,
            output_dir=output_dir,
            dry_run=dry_run,
            fail_fast=fail_fast,
            max_concurrent=max_concurrent
        )
    )
