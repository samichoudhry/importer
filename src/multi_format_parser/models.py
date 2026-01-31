"""
Data models and structures for the multi-format parser.
"""

import time
from dataclasses import dataclass, field
from typing import Any, List, Optional


@dataclass
class FieldDef:
    """Field definition."""
    name: str
    path: Optional[str] = None
    type: str = "string"
    nullable: bool = True
    computed_field: Optional[str] = None
    start: Optional[int] = None
    end: Optional[int] = None
    width: Optional[int] = None
    regex: Optional[str] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    default: Optional[Any] = None


@dataclass
class ContextDef:
    """Context definition."""
    name: str
    from_expr: Optional[str] = None
    value: Optional[Any] = None


@dataclass
class RecordDef:
    """Record definition."""
    name: str
    select: Optional[str] = None
    context: List[ContextDef] = field(default_factory=list)
    fields: List[FieldDef] = field(default_factory=list)


@dataclass
class ParsingStats:
    """Parsing statistics."""
    total_rows: int = 0
    success_rows: int = 0
    failed_rows: int = 0
    skipped_rows: int = 0  # Count of rows skipped (empty, blank, or continued on error)
    validation_errors: int = 0
    file_parse_failures: int = 0  # Count of files that failed to parse (ignoreBrokenFiles mode)
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None

    @property
    def duration(self) -> float:
        """Get parsing duration in seconds."""
        end = self.end_time or time.time()
        return end - self.start_time

    @property
    def rows_per_second(self) -> float:
        """Get processing throughput."""
        duration = self.duration
        return self.success_rows / duration if duration > 0 else 0
