"""
Observability and metrics hooks for monitoring parser performance.

This module provides a flexible callback system for collecting metrics,
logging events, and integrating with monitoring systems.
"""

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class MetricType(Enum):
    """Types of metrics that can be emitted."""
    COUNTER = "counter"  # Monotonically increasing count
    GAUGE = "gauge"  # Point-in-time value
    HISTOGRAM = "histogram"  # Distribution of values
    TIMER = "timer"  # Duration measurement


class EventType(Enum):
    """Types of events that can be emitted."""
    FILE_START = "file_start"
    FILE_COMPLETE = "file_complete"
    FILE_ERROR = "file_error"
    RECORD_START = "record_start"
    RECORD_COMPLETE = "record_complete"
    ROW_PROCESSED = "row_processed"
    ROW_ERROR = "row_error"
    VALIDATION_ERROR = "validation_error"
    FLUSH = "flush"


@dataclass
class MetricEvent:
    """Represents a metric event."""
    metric_type: MetricType
    name: str
    value: float
    timestamp: float = field(default_factory=time.time)
    tags: Dict[str, str] = field(default_factory=dict)

    def __str__(self) -> str:
        tags_str = ",".join(f"{k}={v}" for k, v in self.tags.items())
        return f"{self.name}:{self.value}|{self.metric_type.value}|{tags_str}"


@dataclass
class Event:
    """Represents a parsing event."""
    event_type: EventType
    timestamp: float = field(default_factory=time.time)
    file_path: Optional[Path] = None
    record_name: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        parts = [self.event_type.value]
        if self.file_path:
            parts.append(f"file={self.file_path.name}")
        if self.record_name:
            parts.append(f"record={self.record_name}")
        if self.details:
            details_str = ",".join(f"{k}={v}" for k, v in self.details.items())
            parts.append(details_str)
        return " ".join(parts)


class ObservabilityHook:
    """Base class for observability hooks."""

    def on_metric(self, metric: MetricEvent) -> None:
        """Called when a metric is emitted."""
        pass

    def on_event(self, event: Event) -> None:
        """Called when an event occurs."""
        pass

    def on_error(self, error: Exception, context: Dict[str, Any]) -> None:
        """Called when an error occurs."""
        pass


class LoggingHook(ObservabilityHook):
    """Hook that logs metrics and events to Python logging."""

    def __init__(self, log_metrics: bool = True, log_events: bool = True):
        self.log_metrics = log_metrics
        self.log_events = log_events

    def on_metric(self, metric: MetricEvent) -> None:
        if self.log_metrics:
            logger.debug(f"METRIC: {metric}")

    def on_event(self, event: Event) -> None:
        if self.log_events:
            level = logging.WARNING if event.event_type in (EventType.FILE_ERROR, EventType.ROW_ERROR) else logging.INFO
            logger.log(level, f"EVENT: {event}")

    def on_error(self, error: Exception, context: Dict[str, Any]) -> None:
        logger.error(f"ERROR: {error} | Context: {context}")


class PrometheusHook(ObservabilityHook):
    """Hook that exports metrics to Prometheus format.
    
    Requires prometheus_client library:
        pip install prometheus-client
    """

    def __init__(self):
        try:
            from prometheus_client import Counter, Gauge, Histogram
            self.Counter = Counter
            self.Gauge = Gauge
            self.Histogram = Histogram
            self._metrics: Dict[str, Any] = {}
        except ImportError:
            raise ImportError(
                "prometheus_client is required for PrometheusHook. "
                "Install with: pip install prometheus-client"
            )

    def _get_or_create_metric(self, metric: MetricEvent):
        """Get or create a Prometheus metric."""
        key = f"{metric.name}_{metric.metric_type.value}"

        if key not in self._metrics:
            labels = list(metric.tags.keys())

            if metric.metric_type == MetricType.COUNTER:
                self._metrics[key] = self.Counter(
                    metric.name, f"Parser {metric.name}", labels
                )
            elif metric.metric_type == MetricType.GAUGE:
                self._metrics[key] = self.Gauge(
                    metric.name, f"Parser {metric.name}", labels
                )
            elif metric.metric_type == MetricType.HISTOGRAM:
                self._metrics[key] = self.Histogram(
                    metric.name, f"Parser {metric.name}", labels
                )

        return self._metrics[key]

    def on_metric(self, metric: MetricEvent) -> None:
        prom_metric = self._get_or_create_metric(metric)

        if metric.tags:
            prom_metric = prom_metric.labels(**metric.tags)

        if metric.metric_type == MetricType.COUNTER:
            prom_metric.inc(metric.value)
        elif metric.metric_type == MetricType.GAUGE:
            prom_metric.set(metric.value)
        elif metric.metric_type == MetricType.HISTOGRAM:
            prom_metric.observe(metric.value)


class StatsDHook(ObservabilityHook):
    """Hook that sends metrics to StatsD.
    
    Requires statsd library:
        pip install statsd
    """

    def __init__(self, host: str = "localhost", port: int = 8125, prefix: str = "parser"):
        try:
            import statsd
            self.client = statsd.StatsClient(host, port, prefix=prefix)
        except ImportError:
            raise ImportError(
                "statsd is required for StatsDHook. "
                "Install with: pip install statsd"
            )

    def on_metric(self, metric: MetricEvent) -> None:
        # Format metric name with tags
        name = metric.name
        if metric.tags:
            tags_str = ".".join(f"{k}.{v}" for k, v in metric.tags.items())
            name = f"{name}.{tags_str}"

        if metric.metric_type == MetricType.COUNTER:
            self.client.incr(name, int(metric.value))
        elif metric.metric_type == MetricType.GAUGE:
            self.client.gauge(name, metric.value)
        elif metric.metric_type == MetricType.HISTOGRAM:
            self.client.timing(name, metric.value)


class ObservabilityManager:
    """Manages observability hooks and emits metrics/events."""

    def __init__(self):
        self.hooks: List[ObservabilityHook] = []
        self._timers: Dict[str, float] = {}

    def register_hook(self, hook: ObservabilityHook) -> None:
        """Register an observability hook."""
        self.hooks.append(hook)

    def emit_metric(
        self,
        metric_type: MetricType,
        name: str,
        value: float,
        tags: Optional[Dict[str, str]] = None
    ) -> None:
        """Emit a metric to all registered hooks."""
        metric = MetricEvent(
            metric_type=metric_type,
            name=name,
            value=value,
            tags=tags or {}
        )

        for hook in self.hooks:
            try:
                hook.on_metric(metric)
            except Exception as e:
                logger.error(f"Error in observability hook: {e}")

    def emit_event(
        self,
        event_type: EventType,
        file_path: Optional[Path] = None,
        record_name: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ) -> None:
        """Emit an event to all registered hooks."""
        event = Event(
            event_type=event_type,
            file_path=file_path,
            record_name=record_name,
            details=details or {}
        )

        for hook in self.hooks:
            try:
                hook.on_event(event)
            except Exception as e:
                logger.error(f"Error in observability hook: {e}")

    def emit_error(self, error: Exception, context: Optional[Dict[str, Any]] = None) -> None:
        """Emit an error to all registered hooks."""
        for hook in self.hooks:
            try:
                hook.on_error(error, context or {})
            except Exception as e:
                logger.error(f"Error in observability hook: {e}")

    def start_timer(self, name: str) -> None:
        """Start a named timer."""
        self._timers[name] = time.time()

    def end_timer(self, name: str, tags: Optional[Dict[str, str]] = None) -> float:
        """End a named timer and emit the duration."""
        if name not in self._timers:
            logger.warning(f"Timer '{name}' was not started")
            return 0.0

        duration = time.time() - self._timers[name]
        del self._timers[name]

        self.emit_metric(
            metric_type=MetricType.TIMER,
            name=name,
            value=duration * 1000,  # Convert to milliseconds
            tags=tags
        )

        return duration

    # Convenience methods

    def counter(self, name: str, value: float = 1.0, tags: Optional[Dict[str, str]] = None) -> None:
        """Emit a counter metric."""
        self.emit_metric(MetricType.COUNTER, name, value, tags)

    def gauge(self, name: str, value: float, tags: Optional[Dict[str, str]] = None) -> None:
        """Emit a gauge metric."""
        self.emit_metric(MetricType.GAUGE, name, value, tags)

    def histogram(self, name: str, value: float, tags: Optional[Dict[str, str]] = None) -> None:
        """Emit a histogram metric."""
        self.emit_metric(MetricType.HISTOGRAM, name, value, tags)


# Global observability manager instance
_global_manager: Optional[ObservabilityManager] = None


def get_observability_manager() -> ObservabilityManager:
    """Get the global observability manager instance."""
    global _global_manager
    if _global_manager is None:
        _global_manager = ObservabilityManager()
    return _global_manager


def configure_observability(hooks: List[ObservabilityHook]) -> None:
    """Configure the global observability manager with hooks.
    
    Example:
        >>> from multi_format_parser.observability import configure_observability, LoggingHook
        >>> configure_observability([LoggingHook()])
    """
    manager = get_observability_manager()
    for hook in hooks:
        manager.register_hook(hook)
