"""
Structured Logging for Nanobot

Provides JSON-formatted structured logging for observability, monitoring, and debugging.
All logs are emitted in JSON format for easy parsing by log aggregation systems.

Usage:
    from nanobot.logging import get_logger, configure_logging
    
    # Configure logging at application startup
    configure_logging(level="INFO", json_format=True)
    
    # Get a logger instance
    logger = get_logger("nanobot.agent")
    logger.info("Agent started", extra={"user_id": "123", "session_id": "abc"})
    
    # Logs will be emitted as:
    # {"timestamp": "...", "level": "INFO", "logger": "nanobot.agent", 
    #  "message": "Agent started", "user_id": "123", "session_id": "abc"}
"""

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Optional


class JSONFormatter(logging.Formatter):
    """
    Custom JSON formatter for structured logging.
    
    Converts log records to JSON format with all relevant context.
    """
    
    def __init__(self, include_extra: bool = True):
        super().__init__()
        self.include_extra = include_extra
    
    def format(self, record: logging.LogRecord) -> str:
        """Format the log record as JSON."""
        log_data = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        
        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        # Add extra fields if enabled
        if self.include_extra:
            # Filter out standard logging attributes
            extra_fields = {
                key: value
                for key, value in record.__dict__.items()
                if key not in {
                    'name', 'msg', 'args', 'created', 'filename', 'funcName',
                    'levelname', 'levelno', 'lineno', 'module', 'msecs',
                    'pathname', 'process', 'processName', 'relativeCreated',
                    'stack_info', 'exc_info', 'exc_text', 'thread', 'threadName',
                    'message', 'asctime'
                }
                and not key.startswith('_')
            }
            
            if extra_fields:
                log_data.update(extra_fields)
        
        return json.dumps(log_data, default=str, ensure_ascii=False)


class ColoredFormatter(logging.Formatter):
    """
    Colored console formatter for development.
    
    Provides human-readable colored output for local development.
    """
    
    COLORS = {
        'DEBUG': '\033[36m',     # Cyan
        'INFO': '\033[32m',      # Green
        'WARNING': '\033[33m',   # Yellow
        'ERROR': '\033[31m',     # Red
        'CRITICAL': '\033[35m',  # Magenta
    }
    RESET = '\033[0m'
    
    def format(self, record: logging.LogRecord) -> str:
        """Format the log record with colors."""
        color = self.COLORS.get(record.levelname, self.RESET)
        timestamp = datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S')
        
        formatted_message = super().format(record)
        
        return f"{color}[{timestamp}] {record.levelname:8}{self.RESET} {formatted_message}"


def configure_logging(
    level: str = "INFO",
    json_format: bool = True,
    log_file: Optional[str] = None,
    console_output: bool = True,
    include_extra: bool = True,
) -> None:
    """
    Configure the logging system for Nanobot.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        json_format: If True, use JSON format. If False, use colored console format.
        log_file: Optional file path to write logs to.
        console_output: Whether to output logs to console.
        include_extra: Whether to include extra fields in JSON output.
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    
    # Clear existing handlers
    root_logger.handlers.clear()
    
    # Create formatter based on format type
    if json_format:
        formatter = JSONFormatter(include_extra=include_extra)
    else:
        formatter = ColoredFormatter('%(name)s - %(message)s')
    
    # Console handler
    if console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(getattr(logging, level.upper(), logging.INFO))
        root_logger.addHandler(console_handler)
    
    # File handler (if specified)
    if log_file:
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(JSONFormatter(include_extra=include_extra))
        file_handler.setLevel(getattr(logging, level.upper(), logging.INFO))
        root_logger.addHandler(file_handler)
    
    # Reduce noise from third-party libraries
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('httpcore').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('aiosqlite').setLevel(logging.WARNING)
    
    # Log configuration
    logger = logging.getLogger('nanobot.logging')
    logger.info(
        "Logging configured",
        extra={
            "level": level,
            "json_format": json_format,
            "log_file": log_file,
            "console_output": console_output,
        }
    )


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance with the specified name.
    
    Args:
        name: Logger name (typically module name like 'nanobot.agent')
        
    Returns:
        logging.Logger: Configured logger instance
    """
    return logging.getLogger(name)


def log_async_operation(
    logger: logging.Logger,
    operation: str,
    status: str,
    duration_ms: Optional[float] = None,
    **extra_context: Any,
) -> None:
    """
    Log an async operation with standardized format.
    
    Args:
        logger: Logger instance
        operation: Name of the operation (e.g., 'tool_execution', 'llm_call')
        status: Status of the operation ('started', 'completed', 'failed', 'timeout')
        duration_ms: Duration in milliseconds (if completed)
        **extra_context: Additional context to include in the log
    """
    log_data = {
        "operation": operation,
        "status": status,
        **extra_context,
    }
    
    if duration_ms is not None:
        log_data["duration_ms"] = duration_ms
    
    if status == "failed" or status == "timeout":
        logger.error(f"Async operation {operation} {status}", extra=log_data)
    elif status == "completed":
        logger.info(f"Async operation {operation} {status}", extra=log_data)
    else:
        logger.debug(f"Async operation {operation} {status}", extra=log_data)


class AsyncLogContext:
    """
    Context manager for logging async operations with automatic timing.
    
    Usage:
        logger = get_logger("nanobot.tools")
        with AsyncLogContext(logger, "web_search", query=query):
            result = await search(query)
    """
    
    def __init__(self, logger: logging.Logger, operation: str, **context: Any):
        self.logger = logger
        self.operation = operation
        self.context = context
        self.start_time: Optional[float] = None
    
    def __enter__(self):
        import time
        self.start_time = time.time()
        self.logger.debug(
            f"Starting async operation: {self.operation}",
            extra={**self.context, "status": "started"}
        )
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        import time
        duration_ms = (time.time() - self.start_time) * 1000 if self.start_time else 0
        
        if exc_type is not None:
            self.logger.error(
                f"Async operation failed: {self.operation}",
                extra={
                    **self.context,
                    "status": "failed",
                    "duration_ms": duration_ms,
                    "exception": str(exc_val),
                }
            )
        else:
            self.logger.info(
                f"Async operation completed: {self.operation}",
                extra={
                    **self.context,
                    "status": "completed",
                    "duration_ms": duration_ms,
                }
            )
        
        return False  # Don't suppress exceptions


# Default logger for the nanobot package
default_logger = get_logger("nanobot")
