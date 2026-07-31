"""
Worker module for Nanobot.

Provides distributed worker pool for message processing.
"""

from .main import Worker, WorkerPool

__all__ = [
    "Worker",
    "WorkerPool",
]
