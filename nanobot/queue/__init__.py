"""
Queue module for Nanobot.

Provides distributed message queuing with Redis Streams.
"""

from .message_queue import MessageQueue, QueuedMessage

__all__ = [
    "MessageQueue",
    "QueuedMessage",
]
