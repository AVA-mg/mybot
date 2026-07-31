"""
Streaming Tool Progress for Nanobot

Provides real-time streaming of tool execution progress to users via WebSocket events.
Sends structured events for tool start, progress updates, and completion.

Usage:
    from nanobot.agent.streaming import ToolProgressStreamer
    
    streamer = ToolProgressStreamer(websocket_send_fn)
    
    # Send tool start event
    await streamer.send_tool_start(tool_call_id, tool_name, arguments)
    
    # Send progress update (optional)
    await streamer.send_tool_progress(tool_call_id, progress_percent, message)
    
    # Send tool complete event
    await streamer.send_tool_complete(tool_call_id, result)
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Awaitable

from loguru import logger


class ToolEvent(Enum):
    """Types of tool execution events."""
    
    TOOL_START = "tool_start"
    TOOL_PROGRESS = "tool_progress"
    TOOL_COMPLETE = "tool_complete"
    TOOL_ERROR = "tool_error"


@dataclass
class ToolEventMessage:
    """Structured tool execution event."""
    
    event_type: ToolEvent
    tool_call_id: str
    tool_name: str
    timestamp: float = field(default_factory=time.time)
    
    # For TOOL_START
    arguments: dict[str, Any] | None = None
    
    # For TOOL_PROGRESS
    progress_percent: float | None = None
    progress_message: str | None = None
    
    # For TOOL_COMPLETE/TOOL_ERROR
    result: Any | None = None
    error: str | None = None
    
    # Metadata
    session_id: str | None = None
    user_id: str | None = None
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        data = {
            "event": self.event_type.value,
            "tool_call_id": self.tool_call_id,
            "tool_name": self.tool_name,
            "timestamp": self.timestamp,
        }
        
        if self.event_type == ToolEvent.TOOL_START and self.arguments:
            data["arguments"] = self.arguments
        
        if self.event_type == ToolEvent.TOOL_PROGRESS:
            data["progress"] = {
                "percent": self.progress_percent,
                "message": self.progress_message,
            }
        
        if self.event_type == ToolEvent.TOOL_COMPLETE:
            data["result"] = self.result
        
        if self.event_type == ToolEvent.TOOL_ERROR:
            data["error"] = self.error
        
        if self.session_id:
            data["session_id"] = self.session_id
        
        if self.user_id:
            data["user_id"] = self.user_id
        
        return data


class ToolProgressStreamer:
    """
    Streams tool execution progress to clients in real-time.
    
    Features:
    - Real-time event streaming via WebSocket or other channels
    - Structured event format for easy client-side parsing
    - Optional progress callbacks for custom handling
    - Session-aware event routing
    """
    
    def __init__(
        self,
        send_fn: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
        session_id: str | None = None,
        user_id: str | None = None,
        enable_progress_events: bool = True,
    ):
        """
        Initialize the streamer.
        
        Args:
            send_fn: Async function to send events to client
            session_id: Current session identifier
            user_id: Current user identifier
            enable_progress_events: Whether to send intermediate progress events
        """
        self._send_fn = send_fn
        self._session_id = session_id
        self._user_id = user_id
        self._enable_progress = enable_progress_events
        
        # Track active tool executions
        self._active_tools: dict[str, dict[str, Any]] = {}
        self._lock = asyncio.Lock()
    
    @property
    def session_id(self) -> str | None:
        """Get the current session ID."""
        return self._session_id
    
    @session_id.setter
    def session_id(self, value: str | None) -> None:
        """Set the session ID."""
        self._session_id = value
    
    @property
    def user_id(self) -> str | None:
        """Get the current user ID."""
        return self._user_id
    
    @user_id.setter
    def user_id(self, value: str | None) -> None:
        """Set the user ID."""
        self._user_id = value
    
    async def _send_event(self, event: ToolEventMessage) -> None:
        """Send an event to the client."""
        if not self._send_fn:
            logger.debug(f"Event not sent (no send_fn): {event.event_type.value}")
            return
        
        try:
            await self._send_fn(event.to_dict())
            logger.debug(f"Sent event: {event.event_type.value} for {event.tool_name}")
        except Exception as exc:
            logger.warning(f"Failed to send event: {exc}")
    
    async def send_tool_start(
        self,
        tool_call_id: str,
        tool_name: str,
        arguments: dict[str, Any] | None = None,
    ) -> None:
        """
        Send tool execution start event.
        
        Args:
            tool_call_id: Unique identifier for this tool call
            tool_name: Name of the tool being executed
            arguments: Tool arguments (sanitized, no sensitive data)
        """
        event = ToolEventMessage(
            event_type=ToolEvent.TOOL_START,
            tool_call_id=tool_call_id,
            tool_name=tool_name,
            arguments=arguments,
            session_id=self._session_id,
            user_id=self._user_id,
        )
        
        async with self._lock:
            self._active_tools[tool_call_id] = {
                "name": tool_name,
                "started_at": time.time(),
                "last_progress": 0.0,
            }
        
        await self._send_event(event)
    
    async def send_tool_progress(
        self,
        tool_call_id: str,
        progress_percent: float,
        message: str | None = None,
    ) -> None:
        """
        Send tool execution progress update.
        
        Args:
            tool_call_id: Unique identifier for this tool call
            progress_percent: Progress percentage (0-100)
            message: Optional progress message
        """
        if not self._enable_progress:
            return
        
        # Clamp progress to valid range
        progress_percent = max(0.0, min(100.0, progress_percent))
        
        event = ToolEventMessage(
            event_type=ToolEvent.TOOL_PROGRESS,
            tool_call_id=tool_call_id,
            tool_name=self._active_tools.get(tool_call_id, {}).get("name", "unknown"),
            progress_percent=progress_percent,
            progress_message=message,
            session_id=self._session_id,
            user_id=self._user_id,
        )
        
        async with self._lock:
            if tool_call_id in self._active_tools:
                self._active_tools[tool_call_id]["last_progress"] = progress_percent
        
        await self._send_event(event)
    
    async def send_tool_complete(
        self,
        tool_call_id: str,
        result: Any | None = None,
    ) -> None:
        """
        Send tool execution completion event.
        
        Args:
            tool_call_id: Unique identifier for this tool call
            result: Tool execution result
        """
        event = ToolEventMessage(
            event_type=ToolEvent.TOOL_COMPLETE,
            tool_call_id=tool_call_id,
            tool_name=self._active_tools.get(tool_call_id, {}).get("name", "unknown"),
            result=result,
            session_id=self._session_id,
            user_id=self._user_id,
        )
        
        async with self._lock:
            # Calculate execution time
            if tool_call_id in self._active_tools:
                started_at = self._active_tools[tool_call_id].get("started_at", time.time())
                execution_time = time.time() - started_at
                event.to_dict()["execution_time_ms"] = execution_time * 1000
                del self._active_tools[tool_call_id]
        
        await self._send_event(event)
    
    async def send_tool_error(
        self,
        tool_call_id: str,
        error: str,
    ) -> None:
        """
        Send tool execution error event.
        
        Args:
            tool_call_id: Unique identifier for this tool call
            error: Error message
        """
        event = ToolEventMessage(
            event_type=ToolEvent.TOOL_ERROR,
            tool_call_id=tool_call_id,
            tool_name=self._active_tools.get(tool_call_id, {}).get("name", "unknown"),
            error=error,
            session_id=self._session_id,
            user_id=self._user_id,
        )
        
        async with self._lock:
            if tool_call_id in self._active_tools:
                del self._active_tools[tool_call_id]
        
        await self._send_event(event)
    
    async def get_active_tools(self) -> list[dict[str, Any]]:
        """Get list of currently active tool executions."""
        async with self._lock:
            return [
                {
                    "tool_call_id": tool_id,
                    "tool_name": info["name"],
                    "started_at": info["started_at"],
                    "last_progress": info["last_progress"],
                    "duration_ms": (time.time() - info["started_at"]) * 1000,
                }
                for tool_id, info in self._active_tools.items()
            ]
    
    def set_send_function(
        self,
        send_fn: Callable[[dict[str, Any]], Awaitable[None]] | None,
    ) -> None:
        """Update the send function at runtime."""
        self._send_fn = send_fn


class StreamingContext:
    """Context manager for streaming tool execution."""
    
    def __init__(
        self,
        streamer: ToolProgressStreamer,
        tool_call_id: str,
        tool_name: str,
        arguments: dict[str, Any] | None = None,
    ):
        self.streamer = streamer
        self.tool_call_id = tool_call_id
        self.tool_name = tool_name
        self.arguments = arguments
        self.result: Any = None
        self.error: str | None = None
    
    async def __aenter__(self) -> StreamingContext:
        await self.streamer.send_tool_start(
            self.tool_call_id,
            self.tool_name,
            self.arguments,
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is not None:
            self.error = f"{type(exc_val).__name__}: {exc_val}"
            await self.streamer.send_tool_error(self.tool_call_id, self.error)
        else:
            await self.streamer.send_tool_complete(self.tool_call_id, self.result)
