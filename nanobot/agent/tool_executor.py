"""
Parallel Tool Executor for Nanobot

Provides parallel execution of tool calls with deduplication, timeout handling,
and result caching for improved performance and scalability.

Usage:
    from nanobot.agent.tool_executor import ParallelToolExecutor
    
    executor = ParallelToolExecutor(max_concurrency=10, default_timeout=15)
    results = await executor.execute_batch(tool_calls)
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, cast

from loguru import logger

from nanobot.agent.tools.base import ToolResult
from nanobot.providers.base import ToolCallRequest


@dataclass
class ToolExecutionResult:
    """Result of a tool execution with metadata."""
    
    tool_name: str
    tool_call_id: str
    result: Any
    error: str | None = None
    execution_time_ms: float = 0.0
    cached: bool = False
    cacheable: bool = True
    
    def to_tool_result(self) -> ToolResult:
        """Convert to ToolResult format."""
        if self.error:
            return ToolResult.error(self.error)
        return ToolResult(str(self.result)) if self.result is not None else ToolResult("")


@dataclass
class CacheEntry:
    """Cached tool execution result."""
    
    result: Any
    timestamp: float
    expires_at: float
    hit_count: int = 0
    
    def is_expired(self) -> bool:
        """Check if cache entry has expired."""
        return time.time() > self.expires_at


class ParallelToolExecutor:
    """
    Executes tool calls in parallel with deduplication and timeout handling.
    
    Features:
    - Deduplication: Prevents duplicate tool calls in the same batch
    - Parallel execution: Runs multiple tools concurrently with semaphore limiting
    - Timeout handling: Enforces timeouts on individual tool executions
    - Result caching: Caches deterministic tool results for reuse
    - Error isolation: One failing tool doesn't block others
    """
    
    DEFAULT_TIMEOUT = 15.0  # seconds
    DEFAULT_MAX_CONCURRENCY = 20
    CACHE_TTL = 300.0  # 5 minutes default
    
    def __init__(
        self,
        max_concurrency: int | None = None,
        default_timeout: float | None = None,
        cache_enabled: bool = True,
        cache_ttl: float | None = None,
    ):
        self.max_concurrency = max_concurrency or self.DEFAULT_MAX_CONCURRENCY
        self.default_timeout = default_timeout or self.DEFAULT_TIMEOUT
        self.cache_enabled = cache_enabled
        self.cache_ttl = cache_ttl or self.CACHE_TTL
        
        self._semaphore = asyncio.Semaphore(self.max_concurrency)
        self._cache: dict[str, CacheEntry] = {}
        self._cache_lock = asyncio.Lock()
        
        # Track recent executions for deduplication window
        self._recent_executions: dict[str, float] = {}
        self._dedup_window = 60.0  # 1 minute deduplication window
    
    @property
    def semaphore(self) -> asyncio.Semaphore:
        """Get the concurrency semaphore."""
        return self._semaphore
    
    def _generate_cache_key(self, tool_name: str, arguments: dict[str, Any]) -> str:
        """Generate a cache key for tool call."""
        key_data = {
            "tool": tool_name,
            "args": json.dumps(arguments, sort_keys=True),
        }
        return hashlib.sha256(json.dumps(key_data, sort_keys=True).encode()).hexdigest()
    
    def _generate_dedup_key(self, tool_name: str, arguments: dict[str, Any]) -> str:
        """Generate a deduplication key for tool call."""
        return f"{tool_name}:{json.dumps(arguments, sort_keys=True)}"
    
    def _is_duplicate(self, dedup_key: str) -> bool:
        """Check if tool call is a duplicate within the dedup window."""
        current_time = time.time()
        
        # Clean old entries
        cutoff = current_time - self._dedup_window
        self._recent_executions = {
            k: v for k, v in self._recent_executions.items() 
            if v > cutoff
        }
        
        # Check for duplicate
        if dedup_key in self._recent_executions:
            return True
        
        # Mark as executed
        self._recent_executions[dedup_key] = current_time
        return False
    
    async def _get_cached_result(self, cache_key: str) -> Any | None:
        """Get cached result if available and not expired."""
        if not self.cache_enabled:
            return None
        
        async with self._cache_lock:
            entry = self._cache.get(cache_key)
            if entry and not entry.is_expired():
                entry.hit_count += 1
                logger.debug(f"Cache hit for {cache_key[:16]}...")
                return entry.result
            elif entry and entry.is_expired():
                # Remove expired entry
                del self._cache[cache_key]
        
        return None
    
    async def _store_cache(self, cache_key: str, result: Any, cacheable: bool = True) -> None:
        """Store result in cache if cacheable."""
        if not self.cache_enabled or not cacheable:
            return
        
        async with self._cache_lock:
            self._cache[cache_key] = CacheEntry(
                result=result,
                timestamp=time.time(),
                expires_at=time.time() + self.cache_ttl,
            )
            logger.debug(f"Cached result for {cache_key[:16]}...")
    
    async def _execute_with_timeout(
        self,
        tool_call: ToolCallRequest,
        execute_fn: Callable[[ToolCallRequest], Any],
        timeout: float | None = None,
    ) -> ToolExecutionResult:
        """Execute a single tool call with timeout protection."""
        timeout = timeout or self.default_timeout
        start_time = time.time()
        
        try:
            result = await asyncio.wait_for(
                execute_fn(tool_call),
                timeout=timeout
            )
            
            execution_time = (time.time() - start_time) * 1000  # ms
            
            return ToolExecutionResult(
                tool_name=tool_call.name,
                tool_call_id=tool_call.id or "",
                result=result,
                execution_time_ms=execution_time,
                cacheable=getattr(result, 'cacheable', True),
            )
            
        except asyncio.TimeoutError:
            execution_time = (time.time() - start_time) * 1000
            logger.warning(f"Tool '{tool_call.name}' timed out after {timeout}s")
            
            return ToolExecutionResult(
                tool_name=tool_call.name,
                tool_call_id=tool_call.id or "",
                result=None,
                error=f"Tool '{tool_call.name}' timed out after {timeout} seconds",
                execution_time_ms=execution_time,
                cacheable=False,
            )
            
        except asyncio.CancelledError:
            raise
            
        except Exception as exc:
            execution_time = (time.time() - start_time) * 1000
            logger.exception(f"Tool '{tool_call.name}' failed: {exc}")
            
            return ToolExecutionResult(
                tool_name=tool_call.name,
                tool_call_id=tool_call.id or "",
                result=None,
                error=f"Error executing '{tool_call.name}': {type(exc).__name__}: {exc}",
                execution_time_ms=execution_time,
                cacheable=False,
            )
    
    def _deduplicate(self, tool_calls: list[ToolCallRequest]) -> list[ToolCallRequest]:
        """Remove duplicate tool calls from the batch."""
        seen: set[str] = set()
        unique_calls: list[ToolCallRequest] = []
        duplicates_removed = 0
        
        for call in tool_calls:
            dedup_key = self._generate_dedup_key(call.name, call.arguments or {})
            
            if dedup_key not in seen:
                seen.add(dedup_key)
                unique_calls.append(call)
            else:
                duplicates_removed += 1
                logger.debug(f"Removed duplicate tool call: {call.name}")
        
        if duplicates_removed > 0:
            logger.info(f"Deduplication removed {duplicates_removed} duplicate tool calls")
        
        return unique_calls
    
    async def execute_batch(
        self,
        tool_calls: list[ToolCallRequest],
        execute_fn: Callable[[ToolCallRequest], Any],
        timeout: float | None = None,
    ) -> list[ToolExecutionResult]:
        """
        Execute a batch of tool calls in parallel.
        
        Args:
            tool_calls: List of tool calls to execute
            execute_fn: Async function to execute each tool call
            timeout: Optional timeout override for all tools
            
        Returns:
            List of ToolExecutionResult objects
        """
        if not tool_calls:
            return []
        
        # Step 1: Deduplication
        unique_calls = self._deduplicate(tool_calls)
        logger.info(f"Executing {len(unique_calls)} unique tool calls (from {len(tool_calls)} total)")
        
        # Step 2: Check cache for each call
        tasks = []
        for call in unique_calls:
            cache_key = self._generate_cache_key(call.name, call.arguments or {})
            
            # Try to get cached result
            cached_result = await self._get_cached_result(cache_key)
            if cached_result is not None:
                # Return cached result immediately
                tasks.append(asyncio.coroutine(lambda c=call, r=cached_result: ToolExecutionResult(
                    tool_name=c.name,
                    tool_call_id=c.id or "",
                    result=r,
                    cached=True,
                    cacheable=True,
                ))())
            else:
                # Execute with timeout
                tasks.append(self._execute_with_timeout(call, execute_fn, timeout))
        
        # Step 3: Parallel execution with semaphore
        async with self.semaphore:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Step 4: Process results and cache successful ones
        processed_results: list[ToolExecutionResult] = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                # Handle unexpected exceptions from gather
                call = unique_calls[i]
                processed_results.append(ToolExecutionResult(
                    tool_name=call.name,
                    tool_call_id=call.id or "",
                    result=None,
                    error=f"Unexpected error: {type(result).__name__}: {result}",
                    cacheable=False,
                ))
            else:
                exec_result = cast(ToolExecutionResult, result)
                processed_results.append(exec_result)
                
                # Cache successful results
                if not exec_result.error and not exec_result.cached:
                    cache_key = self._generate_cache_key(
                        exec_result.tool_name,
                        tool_calls[i].arguments or {}
                    )
                    await self._store_cache(
                        cache_key,
                        exec_result.result,
                        exec_result.cacheable
                    )
        
        # Log summary
        total_time = sum(r.execution_time_ms for r in processed_results)
        cached_count = sum(1 for r in processed_results if r.cached)
        error_count = sum(1 for r in processed_results if r.error)
        
        logger.info(
            f"Batch execution complete: {len(processed_results)} tools, "
            f"{cached_count} cached, {error_count} errors, "
            f"total time: {total_time:.0f}ms"
        )
        
        return processed_results
    
    async def clear_cache(self) -> None:
        """Clear all cached results."""
        async with self._cache_lock:
            self._cache.clear()
        logger.info("Tool execution cache cleared")
    
    def get_cache_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        total_entries = len(self._cache)
        total_hits = sum(entry.hit_count for entry in self._cache.values())
        
        return {
            "entries": total_entries,
            "total_hits": total_hits,
            "max_concurrency": self.max_concurrency,
            "cache_enabled": self.cache_enabled,
            "cache_ttl": self.cache_ttl,
        }
