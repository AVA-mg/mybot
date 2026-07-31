"""
Circuit Breaker implementation for Nanobot.

Provides resilience against cascading failures in distributed systems
by failing fast when services are unhealthy.

Usage:
    from nanobot.utils.circuit_breaker import CircuitBreaker
    
    breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=30)
    
    @breaker
    async def call_external_service():
        return await http_client.get(...)
"""

import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, TypeVar

from loguru import logger

T = TypeVar('T')


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing fast
    HALF_OPEN = "half_open"  # Testing recovery


@dataclass
class CircuitStats:
    """Statistics for a circuit breaker."""
    state: CircuitState
    failure_count: int
    success_count: int
    last_failure_time: float | None
    last_success_time: float | None
    open_since: float | None
    total_calls: int = 0
    total_failures: int = 0
    total_successes: int = 0


class CircuitBreakerError(Exception):
    """Raised when circuit breaker is open."""
    pass


class ServiceUnavailableError(Exception):
    """Raised when external service is unavailable."""
    pass


@dataclass
class CircuitConfig:
    """Configuration for circuit breaker."""
    failure_threshold: int = 5          # Failures before opening
    success_threshold: int = 3          # Successes to close from half-open
    recovery_timeout: float = 30.0      # Seconds before trying half-open
    timeout: float = 10.0               # Default timeout for calls
    expected_exceptions: tuple = (Exception,)  # Exceptions that count as failures


class CircuitBreaker:
    """
    Circuit breaker for protecting against cascading failures.
    
    Implements the circuit breaker pattern with three states:
    - CLOSED: Normal operation, requests pass through
    - OPEN: Failing fast, requests immediately rejected
    - HALF_OPEN: Testing if service recovered, limited requests allowed
    
    For 1000+ concurrent users:
    - Use per-service circuit breakers (not global)
    - Tune thresholds based on your SLA requirements
    - Monitor circuit state transitions via metrics
    """
    
    def __init__(self, config: CircuitConfig | None = None):
        self.config = config or CircuitConfig()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: float | None = None
        self._last_success_time: float | None = None
        self._open_since: float | None = None
        self._total_calls = 0
        self._total_failures = 0
        self._total_successes = 0
        self._lock = asyncio.Lock()
        self._half_open_calls = 0
    
    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        return self._state
    
    @property
    def stats(self) -> CircuitStats:
        """Get circuit statistics."""
        return CircuitStats(
            state=self._state,
            failure_count=self._failure_count,
            success_count=self._success_count,
            last_failure_time=self._last_failure_time,
            last_success_time=self._last_success_time,
            open_since=self._open_since,
            total_calls=self._total_calls,
            total_failures=self._total_failures,
            total_successes=self._total_successes,
        )
    
    async def _check_state(self) -> bool:
        """
        Check and potentially update circuit state.
        
        Returns:
            True if request should be allowed
        """
        async with self._lock:
            if self._state == CircuitState.CLOSED:
                return True
            
            if self._state == CircuitState.OPEN:
                # Check if recovery timeout has passed
                if self._open_since and (time.time() - self._open_since) >= self.config.recovery_timeout:
                    logger.info("Circuit breaker transitioning from OPEN to HALF_OPEN")
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_calls = 0
                    return True
                else:
                    return False
            
            if self._state == CircuitState.HALF_OPEN:
                # Allow limited calls in half-open state
                if self._half_open_calls < self.config.success_threshold:
                    self._half_open_calls += 1
                    return True
                else:
                    return False
            
            return True
    
    async def _record_success(self) -> None:
        """Record successful call."""
        async with self._lock:
            self._success_count += 1
            self._total_successes += 1
            self._last_success_time = time.time()
            
            if self._state == CircuitState.HALF_OPEN:
                if self._success_count >= self.config.success_threshold:
                    logger.info("Circuit breaker transitioning from HALF_OPEN to CLOSED")
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    self._success_count = 0
                    self._open_since = None
            
            elif self._state == CircuitState.CLOSED:
                # Reset failure count on success
                self._failure_count = 0
    
    async def _record_failure(self) -> None:
        """Record failed call."""
        async with self._lock:
            self._failure_count += 1
            self._total_failures += 1
            self._last_failure_time = time.time()
            
            if self._state == CircuitState.HALF_OPEN:
                logger.warning("Circuit breaker transitioning from HALF_OPEN to OPEN (failure during test)")
                self._state = CircuitState.OPEN
                self._open_since = time.time()
                self._success_count = 0
            
            elif self._state == CircuitState.CLOSED:
                if self._failure_count >= self.config.failure_threshold:
                    logger.warning(
                        f"Circuit breaker transitioning from CLOSED to OPEN "
                        f"(threshold {self.config.failure_threshold} reached)"
                    )
                    self._state = CircuitState.OPEN
                    self._open_since = time.time()
    
    async def call(
        self,
        func: Callable[..., Any],
        *args: Any,
        timeout: float | None = None,
        **kwargs: Any,
    ) -> Any:
        """
        Execute function with circuit breaker protection.
        
        Args:
            func: Async function to execute
            *args: Positional arguments for func
            timeout: Optional timeout override
            **kwargs: Keyword arguments for func
        
        Returns:
            Result from func
        
        Raises:
            CircuitBreakerError: If circuit is open
            ServiceUnavailableError: If service times out
        """
        self._total_calls += 1
        
        # Check if circuit allows request
        if not await self._check_state():
            logger.warning(f"Circuit breaker OPEN, rejecting call to {func.__name__}")
            raise CircuitBreakerError(
                f"Circuit breaker is open for {func.__name__}. "
                f"Retry after {self.config.recovery_timeout}s"
            )
        
        # Execute with timeout
        effective_timeout = timeout or self.config.timeout
        try:
            result = await asyncio.wait_for(func(*args, **kwargs), timeout=effective_timeout)
            await self._record_success()
            return result
        
        except asyncio.TimeoutError:
            logger.warning(f"Call to {func.__name__} timed out after {effective_timeout}s")
            await self._record_failure()
            raise ServiceUnavailableError(f"Service {func.__name__} timed out")
        
        except asyncio.CancelledError:
            raise
        
        except self.config.expected_exceptions as e:
            logger.warning(f"Call to {func.__name__} failed: {type(e).__name__}: {e}")
            await self._record_failure()
            raise
    
    def reset(self) -> None:
        """Reset circuit breaker to initial state."""
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = None
        self._last_success_time = None
        self._open_since = None
        self._half_open_calls = 0
        logger.info(f"Circuit breaker reset for")
    
    def __call__(self, func: Callable[..., Any]) -> Callable[..., Any]:
        """Decorator usage."""
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            return await self.call(func, *args, **kwargs)
        return wrapper


import functools


class CircuitBreakerRegistry:
    """
    Registry for managing multiple circuit breakers by service name.
    
    Usage:
        registry = CircuitBreakerRegistry()
        breaker = registry.get("external_api")
        result = await breaker.call(my_async_func)
    """
    
    def __init__(self, default_config: CircuitConfig | None = None):
        self.default_config = default_config or CircuitConfig()
        self._breakers: dict[str, CircuitBreaker] = {}
        self._lock = asyncio.Lock()
    
    async def get(self, service_name: str, config: CircuitConfig | None = None) -> CircuitBreaker:
        """Get or create circuit breaker for service."""
        async with self._lock:
            if service_name not in self._breakers:
                self._breakers[service_name] = CircuitBreaker(config or self.default_config)
                logger.info(f"Created circuit breaker for service: {service_name}")
            return self._breakers[service_name]
    
    def get_sync(self, service_name: str, config: CircuitConfig | None = None) -> CircuitBreaker:
        """Synchronous version - use only if registry already initialized."""
        if service_name not in self._breakers:
            self._breakers[service_name] = CircuitBreaker(config or self.default_config)
        return self._breakers[service_name]
    
    async def get_all_stats(self) -> dict[str, CircuitStats]:
        """Get stats for all circuit breakers."""
        async with self._lock:
            return {name: breaker.stats for name, breaker in self._breakers.items()}
    
    async def reset_all(self) -> None:
        """Reset all circuit breakers."""
        async with self._lock:
            for breaker in self._breakers.values():
                breaker.reset()


# Global registry instance
_global_registry: CircuitBreakerRegistry | None = None


def get_circuit_registry() -> CircuitBreakerRegistry:
    """Get global circuit breaker registry."""
    global _global_registry
    if _global_registry is None:
        _global_registry = CircuitBreakerRegistry()
    return _global_registry


async def protect_with_circuit(
    func: Callable[..., Any],
    service_name: str,
    *args: Any,
    config: CircuitConfig | None = None,
    **kwargs: Any,
) -> Any:
    """
    Execute function with circuit breaker protection.
    
    Convenience function for one-off circuit breaker usage.
    
    Args:
        func: Async function to protect
        service_name: Name for identifying this service
        *args: Arguments for func
        config: Optional custom configuration
        **kwargs: Keyword arguments for func
    
    Returns:
        Result from func
    """
    registry = get_circuit_registry()
    breaker = await registry.get(service_name, config)
    return await breaker.call(func, *args, **kwargs)
