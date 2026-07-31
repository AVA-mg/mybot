"""
Distributed Lock implementation using Redis SETNX.

Provides distributed locking for session access and other critical operations
in a multi-worker environment.
"""

import os
import uuid
import asyncio
from typing import Optional
from datetime import timedelta
from redis.asyncio import Redis as AsyncRedis
from nanobot.logging import get_logger

logger = get_logger(__name__)


class DistributedLock:
    """
    Distributed lock using Redis SETNX with automatic expiration.

    Features:
    - Automatic lock expiration (prevents deadlocks)
    - Lock ownership verification
    - Reentrant lock support (optional)
    - Async context manager interface

    Usage:
        async with DistributedLock(redis, "session:123"):
            # Critical section - only one worker can execute this
            await process_session(session_id)
    """

    DEFAULT_TTL = int(os.getenv("LOCK_DEFAULT_TTL", "30"))  # 30 seconds
    DEFAULT_TIMEOUT = int(os.getenv("LOCK_TIMEOUT", "10"))  # 10 seconds to acquire
    RETRY_INTERVAL = 0.1  # 100ms between retry attempts

    def __init__(
        self,
        redis_client: AsyncRedis | None = None,
        key: str = "",
        ttl: int | None = None,
        timeout: int | None = None,
        reentrant: bool = False,
    ):
        """
        Initialize a distributed lock.

        Args:
            redis_client: AsyncRedis client instance
            key: Lock key (e.g., "session:123:lock")
            ttl: Lock time-to-live in seconds (auto-release)
            timeout: Maximum time to wait for acquiring the lock
            reentrant: Allow same owner to reacquire lock
        """
        self.redis = redis_client
        self.key = key
        self.ttl = ttl or self.DEFAULT_TTL
        self.timeout = timeout or self.DEFAULT_TIMEOUT
        self.reentrant = reentrant
        self._owner_id = str(uuid.uuid4())
        self._acquired = False
        self._local_lock = asyncio.Lock()
        self._reentrant_count = 0

    async def initialize(self) -> None:
        """Initialize Redis connection if not provided."""
        if self.redis is None:
            redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
            self.redis = AsyncRedis.from_url(
                redis_url,
                encoding="utf-8",
                decode_responses=True,
            )
            try:
                await self.redis.ping()
                logger.debug("Connected to Redis for distributed locking")
            except Exception as e:
                logger.warning(f"Redis connection failed: {e}")

    @property
    def is_acquired(self) -> bool:
        """Check if lock is currently acquired by this instance."""
        return self._acquired

    async def acquire(self) -> bool:
        """
        Attempt to acquire the distributed lock.

        Returns:
            True if lock was acquired, False if timeout occurred
        """
        if not self.redis:
            await self.initialize()

        if self._acquired:
            # Already acquired - check if reentrant
            if self.reentrant:
                self._reentrant_count += 1
                return True
            return False

        start_time = asyncio.get_event_loop().time()
        lock_key = f"lock:{self.key}"

        while True:
            try:
                # Try to set lock with NX (only if not exists) and EX (expiration)
                acquired = await self.redis.set(
                    lock_key,
                    self._owner_id,
                    nx=True,
                    ex=self.ttl,
                )

                if acquired:
                    self._acquired = True
                    if self.reentrant:
                        self._reentrant_count = 1
                    logger.debug(f"Lock acquired: {self.key} (owner={self._owner_id[:8]})")
                    return True

                # Check if we already own the lock (for reentrant case)
                if self.reentrant:
                    current_owner = await self.redis.get(lock_key)
                    if current_owner == self._owner_id:
                        # Extend TTL
                        await self.redis.expire(lock_key, self.ttl)
                        self._reentrant_count += 1
                        self._acquired = True
                        logger.debug(f"Reentrant lock extended: {self.key}")
                        return True

                # Check timeout
                elapsed = asyncio.get_event_loop().time() - start_time
                if elapsed >= self.timeout:
                    logger.warning(f"Lock acquisition timeout: {self.key}")
                    return False

                # Wait before retry
                await asyncio.sleep(self.RETRY_INTERVAL)

            except Exception as e:
                logger.error(f"Lock acquisition error for {self.key}: {e}")
                # Check timeout
                elapsed = asyncio.get_event_loop().time() - start_time
                if elapsed >= self.timeout:
                    return False
                await asyncio.sleep(self.RETRY_INTERVAL)

    async def release(self) -> bool:
        """
        Release the distributed lock.

        Returns:
            True if lock was released, False otherwise
        """
        if not self._acquired:
            return False

        # Handle reentrant locks
        if self.reentrant and self._reentrant_count > 1:
            self._reentrant_count -= 1
            return True

        if not self.redis:
            logger.warning("Cannot release lock: Redis not initialized")
            return False

        lock_key = f"lock:{self.key}"

        try:
            # Verify ownership before releasing (Lua script for atomicity)
            lua_script = """
            if redis.call("get", KEYS[1]) == ARGV[1] then
                return redis.call("del", KEYS[1])
            else
                return 0
            end
            """
            result = await self.redis.eval(lua_script, 1, lock_key, self._owner_id)

            self._acquired = False
            self._reentrant_count = 0

            if result:
                logger.debug(f"Lock released: {self.key}")
                return True
            else:
                logger.warning(f"Lock release failed (not owner): {self.key}")
                return False

        except Exception as e:
            logger.error(f"Lock release error for {self.key}: {e}")
            self._acquired = False
            self._reentrant_count = 0
            return False

    async def extend(self, additional_ttl: int | None = None) -> bool:
        """
        Extend the lock TTL.

        Args:
            additional_ttl: Additional seconds to add (default: original TTL)

        Returns:
            True if extended successfully, False otherwise
        """
        if not self._acquired or not self.redis:
            return False

        lock_key = f"lock:{self.key}"
        ttl_to_set = additional_ttl or self.ttl

        try:
            # Verify ownership before extending
            current_owner = await self.redis.get(lock_key)
            if current_owner != self._owner_id:
                logger.warning(f"Cannot extend lock (not owner): {self.key}")
                return False

            await self.redis.expire(lock_key, ttl_to_set)
            logger.debug(f"Lock extended: {self.key} (ttl={ttl_to_set})")
            return True

        except Exception as e:
            logger.error(f"Lock extension error for {self.key}: {e}")
            return False

    async def __aenter__(self) -> "DistributedLock":
        """Async context manager entry."""
        acquired = await self.acquire()
        if not acquired:
            raise TimeoutError(f"Failed to acquire lock: {self.key}")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.release()

    async def close(self) -> None:
        """Release lock and close Redis connection if owned."""
        if self._acquired:
            await self.release()
        if self.redis and hasattr(self.redis, "close"):
            # Only close if we created the connection
            pass  # Don't close shared Redis connections


class LockManager:
    """
    Manager for multiple distributed locks.

    Provides convenient methods for acquiring/releasing locks on various resources.
    """

    def __init__(self, redis_client: AsyncRedis | None = None):
        """
        Initialize the lock manager.

        Args:
            redis_client: Shared AsyncRedis client
        """
        self.redis = redis_client
        self._locks: dict[str, DistributedLock] = {}

    def get_lock(
        self,
        key: str,
        ttl: int | None = None,
        timeout: int | None = None,
        reentrant: bool = False,
    ) -> DistributedLock:
        """
        Get or create a distributed lock for a key.

        Args:
            key: Lock key
            ttl: Lock TTL in seconds
            timeout: Acquisition timeout in seconds
            reentrant: Allow reentrant locking

        Returns:
            DistributedLock instance
        """
        if key not in self._locks:
            self._locks[key] = DistributedLock(
                redis_client=self.redis,
                key=key,
                ttl=ttl,
                timeout=timeout,
                reentrant=reentrant,
            )
        return self._locks[key]

    async def acquire_session_lock(
        self, session_key: str, timeout: int = 5
    ) -> DistributedLock:
        """
        Acquire a lock for a specific session.

        Args:
            session_key: Session identifier
            timeout: Acquisition timeout

        Returns:
            Acquired DistributedLock instance
        """
        lock = self.get_lock(f"session:{session_key}", ttl=30, timeout=timeout)
        await lock.acquire()
        return lock

    async def release_all(self) -> None:
        """Release all managed locks."""
        for lock in self._locks.values():
            if lock.is_acquired:
                await lock.release()
        self._locks.clear()

    async def close(self) -> None:
        """Release all locks and cleanup."""
        await self.release_all()
