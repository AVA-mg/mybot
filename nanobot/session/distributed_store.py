"""
Distributed Session Store for Nanobot.

Two-tier storage: Redis (hot/caching) + PostgreSQL (cold/durable).
Supports horizontal scaling with multiple workers.
"""

import os
import msgpack
from typing import Any, Dict
from datetime import datetime
from redis.asyncio import Redis as AsyncRedis
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from nanobot.db.models import SessionModel
from nanobot.logging import get_logger

logger = get_logger(__name__)


class DistributedSessionStore:
    """
    Distributed session store with two-tier caching.

    - Tier 1: Redis for fast access (hot cache)
    - Tier 2: PostgreSQL for durability (cold storage)

    Features:
    - Automatic cache invalidation
    - TTL-based expiration in Redis
    - Async operations for high concurrency
    """

    REDIS_PREFIX = "session:"
    REDIS_TTL = int(os.getenv("SESSION_REDIS_TTL", "86400"))  # 24 hours default

    def __init__(self, redis_client: AsyncRedis | None = None, db_engine=None):
        """
        Initialize the distributed session store.

        Args:
            redis_client: AsyncRedis client instance
            db_engine: SQLAlchemy async engine or session factory
        """
        self.redis = redis_client
        self.db_engine = db_engine
        self._initialized = False

    async def initialize(self) -> None:
        """Initialize Redis and database connections."""
        if self._initialized:
            return

        # Initialize Redis connection
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
        if self.redis is None:
            self.redis = AsyncRedis.from_url(
                redis_url,
                encoding="utf-8",
                decode_responses=False,
            )

        # Test Redis connection
        try:
            await self.redis.ping()
            logger.info("Connected to Redis for session storage")
        except Exception as e:
            logger.warning(f"Redis connection failed: {e}. Sessions will use DB only.")
            self.redis = None

        self._initialized = True

    def _get_redis_key(self, session_key: str) -> str:
        """Generate Redis key for a session."""
        return f"{self.REDIS_PREFIX}{session_key}"

    async def load(self, session_key: str) -> Dict[str, Any] | None:
        """
        Load session data from the distributed store.

        Args:
            session_key: Unique session identifier

        Returns:
            Session data as dictionary, or None if not found
        """
        if not self._initialized:
            await self.initialize()

        # Tier 1: Try Redis (fast path)
        if self.redis:
            try:
                cached = await self.redis.get(self._get_redis_key(session_key))
                if cached:
                    logger.debug(f"Session cache hit for {session_key}")
                    return msgpack.unpackb(cached, raw=False)
            except Exception as e:
                logger.warning(f"Redis read error for session {session_key}: {e}")

        # Tier 2: Fallback to PostgreSQL
        try:
            from nanobot.db import db_config

            if db_config.async_session_factory is None:
                db_config.initialize()

            async with db_config.async_session_factory() as db:
                result = await db.execute(
                    select(SessionModel).where(SessionModel.key == session_key)
                )
                session_row = result.scalar_one_or_none()

                if session_row:
                    session_data = {
                        "id": session_row.id,
                        "key": session_row.key,
                        "user_id": session_row.user_id,
                        "bot_id": session_row.bot_id,
                        "state": session_row.state,
                        "context": session_row.context,
                        "metadata": session_row.metadata_,
                        "message_count": session_row.message_count,
                        "created_at": session_row.created_at.isoformat() if session_row.created_at else None,
                        "updated_at": session_row.updated_at.isoformat() if session_row.updated_at else None,
                        "last_activity": session_row.last_activity.isoformat() if session_row.last_activity else None,
                    }

                    # Backfill Redis cache
                    if self.redis:
                        try:
                            await self.redis.setex(
                                self._get_redis_key(session_key),
                                self.REDIS_TTL,
                                msgpack.packb(session_data),
                            )
                        except Exception as e:
                            logger.warning(f"Redis cache backfill failed: {e}")

                    logger.debug(f"Session loaded from DB for {session_key}")
                    return session_data
                else:
                    logger.debug(f"Session not found: {session_key}")
                    return None

        except Exception as e:
            logger.error(f"Database read error for session {session_key}: {e}")
            return None

    async def save(self, session_key: str, data: Dict[str, Any]) -> bool:
        """
        Save session data to both Redis and PostgreSQL.

        Args:
            session_key: Unique session identifier
            data: Session data dictionary

        Returns:
            True if successful, False otherwise
        """
        if not self._initialized:
            await self.initialize()

        success = True

        # Write to Redis (hot tier)
        if self.redis:
            try:
                await self.redis.setex(
                    self._get_redis_key(session_key),
                    self.REDIS_TTL,
                    msgpack.packb(data),
                )
                logger.debug(f"Session cached in Redis: {session_key}")
            except Exception as e:
                logger.warning(f"Redis write error for session {session_key}: {e}")
                success = False

        # Write to PostgreSQL (cold tier)
        try:
            from nanobot.db import db_config

            if db_config.async_session_factory is None:
                db_config.initialize()

            async with db_config.async_session_factory() as db:
                # Check if session exists
                result = await db.execute(
                    select(SessionModel).where(SessionModel.key == session_key)
                )
                existing = result.scalar_one_or_none()

                if existing:
                    # Update existing session
                    existing.state = data.get("state", {})
                    existing.context = data.get("context", [])
                    existing.metadata_ = data.get("metadata", {})
                    existing.message_count = data.get("message_count", existing.message_count)
                    existing.last_activity = datetime.utcnow()
                    existing.updated_at = datetime.utcnow()
                else:
                    # Create new session
                    new_session = SessionModel(
                        key=session_key,
                        user_id=data.get("user_id"),
                        bot_id=data.get("bot_id", "default"),
                        state=data.get("state", {}),
                        context=data.get("context", []),
                        metadata_=data.get("metadata", {}),
                        message_count=data.get("message_count", 0),
                    )
                    db.add(new_session)

                await db.commit()
                logger.debug(f"Session saved to DB: {session_key}")

        except Exception as e:
            logger.error(f"Database write error for session {session_key}: {e}")
            success = False

        return success

    async def delete(self, session_key: str) -> bool:
        """
        Delete a session from both Redis and PostgreSQL.

        Args:
            session_key: Unique session identifier

        Returns:
            True if successful, False otherwise
        """
        if not self._initialized:
            await self.initialize()

        success = True

        # Delete from Redis
        if self.redis:
            try:
                await self.redis.delete(self._get_redis_key(session_key))
                logger.debug(f"Session deleted from Redis: {session_key}")
            except Exception as e:
                logger.warning(f"Redis delete error for session {session_key}: {e}")
                success = False

        # Delete from PostgreSQL
        try:
            from nanobot.db import db_config

            if db_config.async_session_factory is None:
                db_config.initialize()

            async with db_config.async_session_factory() as db:
                result = await db.execute(
                    select(SessionModel).where(SessionModel.key == session_key)
                )
                session_row = result.scalar_one_or_none()

                if session_row:
                    await db.delete(session_row)
                    await db.commit()
                    logger.debug(f"Session deleted from DB: {session_key}")

        except Exception as e:
            logger.error(f"Database delete error for session {session_key}: {e}")
            success = False

        return success

    async def exists(self, session_key: str) -> bool:
        """Check if a session exists."""
        if not self._initialized:
            await self.initialize()

        # Quick check in Redis
        if self.redis:
            try:
                exists = await self.redis.exists(self._get_redis_key(session_key))
                if exists:
                    return True
            except Exception:
                pass

        # Fallback to DB check
        try:
            from nanobot.db import db_config

            if db_config.async_session_factory is None:
                db_config.initialize()

            async with db_config.async_session_factory() as db:
                result = await db.execute(
                    select(SessionModel.id).where(SessionModel.key == session_key)
                )
                return result.scalar_one_or_none() is not None
        except Exception:
            return False

    async def close(self) -> None:
        """Close Redis connection."""
        if self.redis:
            await self.redis.close()
            self.redis = None
        self._initialized = False
