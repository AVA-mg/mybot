"""
Message Queue implementation using Redis Streams.

Provides reliable message queuing with consumer groups for distributed worker processing.
"""

import os
import json
import uuid
from typing import Any, Dict, Optional
from datetime import datetime
from dataclasses import dataclass, field
from redis.asyncio import Redis as AsyncRedis
from nanobot.logging import get_logger

logger = get_logger(__name__)


@dataclass
class QueuedMessage:
    """Represents a message in the queue."""

    session_key: str
    user_id: str | None
    content: str
    reply_channel: str
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert message to dictionary for serialization."""
        return {
            "session_key": self.session_key,
            "user_id": self.user_id or "",
            "content": self.content,
            "reply_channel": self.reply_channel,
            "message_id": self.message_id,
            "timestamp": self.timestamp,
            "metadata": json.dumps(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, str]) -> "QueuedMessage":
        """Create message from dictionary."""
        return cls(
            session_key=data.get("session_key", ""),
            user_id=data.get("user_id") or None,
            content=data.get("content", ""),
            reply_channel=data.get("reply_channel", ""),
            message_id=data.get("message_id", str(uuid.uuid4())),
            timestamp=data.get("timestamp", datetime.utcnow().isoformat()),
            metadata=json.loads(data.get("metadata", "{}")),
        )

    def to_json(self) -> str:
        """Serialize message to JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> "QueuedMessage":
        """Deserialize message from JSON string."""
        return cls.from_dict(json.loads(json_str))


class MessageQueue:
    """
    Distributed message queue using Redis Streams.

    Features:
    - Consumer groups for parallel processing
    - Message acknowledgment
    - Pending message tracking
    - Dead letter queue support

    Usage:
        mq = MessageQueue()
        await mq.initialize()

        # Enqueue a message
        msg = QueuedMessage(session_key="sess_123", user_id="user_456", ...)
        await mq.enqueue(msg)

        # Dequeue messages (worker side)
        msg = await mq.dequeue("worker-1")
        if msg:
            await process_message(msg)
            await mq.acknowledge(msg.message_id)
    """

    STREAM_KEY = "nanobot:inbound"
    GROUP_NAME = "agent-workers"
    DLQ_STREAM_KEY = "nanobot:dlq"  # Dead Letter Queue
    MAX_RETRIES = int(os.getenv("MQ_MAX_RETRIES", "3"))
    RETRY_DELAY = int(os.getenv("MQ_RETRY_DELAY", "5"))  # seconds

    def __init__(self, redis_client: AsyncRedis | None = None):
        """
        Initialize the message queue.

        Args:
            redis_client: AsyncRedis client instance
        """
        self.redis = redis_client
        self._initialized = False
        self._consumer_id: str | None = None

    async def initialize(self, consumer_name: str | None = None) -> None:
        """
        Initialize Redis connection and create consumer group.

        Args:
            consumer_name: Unique consumer identifier (auto-generated if not provided)
        """
        if self._initialized:
            return

        # Initialize Redis connection
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
        if self.redis is None:
            self.redis = AsyncRedis.from_url(
                redis_url,
                encoding="utf-8",
                decode_responses=True,
            )

        try:
            await self.redis.ping()
            logger.info("Connected to Redis for message queue")
        except Exception as e:
            logger.warning(f"Redis connection failed: {e}")
            raise

        # Set consumer ID
        self._consumer_id = consumer_name or f"worker-{uuid.uuid4().hex[:8]}"

        # Create consumer group if it doesn't exist
        try:
            await self.redis.xgroup_create(
                name=self.STREAM_KEY,
                groupname=self.GROUP_NAME,
                id="0",
                mkstream=True,
            )
            logger.info(f"Created consumer group: {self.GROUP_NAME}")
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                logger.warning(f"Consumer group creation error: {e}")
            else:
                logger.debug(f"Consumer group already exists: {self.GROUP_NAME}")

        self._initialized = True

    async def enqueue(self, msg: QueuedMessage) -> str:
        """
        Add a message to the queue.

        Args:
            msg: QueuedMessage instance

        Returns:
            Message ID assigned by Redis
        """
        if not self._initialized:
            await self.initialize()

        try:
            message_id = await self.redis.xadd(
                self.STREAM_KEY,
                msg.to_dict(),
            )
            logger.debug(f"Message enqueued: {message_id} (session={msg.session_key})")
            return message_id
        except Exception as e:
            logger.error(f"Failed to enqueue message: {e}")
            raise

    async def dequeue(
        self,
        consumer_name: str | None = None,
        count: int = 1,
        block_ms: int = 5000,
    ) -> QueuedMessage | None:
        """
        Retrieve and lock a message from the queue.

        Args:
            consumer_name: Consumer identifier (uses initialized consumer if not provided)
            count: Maximum number of messages to retrieve
            block_ms: Block time in milliseconds (0 = no block)

        Returns:
            QueuedMessage or None if no messages available
        """
        if not self._initialized:
            await self.initialize()

        consumer = consumer_name or self._consumer_id
        if not consumer:
            raise RuntimeError("Consumer name not specified")

        try:
            results = await self.redis.xreadgroup(
                groupname=self.GROUP_NAME,
                consumername=consumer,
                streams={self.STREAM_KEY: ">"},
                count=count,
                block=block_ms,
            )

            if not results:
                return None

            # Extract first message from results
            stream_name, messages = results[0]
            if not messages:
                return None

            message_id, message_data = messages[0]
            msg = QueuedMessage.from_dict(message_data)

            # Attach Redis message ID for acknowledgment
            msg.metadata["_redis_id"] = message_id

            logger.debug(f"Message dequeued: {message_id} (session={msg.session_key})")
            return msg

        except Exception as e:
            logger.error(f"Failed to dequeue message: {e}")
            return None

    async def acknowledge(self, message_id: str) -> bool:
        """
        Acknowledge successful processing of a message.

        Args:
            message_id: Redis stream message ID

        Returns:
            True if acknowledged successfully
        """
        if not self._initialized:
            await self.initialize()

        try:
            await self.redis.xack(self.STREAM_KEY, self.GROUP_NAME, message_id)
            logger.debug(f"Message acknowledged: {message_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to acknowledge message: {e}")
            return False

    async def nack(
        self,
        message_id: str,
        msg: QueuedMessage,
        requeue: bool = True,
    ) -> bool:
        """
        Negative acknowledgment - mark message as failed.

        Args:
            message_id: Redis stream message ID
            msg: Original message
            requeue: If True, add to dead letter queue or retry

        Returns:
            True if handled successfully
        """
        if not self._initialized:
            await self.initialize()

        try:
            # Acknowledge to remove from pending
            await self.redis.xack(self.STREAM_KEY, self.GROUP_NAME, message_id)

            if requeue:
                # Check retry count
                retry_count = msg.metadata.get("_retry_count", 0)
                if retry_count >= self.MAX_RETRIES:
                    # Move to dead letter queue
                    await self._move_to_dlq(msg, f"Max retries exceeded ({retry_count})")
                else:
                    # Requeue with delay (simplified - in production use delayed queue)
                    msg.metadata["_retry_count"] = retry_count + 1
                    msg.metadata["_last_error"] = "Processing failed"
                    await self.redis.xadd(self.STREAM_KEY, msg.to_dict())
                    logger.warning(f"Message requeued (attempt {retry_count + 1}): {message_id}")

            return True

        except Exception as e:
            logger.error(f"Failed to nack message: {e}")
            return False

    async def _move_to_dlq(self, msg: QueuedMessage, error: str) -> None:
        """Move a message to the dead letter queue."""
        try:
            msg.metadata["_dlq_reason"] = error
            msg.metadata["_dlq_timestamp"] = datetime.utcnow().isoformat()
            await self.redis.xadd(self.DLQ_STREAM_KEY, msg.to_dict())
            logger.warning(f"Message moved to DLQ: {msg.message_id} (reason={error})")
        except Exception as e:
            logger.error(f"Failed to move message to DLQ: {e}")

    async def get_pending_messages(
        self,
        consumer_name: str | None = None,
        count: int = 10,
    ) -> list[tuple]:
        """
        Get pending (unacknowledged) messages for a consumer.

        Args:
            consumer_name: Consumer identifier
            count: Maximum number of messages to retrieve

        Returns:
            List of pending message info tuples
        """
        if not self._initialized:
            await self.initialize()

        consumer = consumer_name or self._consumer_id

        try:
            pending = await self.redis.xpending_range(
                name=self.STREAM_KEY,
                groupname=self.GROUP_NAME,
                min="-",
                max="+",
                count=count,
                consumername=consumer,
            )
            return pending
        except Exception as e:
            logger.error(f"Failed to get pending messages: {e}")
            return []

    async def claim_pending(
        self,
        consumer_name: str,
        min_idle_time_ms: int = 60000,
        count: int = 10,
    ) -> list[QueuedMessage]:
        """
        Claim pending messages that have been idle for too long.

        Args:
            consumer_name: Consumer to claim messages for
            min_idle_time_ms: Minimum idle time in milliseconds
            count: Maximum number of messages to claim

        Returns:
            List of claimed messages
        """
        if not self._initialized:
            await self.initialize()

        try:
            claimed = await self.redis.xclaim(
                name=self.STREAM_KEY,
                groupname=self.GROUP_NAME,
                consumername=consumer_name,
                min_idle_time=min_idle_time_ms,
                message_ids=["0"],  # Will be replaced with actual IDs
                count=count,
            )

            messages = []
            for message_id, message_data in claimed:
                msg = QueuedMessage.from_dict(message_data)
                msg.metadata["_redis_id"] = message_id
                messages.append(msg)

            logger.info(f"Claimed {len(messages)} pending messages")
            return messages

        except Exception as e:
            logger.error(f"Failed to claim pending messages: {e}")
            return []

    async def get_queue_stats(self) -> Dict[str, Any]:
        """Get statistics about the queue."""
        if not self._initialized:
            await self.initialize()

        try:
            info = await self.redis.xinfo_stream(self.STREAM_KEY)
            group_info = await self.redis.xinfo_groups(self.STREAM_KEY)

            return {
                "stream_length": info.get("length", 0),
                "first_entry": info.get("first-entry"),
                "last_entry": info.get("last-entry"),
                "groups": len(info.get("groups", [])),
                "consumer_groups": group_info,
            }
        except Exception as e:
            logger.error(f"Failed to get queue stats: {e}")
            return {}

    async def close(self) -> None:
        """Close Redis connection."""
        if self.redis:
            await self.redis.close()
            self.redis = None
        self._initialized = False
