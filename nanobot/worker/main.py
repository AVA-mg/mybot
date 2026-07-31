"""
Worker Pool for Nanobot.

Pulls messages from queue, processes them through the agent, and publishes responses.
Designed to run as independent, stateless workers that can scale horizontally.
"""

import os
import sys
import asyncio
import signal
from typing import Dict, Any, Optional
from datetime import datetime
from redis.asyncio import Redis as AsyncRedis
from nanobot.logging import get_logger
from nanobot.queue.message_queue import MessageQueue, QueuedMessage
from nanobot.session.distributed_store import DistributedSessionStore
from nanobot.session.lock import DistributedLock, LockManager

logger = get_logger(__name__)


class Worker:
    """
    Individual worker that processes messages from the queue.

    Lifecycle:
    1. Initialize connections (Redis, DB, etc.)
    2. Pull messages from queue
    3. Acquire session lock
    4. Load session state
    5. Process message through agent
    6. Save session state
    7. Publish response via Pub/Sub
    8. Acknowledge message
    9. Repeat
    """

    def __init__(
        self,
        worker_id: str | None = None,
        max_concurrency: int = 1,
        poll_interval_ms: int = 100,
    ):
        """
        Initialize a worker.

        Args:
            worker_id: Unique worker identifier (auto-generated if not provided)
            max_concurrency: Maximum concurrent message processing
            poll_interval_ms: Interval between queue polls when idle
        """
        import uuid
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.max_concurrency = max_concurrency
        self.poll_interval_ms = poll_interval_ms

        self.redis: AsyncRedis | None = None
        self.message_queue: MessageQueue | None = None
        self.session_store: DistributedSessionStore | None = None
        self.lock_manager: LockManager | None = None

        self._running = False
        self._tasks: set[asyncio.Task] = set()
        self._processed_count = 0
        self._error_count = 0
        self._last_activity: datetime | None = None

    async def initialize(self) -> None:
        """Initialize worker components."""
        logger.info(f"Initializing worker: {self.worker_id}")

        # Initialize Redis
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
        self.redis = AsyncRedis.from_url(
            redis_url,
            encoding="utf-8",
            decode_responses=True,
        )
        await self.redis.ping()
        logger.info(f"Worker {self.worker_id} connected to Redis")

        # Initialize message queue
        self.message_queue = MessageQueue(redis_client=self.redis)
        await self.message_queue.initialize(consumer_name=self.worker_id)
        logger.info(f"Worker {self.worker_id} message queue initialized")

        # Initialize session store
        self.session_store = DistributedSessionStore(redis_client=self.redis)
        await self.session_store.initialize()
        logger.info(f"Worker {self.worker_id} session store initialized")

        # Initialize lock manager
        self.lock_manager = LockManager(redis_client=self.redis)
        logger.info(f"Worker {self.worker_id} lock manager initialized")

        logger.info(f"Worker {self.worker_id} initialization complete")

    async def process_message(self, msg: QueuedMessage) -> bool:
        """
        Process a single message.

        Args:
            msg: Message to process

        Returns:
            True if processing succeeded
        """
        session_key = msg.session_key
        logger.info(f"Processing message for session {session_key} (worker={self.worker_id})")

        # Acquire distributed lock for session
        lock = self.lock_manager.get_lock(f"session:{session_key}", ttl=30, timeout=10)
        acquired = await lock.acquire()

        if not acquired:
            logger.warning(f"Failed to acquire lock for session {session_key}, skipping")
            # Nack to allow requeue
            if msg.metadata.get("_redis_id"):
                await self.message_queue.nack(msg.metadata["_redis_id"], msg, requeue=True)
            return False

        try:
            # Load session state
            session_data = await self.session_store.load(session_key)
            if not session_data:
                session_data = {
                    "key": session_key,
                    "user_id": msg.user_id,
                    "state": {},
                    "context": [],
                    "message_count": 0,
                }

            # Process through agent (placeholder - integrate with actual agent)
            response = await self._run_agent(msg, session_data)

            # Update session state
            session_data["message_count"] = session_data.get("message_count", 0) + 1
            session_data["last_activity"] = datetime.utcnow().isoformat()

            # Add response to context
            if isinstance(response, dict):
                session_data.setdefault("context", []).append({
                    "role": "assistant",
                    "content": response.get("content", ""),
                    "timestamp": datetime.utcnow().isoformat(),
                })

            # Save session state
            await self.session_store.save(session_key, session_data)

            # Publish response via Pub/Sub
            reply_channel = msg.reply_channel
            response_payload = {
                "type": "response",
                "message_id": msg.message_id,
                "session_key": session_key,
                "content": response.get("content", ""),
                "metadata": response.get("metadata", {}),
                "timestamp": datetime.utcnow().isoformat(),
                "worker_id": self.worker_id,
            }

            if self.redis:
                await self.redis.publish(reply_channel, self._json_dumps(response_payload))
                logger.debug(f"Response published to {reply_channel}")

            # Acknowledge message
            if msg.metadata.get("_redis_id"):
                await self.message_queue.acknowledge(msg.metadata["_redis_id"])

            self._processed_count += 1
            self._last_activity = datetime.utcnow()
            logger.info(f"Message processed successfully for session {session_key}")

            return True

        except Exception as e:
            logger.error(f"Error processing message for session {session_key}: {e}")
            self._error_count += 1

            # Nack to allow requeue
            if msg.metadata.get("_redis_id"):
                await self.message_queue.nack(msg.metadata["_redis_id"], msg, requeue=True)

            return False

        finally:
            # Release lock
            await lock.release()

    async def _run_agent(self, msg: QueuedMessage, session_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run the agent to generate a response.

        This is a placeholder - integrate with the actual Nanobot agent.

        Args:
            msg: Input message
            session_data: Current session state

        Returns:
            Response dictionary
        """
        # TODO: Integrate with actual Nanobot agent
        # For now, return a simple echo response
        content = msg.content

        # Simulate processing delay
        await asyncio.sleep(0.1)

        return {
            "content": f"Echo: {content}",
            "metadata": {
                "worker_id": self.worker_id,
                "processing_time_ms": 100,
            },
        }

    def _json_dumps(self, obj: Any) -> str:
        """Serialize object to JSON string."""
        import json
        return json.dumps(obj, default=str)

    async def _worker_loop(self) -> None:
        """Main worker loop - continuously pull and process messages."""
        logger.info(f"Worker {self.worker_id} starting main loop")

        while self._running:
            try:
                # Dequeue a message
                msg = await self.message_queue.dequeue(
                    consumer_name=self.worker_id,
                    count=1,
                    block_ms=self.poll_interval_ms,
                )

                if msg:
                    # Process in a separate task for concurrency
                    task = asyncio.create_task(self.process_message(msg))
                    self._tasks.add(task)
                    task.add_done_callback(self._tasks.discard)

                    # Wait if at max concurrency
                    if len(self._tasks) >= self.max_concurrency:
                        await asyncio.sleep(0.1)
                else:
                    # No messages available, brief pause
                    await asyncio.sleep(self.poll_interval_ms / 1000)

            except asyncio.CancelledError:
                logger.info(f"Worker {self.worker_id} loop cancelled")
                break
            except Exception as e:
                logger.error(f"Worker {self.worker_id} loop error: {e}")
                await asyncio.sleep(1)  # Back off on error

        logger.info(f"Worker {self.worker_id} main loop stopped")

    async def start(self) -> None:
        """Start the worker."""
        if self._running:
            logger.warning(f"Worker {self.worker_id} is already running")
            return

        self._running = True
        await self.initialize()

        # Setup signal handlers
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.stop()))

        # Start main loop
        await self._worker_loop()

    async def stop(self) -> None:
        """Stop the worker gracefully."""
        if not self._running:
            return

        logger.info(f"Stopping worker {self.worker_id}...")
        self._running = False

        # Wait for pending tasks
        if self._tasks:
            logger.info(f"Waiting for {len(self._tasks)} pending tasks...")
            await asyncio.gather(*self._tasks, return_exceptions=True)

        # Close connections
        await self.close()

        logger.info(
            f"Worker {self.worker_id} stopped. "
            f"Processed: {self._processed_count}, Errors: {self._error_count}"
        )

    async def close(self) -> None:
        """Close worker resources."""
        if self.message_queue:
            await self.message_queue.close()
        if self.session_store:
            await self.session_store.close()
        if self.redis:
            await self.redis.close()

        self._tasks.clear()
        self._running = False

    def get_stats(self) -> Dict[str, Any]:
        """Get worker statistics."""
        return {
            "worker_id": self.worker_id,
            "running": self._running,
            "processed_count": self._processed_count,
            "error_count": self._error_count,
            "active_tasks": len(self._tasks),
            "last_activity": self._last_activity.isoformat() if self._last_activity else None,
        }


class WorkerPool:
    """
    Manager for multiple worker instances.

    Allows running multiple workers in a single process for testing
    or small deployments.
    """

    def __init__(self, num_workers: int = 3):
        """
        Initialize worker pool.

        Args:
            num_workers: Number of workers to run
        """
        self.num_workers = num_workers
        self.workers: list[Worker] = []
        self._running = False

    async def start(self) -> None:
        """Start all workers in the pool."""
        self._running = True
        self.workers = [
            Worker(worker_id=f"worker-{i}", max_concurrency=1)
            for i in range(self.num_workers)
        ]

        # Start all workers concurrently
        tasks = [asyncio.create_task(worker.start()) for worker in self.workers]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def stop(self) -> None:
        """Stop all workers."""
        self._running = False
        for worker in self.workers:
            await worker.stop()

    def get_stats(self) -> list[Dict[str, Any]]:
        """Get statistics for all workers."""
        return [worker.get_stats() for worker in self.workers]


# CLI entry point
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Nanobot Worker")
    parser.add_argument(
        "--workers",
        type=int,
        default=int(os.getenv("WORKER_COUNT", "1")),
        help="Number of workers to run",
    )
    args = parser.parse_args()

    print(f"Starting Nanobot Worker Pool with {args.workers} workers...")

    pool = WorkerPool(num_workers=args.workers)

    try:
        asyncio.run(pool.start())
    except KeyboardInterrupt:
        print("\nShutting down...")
        asyncio.run(pool.stop())
