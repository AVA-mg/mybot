"""
Stateless Gateway for Nanobot.

Handles WebSocket connections and message enqueueing without processing.
All heavy processing is delegated to worker pool.
"""

import os
import json
import asyncio
from typing import Dict, Any, Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import JSONResponse
from redis.asyncio import Redis as AsyncRedis, PubSub
from nanobot.logging import get_logger
from nanobot.queue.message_queue import MessageQueue, QueuedMessage
from nanobot.session.distributed_store import DistributedSessionStore

logger = get_logger(__name__)


class GatewayApp:
    """
    Stateless API Gateway for Nanobot.

    Responsibilities:
    - Accept WebSocket connections from clients
    - Validate and enqueue messages to queue
    - Subscribe to response channels via Redis Pub/Sub
    - Forward responses back to clients

    Does NOT:
    - Process messages
    - Run LLM inference
    - Execute tools
    """

    def __init__(self):
        self.app = FastAPI(
            title="Nanobot Gateway",
            description="Stateless gateway for distributed Nanobot architecture",
            version="2.0.0",
        )
        self.redis: AsyncRedis | None = None
        self.message_queue: MessageQueue | None = None
        self.session_store: DistributedSessionStore | None = None
        self._pubsub_tasks: Dict[str, asyncio.Task] = {}
        self._websocket_connections: Dict[str, WebSocket] = {}
        self._initialized = False

        self._setup_routes()

    def _setup_routes(self) -> None:
        """Setup FastAPI routes."""

        @self.app.get("/health")
        async def health_check():
            """Health check endpoint."""
            return {"status": "healthy", "component": "gateway"}

        @self.app.get("/stats")
        async def get_stats():
            """Get gateway statistics."""
            stats = {
                "active_connections": len(self._websocket_connections),
                "pubsub_subscriptions": len(self._pubsub_tasks),
            }
            if self.message_queue:
                stats["queue"] = await self.message_queue.get_queue_stats()
            return stats

        @self.app.websocket("/ws/{session_key}")
        async def websocket_endpoint(websocket: WebSocket, session_key: str):
            """WebSocket endpoint for client connections."""
            await self._handle_websocket(websocket, session_key)

        @self.app.post("/api/v1/message")
        async def send_message(request_data: Dict[str, Any]):
            """HTTP endpoint for sending messages (alternative to WebSocket)."""
            if not self.message_queue:
                raise HTTPException(status_code=503, detail="Gateway not initialized")

            session_key = request_data.get("session_key")
            user_id = request_data.get("user_id")
            content = request_data.get("content")
            reply_channel = request_data.get("reply_channel", f"response:{session_key}")

            if not all([session_key, content]):
                raise HTTPException(status_code=400, detail="Missing required fields")

            msg = QueuedMessage(
                session_key=session_key,
                user_id=user_id,
                content=content,
                reply_channel=reply_channel,
                metadata=request_data.get("metadata", {}),
            )

            message_id = await self.message_queue.enqueue(msg)
            return {"message_id": message_id, "status": "queued"}

    async def initialize(self) -> None:
        """Initialize gateway components."""
        if self._initialized:
            return

        # Initialize Redis
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
        self.redis = AsyncRedis.from_url(
            redis_url,
            encoding="utf-8",
            decode_responses=True,
        )
        await self.redis.ping()
        logger.info("Gateway connected to Redis")

        # Initialize message queue
        self.message_queue = MessageQueue(redis_client=self.redis)
        await self.message_queue.initialize()
        logger.info("Gateway message queue initialized")

        # Initialize session store
        self.session_store = DistributedSessionStore(redis_client=self.redis)
        await self.session_store.initialize()
        logger.info("Gateway session store initialized")

        self._initialized = True
        logger.info("Gateway initialization complete")

    async def _handle_websocket(self, websocket: WebSocket, session_key: str) -> None:
        """Handle WebSocket connection lifecycle."""
        await websocket.accept()
        self._websocket_connections[session_key] = websocket

        reply_channel = f"response:{session_key}"
        logger.info(f"WebSocket connected: session={session_key}, channel={reply_channel}")

        # Subscribe to response channel
        pubsub_task = asyncio.create_task(
            self._pubsub_listener(session_key, reply_channel, websocket)
        )
        self._pubsub_tasks[session_key] = pubsub_task

        try:
            while True:
                # Receive messages from client
                data = await websocket.receive_text()
                message_data = json.loads(data)

                # Create queued message
                msg = QueuedMessage(
                    session_key=session_key,
                    user_id=message_data.get("user_id"),
                    content=message_data.get("content", ""),
                    reply_channel=reply_channel,
                    metadata=message_data.get("metadata", {}),
                )

                # Enqueue for processing
                if self.message_queue:
                    message_id = await self.message_queue.enqueue(msg)
                    logger.debug(f"Message enqueued: {message_id}")

                    # Send acknowledgment
                    await websocket.send_json({
                        "type": "ack",
                        "message_id": message_id,
                        "status": "queued",
                    })

        except WebSocketDisconnect:
            logger.info(f"WebSocket disconnected: session={session_key}")
        except Exception as e:
            logger.error(f"WebSocket error for session {session_key}: {e}")
        finally:
            # Cleanup
            self._websocket_connections.pop(session_key, None)
            pubsub_task.cancel()
            self._pubsub_tasks.pop(session_key, None)

            try:
                await websocket.close()
            except Exception:
                pass

    async def _pubsub_listener(
        self,
        session_key: str,
        channel: str,
        websocket: WebSocket,
    ) -> None:
        """Listen for responses on Redis Pub/Sub and forward to WebSocket."""
        if not self.redis:
            return

        pubsub = self.redis.pubsub()
        await pubsub.subscribe(channel)

        try:
            async for message in pubsub.listen():
                if message["type"] == "message":
                    response_data = json.loads(message["data"])

                    # Forward to WebSocket
                    if session_key in self._websocket_connections:
                        ws = self._websocket_connections[session_key]
                        try:
                            await ws.send_json(response_data)
                        except Exception as e:
                            logger.warning(f"Failed to send to WebSocket: {e}")
                            break

        except asyncio.CancelledError:
            logger.debug(f"PubSub listener cancelled for session {session_key}")
        except Exception as e:
            logger.error(f"PubSub listener error for session {session_key}: {e}")
        finally:
            await pubsub.unsubscribe(channel)
            await pubsub.close()

    async def publish_response(self, reply_channel: str, response: Dict[str, Any]) -> bool:
        """
        Publish a response to Redis Pub/Sub.

        This is called by workers after processing a message.

        Args:
            reply_channel: Redis channel name
            response: Response data to publish

        Returns:
            True if published successfully
        """
        if not self.redis:
            return False

        try:
            await self.redis.publish(reply_channel, json.dumps(response))
            logger.debug(f"Response published to {reply_channel}")
            return True
        except Exception as e:
            logger.error(f"Failed to publish response: {e}")
            return False

    async def close(self) -> None:
        """Close gateway and cleanup resources."""
        # Cancel all pubsub tasks
        for task in self._pubsub_tasks.values():
            task.cancel()

        # Close all WebSocket connections
        for ws in self._websocket_connections.values():
            try:
                await ws.close()
            except Exception:
                pass

        # Close Redis connections
        if self.message_queue:
            await self.message_queue.close()
        if self.session_store:
            await self.session_store.close()
        if self.redis:
            await self.redis.close()

        self._pubsub_tasks.clear()
        self._websocket_connections.clear()
        self._initialized = False
        logger.info("Gateway closed")


# Global gateway instance
gateway = GatewayApp()
