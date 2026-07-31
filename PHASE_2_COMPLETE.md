# فاز ۲: جداسازی معماری + State Management

## 🎯 هدف
امکان مقیاس‌افقی با Gateway/Worker Split

## 📊 معیار موفقیت
- ۳ Worker مستقل بدون Shared Memory
- Session در Redis/PG ذخیره شود
- قابلیت اجرای موازی چندین Gateway و Worker

---

## ✅ تغییرات انجام شده

### ۱. PostgreSQL Schema (`nanobot/db/models.py`)
جدول‌های جدید:
- `users`: اطلاعات کاربران
- `sessions`: sessionهای conversation با state
- `memories`: حافظه بلندمدت
- `cron_jobs`: کارهای زمان‌بندی شده

**ویژگی‌ها:**
- Async SQLAlchemy ORM
- Indexes بهینه برای queryهای سریع
- Relationships بین مدل‌ها
- Metadata JSON برای انعطاف‌پذیری

### ۲. Distributed Session Store (`nanobot/session/distributed_store.py`)
ذخیره‌سازی دو لایه‌ای:
- **لایه ۱ (Redis)**: Cache سریع با TTL 24 ساعته
- **لایه ۲ (PostgreSQL)**: ذخیره‌سازی دائمی

**متدهای اصلی:**
```python
store = DistributedSessionStore()
await store.initialize()

# Load session (اول Redis، سپس PG)
data = await store.load("session_key_123")

# Save session (همزمان به Redis و PG)
await store.save("session_key_123", {"state": {...}})

# Delete session
await store.delete("session_key_123")
```

### ۳. Distributed Lock (`nanobot/session/lock.py`)
قفل توزیع‌شده با Redis SETNX

**ویژگی‌ها:**
- Auto-expiration برای جلوگیری از deadlock
- Ownership verification
- Reentrant lock support
- Async context manager

**مثال استفاده:**
```python
from nanobot.session.lock import DistributedLock

async with DistributedLock(redis, "session:123"):
    # Critical section - فقط یک worker اجرا می‌کند
    await process_session(session_id)
```

### ۴. Message Queue (`nanobot/queue/message_queue.py`)
صف پیام با Redis Streams

**ویژگی‌ها:**
- Consumer groups برای پردازش موازی
- Message acknowledgment
- Pending message tracking
- Dead letter queue (DLQ)

**مثال استفاده:**
```python
from nanobot.queue import MessageQueue, QueuedMessage

mq = MessageQueue()
await mq.initialize()

# Enqueue
msg = QueuedMessage(
    session_key="sess_123",
    user_id="user_456",
    content="Hello",
    reply_channel="response:sess_123"
)
await mq.enqueue(msg)

# Dequeue (worker side)
msg = await mq.dequeue("worker-1")
if msg:
    await process_message(msg)
    await mq.acknowledge(msg.metadata["_redis_id"])
```

### ۵. Stateless Gateway (`nanobot/gateway/app.py`)
Gateway بدون state برای پذیرش اتصالات

**مسئولیت‌ها:**
- WebSocket connections
- Message enqueueing
- Response routing via Pub/Sub

**ندارد:**
- پردازش پیام
- LLM inference
- Tool execution

**Endpoints:**
- `GET /health` - Health check
- `GET /stats` - آمار Gateway
- `WebSocket /ws/{session_key}` - اتصال کلاینت
- `POST /api/v1/message` - ارسال پیام HTTP

### ۶. Worker Pool (`nanobot/worker/main.py`)
Workerهای مستقل برای پردازش پیام

**چرخه حیات Worker:**
1. Pull از queue
2. Acquire distributed lock
3. Load session state
4. Process message
5. Save session state
6. Publish response
7. Acknowledge message

**اجرا:**
```bash
# تک worker
python -m nanobot.worker.main

# چند worker
python -m nanobot.worker.main --workers 3
```

---

## 🏗️ معماری جدید

```
┌─────────────┐      ┌──────────────┐      ┌─────────────┐
│   Client    │◄────►│   Gateway    │◄────►│    Redis    │
│  (Browser)  │ WebSocket           │      │  (Pub/Sub)  │
└─────────────┘      └──────────────┘      └─────────────┘
                            │                     │
                            │ enqueue             │ subscribe
                            ▼                     │
                     ┌──────────────┐            │
                     │    Queue     │────────────┘
                     │ (Redis Stream)
                     └──────────────┘
                            │
              ┌─────────────┼─────────────┐
              │             │             │
              ▼             ▼             ▼
       ┌──────────┐  ┌──────────┐  ┌──────────┐
       │ Worker 1 │  │ Worker 2 │  │ Worker 3 │
       └──────────┘  └──────────┘  └──────────┘
              │             │             │
              └─────────────┼─────────────┘
                            │
                            ▼
                     ┌──────────────┐
                     │  PostgreSQL  │
                     │  (Sessions)  │
                     └──────────────┘
```

---

## 🔧 تنظیمات Environment Variables

```bash
# Database
DATABASE_URL=postgresql+asyncpg://nanobot:nanobot@localhost:5432/nanobot
DB_POOL_SIZE=10
DB_MAX_OVERFLOW=20

# Redis
REDIS_URL=redis://localhost:6379

# Session
SESSION_REDIS_TTL=86400  # 24 hours

# Lock
LOCK_DEFAULT_TTL=30
LOCK_TIMEOUT=10

# Message Queue
MQ_MAX_RETRIES=3
MQ_RETRY_DELAY=5

# Worker
WORKER_COUNT=3
```

---

## 🚀 راه‌اندازی

### ۱. راه‌اندازی زیرساخت
```bash
docker-compose -f docker-compose.infra.yml up -d
```

### ۲. ایجاد جداول دیتابیس
```python
from nanobot.db import init_database
await init_database()
```

### ۳. اجرای Gateway
```python
from nanobot.gateway import gateway

await gateway.initialize()
# Run with uvicorn
# uvicorn nanobot.gateway.app:gateway.app --host 0.0.0.0 --port 8000
```

### ۴. اجرای Workers
```bash
# تک worker
python -m nanobot.worker.main

# چند worker
python -m nanobot.worker.main --workers 3
```

---

## 📈 مقیاس‌پذیری

### افقی (Horizontal Scaling)
- **Gateway**: چندین instance پشت load balancer
- **Workers**: افزایش تعداد workers با `WORKER_COUNT`
- **Redis Cluster**: برای حجم بسیار بالا
- **PostgreSQL Read Replicas**: برای read-heavy workload

### عمودی (Vertical Scaling)
- افزایش `pool_size` دیتابیس
- افزایش `max_concurrency` workers
- افزایش memory Redis

---

## 🔍 Monitoring

### آمار Gateway
```bash
curl http://localhost:8000/stats
```

### آمار Worker
```python
worker = Worker()
stats = worker.get_stats()
# {
#   "worker_id": "worker-abc123",
#   "running": True,
#   "processed_count": 1234,
#   "error_count": 5,
#   "active_tasks": 0
# }
```

### آمار Queue
```python
stats = await mq.get_queue_stats()
# {
#   "stream_length": 10,
#   "groups": 1,
#   "consumer_groups": [...]
# }
```

---

## ⚠️ نکات مهم

### ۱. Session Consistency
- همیشه از distributed lock برای session استفاده کنید
- از race condition با LockManager جلوگیری کنید

### ۲. Message Reliability
- از acknowledgment صحیح استفاده کنید
- DLQ را مانیتور کنید
- Retry logic را tune کنید

### ۳. Performance
- Redis TTL را مناسب تنظیم کنید
- Connection pool sizes را optimize کنید
- از batch processing استفاده کنید

### ۴. Error Handling
- تمام خطاها را log کنید
- Graceful shutdown پیاده‌سازی کنید
- Circuit breaker اضافه کنید (فاز ۴)

---

## 📝 تست

### تست Session Store
```python
import asyncio
from nanobot.session import DistributedSessionStore

async def test_session():
    store = DistributedSessionStore()
    await store.initialize()
    
    # Save
    await store.save("test_123", {"key": "test_123", "state": {"count": 1}})
    
    # Load
    data = await store.load("test_123")
    assert data["state"]["count"] == 1
    
    # Delete
    await store.delete("test_123")
    assert await store.exists("test_123") == False
    
    await store.close()

asyncio.run(test_session())
```

### تست Message Queue
```python
import asyncio
from nanobot.queue import MessageQueue, QueuedMessage

async def test_queue():
    mq = MessageQueue()
    await mq.initialize("test-consumer")
    
    # Enqueue
    msg = QueuedMessage(
        session_key="test",
        user_id="user1",
        content="test message",
        reply_channel="response:test"
    )
    msg_id = await mq.enqueue(msg)
    
    # Dequeue
    received = await mq.dequeue(count=1, block_ms=1000)
    assert received is not None
    assert received.content == "test message"
    
    # Acknowledge
    await mq.acknowledge(received.metadata["_redis_id"])
    
    await mq.close()

asyncio.run(test_queue())
```

---

## 🔜 فاز بعدی: فاز ۳

بهینه‌سازی Providerها و background jobs
