# Nanobot Scalability Guide for 1000+ Concurrent Users

این راهنما تغییرات اعمال شده برای اسکیل‌پذیری پروژه Nanobot به منظور پاسخگویی به ۱۰۰۰ کاربر همزمان را توضیح می‌دهد.

## تغییرات اعمال شده

### ۱. بهینه‌سازی API Server (`nanobot/api/server.py`)

- **افزایش محدودیت‌های HTTP**: تنظیم `max_field_size` و `max_line_size` به 8192 بایت
- **اضافه کردن Metrics Middleware**: ردیابی تعداد درخواست‌ها، اتصالات فعال، و میانگین زمان پاسخ
- **Endpoint جدید `/metrics`**: برای مانیتورینگ عملکرد سرور
- **بهبود مدیریت Session Locks**: برای کاهش contention در محیط‌های پرترافیک

```python
# نمونه استفاده از endpoint metrics
curl http://localhost:8900/metrics
# خروجی: {"active_connections": 45, "total_requests": 1234, "failed_requests": 2, "avg_response_time_ms": 156.7}
```

### ۲. بهینه‌سازی WebSocket Channel (`nanobot/channels/websocket/runtime.py`)

- **تنظیم `max_connections`**: پیش‌فرض ۱۰۰۰ اتصال همزمان (قابل تنظیم تا ۱۰۰۰۰)
- **Connection Timeout قابل تنظیم**: پیش‌فرض ۳۰ ثانیه
- **Connection Tracking**: شمارش و مدیریت اتصالات فعال
- **Rate Limiting داخلی**: جلوگیری از overload سرور
- **بهینه‌سازی Ping/Pong**: برای تشخیص سریع اتصالات قطع شده

```yaml
# پیکربندی WebSocket در config.yaml
websocket:
  max_connections: 1000
  connection_timeout_s: 30.0
  ping_interval_s: 20.0
  ping_timeout_s: 20.0
```

### ۳. Load Balancer با Nginx (`nginx.conf`)

- **Upstream Configuration**: توزیع بار بین چندین instance
- **Least Connections Algorithm**: برای API servers
- **Sticky Sessions**: برای WebSocket connections (با ip_hash)
- **Keepalive Connections**: کاهش overhead اتصالات مکرر
- **Health Checks**: تشخیص خودکار سرویس‌های ناسالم

```bash
# اجرای nginx به عنوان load balancer
docker-compose up -d nginx
```

### ۴. Docker Compose با Horizontal Scaling (`docker-compose.yml`)

- **۳ Replica از API Service**: api-1, api-2, api-3
- **۳ Replica از WebSocket Service**: ws-1, ws-2, ws-3
- **Redis برای Session Management**: caching و session persistence
- **Network ایزوله**: برای ارتباط امن بین سرویس‌ها
- **Resource Limits**: مدیریت مصرف CPU و Memory

```bash
# شروع تمام سرویس‌ها
docker-compose up -d

# مشاهده وضعیت سرویس‌ها
docker-compose ps

# مشاهده لاگ‌ها
docker-compose logs -f nginx
```

### ۵. معماری نهایی

```
                    ┌─────────────┐
                    │   Clients   │
                    │  (1000+)    │
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │    Nginx    │
                    │Load Balancer│
                    └──────┬──────┘
                           │
          ┌────────────────┼────────────────┐
          │                │                │
    ┌─────▼─────┐   ┌─────▼─────┐   ┌─────▼─────┐
    │  API-1    │   │  API-2    │   │  API-3    │
    │ :8900     │   │ :8900     │   │ :8900     │
    └───────────┘   └───────────┘   └───────────┘
    
          ┌────────────────────────────────┐
          │                │                │
    ┌─────▼─────┐   ┌─────▼─────┐   ┌─────▼─────┐
    │  WS-1     │   │  WS-2     │   │  WS-3     │
    │ :8765     │   │ :8765     │   │ :8765     │
    └─────┬─────┘   └─────┬─────┘   └─────┬─────┘
          │               │               │
          └───────────────┼───────────────┘
                          │
                    ┌─────▼─────┐
                    │   Redis   │
                    │ :6379     │
                    └───────────┘
```

## دستورالعمل استقرار

### روش ۱: استفاده از Docker Compose (توصیه شده)

```bash
# کپی فایل پیکربندی
cp .env.example .env

# تنظیم متغیرهای محیطی
echo "NANOBOT_CHANNELS=whatsapp" >> .env

# شروع سرویس‌ها
docker-compose up -d

# بررسی وضعیت
docker-compose ps

# مشاهده لاگ‌ها
docker-compose logs -f
```

### روش ۲: استقرار دستی با Nginx

```bash
# نصب nginx
sudo apt-get install nginx

# کپی پیکربندی
sudo cp nginx.conf /etc/nginx/nginx.conf

# راه‌اندازی مجدد nginx
sudo systemctl restart nginx

# شروع نانوبات instances
nanobot serve --host 0.0.0.0 --port 8901 &
nanobot serve --host 0.0.0.0 --port 8902 &
nanobot serve --host 0.0.0.0 --port 8903 &
```

## مانیتورینگ و تنظیم Performance

### بررسی Metrics

```bash
# دریافت metrics از API
curl http://localhost/metrics

# مشاهده اتصالات فعال WebSocket
docker-compose exec ws-1 netstat -an | grep ESTABLISHED | wc -l
```

### تنظیم پارامترها بر اساس Load

اگر تعداد کاربران بیشتر شد:

1. افزایش replicaها در docker-compose.yml
2. افزایش `max_connections` در WebSocket config
3. افزودن upstream serverهای بیشتر در nginx.conf
4. افزایش منابع (CPU/Memory) allocated به هر container

### عیب‌یابی

```bash
# بررسی سلامت سرویس‌ها
docker-compose ps

# مشاهده لاگ nginx
docker-compose logs nginx

# مشاهده لاگ API
docker-compose logs api-1

# مشاهده لاگ WebSocket
docker-compose logs ws-1

# تست connectivity
curl http://localhost/health
```

## بهترین روش‌ها

1. **همیشه از HTTPS استفاده کنید** در production
2. **Rate Limiting** را برای API endpoints فعال کنید
3. **Monitoring** را با ابزارهایی مثل Prometheus/Grafana تنظیم کنید
4. **Auto-scaling** را در صورت امکان فعال کنید
5. **Backup** منظم از داده‌ها بگیرید
6. **Load Testing** قبل از deployment انجام دهید

## تست Load

```bash
# نصب apache-bench
sudo apt-get install apache2-utils

# تست API با 1000 درخواست همزمان
ab -n 10000 -c 1000 http://localhost/v1/chat/completions

# تست WebSocket با wsbench
npm install -g wsbench
wsbench run -c 1000 -n 10000 ws://localhost/ws
```

## نتیجه‌گیری

با این تغییرات، پروژه Nanobot آماده پاسخگویی به ۱۰۰۰+ کاربر همزمان است. معماری طراحی شده شامل:

- ✅ Load Balancing با Nginx
- ✅ Horizontal Scaling با Docker Compose
- ✅ Session Management با Redis
- ✅ Connection Limits و Rate Limiting
- ✅ Monitoring و Metrics
- ✅ Health Checks

برای اطلاعات بیشتر به مستندات هر فایل مراجعه کنید.
