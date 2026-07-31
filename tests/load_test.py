"""
Nanobot Load Test - Realistic WebSocket + LLM Simulation

This test simulates multiple concurrent users interacting with the Nanobot system
to verify that the Gateway/Worker architecture can handle 1000+ concurrent users.

Usage:
    python -m tests.load_test --users 100 --duration 60
    
Requirements:
    - Running Nanobot instance with Gateway/Worker architecture
    - Redis and PostgreSQL available
    - Feature flags enabled for parallel_tools and async_tools
"""

import asyncio
import json
import time
import argparse
from typing import Optional
from dataclasses import dataclass, field
from collections import defaultdict

try:
    import websockets
except ImportError:
    print("Please install websockets: pip install websockets")
    exit(1)


@dataclass
class TestMetrics:
    """Collect metrics during load testing."""
    
    total_users: int = 0
    successful_connections: int = 0
    failed_connections: int = 0
    successful_messages: int = 0
    failed_messages: int = 0
    echo_responses: int = 0  # Detect if Worker is still returning echo
    avg_latency_ms: float = 0.0
    p95_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0
    latencies: list[float] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    
    def record_latency(self, latency_ms: float):
        self.latencies.append(latency_ms)
        
    def calculate_percentiles(self):
        if not self.latencies:
            return
        
        self.latencies.sort()
        n = len(self.latencies)
        self.avg_latency_ms = sum(self.latencies) / n
        self.p95_latency_ms = self.latencies[int(n * 0.95)] if n > 0 else 0
        self.p99_latency_ms = self.latencies[int(n * 0.99)] if n > 0 else 0


async def simulate_user(
    user_id: int,
    ws_url: str,
    metrics: TestMetrics,
    message: str = "سلام، حالت چطور است؟",
    timeout: float = 60.0,
) -> bool:
    """
    Simulate a single user connecting via WebSocket and sending a message.
    
    Args:
        user_id: Unique user identifier
        ws_url: WebSocket server URL
        metrics: Metrics collector
        message: Test message to send
        timeout: Response timeout in seconds
        
    Returns:
        True if test passed, False otherwise
    """
    session_key = f"test_user_{user_id}"
    start_time = time.time()
    
    try:
        async with websockets.connect(ws_url, close_timeout=5) as ws:
            # Send initial message
            payload = {
                "content": message,
                "session_key": session_key,
            }
            
            await ws.send(json.dumps(payload))
            
            # Wait for response
            try:
                response = await asyncio.wait_for(ws.recv(), timeout=timeout)
                latency_ms = (time.time() - start_time) * 1000
                
                metrics.record_latency(latency_ms)
                
                # Parse response
                try:
                    data = json.loads(response)
                    content = data.get("content", "")
                    
                    # Check if this is an echo response (Worker not properly integrated)
                    if content.startswith("Echo:"):
                        metrics.echo_responses += 1
                        metrics.errors.append(
                            f"User {user_id}: Worker returned echo response! "
                            "Agent loop not properly integrated."
                        )
                        metrics.failed_messages += 1
                        return False
                    
                    # Check for actual LLM response or tool execution
                    if content or data.get("type") == "message":
                        metrics.successful_messages += 1
                        return True
                    else:
                        metrics.failed_messages += 1
                        return False
                        
                except json.JSONDecodeError:
                    # Raw text response is also acceptable
                    if response and not response.startswith("Echo:"):
                        metrics.successful_messages += 1
                        return True
                    else:
                        metrics.echo_responses += 1
                        metrics.failed_messages += 1
                        return False
                        
            except asyncio.TimeoutError:
                latency_ms = (time.time() - start_time) * 1000
                metrics.record_latency(latency_ms)
                metrics.failed_messages += 1
                metrics.errors.append(f"User {user_id}: Timeout after {timeout}s")
                return False
                
    except Exception as e:
        metrics.failed_connections += 1
        metrics.errors.append(f"User {user_id}: Connection failed - {str(e)}")
        return False


async def run_load_test(
    ws_url: str,
    num_users: int,
    duration_seconds: int,
    batch_size: int = 10,
) -> TestMetrics:
    """
    Run load test with specified number of concurrent users.
    
    Args:
        ws_url: WebSocket server URL
        num_users: Total number of users to simulate
        duration_seconds: Test duration in seconds
        batch_size: Number of users to start in each batch
        
    Returns:
        TestMetrics with collected statistics
    """
    metrics = TestMetrics(total_users=num_users)
    
    print(f"\n{'='*60}")
    print(f"NANOBOT LOAD TEST")
    print(f"{'='*60}")
    print(f"WebSocket URL: {ws_url}")
    print(f"Total Users: {num_users}")
    print(f"Duration: {duration_seconds}s")
    print(f"Batch Size: {batch_size}")
    print(f"{'='*60}\n")
    
    start_time = time.time()
    tasks = []
    
    # Start users in batches to avoid overwhelming the system
    for batch_start in range(0, num_users, batch_size):
        batch_end = min(batch_start + batch_size, num_users)
        batch_users = list(range(batch_start, batch_end))
        
        print(f"Starting batch {batch_start//batch_size + 1}: "
              f"users {batch_start}-{batch_end-1}")
        
        batch_tasks = [
            asyncio.create_task(
                simulate_user(user_id, ws_url, metrics)
            )
            for user_id in batch_users
        ]
        
        tasks.extend(batch_tasks)
        
        # Small delay between batches
        if batch_end < num_users:
            await asyncio.sleep(0.5)
    
    # Wait for all tasks with overall timeout
    try:
        await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=True),
            timeout=duration_seconds
        )
    except asyncio.TimeoutError:
        print(f"\n⚠️  Test reached duration limit ({duration_seconds}s)")
        # Cancel remaining tasks
        for task in tasks:
            if not task.done():
                task.cancel()
    
    # Calculate final metrics
    metrics.successful_connections = metrics.successful_messages
    metrics.failed_connections = metrics.failed_messages
    metrics.calculate_percentiles()
    
    elapsed = time.time() - start_time
    
    # Print results
    print(f"\n{'='*60}")
    print(f"LOAD TEST RESULTS")
    print(f"{'='*60}")
    print(f"Total Duration: {elapsed:.2f}s")
    print(f"Total Users: {metrics.total_users}")
    print(f"Successful Messages: {metrics.successful_messages}")
    print(f"Failed Messages: {metrics.failed_messages}")
    print(f"Echo Responses (ERROR): {metrics.echo_responses}")
    print(f"\nLatency Statistics:")
    print(f"  Average: {metrics.avg_latency_ms:.2f}ms")
    print(f"  P95: {metrics.p95_latency_ms:.2f}ms")
    print(f"  P99: {metrics.p99_latency_ms:.2f}ms")
    
    if metrics.errors:
        print(f"\nErrors ({len(metrics.errors)}):")
        for error in metrics.errors[:10]:  # Show first 10 errors
            print(f"  - {error}")
        if len(metrics.errors) > 10:
            print(f"  ... and {len(metrics.errors) - 10} more")
    
    # Validation checks
    print(f"\n{'='*60}")
    print(f"VALIDATION")
    print(f"{'='*60}")
    
    success_rate = (metrics.successful_messages / max(metrics.total_users, 1)) * 100
    print(f"Success Rate: {success_rate:.1f}%")
    
    if metrics.echo_responses > 0:
        print("❌ FAIL: Worker is still returning echo responses!")
        print("   Action Required: Integrate Worker with Agent Loop")
    elif success_rate >= 95:
        print("✅ PASS: Success rate ≥ 95%")
    else:
        print(f"⚠️  WARNING: Success rate below 95%")
    
    if metrics.p95_latency_ms < 5000:
        print("✅ PASS: P95 latency < 5s")
    else:
        print(f"⚠️  WARNING: P95 latency {metrics.p95_latency_ms:.2f}ms ≥ 5s")
    
    print(f"{'='*60}\n")
    
    return metrics


def main():
    parser = argparse.ArgumentParser(description="Nanobot Load Test")
    parser.add_argument(
        "--url", "-u",
        default="ws://localhost:8765",
        help="WebSocket server URL (default: ws://localhost:8765)"
    )
    parser.add_argument(
        "--users", "-n",
        type=int,
        default=100,
        help="Number of concurrent users to simulate (default: 100)"
    )
    parser.add_argument(
        "--duration", "-d",
        type=int,
        default=120,
        help="Test duration in seconds (default: 120)"
    )
    parser.add_argument(
        "--batch-size", "-b",
        type=int,
        default=20,
        help="Users per batch (default: 20)"
    )
    
    args = parser.parse_args()
    
    asyncio.run(run_load_test(
        ws_url=args.url,
        num_users=args.users,
        duration_seconds=args.duration,
        batch_size=args.batch_size,
    ))


if __name__ == "__main__":
    main()
