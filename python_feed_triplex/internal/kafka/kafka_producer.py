"""JetStream producer wrapper for tick publishing."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import threading
from typing import Optional

from python_feed_triplex import metrics

try:
    from nats.aio.client import Client as NATS
    from nats.js.api import RetentionPolicy, StorageType, StreamConfig, DiscardPolicy
except Exception:  # pylint: disable=broad-except
    NATS = None
    StreamConfig = None
    StorageType = None
    RetentionPolicy = None
    DiscardPolicy = None


class KafkaTickProducer:
    """Thin wrapper for publishing ticks to NATS JetStream."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        acks: str,
        linger_ms: int,
        batch_size: int,
        logger: logging.Logger,
    ) -> None:
        """Initialize the background event loop and connect once to JetStream."""
        del acks, linger_ms, batch_size
        self.topic = topic
        self.logger = logger
        self._bootstrap_servers = bootstrap_servers
        self._nc: Optional[NATS] = None
        self._js = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._loop_thread: Optional[threading.Thread] = None
        self.logger.info(
            "Initializing JetStream producer: servers=%s subject_prefix=%s",
            bootstrap_servers,
            topic,
        )
        if NATS:
            try:
                self._start_loop()
                future = asyncio.run_coroutine_threadsafe(self._connect(), self._loop)
                future.result(timeout=10)
                metrics.KAFKA_CONNECTED.set(1)
                self.logger.info("JetStream producer created successfully")
            except Exception as exc:  # pylint: disable=broad-except
                self.logger.error("Failed to create JetStream producer: %s", exc)
                metrics.KAFKA_CONNECTED.set(0)
        else:
            self.logger.warning("nats-py not installed; JetStream publishing disabled")
            metrics.KAFKA_CONNECTED.set(0)

    def send_tick(self, tick: dict) -> None:
        """Publish one normalized tick to a symbol-scoped JetStream subject."""
        if not self._loop or not self._js:
            self.logger.debug("JetStream disabled; dropping tick for %s", tick.get("symbol"))
            return
        try:
            symbol = str(tick.get("symbol", "")).strip().upper()
            subject = f"{self.topic}.python_feed_triplex.{_sanitize_token(symbol)}"
            payload = json.dumps(tick).encode("utf-8")
            future = asyncio.run_coroutine_threadsafe(self._js.publish(subject, payload), self._loop)
            future.result(timeout=5)
            metrics.KAFKA_CONNECTED.set(1)
            metrics.TICKS_PUBLISHED.inc()
        except Exception as exc:  # pylint: disable=broad-except
            self.logger.error("JetStream publish failed: %s", exc)
            metrics.KAFKA_CONNECTED.set(0)
            metrics.ERRORS_TOTAL.inc()

    def flush(self) -> None:
        return

    def close(self) -> None:
        self.logger.info("Closing JetStream producer")
        if self._loop and self._nc:
            try:
                future = asyncio.run_coroutine_threadsafe(self._nc.drain(), self._loop)
                future.result(timeout=5)
            except Exception:  # pylint: disable=broad-except
                pass
        if self._loop:
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._loop_thread and self._loop_thread.is_alive():
            self._loop_thread.join(timeout=5)
        self._nc = None
        self._js = None
        self._loop = None
        self._loop_thread = None
        metrics.KAFKA_CONNECTED.set(0)

    async def _connect(self) -> None:
        """Open the NATS connection and ensure the shared tick stream exists."""
        servers = [_normalize_nats_url(self._bootstrap_servers)]
        self._nc = NATS()
        await self._nc.connect(servers=servers, reconnect_time_wait=2, max_reconnect_attempts=-1)
        self._js = self._nc.jetstream()
        await _ensure_stream(self._js, self.topic)

    def _start_loop(self) -> None:
        self._loop = asyncio.new_event_loop()

        def _runner() -> None:
            asyncio.set_event_loop(self._loop)
            self._loop.run_forever()

        self._loop_thread = threading.Thread(target=_runner, name="python-feed-jetstream", daemon=True)
        self._loop_thread.start()


async def _ensure_stream(js, prefix: str) -> None:
    """Create the tick stream once if it is not already present."""
    name = prefix.upper().replace(".", "_").replace("-", "_")
    try:
        await js.stream_info(name)
        return
    except Exception:  # pylint: disable=broad-except
        pass
    await js.add_stream(
        StreamConfig(
            name=name,
            subjects=[f"{prefix}.>"],
            storage=StorageType.FILE,
            retention=RetentionPolicy.LIMITS,
            discard=DiscardPolicy.OLD,
            max_age=72 * 60 * 60,
            num_replicas=1,
        )
    )


def _normalize_nats_url(raw: str) -> str:
    value = raw.split(",")[0].strip()
    if value.startswith("nats://") or value.startswith("tls://"):
        return value
    return f"nats://{value}"


def _sanitize_token(value: str) -> str:
    cleaned = re.sub(r"[^A-Z0-9_-]+", "_", value.strip().upper())
    return cleaned or "UNKNOWN"
