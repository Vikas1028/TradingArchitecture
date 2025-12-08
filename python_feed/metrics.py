"""Prometheus metrics for python_feed service."""

from __future__ import annotations

from prometheus_client import Counter, Gauge, start_http_server

# Gauges
# 1 when websocket connected, 0 when disconnected.
WS_CONNECTED = Gauge("python_feed_ws_connected", "WebSocket connection status (1=connected,0=disconnected)")
# 1 when Kafka producer is healthy, 0 on failure.
KAFKA_CONNECTED = Gauge("python_feed_kafka_connected", "Kafka producer status (1=connected,0=disconnected)")

# Counters
# Total ticks received from SmartAPI websocket.
TICKS_RECEIVED = Counter("python_feed_ticks_received_total", "Total ticks received from websocket")
# Total ticks successfully published to Kafka.
TICKS_PUBLISHED = Counter("python_feed_ticks_published_total", "Total ticks published to Kafka")
# Total websocket reconnect attempts.
WS_RECONNECTS = Counter("python_feed_ws_reconnects_total", "Total websocket reconnect attempts")
# Total errors encountered.
ERRORS_TOTAL = Counter("python_feed_errors_total", "Total errors in python_feed")


def start_metrics_server(port: int = 9000) -> None:
    """Start Prometheus metrics HTTP server on the given port."""
    start_http_server(port)
