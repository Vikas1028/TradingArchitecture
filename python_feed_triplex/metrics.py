"""Prometheus metrics for python_feed service."""

from __future__ import annotations

import platform
import resource
import threading
import time

from prometheus_client import Counter, Gauge, start_http_server

# Gauges
# 1 when websocket connected, 0 when disconnected.
WS_CONNECTED = Gauge("python_feed_ws_connected", "WebSocket connection status (1=connected,0=disconnected)")
# 1 when Kafka producer is healthy, 0 on failure.
KAFKA_CONNECTED = Gauge("python_feed_kafka_connected", "Kafka producer status (1=connected,0=disconnected)")
# Number of currently connected websocket clients.
WS_CLIENTS_ACTIVE = Gauge("python_feed_ws_clients_active", "Active websocket client count")
# Per-client websocket connected state.
WS_CLIENT_CONNECTED = Gauge(
    "python_feed_ws_client_connected",
    "Per-client websocket connection status (1=connected,0=disconnected)",
    ["client"],
)
# Last reconnect/disconnect/no-tick-relogin unix timestamps by client.
WS_CLIENT_LAST_CONNECT_UNIX = Gauge(
    "python_feed_ws_client_last_connect_unixtime",
    "Unix timestamp of last successful websocket connect by client",
    ["client"],
)
WS_CLIENT_LAST_DISCONNECT_UNIX = Gauge(
    "python_feed_ws_client_last_disconnect_unixtime",
    "Unix timestamp of last websocket disconnect by client",
    ["client"],
)
WS_CLIENT_LAST_NO_TICK_RELOGIN_UNIX = Gauge(
    "python_feed_ws_client_last_no_tick_relogin_unixtime",
    "Unix timestamp of last no-tick forced relogin by client",
    ["client"],
)

# Counters
# Total ticks received from SmartAPI websocket.
TICKS_RECEIVED = Counter("python_feed_ticks_received_total", "Total ticks received from websocket")
# Total ticks received by client.
TICKS_RECEIVED_BY_CLIENT = Counter(
    "python_feed_ticks_received_by_client_total",
    "Total ticks received from websocket by client",
    ["client"],
)
# Total ticks successfully published to Kafka.
TICKS_PUBLISHED = Counter("python_feed_ticks_published_total", "Total ticks published to Kafka")
# Total ticks published to Kafka by source client.
TICKS_PUBLISHED_BY_CLIENT = Counter(
    "python_feed_ticks_published_by_client_total",
    "Total ticks published to Kafka by source websocket client",
    ["client"],
)
# Total duplicate ticks dropped by source client.
TICKS_DUPLICATES_DROPPED = Counter(
    "python_feed_ticks_duplicates_dropped_total",
    "Total duplicate ticks dropped by source websocket client",
    ["client"],
)
# Total websocket reconnect attempts.
WS_RECONNECTS = Counter("python_feed_ws_reconnects_total", "Total websocket reconnect attempts")
# Total websocket open events (initial connect + subsequent reopens).
WS_OPEN_EVENTS = Counter("python_feed_ws_open_events_total", "Total websocket on_open events")
# Per-client connect/disconnect counters.
WS_CLIENT_CONNECTS = Counter(
    "python_feed_ws_client_connects_total",
    "Total websocket connect events by client",
    ["client"],
)
WS_CLIENT_DISCONNECTS = Counter(
    "python_feed_ws_client_disconnects_total",
    "Total websocket disconnect events by client and reason",
    ["client", "reason"],
)
# Total reconnects forced by no-tick watchdog.
WS_STALL_RECONNECTS = Counter(
    "python_feed_ws_stall_reconnects_total",
    "Total websocket reconnects forced due to no-tick stall",
)
# Per-client no-tick relogin counter.
WS_CLIENT_NO_TICK_RELOGINS = Counter(
    "python_feed_ws_client_no_tick_relogin_total",
    "Total no-tick forced relogins by client",
    ["client"],
)
# Total errors encountered.
ERRORS_TOTAL = Counter("python_feed_errors_total", "Total errors in python_feed")
# Age of the most recent tick in seconds.
LAST_TICK_AGE_SECONDS = Gauge("python_feed_last_tick_age_seconds", "Seconds since the last received tick")
# Explicit process gauges for macOS, where the default Prometheus process collector is not reliable in this setup.
PROCESS_CPU_SECONDS = Gauge(
    "python_feed_process_cpu_seconds_total",
    "Total process CPU seconds consumed by python_feed",
)
PROCESS_RESIDENT_MEMORY_BYTES = Gauge(
    "python_feed_process_resident_memory_bytes",
    "Resident memory used by python_feed in bytes",
)


def _resident_memory_bytes() -> int:
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if platform.system().lower() == "darwin":
        return int(value)
    return int(value * 1024)


def _process_sampler() -> None:
    while True:
        PROCESS_CPU_SECONDS.set(time.process_time())
        PROCESS_RESIDENT_MEMORY_BYTES.set(_resident_memory_bytes())
        time.sleep(1.0)


def start_metrics_server(port: int = 9000) -> None:
    """Start Prometheus metrics HTTP server on the given port."""
    start_http_server(port)
    threading.Thread(target=_process_sampler, name="python-feed-metrics", daemon=True).start()
