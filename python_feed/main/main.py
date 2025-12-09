"""Entrypoint for the WebSocket feed service."""

from __future__ import annotations

import argparse
import logging
import signal
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

from python_feed.internal.config.config_loader import load_config
from python_feed.internal.health.health import HealthMonitor
from python_feed.internal.kafka.kafka_producer import KafkaTickProducer
from python_feed.internal.websocket.angel_ws_client import AngelWSClient
from python_feed.internal.logging.logger_setup import setup_logging
from python_feed import metrics

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "ws_feed_config.json"


# _derive_symbol normalizes configured instrument identifiers to (symbol, exchange).
# Parameters:
# - instrument: instrument string like "NSE:RELIANCE-EQ".
# Returns:
# - tuple of (symbol, exchange).
# Flow: split exchange/symbol, strip suffixes, handle index aliases.
def _derive_symbol(instrument: str) -> Tuple[str, str]:
    logging.getLogger("ws_feed_service").debug("Deriving symbol for instrument=%s", instrument)
    if ":" in instrument:
        exchange, symbol_part = instrument.split(":", 1)
    else:
        exchange, symbol_part = "NSE", instrument
    base = symbol_part.split("-")[0].upper()
    if base == "NIFTY50":
        base = "NIFTY"
    if base == "NIFTYBANK":
        base = "BANKNIFTY"
    return base, exchange.upper()


# build_token_maps constructs token mappings for subscription and normalization.
# Parameters:
# - instruments: list of instrument strings from config.
# Returns:
# - tuple: (tokens list, token->(symbol, exchange) map, instrument->token map).
# Flow: assign placeholder tokens (until real lookup exists), derive symbols/exchange for mapping.
def build_token_maps(
    instruments: List[str],
    explicit_token_map: Dict[str, str] | None = None,
) -> Tuple[List[str], Dict[str, Tuple[str, str]], Dict[str, str]]:
    logger = logging.getLogger("ws_feed_service")
    logger.debug("Building token maps for %s instruments (explicit map size=%s)", len(instruments), len(explicit_token_map or {}))
    tokens: List[str] = []
    token_to_meta: Dict[str, Tuple[str, str]] = {}
    instrument_to_token: Dict[str, str] = {}
    for idx, instrument in enumerate(instruments, start=1):
        symbol, exchange = _derive_symbol(instrument)
        token = None
        if explicit_token_map and instrument in explicit_token_map:
            token = str(explicit_token_map[instrument])
        if token is None:
            token = str(100000 + idx)  # fallback placeholder
        tokens.append(token)
        token_to_meta[token] = (symbol, exchange)
        instrument_to_token[instrument] = token
        logger.debug("Mapped instrument=%s -> token=%s symbol=%s exchange=%s", instrument, token, symbol, exchange)
    return tokens, token_to_meta, instrument_to_token


# _install_signal_handlers registers SIGINT/SIGTERM handlers to trigger shutdown.
# Parameters:
# - shutdown_event: event to signal termination.
# - logger: logger for diagnostics.
# Returns: None.
# Flow: bind signal handlers that set shutdown_event and log receipt.
def _install_signal_handlers(shutdown_event: threading.Event, logger: logging.Logger) -> None:
    def _handler(signum: int, frame: Any) -> None:  # pylint: disable=unused-argument
        logger.info("Received signal %s; shutting down", signum)
        shutdown_event.set()

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)
    logger.debug("Signal handlers installed for SIGINT and SIGTERM")


# _start_health_thread spins a background thread to periodically log health metrics.
# Parameters:
# - health_monitor: HealthMonitor instance.
# - shutdown_event: event to stop the thread.
# Returns:
# - Thread instance started as daemon.
# Flow: loop until shutdown_event is set, calling maybe_report and sleeping briefly.
def _start_health_thread(health_monitor: HealthMonitor, shutdown_event: threading.Event) -> threading.Thread:
    def _runner() -> None:
        while not shutdown_event.is_set():
            health_monitor.maybe_report()
            time.sleep(1)

    thread = threading.Thread(target=_runner, name="health-monitor", daemon=True)
    thread.start()
    logging.getLogger("ws_feed_service").debug("Health monitor thread started (name=%s)", thread.name)
    return thread


# main wires together config, logging, Kafka, websocket client, and lifecycle controls.
# Parameters: None (CLI args parsed internally).
# Returns: None.
# Flow: load config, set up logging/producer/health/client, install signals, run reconnect loop with backoff, flush on exit.
def main() -> None:
    parser = argparse.ArgumentParser(description="Angel SmartAPI websocket -> Kafka tick forwarder")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Path to ws_feed_config.json")
    args = parser.parse_args()

    cfg = load_config(args.config)
    logger = setup_logging(cfg["log"].get("level", "INFO"), cfg["log"].get("file", "logs/ws_feed_service.log"))
    logger.info("Configuration loaded from %s", args.config)
    metrics.start_metrics_server(9000)
    metrics.WS_CONNECTED.set(0)
    metrics.KAFKA_CONNECTED.set(0)

    instruments = (cfg.get("symbols", {}).get("indices") or []) + (cfg.get("symbols", {}).get("equities") or [])
    logger.info("Preparing subscription for %s instruments", len(instruments))
    token_overrides = cfg.get("token_map") or {}
    tokens, token_map, instrument_token_map = build_token_maps(instruments, token_overrides)
    logger.info("Token map built: %s tokens (overrides=%s)", len(tokens), len(token_overrides))

    kafka_cfg = cfg.get("kafka", {})
    producer = KafkaTickProducer(
        bootstrap_servers=kafka_cfg.get("bootstrap_servers", ""),
        topic=kafka_cfg.get("topic", "ticks.raw"),
        acks=str(kafka_cfg.get("acks", "1")),
        linger_ms=int(kafka_cfg.get("linger_ms", 5)),
        batch_size=int(kafka_cfg.get("batch_size", 32768)),
        logger=logger,
    )
    logger.info("Kafka producer initialized for topic=%s", kafka_cfg.get("topic"))

    health_monitor = HealthMonitor(logger, int(cfg.get("health", {}).get("print_stats_interval_sec", 60)))

    # tick_handler bridges websocket ticks to Kafka and health monitoring.
    # Parameters:
    # - tick: normalized tick dict.
    # Returns: None.
    # Flow: increment health counters and forward to Kafka producer.
    def tick_handler(tick: dict) -> None:
        health_monitor.on_tick()
        producer.send_tick(tick)

    angel_client = AngelWSClient(
        cfg=cfg,
        logger=logger,
        tick_handler=tick_handler,
        token_mapping=token_map,
        instrument_token_map=instrument_token_map,
    )

    shutdown_event = threading.Event()
    _install_signal_handlers(shutdown_event, logger)
    health_thread = _start_health_thread(health_monitor, shutdown_event)

    reconnect_cfg = cfg.get("reconnect", {})
    backoff = int(reconnect_cfg.get("backoff_seconds", 5))
    max_retries = int(reconnect_cfg.get("max_retries", 0) or 0)
    retries = 0

    try:
        while not shutdown_event.is_set():
            try:
                logger.info("Connecting to Angel WebSocket...")
                angel_client.connect()
                health_monitor.set_connected(True)
                angel_client.run_forever(shutdown_event)
                health_monitor.set_connected(False)
                if not shutdown_event.is_set():
                    raise RuntimeError("WebSocket loop exited unexpectedly")
            except Exception as exc:  # pylint: disable=broad-except
                health_monitor.set_connected(False)
                retries += 1
                logger.exception("WebSocket loop crashed (attempt %s): %s", retries, exc)
                metrics.ERRORS_TOTAL.inc()
                if max_retries > 0 and retries > max_retries:
                    logger.error("Max retries exceeded, exiting main loop")
                    break
                time.sleep(backoff)
                continue
    finally:
        logger.info("Shutdown requested, closing connections...")
        shutdown_event.set()
        angel_client.disconnect()
        producer.flush()
        health_thread.join(timeout=2)
        logger.info("Shutdown complete")


if __name__ == "__main__":
    main()
