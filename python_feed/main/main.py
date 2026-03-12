"""Entrypoint for the WebSocket feed service."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import signal
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from python_feed.internal.config.config_loader import load_config
from python_feed.internal.kafka.kafka_producer import KafkaTickProducer
from python_feed.internal.login.smartapi_login import create_angel_client
from python_feed.internal.logging.logger_setup import setup_logging
from python_feed.internal.websocket.angel_ws_client import AngelWSClient, PlannedReconnect
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
    upper_part = symbol_part.upper()
    if upper_part.endswith("-EQ"):
        base = upper_part[:-3]
    elif upper_part.endswith("-INDEX"):
        base = upper_part[:-6]
    else:
        base = upper_part
    if base == "NIFTY50":
        base = "NIFTY"
    if base == "NIFTYBANK":
        base = "BANKNIFTY"
    return base, exchange.upper()


# load_equities_from_csv reads a universe CSV and returns NSE equity instruments with optional token overrides.
# Parameters:
# - csv_path: path to a stocks CSV with Symbol and optional Token columns.
# Returns:
# - tuple: (list of instrument strings like NSE:RELIANCE-EQ, instrument->token overrides).
def load_equities_from_csv(csv_path: str | Path) -> Tuple[List[str], Dict[str, str]]:
    path = Path(csv_path).expanduser()
    instruments: List[str] = []
    token_overrides: Dict[str, str] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            symbol = str((row.get("Symbol") or "")).strip()
            if not symbol:
                continue
            instrument = f"NSE:{symbol}-EQ"
            instruments.append(instrument)
            token = str((row.get("Token") or "")).strip()
            if token:
                token_overrides[instrument] = token
    return instruments, token_overrides


def load_indices_from_config(indices_cfg: List[Any]) -> Tuple[List[str], Dict[str, str]]:
    instruments: List[str] = []
    token_overrides: Dict[str, str] = {}
    for item in indices_cfg or []:
        if isinstance(item, str):
            instrument = item.strip()
            if instrument:
                instruments.append(instrument)
            continue
        if not isinstance(item, dict):
            continue
        instrument = str(item.get("instrument") or "").strip()
        if not instrument:
            continue
        instruments.append(instrument)
        token = str(item.get("token") or "").strip()
        if token:
            token_overrides[instrument] = token
    return instruments, token_overrides


def load_tokens_from_scrip_master(
    instruments: List[str],
    scrip_master_path: str | Path,
    logger: logging.Logger,
) -> Dict[str, str]:
    path = Path(scrip_master_path).expanduser()
    if not path.exists():
        logger.warning("Scrip master file not found: %s", path)
        return {}

    try:
        records = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # pylint: disable=broad-except
        logger.warning("Failed to read scrip master %s: %s", path, exc)
        return {}

    exact_lookup: Dict[str, str] = {}
    alias_lookup: Dict[str, str] = {}
    for item in records:
        exch = str(item.get("exch_seg") or "").upper()
        token = str(item.get("token") or "").strip()
        symbol = str(item.get("symbol") or "").upper()
        name = str(item.get("name") or "").upper()
        if exch != "NSE" or not token or not symbol:
            continue

        exact_lookup[f"{exch}:{symbol}"] = token
        if symbol.endswith("-EQ"):
            alias_lookup[f"{exch}:{symbol[:-3]}-EQ"] = token
        elif symbol in {"NIFTY 50", "NIFTY50"}:
            alias_lookup["NSE:NIFTY50-INDEX"] = token
        elif symbol in {"NIFTY BANK", "NIFTYBANK"} or name == "BANKNIFTY":
            alias_lookup["NSE:NIFTYBANK-INDEX"] = token

    resolved: Dict[str, str] = {}
    for instrument in instruments:
        token = exact_lookup.get(instrument.upper()) or alias_lookup.get(instrument.upper())
        if token:
            resolved[instrument] = token

    logger.info("Resolved %s/%s instruments from local scrip master", len(resolved), len(instruments))
    return resolved


def _lookup_token(client: Any, instrument: str, logger: logging.Logger) -> str | None:
    if ":" in instrument:
        exchange, trading_symbol = instrument.split(":", 1)
    else:
        exchange, trading_symbol = "NSE", instrument

    queries = [trading_symbol]
    derived_symbol, _ = _derive_symbol(instrument)
    if derived_symbol not in queries:
        queries.append(derived_symbol)

    for query in queries:
        try:
            result = client.searchScrip(exchange, query)
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Token lookup failed for %s using query %s: %s", instrument, query, exc)
            continue

        for item in (result or {}).get("data") or []:
            item_trading_symbol = str(item.get("tradingsymbol") or "").upper()
            if item_trading_symbol == trading_symbol.upper():
                token = str(item.get("symboltoken") or "").strip()
                if token:
                    return token

    return None


def resolve_instrument_tokens(
    instruments: List[str],
    token_map: Dict[str, str],
    angel_cfg: Dict[str, Any],
    scrip_master_path: str | None,
    logger: logging.Logger,
) -> Dict[str, str]:
    resolved = {key: str(value).strip() for key, value in token_map.items() if str(value).strip()}
    missing = [instrument for instrument in instruments if instrument not in resolved]
    if missing and scrip_master_path:
        resolved.update(load_tokens_from_scrip_master(missing, scrip_master_path, logger))
        missing = [instrument for instrument in instruments if instrument not in resolved]
    if not missing:
        return resolved

    # Prefer the local scrip master as the authoritative source for bulk startup.
    # This avoids broker-side rate limits when subscribing large universes.
    if scrip_master_path:
        logger.warning("Local token resolution missing %s instruments; skipping live lookup", len(missing))
        for instrument in missing:
            logger.warning("No local token found for %s", instrument)
        return resolved

    client = create_angel_client(angel_cfg, logger)
    if client is None:
        logger.warning("Unable to auto-resolve %s missing instrument tokens", len(missing))
        return resolved

    logger.info("Resolving %s missing tokens from SmartAPI", len(missing))
    for instrument in missing:
        token = _lookup_token(client, instrument, logger)
        if token:
            resolved[instrument] = token
        else:
            logger.warning("No SmartAPI token found for %s", instrument)

    return resolved


# build_token_maps constructs token mappings for subscription and normalization.
# Parameters:
# - instruments: list of instrument strings from config.
# Returns:
# - tuple: (tokens list, token->(symbol, exchange) map, instrument->token map).
# Flow: use explicit token overrides only; if a token is missing, skip that instrument and log.
def build_token_maps(
    instruments: List[str],
    explicit_token_map: Dict[str, str] | None = None,
) -> Tuple[List[str], Dict[str, Tuple[str, str]], Dict[str, str]]:
    logger = logging.getLogger("ws_feed_service")
    logger.debug("Building token maps for %s instruments (explicit map size=%s)", len(instruments), len(explicit_token_map or {}))
    tokens: List[str] = []
    token_to_meta: Dict[str, Tuple[str, str]] = {}
    instrument_to_token: Dict[str, str] = {}
    for instrument in instruments:
        symbol, exchange = _derive_symbol(instrument)
        token = None
        if explicit_token_map and instrument in explicit_token_map:
            token = str(explicit_token_map[instrument]).strip()
        if not token:
            logger.warning("Skipping instrument without configured token: %s", instrument)
            continue
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


@dataclass
class _TickSeen:
    first_seen: float
    count: int


class TickDeduper:
    """TTL + count based dedupe for overlapping websocket streams."""

    def __init__(self, ttl_sec: int, expected_count_fn: Callable[[], int], logger: logging.Logger) -> None:
        self.ttl_sec = max(1, ttl_sec)
        self.expected_count_fn = expected_count_fn
        self.logger = logger
        self._lock = threading.Lock()
        self._seen: Dict[str, _TickSeen] = {}
        self._last_cleanup = time.monotonic()

    def _tick_key(self, tick: Dict[str, Any]) -> str:
        return "|".join(
            [
                str(tick.get("symbol") or ""),
                str(tick.get("time") or ""),
                str(tick.get("ltp") or ""),
                str(tick.get("volume") or ""),
            ]
        )

    def _cleanup_locked(self, now_mono: float) -> None:
        if (now_mono - self._last_cleanup) < 1:
            return
        expired = [k for k, seen in self._seen.items() if (now_mono - seen.first_seen) > self.ttl_sec]
        for key in expired:
            self._seen.pop(key, None)
        self._last_cleanup = now_mono

    def should_publish(self, tick: Dict[str, Any]) -> bool:
        key = self._tick_key(tick)
        now_mono = time.monotonic()
        with self._lock:
            self._cleanup_locked(now_mono)
            seen = self._seen.get(key)
            if seen is None:
                self._seen[key] = _TickSeen(first_seen=now_mono, count=1)
                return True
            seen.count += 1
            expected = max(1, self.expected_count_fn())
            if seen.count >= expected:
                self._seen.pop(key, None)
            return False


class WSWorker:
    """Owns one websocket lifecycle in a background thread."""

    def __init__(
        self,
        name: str,
        cfg: Dict[str, Any],
        logger: logging.Logger,
        token_map: Dict[str, Tuple[str, str]],
        instrument_token_map: Dict[str, str],
        tick_callback: Callable[[Dict[str, Any]], None],
        backoff_seconds: int,
    ) -> None:
        self.name = name
        self.cfg = cfg
        self.logger = logger
        self.token_map = token_map
        self.instrument_token_map = instrument_token_map
        self.tick_callback = tick_callback
        self.backoff_seconds = max(1, backoff_seconds)

        self.stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._state_lock = threading.Lock()
        self._started_at = 0.0
        self._last_tick_mono = 0.0
        self._total_ticks = 0
        self._connected = False

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self.stop_event.clear()
        self._thread = threading.Thread(target=self._run, name=f"ws-worker-{self.name}", daemon=True)
        self._thread.start()
        with self._state_lock:
            self._started_at = time.monotonic()
            self._last_tick_mono = self._started_at
            self._total_ticks = 0
        self.logger.info("Started websocket worker %s", self.name)

    def stop(self) -> None:
        self.stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        self.logger.info("Stopped websocket worker %s", self.name)

    def is_running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    def connected(self) -> bool:
        with self._state_lock:
            return self._connected

    def tick_age_seconds(self) -> float:
        with self._state_lock:
            return time.monotonic() - self._last_tick_mono

    def uptime_seconds(self) -> float:
        with self._state_lock:
            return max(0.0, time.monotonic() - self._started_at)

    def total_ticks(self) -> int:
        with self._state_lock:
            return self._total_ticks

    def _on_tick(self, tick: Dict[str, Any]) -> None:
        with self._state_lock:
            self._last_tick_mono = time.monotonic()
            self._total_ticks += 1
        self.tick_callback(tick)

    def _run(self) -> None:
        while not self.stop_event.is_set():
            self.logger.info("Worker %s creating websocket client/session", self.name)
            client = AngelWSClient(
                cfg=self.cfg,
                logger=self.logger,
                tick_handler=self._on_tick,
                token_mapping=self.token_map,
                instrument_token_map=self.instrument_token_map,
                connection_id=self.name,
            )
            try:
                client.connect()
                with self._state_lock:
                    self._connected = True
                client.run_forever(self.stop_event)
                with self._state_lock:
                    self._connected = False
                if not self.stop_event.is_set():
                    raise RuntimeError(f"worker {self.name} websocket loop exited unexpectedly")
            except PlannedReconnect:
                with self._state_lock:
                    self._connected = False
                if not self.stop_event.is_set():
                    self.logger.warning("Worker %s planned rotate completed; reconnecting", self.name)
                    time.sleep(1)
            except Exception as exc:  # pylint: disable=broad-except
                with self._state_lock:
                    self._connected = False
                if self.stop_event.is_set():
                    break
                self.logger.exception("Worker %s crashed; recreating websocket after %ss: %s", self.name, self.backoff_seconds, exc)
                metrics.ERRORS_TOTAL.inc()
                time.sleep(self.backoff_seconds)
            finally:
                client.disconnect()


class MultiWSManager:
    """Manages 2 active websocket workers + on-demand rescue worker."""

    def __init__(
        self,
        cfg: Dict[str, Any],
        logger: logging.Logger,
        token_map: Dict[str, Tuple[str, str]],
        instrument_token_map: Dict[str, str],
        producer: KafkaTickProducer,
    ) -> None:
        self.cfg = cfg
        self.logger = logger
        self.token_map = token_map
        self.instrument_token_map = instrument_token_map
        self.producer = producer
        reconnect_cfg = cfg.get("reconnect", {})
        self.monitor_interval_sec = float(reconnect_cfg.get("manager_monitor_interval_sec", 1))
        self.stale_sec = float(reconnect_cfg.get("stale_connection_sec", 5))
        self.stale_warmup_sec = float(reconnect_cfg.get("stale_connection_warmup_sec", 65))
        self.rescue_stabilize_sec = float(reconnect_cfg.get("rescue_stabilize_sec", 10))
        self.global_stall_recover_sec = float(reconnect_cfg.get("global_stall_recover_sec", 90))
        self.failover_cooldown_sec = float(reconnect_cfg.get("failover_cooldown_sec", 60))
        self.backoff_seconds = int(reconnect_cfg.get("backoff_seconds", 5))
        self.dedupe = TickDeduper(
            ttl_sec=int(reconnect_cfg.get("dedupe_ttl_sec", 10)),
            expected_count_fn=self.expected_stream_count,
            logger=logger,
        )
        self._lock = threading.Lock()
        self.active_workers: List[str] = ["ws-a", "ws-b"]
        self.standby_worker = "ws-c"
        self.rescue_started_at = 0.0
        self.last_failover_mono = 0.0
        self.workers: Dict[str, WSWorker] = {}
        self.shutdown_event = threading.Event()

    def _in_failover_cooldown(self) -> bool:
        if self.failover_cooldown_sec <= 0:
            return False
        return (time.monotonic() - self.last_failover_mono) < self.failover_cooldown_sec

    def _in_trading_window(self) -> bool:
        now = datetime.now().astimezone()
        if now.weekday() > 4:
            return False
        minute_of_day = now.hour * 60 + now.minute
        return (9 * 60 + 15) <= minute_of_day <= (15 * 60 + 30)

    def expected_stream_count(self) -> int:
        with self._lock:
            return 3 if self.rescue_started_at > 0 else 2

    def _tick_handler(self, tick: Dict[str, Any]) -> None:
        if self.dedupe.should_publish(tick):
            self.producer.send_tick(tick)

    def _ensure_worker(self, name: str) -> WSWorker:
        worker = self.workers.get(name)
        if worker is None:
            worker = WSWorker(
                name=name,
                cfg=self.cfg,
                logger=self.logger,
                token_map=self.token_map,
                instrument_token_map=self.instrument_token_map,
                tick_callback=self._tick_handler,
                backoff_seconds=self.backoff_seconds,
            )
            self.workers[name] = worker
        return worker

    def _start_active_set(self) -> None:
        for name in list(self.active_workers):
            self._ensure_worker(name).start()

    def _start_rescue_if_needed(self) -> None:
        if self._in_failover_cooldown():
            return
        with self._lock:
            rescue_active = self.rescue_started_at > 0
            rescue_name = self.standby_worker
        if rescue_active:
            return
        worker = self._ensure_worker(rescue_name)
        worker.start()
        with self._lock:
            self.rescue_started_at = time.monotonic()
        self.logger.warning("Started rescue websocket %s", rescue_name)

    def _all_running_workers(self) -> List[WSWorker]:
        return [worker for worker in self.workers.values() if worker.is_running()]

    def _stale_active_names(self) -> List[str]:
        if not self._in_trading_window():
            return []
        stale: List[str] = []
        with self._lock:
            active_snapshot = list(self.active_workers)
        for name in active_snapshot:
            worker = self.workers.get(name)
            if worker is None or not worker.is_running():
                stale.append(name)
                continue
            if worker.uptime_seconds() < self.stale_warmup_sec:
                continue
            # Treat as stale only after the worker had enough time to bootstrap.
            if worker.tick_age_seconds() >= self.stale_sec:
                stale.append(name)
        return stale

    def _promote_rescue_if_stable(self) -> None:
        with self._lock:
            rescue_name = self.standby_worker
            rescue_started_at = self.rescue_started_at
            active_snapshot = list(self.active_workers)
        if rescue_started_at <= 0:
            return
        rescue_worker = self.workers.get(rescue_name)
        if rescue_worker is None or not rescue_worker.is_running():
            return
        if rescue_worker.total_ticks() <= 0:
            return
        if (time.monotonic() - rescue_started_at) < self.rescue_stabilize_sec:
            return

        stale_candidates: List[Tuple[str, float]] = []
        for name in active_snapshot:
            worker = self.workers.get(name)
            if worker is None:
                stale_candidates.append((name, 10_000))
                continue
            stale_candidates.append((name, worker.tick_age_seconds()))
        stale_name = max(stale_candidates, key=lambda item: item[1])[0]
        stale_worker = self.workers.get(stale_name)
        if stale_worker:
            stale_worker.stop()

        with self._lock:
            self.active_workers = [name for name in self.active_workers if name != stale_name]
            self.active_workers.append(rescue_name)
            self.standby_worker = stale_name
            self.rescue_started_at = 0.0
            self.last_failover_mono = time.monotonic()

        self.logger.warning(
            "Promoted rescue connection %s; retired stale connection %s",
            rescue_name,
            stale_name,
        )

    def _recover_global_stall_if_needed(self) -> None:
        if not self._in_trading_window():
            return
        running = self._all_running_workers()
        if not running:
            return
        warmed = [worker for worker in running if worker.uptime_seconds() >= self.stale_warmup_sec]
        if not warmed:
            return
        if self._in_failover_cooldown():
            return
        all_stale = all(worker.tick_age_seconds() >= self.global_stall_recover_sec for worker in warmed)
        if not all_stale:
            return
        self.logger.error("All websocket workers appear stalled; restarting active workers")
        with self._lock:
            active_snapshot = list(self.active_workers)
            self.rescue_started_at = 0.0
            self.last_failover_mono = time.monotonic()
        for name in active_snapshot:
            worker = self._ensure_worker(name)
            worker.stop()
            time.sleep(1)
            worker.start()
        standby = self._ensure_worker(self.standby_worker)
        standby.stop()

    def _monitor_once(self) -> None:
        self._start_active_set()
        stale = self._stale_active_names()
        if stale:
            self.logger.warning("Detected stale active connections (no recent ticks): %s", ",".join(stale))
            self._start_rescue_if_needed()
        self._promote_rescue_if_stable()
        self._recover_global_stall_if_needed()

        running = self._all_running_workers()
        connected = any(worker.connected() for worker in running)
        metrics.WS_CONNECTED.set(1 if connected else 0)

    def run(self) -> None:
        self._start_active_set()
        while not self.shutdown_event.is_set():
            try:
                self._monitor_once()
            except Exception as exc:  # pylint: disable=broad-except
                self.logger.exception("WS manager monitor error: %s", exc)
                metrics.ERRORS_TOTAL.inc()
            time.sleep(max(0.2, self.monitor_interval_sec))

    def shutdown(self) -> None:
        self.shutdown_event.set()
        for worker in list(self.workers.values()):
            worker.stop()


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

    symbols_cfg = cfg.setdefault("symbols", {})
    indices, index_token_overrides = load_indices_from_config(symbols_cfg.get("indices") or [])
    symbols_cfg["indices"] = indices
    equities_csv = symbols_cfg.get("equities_csv")
    csv_token_overrides: Dict[str, str] = {}
    if equities_csv:
        equities, csv_token_overrides = load_equities_from_csv(equities_csv)
        symbols_cfg["equities"] = equities
        logger.info(
            "Loaded %s equities from CSV universe %s (token overrides=%s)",
            len(equities),
            equities_csv,
            len(csv_token_overrides),
        )

    instruments = (symbols_cfg.get("indices") or []) + (symbols_cfg.get("equities") or [])
    logger.info("Preparing subscription for %s instruments", len(instruments))
    configured_token_map = {key: str(value).strip() for key, value in (cfg.get("token_map") or {}).items() if str(value).strip()}
    configured_token_map.update(index_token_overrides)
    configured_token_map.update(csv_token_overrides)
    token_overrides = resolve_instrument_tokens(
        instruments,
        configured_token_map,
        cfg.get("angel", {}),
        symbols_cfg.get("scrip_master_path"),
        logger,
    )
    _, token_map, instrument_token_map = build_token_maps(instruments, token_overrides)
    logger.info("Token map built: %s tokens (overrides=%s)", len(instrument_token_map), len(token_overrides))

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

    shutdown_event = threading.Event()
    _install_signal_handlers(shutdown_event, logger)
    manager = MultiWSManager(
        cfg=cfg,
        logger=logger,
        token_map=token_map,
        instrument_token_map=instrument_token_map,
        producer=producer,
    )

    try:
        manager_thread = threading.Thread(target=manager.run, name="ws-manager", daemon=True)
        manager_thread.start()
        while not shutdown_event.is_set():
            time.sleep(0.5)
    finally:
        logger.info("Shutdown requested, closing connections...")
        shutdown_event.set()
        manager.shutdown()
        producer.flush()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    main()
