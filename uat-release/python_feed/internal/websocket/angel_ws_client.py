"""Angel SmartAPI WebSocket client wrapper."""

from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from python_feed import metrics
from python_feed.internal.login.smartapi_login import AngelSession, create_angel_session

try:
    from SmartApi.smartWebSocketV2 import SmartWebSocketV2
except Exception:  # pylint: disable=broad-except
    class SmartWebSocketV2:  # type: ignore[too-many-instance-attributes]
        """Placeholder SmartAPI websocket client."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.on_open: Optional[Callable[[Any], None]] = None
            self.on_data: Optional[Callable[[Any, Any], None]] = None
            self.on_error: Optional[Callable[[Any, Any], None]] = None
            self.on_close: Optional[Callable[[Any], None]] = None
            self._connected = False

        def connect(self) -> None:
            self._connected = True
            if self.on_open:
                self.on_open(self)
            while self._connected:
                time.sleep(0.25)

        def subscribe(self, *args: Any, **kwargs: Any) -> None:
            return None

        def close_connection(self) -> None:
            self._connected = False
            if self.on_close:
                self.on_close(self)


class PlannedReconnect(Exception):
    """Raised when a planned full session rotate is requested."""


class AngelWSClient:
    """Encapsulates SmartAPI websocket connection, subscription, and tick normalization."""

    # __init__ stores dependencies and mapping for later use.
    # Parameters:
    # - cfg: full configuration dictionary.
    # - logger: logger for diagnostics.
    # - tick_handler: callback to forward normalized ticks.
    # - token_mapping: map token -> (symbol, exchange).
    # - instrument_token_map: map instrument string -> token.
    # Returns: None.
    # Flow: keep references, prepare state flags.
    def __init__(
        self,
        cfg: Dict[str, Any],
        logger: logging.Logger,
        tick_handler: Callable[[Dict[str, Any]], None],
        token_mapping: Dict[str, Tuple[str, str]],
        instrument_token_map: Dict[str, str],
        connection_id: str = "ws-primary",
    ) -> None:
        self.cfg = cfg
        self.logger = logger
        self.tick_handler = tick_handler
        self.token_mapping = token_mapping
        self.instrument_token_map = instrument_token_map
        self.connection_id = connection_id
        self._ws: Optional[SmartWebSocketV2] = None
        self._session: Optional[AngelSession] = None
        self._connected = threading.Event()
        self._last_tick_monotonic = time.monotonic()
        self._open_events = 0
        self.logger.debug(
            "AngelWSClient initialized: id=%s instruments=%s tokens=%s",
            self.connection_id,
            len((cfg.get("symbols", {}).get("indices") or []) + (cfg.get("symbols", {}).get("equities") or [])),
            len(token_mapping),
        )

    # connect creates an authenticated session and initializes websocket callbacks.
    # Parameters: None.
    # Returns: None.
    # Flow: build Angel session, instantiate SmartWebSocketV2, set callbacks, ready for run_forever.
    def connect(self) -> None:
        self._session = create_angel_session(self.cfg.get("angel", {}), self.logger)
        self.logger.info(
            "Starting websocket connect id=%s using client_id=%s feed_token_present=%s",
            self.connection_id,
            self._session.client_id,
            bool(self._session.feed_token),
        )
        ws = SmartWebSocketV2(
            auth_token=self._session.access_token or self._session.refresh_token,
            api_key=self._session.api_key,
            client_code=self._session.client_id,
            feed_token=self._session.feed_token,
            max_retry_attempt=10,
            retry_strategy=1,
        )
        ws.on_open = self._on_open
        ws.on_data = self._on_data
        ws.on_error = self._on_error
        ws.on_close = self._on_close
        self._ws = ws
        self.logger.debug("Websocket callbacks registered")

    # disconnect closes websocket connection and resets flags.
    # Parameters: None.
    # Returns: None.
    # Flow: call close_connection, clear connected event.
    def disconnect(self) -> None:
        if self._ws:
            try:
                self._ws.close_connection()
            except Exception as exc:  # pylint: disable=broad-except
                self.logger.warning("Failed to close websocket cleanly: %s", exc)
            else:
                self.logger.info("Websocket close requested id=%s", self.connection_id)
        self._connected.clear()
        metrics.WS_CONNECTED.set(0)

    # subscribe translates instrument identifiers to tokens and sends subscription request.
    # Parameters:
    # - instruments: list of instrument strings (e.g., "NSE:RELIANCE-EQ").
    # Returns: None.
    # Flow: map instruments -> tokens, build payload, call SmartAPI subscribe.
    def subscribe(self, instruments: List[str]) -> None:
        if not self._ws:
            raise RuntimeError("Websocket not initialized; call connect() first.")
        tokens = [self.instrument_token_map.get(inst) for inst in instruments]
        tokens = [tok for tok in tokens if tok]
        if not tokens:
            self.logger.warning("No tokens derived for subscription; instruments=%s", instruments)
            return
        payload = [{"exchangeType": 1, "tokens": tokens}]
        try:
            self._ws.subscribe("python-feed", 2, payload)
            self.logger.info("Subscribed %s tokens id=%s", len(tokens), self.connection_id)
            self.logger.debug("Subscribed token list: %s", tokens)
        except Exception as exc:  # pylint: disable=broad-except
            self.logger.error("Failed to subscribe tokens: %s", exc)
            raise

    # run_forever starts the websocket loop and processes ticks until shutdown_event is set.
    # Parameters:
    # - shutdown_event: threading.Event to signal termination.
    # Returns: None.
    # Flow: spin up connect thread, wait until stop requested or thread dies, then disconnect.
    def run_forever(self, shutdown_event: threading.Event) -> None:
        if not self._ws:
            raise RuntimeError("Websocket not initialized. Call connect() first.")
        ws = self._ws
        self.logger.info("Starting websocket loop id=%s", self.connection_id)
        stall_timeout_sec = int(self.cfg.get("reconnect", {}).get("no_ticks_reconnect_sec", 300))
        rotate_interval_sec = int(self.cfg.get("reconnect", {}).get("force_full_reconnect_sec", 0))
        loop_start_monotonic = time.monotonic()

        def _connect_blocking() -> None:
            try:
                ws.connect()
            except Exception as exc:  # pylint: disable=broad-except
                self.logger.error("Websocket connect failed: %s", exc)
                self._connected.clear()
                metrics.WS_CONNECTED.set(0)
                metrics.WS_RECONNECTS.inc()
                metrics.ERRORS_TOTAL.inc()

        thread = threading.Thread(target=_connect_blocking, name="angel-ws", daemon=True)
        thread.start()
        self.logger.debug("Websocket connect thread started (alive=%s)", thread.is_alive())

        try:
            while not shutdown_event.is_set() and thread.is_alive():
                if rotate_interval_sec > 0 and (time.monotonic() - loop_start_monotonic) >= rotate_interval_sec:
                    self.logger.warning(
                        "Planned full session rotate id=%s after %ss; recreating SmartAPI websocket session",
                        self.connection_id,
                        rotate_interval_sec,
                    )
                    metrics.WS_RECONNECTS.inc()
                    try:
                        ws.close_connection()
                    except Exception as exc:  # pylint: disable=broad-except
                        self.logger.warning("Failed closing websocket during planned rotate: %s", exc)
                    raise PlannedReconnect("planned websocket session rotate")
                if self._connected.is_set() and stall_timeout_sec > 0:
                    stalled_for = time.monotonic() - self._last_tick_monotonic
                    metrics.LAST_TICK_AGE_SECONDS.set(stalled_for)
                    if stalled_for >= stall_timeout_sec:
                        if self._in_trading_window():
                            self.logger.warning(
                                "No ticks for %.1fs in trading window; forcing websocket reconnect",
                                stalled_for,
                            )
                            metrics.WS_STALL_RECONNECTS.inc()
                            metrics.WS_RECONNECTS.inc()
                            try:
                                ws.close_connection()
                            except Exception as exc:  # pylint: disable=broad-except
                                self.logger.warning("Failed closing stalled websocket: %s", exc)
                            raise RuntimeError("Websocket tick stream stalled")
                        self.logger.info(
                            "No ticks for %.1fs outside trading window; reconnect not forced",
                            stalled_for,
                        )
                time.sleep(0.5)
            if shutdown_event.is_set():
                self.logger.info("Shutdown event set; closing websocket id=%s", self.connection_id)
            if not thread.is_alive() and not shutdown_event.is_set():
                raise RuntimeError("Websocket loop exited unexpectedly")
        finally:
            self.disconnect()
            thread.join(timeout=2)
            self.logger.debug("Websocket thread joined (alive=%s)", thread.is_alive())

    # _on_open marks connection as live and triggers initial subscription.
    # Parameters:
    # - wsapp: websocket app (unused).
    # Returns: None.
    # Flow: set connected flag and subscribe to configured instruments.
    def _on_open(self, wsapp: Any) -> None:  # pylint: disable=unused-argument
        self._open_events += 1
        self._last_tick_monotonic = time.monotonic()
        self._connected.set()
        metrics.WS_CONNECTED.set(1)
        metrics.WS_OPEN_EVENTS.inc()
        if self._open_events > 1:
            metrics.WS_RECONNECTS.inc()
            self.logger.warning(
                "Websocket reopened/resubscribed id=%s (open_events=%s)",
                self.connection_id,
                self._open_events,
            )
        instruments = (self.cfg.get("symbols", {}).get("indices") or []) + (self.cfg.get("symbols", {}).get("equities") or [])
        self.logger.info("Websocket connected id=%s; subscribing instruments", self.connection_id)
        try:
            self.subscribe(instruments)
        except Exception:
            self._connected.clear()
            metrics.WS_CONNECTED.set(0)
            raise

    # _on_data normalizes raw tick messages and forwards them to the tick handler.
    # Parameters:
    # - wsapp: websocket app (unused).
    # - message: raw message payload.
    # Returns: None.
    # Flow: normalize tick, if valid call tick_handler.
    def _on_data(self, wsapp: Any, message: Any) -> None:  # pylint: disable=unused-argument
        tick = self.normalize_tick(message)
        if tick:
            self._last_tick_monotonic = time.monotonic()
            metrics.LAST_TICK_AGE_SECONDS.set(0)
            metrics.TICKS_RECEIVED.inc()
            tick["source_conn"] = self.connection_id
            self.tick_handler(tick)
            self.logger.debug("Tick processed for %s at %s", tick.get("symbol"), tick.get("time"))
        else:
            self.logger.debug("Dropped tick payload: %s", message)

    # _on_error logs errors and clears connected flag.
    # Parameters:
    # - wsapp: websocket app (unused).
    # - error: error object or text.
    # Returns: None.
    # Flow: log error and mark disconnected.
    def _on_error(self, wsapp: Any, error: Any) -> None:  # pylint: disable=unused-argument
        self.logger.error("Websocket error: %s", error)
        self._connected.clear()
        metrics.WS_CONNECTED.set(0)
        metrics.WS_RECONNECTS.inc()
        metrics.ERRORS_TOTAL.inc()

    # _on_close marks connection as closed.
    # Parameters:
    # - wsapp: websocket app (unused).
    # Returns: None.
    # Flow: log close and clear connected flag.
    def _on_close(self, wsapp: Any) -> None:  # pylint: disable=unused-argument
        self.logger.warning("Websocket closed")
        self._connected.clear()
        metrics.WS_CONNECTED.set(0)
        metrics.WS_RECONNECTS.inc()

    def is_connected(self) -> bool:
        return self._connected.is_set()

    def last_tick_age_seconds(self) -> float:
        return time.monotonic() - self._last_tick_monotonic

    def _in_trading_window(self) -> bool:
        now = datetime.now().astimezone()
        if now.weekday() > 4:
            return False
        minute_of_day = now.hour * 60 + now.minute
        # NSE continuous session guard window.
        return (9 * 60 + 15) <= minute_of_day <= (15 * 60 + 30)

    # normalize_tick converts raw SmartAPI messages into the internal tick schema.
    # Parameters:
    # - raw_msg: raw websocket message (dict or JSON string).
    # Returns:
    # - normalized tick dict or None if invalid/unmapped.
    # Flow: parse message, map token to symbol/exchange, extract price/volume/bid/ask/time.
    def normalize_tick(self, raw_msg: Any) -> Optional[Dict[str, Any]]:
        msg = raw_msg
        if isinstance(msg, str):
            try:
                msg = json.loads(msg)
            except json.JSONDecodeError:
                self.logger.warning("Skip non-JSON tick payload: %s", raw_msg)
                return None
        if not isinstance(msg, dict):
            self.logger.debug("Unexpected tick payload type: %s", type(msg))
            return None

        token = msg.get("token") or msg.get("instrument_token") or msg.get("instrumentToken")
        if not token:
            self.logger.warning("Tick missing token: %s", msg)
            return None
        meta = self.token_mapping.get(str(token))
        if not meta:
            self.logger.warning("Unknown token %s; skipping tick", token)
            return None

        symbol, exchange = meta
        ltp = msg.get("ltp") or msg.get("last_traded_price") or msg.get("close")
        volume = msg.get("lastTradeQty") or msg.get("volume") or msg.get("vol") or 0
        bid = msg.get("best_bid") or msg.get("best_bid_price") or msg.get("bid") or msg.get("buy_price") or 0
        ask = msg.get("best_ask") or msg.get("best_ask_price") or msg.get("ask") or msg.get("sell_price") or 0
        ts_raw = msg.get("exchange_timestamp") or msg.get("timestamp") or msg.get("time") or msg.get("exchangeTime")
        iso_time = self._parse_timestamp(ts_raw)

        try:
            ltp_value = float(ltp) if ltp is not None else None
        except (TypeError, ValueError):
            ltp_value = None
            self.logger.debug("Invalid LTP for token %s payload=%s", token, msg)

        try:
            volume_value = int(volume) if volume is not None else 0
        except (TypeError, ValueError):
            volume_value = 0
            self.logger.debug("Invalid volume for token %s payload=%s", token, msg)

        return {
            "symbol": symbol,
            "exchange": exchange,
            "time": iso_time,
            "ltp": ltp_value,
            "volume": volume_value,
            "bid": float(bid) if bid is not None else 0,
            "ask": float(ask) if ask is not None else 0,
        }

    # _parse_timestamp converts broker timestamp to ISO8601 with timezone.
    # Parameters:
    # - raw_value: timestamp from broker (int, float, str, etc.).
    # Returns:
    # - ISO8601 timestamp string.
    # Flow: attempt to parse, fall back to current time.
    def _parse_timestamp(self, raw_value: Any) -> str:
        if not raw_value:
            self.logger.debug("Missing timestamp in tick; defaulting to now()")
            return datetime.now(timezone.utc).astimezone().isoformat()
        if isinstance(raw_value, (int, float)):
            try:
                return datetime.fromtimestamp(float(raw_value), tz=timezone.utc).astimezone().isoformat()
            except (OverflowError, OSError, ValueError):
                self.logger.debug("Failed to parse numeric timestamp %s; defaulting to now()", raw_value)
                return datetime.now(timezone.utc).astimezone().isoformat()
        text = str(raw_value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt_value = datetime.fromisoformat(text)
            if dt_value.tzinfo is None:
                dt_value = dt_value.replace(tzinfo=timezone.utc)
            return dt_value.astimezone().isoformat()
        except ValueError:
            self.logger.debug("Failed to parse timestamp %s; defaulting to now()", raw_value)
            return datetime.now(timezone.utc).astimezone().isoformat()
