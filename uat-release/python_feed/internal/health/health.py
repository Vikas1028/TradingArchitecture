"""Health metrics tracking."""

from __future__ import annotations

import logging
import time
from typing import Optional


class HealthMonitor:
    """Tracks ticks, TPS, and connection status."""

    # __init__ initializes counters and logging cadence.
    # Parameters:
    # - logger: logger for health reports.
    # - interval_sec: reporting interval in seconds.
    # Returns: None.
    # Flow: set counters to zero, store interval and logger, mark disconnected.
    def __init__(self, logger: logging.Logger, interval_sec: int) -> None:
        self.logger = logger
        self.interval_sec = max(1, interval_sec)
        self.total_ticks = 0
        self.last_total = 0
        self.last_report_time = time.time()
        self.connected = False
        self.logger.info("HealthMonitor initialized interval=%ss", self.interval_sec)

    # on_tick increments tick counter for each processed tick.
    # Parameters: None.
    # Returns: None.
    # Flow: increment total tick count.
    def on_tick(self) -> None:
        self.total_ticks += 1
        # Avoid noisy per-tick logging; rely on periodic reports.

    # set_connected updates connection status flag.
    # Parameters:
    # - value: boolean connected state.
    # Returns: None.
    # Flow: set connected flag used in reports.
    def set_connected(self, value: bool) -> None:
        if self.connected != value:
            self.logger.info("Connection state changed: %s -> %s", self.connected, value)
        self.connected = value

    # maybe_report logs health stats if the interval has elapsed.
    # Parameters: None.
    # Returns: None.
    # Flow: compute elapsed time and TPS, log health line, update markers.
    def maybe_report(self) -> None:
        now = time.time()
        if now - self.last_report_time < self.interval_sec:
            return
        delta = self.total_ticks - self.last_total
        elapsed = max(1e-6, now - self.last_report_time)
        tps = delta / elapsed
        self.logger.info("health: ticks=%s, tps=%.2f, connected=%s", self.total_ticks, tps, self.connected)
        self.last_total = self.total_ticks
        self.last_report_time = now
