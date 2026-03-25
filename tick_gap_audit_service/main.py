#!/usr/bin/env python3
"""Kafka tick gap audit service for per-symbol no-tick windows."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

from confluent_kafka import Consumer, KafkaError, TopicPartition
from zoneinfo import ZoneInfo


@dataclass
class AppConfig:
    bootstrap_servers: str
    topic: str
    group_id: str
    timezone: str
    output_dir: str
    poll_timeout_ms: int
    max_idle_polls: int
    min_gap_seconds: int
    symbols: list[str]


def load_config(path: str) -> AppConfig:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    kafka_cfg = raw.get("kafka", {})
    app_cfg = raw.get("app", {})
    configured_symbols = app_cfg.get("symbols", [])
    symbols = [str(item).strip().upper() for item in configured_symbols if str(item).strip()]
    return AppConfig(
        bootstrap_servers=str(kafka_cfg.get("bootstrap_servers", "localhost:9092")),
        topic=str(kafka_cfg.get("topic", "ticks.raw")),
        group_id=str(kafka_cfg.get("group_id", "tick-gap-audit-service")),
        timezone=str(app_cfg.get("timezone", "Asia/Kolkata")),
        output_dir=str(app_cfg.get("output_dir", "logs/tick_gap_audit")),
        poll_timeout_ms=int(app_cfg.get("poll_timeout_ms", 1000)),
        max_idle_polls=int(app_cfg.get("max_idle_polls", 20)),
        min_gap_seconds=int(app_cfg.get("min_gap_seconds", 5)),
        symbols=symbols,
    )


def previous_trading_day(ref: date) -> date:
    day = ref - timedelta(days=1)
    while day.weekday() > 4:
        day -= timedelta(days=1)
    return day


def trading_day_bounds(day: date, tz: ZoneInfo) -> tuple[datetime, datetime]:
    start = datetime.combine(day, dtime(hour=9, minute=15), tzinfo=tz)
    end = datetime.combine(day, dtime(hour=15, minute=30), tzinfo=tz)
    return start, end


def epoch_ms(dt_value: datetime) -> int:
    return int(dt_value.timestamp() * 1000)


def parse_tick_time(value: object) -> Optional[datetime]:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return parse_tick_time_relaxed(text)


def parse_tick_time_relaxed(text: str) -> Optional[datetime]:
    if "." not in text:
        return None

    main, frac = text.split(".", 1)
    tz_part = ""
    for marker in ("+", "-"):
        idx = frac.find(marker)
        if idx > 0:
            tz_part = frac[idx:]
            frac = frac[:idx]
            break

    digits = "".join(ch for ch in frac if ch.isdigit())
    if not digits:
        return None

    normalized = f"{main}.{(digits + '000000')[:6]}{tz_part}"
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def extract_symbol_and_time(payload: object) -> Tuple[str, Optional[datetime]]:
    if not isinstance(payload, dict):
        return "", None

    symbol = str(payload.get("symbol") or "").upper()
    tick_dt = parse_tick_time(payload.get("time"))
    if symbol and tick_dt is not None:
        return symbol, tick_dt

    nested_payload = payload.get("payload")
    if not isinstance(nested_payload, dict):
        return "", None

    symbol = str(nested_payload.get("symbol") or "").upper()
    tick_dt = parse_tick_time(nested_payload.get("timestamp"))
    if symbol and tick_dt is not None:
        return symbol, tick_dt

    return "", None


def get_partitions(consumer: Consumer, topic: str) -> Iterable[int]:
    meta = consumer.list_topics(topic=topic, timeout=10)
    topic_meta = meta.topics.get(topic)
    if topic_meta is None or topic_meta.error is not None:
        raise RuntimeError(f"topic metadata unavailable for {topic}")
    return sorted(topic_meta.partitions.keys())


def build_start_offsets(
    consumer: Consumer,
    topic: str,
    partitions: Iterable[int],
    start_ms: int,
) -> list[TopicPartition]:
    req = [TopicPartition(topic, p, start_ms) for p in partitions]
    resolved = consumer.offsets_for_times(req, timeout=10)
    starts: list[TopicPartition] = []
    for tp in resolved:
        if tp.offset is None or tp.offset < 0:
            continue
        starts.append(TopicPartition(topic, tp.partition, tp.offset))
    return starts


def build_end_offsets(
    consumer: Consumer,
    topic: str,
    starts: list[TopicPartition],
    end_ms: int,
) -> Dict[int, int]:
    req = [TopicPartition(topic, tp.partition, end_ms) for tp in starts]
    resolved = consumer.offsets_for_times(req, timeout=10)
    end_offsets: Dict[int, int] = {}
    for tp in resolved:
        if tp.offset is not None and tp.offset >= 0:
            end_offsets[tp.partition] = tp.offset
            continue
        _, high = consumer.get_watermark_offsets(TopicPartition(topic, tp.partition), timeout=10)
        end_offsets[tp.partition] = high
    return end_offsets


def normalize_second(tick_dt: datetime, tz: ZoneInfo) -> datetime:
    return tick_dt.astimezone(tz).replace(microsecond=0)


def append_gap(
    gaps_by_symbol: dict[str, list[dict[str, object]]],
    symbol: str,
    gap_start: datetime,
    gap_end: datetime,
) -> None:
    missing_seconds = int((gap_end - gap_start).total_seconds()) + 1
    gaps_by_symbol[symbol].append(
        {
            "start_time": gap_start.isoformat(),
            "end_time": gap_end.isoformat(),
            "missing_seconds": missing_seconds,
        }
    )


def maybe_append_gap(
    gaps_by_symbol: dict[str, list[dict[str, object]]],
    symbol: str,
    previous_second: datetime,
    current_second: datetime,
    min_gap_seconds: int,
) -> None:
    missing_seconds = int((current_second - previous_second).total_seconds()) - 1
    if missing_seconds < min_gap_seconds:
        return
    append_gap(
        gaps_by_symbol,
        symbol,
        previous_second + timedelta(seconds=1),
        current_second - timedelta(seconds=1),
    )


def consume_day(
    cfg: AppConfig,
    target_day: date,
    symbol_filter: set[str],
) -> dict:
    tz = ZoneInfo(cfg.timezone)
    start_dt, end_dt = trading_day_bounds(target_day, tz)
    start_ms = epoch_ms(start_dt)
    end_ms = epoch_ms(end_dt)

    consumer = Consumer(
        {
            "bootstrap.servers": cfg.bootstrap_servers,
            "group.id": f"{cfg.group_id}-{target_day.isoformat()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    try:
        partitions = list(get_partitions(consumer, cfg.topic))
        starts = build_start_offsets(consumer, cfg.topic, partitions, start_ms)
        if not starts:
            return {
                "date": target_day.isoformat(),
                "window_start": start_dt.isoformat(),
                "window_end": end_dt.isoformat(),
                "topic": cfg.topic,
                "min_gap_seconds": cfg.min_gap_seconds,
                "symbols_requested": sorted(symbol_filter),
                "symbols_analyzed": 0,
                "total_ticks": 0,
                "symbols": {},
            }
        end_offsets = build_end_offsets(consumer, cfg.topic, starts, end_ms)
        consumer.assign(starts)

        total_ticks = 0
        done_partitions = set()
        idle_polls = 0
        tick_counts: Dict[str, int] = defaultdict(int)
        first_seen_second: Dict[str, datetime] = {}
        last_seen_second: Dict[str, datetime] = {}
        gaps_by_symbol: Dict[str, list[dict[str, object]]] = defaultdict(list)

        while len(done_partitions) < len(starts):
            msg = consumer.poll(cfg.poll_timeout_ms / 1000.0)
            if msg is None:
                idle_polls += 1
                if idle_polls >= cfg.max_idle_polls:
                    break
                continue
            idle_polls = 0
            if msg.error() is not None:
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    done_partitions.add(msg.partition())
                    continue
                raise RuntimeError(f"kafka consume error: {msg.error()}")

            part = msg.partition()
            msg_offset = msg.offset()
            end_offset = end_offsets.get(part)
            if end_offset is not None and msg_offset >= end_offset:
                if part not in done_partitions:
                    done_partitions.add(part)
                    consumer.pause([TopicPartition(cfg.topic, part)])
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except Exception:
                continue

            symbol, tick_dt = extract_symbol_and_time(payload)
            if not symbol or tick_dt is None:
                continue
            if symbol_filter and symbol not in symbol_filter:
                continue

            tick_second = normalize_second(tick_dt, tz)
            if tick_second.date() != target_day:
                continue

            total_ticks += 1
            tick_counts[symbol] += 1

            if symbol not in first_seen_second:
                first_seen_second[symbol] = tick_second
                if int((tick_second - start_dt).total_seconds()) >= cfg.min_gap_seconds:
                    append_gap(gaps_by_symbol, symbol, start_dt, tick_second - timedelta(seconds=1))
            else:
                previous_second = last_seen_second[symbol]
                if tick_second > previous_second:
                    maybe_append_gap(gaps_by_symbol, symbol, previous_second, tick_second, cfg.min_gap_seconds)

            if symbol not in last_seen_second or tick_second > last_seen_second[symbol]:
                last_seen_second[symbol] = tick_second

        symbols_report: Dict[str, dict[str, object]] = {}
        for symbol in sorted(tick_counts.keys()):
            last_second = last_seen_second[symbol]
            if int((end_dt - last_second).total_seconds()) >= cfg.min_gap_seconds:
                append_gap(gaps_by_symbol, symbol, last_second + timedelta(seconds=1), end_dt)

            symbol_gaps = gaps_by_symbol.get(symbol, [])
            total_missing_seconds = sum(int(gap["missing_seconds"]) for gap in symbol_gaps)
            symbols_report[symbol] = {
                "tick_count": tick_counts[symbol],
                "first_tick_time": first_seen_second[symbol].isoformat(),
                "last_tick_time": last_second.isoformat(),
                "gap_count": len(symbol_gaps),
                "total_missing_seconds": total_missing_seconds,
                "gaps": symbol_gaps,
            }

        return {
            "date": target_day.isoformat(),
            "window_start": start_dt.isoformat(),
            "window_end": end_dt.isoformat(),
            "topic": cfg.topic,
            "min_gap_seconds": cfg.min_gap_seconds,
            "symbols_requested": sorted(symbol_filter),
            "symbols_analyzed": len(symbols_report),
            "total_ticks": total_ticks,
            "symbols": symbols_report,
        }
    finally:
        consumer.close()


def write_report(output_dir: str, report: dict) -> Path:
    base_dir = Path(output_dir).expanduser()
    audit_dir = base_dir / "audit"
    audit_dir.mkdir(parents=True, exist_ok=True)

    today = datetime.now()
    dated_dir = audit_dir / today.strftime("%d") / today.strftime("%m") / today.strftime("%Y")
    dated_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = dated_dir / f"{ts}.json"
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return out_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Tick gap audit for trading day from Kafka")
    parser.add_argument("--config", default="config/tick_gap_audit_config.json", help="Config JSON path")
    parser.add_argument("--date", default="", help="Target day in YYYY-MM-DD (default: last trading day)")
    parser.add_argument("--symbol", action="append", default=[], help="Filter to one or more symbols")
    args = parser.parse_args()

    cfg = load_config(args.config)
    tz = ZoneInfo(cfg.timezone)
    if args.date:
        target_day = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        target_day = previous_trading_day(datetime.now(tz).date())

    cli_symbols = [item.strip().upper() for item in args.symbol if item.strip()]
    symbol_filter = set(cli_symbols or cfg.symbols)

    report = consume_day(cfg, target_day, symbol_filter)
    out_path = write_report(cfg.output_dir, report)

    print(f"Date: {report['date']}")
    print(f"Topic: {report['topic']}")
    print(f"Min gap seconds: {report['min_gap_seconds']}")
    print(f"Symbols analyzed: {report['symbols_analyzed']}")
    print(f"Total ticks: {report['total_ticks']}")
    print(f"Report: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
