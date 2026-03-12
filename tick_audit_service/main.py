#!/usr/bin/env python3
"""Kafka tick audit service for last trading day symbol counts."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
from typing import Dict, Iterable, Optional

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


def load_config(path: str) -> AppConfig:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    kafka_cfg = raw.get("kafka", {})
    app_cfg = raw.get("app", {})
    return AppConfig(
        bootstrap_servers=str(kafka_cfg.get("bootstrap_servers", "localhost:9092")),
        topic=str(kafka_cfg.get("topic", "ticks.raw")),
        group_id=str(kafka_cfg.get("group_id", "tick-audit-service")),
        timezone=str(app_cfg.get("timezone", "Asia/Kolkata")),
        output_dir=str(app_cfg.get("output_dir", "logs/tick_audit")),
        poll_timeout_ms=int(app_cfg.get("poll_timeout_ms", 1000)),
        max_idle_polls=int(app_cfg.get("max_idle_polls", 20)),
    )


def previous_trading_day(ref: date) -> date:
    day = ref - timedelta(days=1)
    while day.weekday() > 4:  # Saturday/Sunday
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
        return None


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


def consume_day(
    cfg: AppConfig,
    target_day: date,
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
                "total_ticks": 0,
                "symbol_count": 0,
                "symbols": {},
            }
        end_offsets = build_end_offsets(consumer, cfg.topic, starts, end_ms)
        consumer.assign(starts)

        symbol_counts: Dict[str, int] = defaultdict(int)
        total_ticks = 0
        done_partitions = set()
        idle_polls = 0

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
            symbol = str(payload.get("symbol") or "").upper()
            tick_dt = parse_tick_time(payload.get("time"))
            if not symbol or tick_dt is None:
                continue
            tick_local = tick_dt.astimezone(tz)
            if tick_local.date() != target_day:
                continue
            total_ticks += 1
            symbol_counts[symbol] += 1

        ordered = dict(sorted(symbol_counts.items(), key=lambda item: item[1], reverse=True))
        return {
            "date": target_day.isoformat(),
            "window_start": start_dt.isoformat(),
            "window_end": end_dt.isoformat(),
            "total_ticks": total_ticks,
            "symbol_count": len(ordered),
            "symbols": ordered,
        }
    finally:
        consumer.close()


def write_report(output_dir: str, report: dict) -> Path:
    out_dir = Path(output_dir).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"tick_audit_{report['date']}_{ts}.json"
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return out_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Tick audit for last trading day from Kafka")
    parser.add_argument("--config", default="config/tick_audit_config.json", help="Config JSON path")
    parser.add_argument("--date", default="", help="Target day in YYYY-MM-DD (default: last trading day)")
    args = parser.parse_args()

    cfg = load_config(args.config)
    tz = ZoneInfo(cfg.timezone)
    if args.date:
        target_day = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        target_day = previous_trading_day(datetime.now(tz).date())

    report = consume_day(cfg, target_day)
    out_path = write_report(cfg.output_dir, report)

    print(f"Date: {report['date']}")
    print(f"Total ticks: {report['total_ticks']}")
    print(f"Symbols with ticks: {report['symbol_count']}")
    print(f"Report: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
