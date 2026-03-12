"""Kafka producer wrapper for tick publishing."""

from __future__ import annotations

import json
import logging
from typing import Any, Optional

from python_feed_triplex import metrics

try:
    from confluent_kafka import KafkaException, Producer
except Exception:  # pylint: disable=broad-except
    Producer = None
    KafkaException = Exception


class KafkaTickProducer:
    """Thin wrapper for producing ticks to Kafka."""

    # __init__ creates the Kafka producer using provided connection parameters.
    # Parameters:
    # - bootstrap_servers: Kafka broker list.
    # - topic: Kafka topic for ticks.
    # - acks, linger_ms, batch_size: producer tuning options.
    # - logger: logger for diagnostics.
    # Returns: None.
    # Flow: build producer config, instantiate Producer if available, warn otherwise.
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        acks: str,
        linger_ms: int,
        batch_size: int,
        logger: logging.Logger,
    ) -> None:
        self.topic = topic
        self.logger = logger
        self._producer: Optional[Producer] = None
        self.logger.info(
            "Initializing Kafka producer: brokers=%s topic=%s acks=%s linger_ms=%s batch_size=%s",
            bootstrap_servers,
            topic,
            acks,
            linger_ms,
            batch_size,
        )
        if Producer:
            conf = {
                "bootstrap.servers": bootstrap_servers,
                "acks": acks,
                "linger.ms": linger_ms,
                "batch.num.messages": batch_size,
                "queue.buffering.max.messages": 1000000,
                "queue.buffering.max.kbytes": 1048576,  # 1 GB buffer
                "message.timeout.ms": 30000,
            }
            try:
                self._producer = Producer(conf)
                metrics.KAFKA_CONNECTED.set(1)
                self.logger.info("Kafka producer created successfully")
            except Exception as exc:  # pylint: disable=broad-except
                self.logger.error("Failed to create Kafka producer: %s", exc)
                metrics.KAFKA_CONNECTED.set(0)
        else:
            self.logger.warning("confluent_kafka not installed; Kafka publishing disabled")
            metrics.KAFKA_CONNECTED.set(0)

    # send_tick encodes and publishes a normalized tick to Kafka.
    # Parameters:
    # - tick: normalized tick dictionary.
    # Returns: None.
    # Flow: JSON-encode tick, produce with symbol key, log delivery errors but do not raise.
    def send_tick(self, tick: dict) -> None:
        payload = json.dumps(tick)
        if not self._producer:
            self.logger.debug("Kafka disabled; dropping tick for %s", tick.get("symbol"))
            return
        try:
            self.logger.debug("Producing tick for %s to topic=%s", tick.get("symbol"), self.topic)
            self._producer.produce(
                topic=self.topic,
                key=str(tick.get("symbol", "")),
                value=payload.encode("utf-8"),
                on_delivery=self._delivery_report,
            )
            # Poll to serve delivery callbacks and drain internal queues.
            self._producer.poll(0)
            metrics.KAFKA_CONNECTED.set(1)
            metrics.TICKS_PUBLISHED.inc()
        except BufferError:
            self.logger.warning("Local Kafka producer queue full; dropping tick for %s", tick.get("symbol"))
            metrics.ERRORS_TOTAL.inc()
        except KafkaException as exc:  # type: ignore[misc]
            self.logger.error("Kafka produce failed: %s", exc)
            metrics.KAFKA_CONNECTED.set(0)
            metrics.ERRORS_TOTAL.inc()

    # flush drains producer buffers to Kafka.
    # Parameters: None.
    # Returns: None.
    # Flow: call Producer.flush if available.
    def flush(self) -> None:
        if self._producer:
            self.logger.info("Flushing Kafka producer buffers")
            self._producer.flush()

    # close flushes and releases producer resources.
    # Parameters: None.
    # Returns: None.
    # Flow: flush outstanding messages, drop producer reference.
    def close(self) -> None:
        self.logger.info("Closing Kafka producer")
        self.flush()
        self._producer = None
        metrics.KAFKA_CONNECTED.set(0)

    # _delivery_report logs delivery errors for produced messages.
    # Parameters:
    # - err: delivery error if any.
    # - msg: message metadata (unused).
    # Returns: None.
    # Flow: log on error, ignore on success.
    def _delivery_report(self, err: Any, msg: Any) -> None:  # pylint: disable=unused-argument
        if err:
            self.logger.error("Kafka delivery error: %s", err)
            metrics.ERRORS_TOTAL.inc()
        else:
            try:
                self.logger.debug(
                    "Kafka delivery success topic=%s partition=%s offset=%s",
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                )
            except Exception:  # pylint: disable=broad-except
                self.logger.debug("Kafka delivery success")
