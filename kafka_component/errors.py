from __future__ import annotations

import logging
from typing import Any, Protocol

logger = logging.getLogger(__name__)


class StartOffsetOutOfRangeError(Exception):
    def __init__(
        self,
        *,
        topic: str,
        partition: int,
        requested_offset: int,
        beginning_offset: int,
        end_offset: int,
    ) -> None:
        self.topic = topic
        self.partition = partition
        self.requested_offset = requested_offset
        self.beginning_offset = beginning_offset
        self.end_offset = end_offset
        super().__init__(
            f"start_offset_resolver returned offset {requested_offset} for "
            f"topic={topic} partition={partition}, outside valid range "
            f"[{beginning_offset}, {end_offset}]"
        )


class Sender(Protocol):
    async def send(
        self, topic: str, value: Any, *, key: bytes | None = None
    ) -> None: ...


class ErrorPolicy(Protocol):
    async def handle(self, message: Any, exc: Exception) -> None: ...


class SkipAndLogPolicy:
    async def handle(self, message: Any, exc: Exception) -> None:
        logger.error(
            "kafka_component: handler failed for topic=%s partition=%s offset=%s: %s",
            getattr(message, "topic", None),
            getattr(message, "partition", None),
            getattr(message, "offset", None),
            exc,
        )


class DeadLetterPolicy:
    def __init__(self, producer: Sender, dlq_topic: str | None = None) -> None:
        self._producer = producer
        self._dlq_topic = dlq_topic

    async def handle(self, message: Any, exc: Exception) -> None:
        topic = self._dlq_topic or f"{message.topic}.DLQ"
        payload = {
            "original_topic": message.topic,
            "original_partition": message.partition,
            "original_offset": message.offset,
            "original_value": message.value,
            "error_type": type(exc).__name__,
            "error_message": str(exc),
        }
        await self._producer.send(topic, payload)
