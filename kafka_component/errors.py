from __future__ import annotations

import logging
from typing import Any, Protocol

logger = logging.getLogger(__name__)


class Sender(Protocol):
    async def send(self, topic: str, value: Any, *, key: bytes | str | None = None) -> None: ...


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
