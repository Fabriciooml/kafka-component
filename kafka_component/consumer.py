from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from aiokafka import AIOKafkaConsumer
from fastapi import APIRouter
from python_components import Component

from kafka_component._routes import build_health_router
from kafka_component.errors import ErrorPolicy, SkipAndLogPolicy


@dataclass(frozen=True)
class KafkaRecord:
    topic: str
    partition: int
    offset: int
    value: bytes
    key: bytes | None = None
    timestamp: int | None = None
    headers: tuple[tuple[str, bytes], ...] = ()


@dataclass(frozen=True)
class PartitionAssignment:
    topic: str
    partition: int
    beginning_offset: int
    end_offset: int


class KafkaConsumerComponent(Component):
    def __init__(
        self,
        *,
        bootstrap_servers: str | list[str],
        group_id: str,
        topics: list[str],
        handler: Callable[[Any], Awaitable[None]],
        error_policy: ErrorPolicy | None = None,
        client_id: str | None = None,
    ) -> None:
        self.using([])
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        self._topics = topics
        self._handler = handler
        self._error_policy: ErrorPolicy = error_policy or SkipAndLogPolicy()
        self._client_id = client_id
        self._consumer: AIOKafkaConsumer | None = None
        self._started = False
        self._last_error: str | None = None
        self._consume_task: asyncio.Task[None] | None = None
        self._stopping: asyncio.Event = asyncio.Event()

    async def start(self) -> None:
        self._stopping = asyncio.Event()
        self._consumer = AIOKafkaConsumer(
            *self._topics,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            client_id=self._client_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        await self._consumer.start()
        self._started = True
        self._consume_task = asyncio.create_task(self._consume_loop())

    async def shutdown(self) -> None:
        self._stopping.set()
        if self._consume_task is not None:
            await self._consume_task
        if self._consumer is not None:
            await self._consumer.stop()
        self._started = False

    async def _consume_loop(self) -> None:
        assert self._consumer is not None
        while not self._stopping.is_set():
            try:
                try:
                    msg = await asyncio.wait_for(self._consumer.getone(), timeout=1.0)
                except TimeoutError:
                    continue

                try:
                    await self._handler(msg.value)
                except Exception as exc:
                    self._last_error = str(exc)
                    try:
                        await self._error_policy.handle(msg, exc)
                    except Exception as policy_exc:
                        self._last_error = str(policy_exc)
                        continue

                await self._consumer.commit()
            except Exception as exc:
                # Anything not already handled above (a deserialization
                # failure surfacing from getone() itself, or a commit()
                # failure) would otherwise kill this task permanently while
                # get_status() kept reporting connected=True. Mark the loop
                # as dead and return normally so shutdown()'s
                # `await self._consume_task` doesn't re-raise and skip
                # `self._consumer.stop()`.
                self._last_error = str(exc)
                self._started = False
                break

    def routes(self) -> APIRouter:
        return build_health_router(self.get_status)

    def get_status(self) -> dict[str, Any]:
        return {"connected": self._started, "last_error": self._last_error}
