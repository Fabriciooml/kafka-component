from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Literal

from aiokafka import AIOKafkaConsumer, ConsumerRebalanceListener
from fastapi import APIRouter
from python_components import Component

from kafka_component._routes import build_health_router
from kafka_component.errors import (
    ErrorPolicy,
    SkipAndLogPolicy,
    StartOffsetOutOfRangeError,
)


@dataclass(frozen=True)
class KafkaRecord:
    topic: str
    partition: int
    offset: int
    value: bytes | None
    key: bytes | None = None
    timestamp: int | None = None
    headers: tuple[tuple[str, bytes], ...] = ()


@dataclass(frozen=True)
class PartitionAssignment:
    topic: str
    partition: int
    beginning_offset: int
    end_offset: int


class _ResolverRebalanceListener(ConsumerRebalanceListener):
    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        resolver: Callable[[PartitionAssignment], Awaitable[int]],
        on_error: Callable[[Exception], None],
    ) -> None:
        self._consumer = consumer
        self._resolver = resolver
        self._on_error = on_error

    async def on_partitions_revoked(self, revoked: Any) -> None:
        pass

    async def on_partitions_assigned(self, assigned: Any) -> None:
        assigned = list(assigned)
        if not assigned:
            return
        self._consumer.pause(*assigned)
        for tp in assigned:
            committed = await self._consumer.committed(tp)
            if committed is not None:
                self._consumer.resume(tp)
                continue
            beginning = (await self._consumer.beginning_offsets([tp]))[tp]
            end = (await self._consumer.end_offsets([tp]))[tp]
            assignment = PartitionAssignment(
                topic=tp.topic,
                partition=tp.partition,
                beginning_offset=beginning,
                end_offset=end,
            )
            target = await self._resolver(assignment)
            if not beginning <= target <= end:
                exc = StartOffsetOutOfRangeError(
                    topic=tp.topic,
                    partition=tp.partition,
                    requested_offset=target,
                    beginning_offset=beginning,
                    end_offset=end,
                )
                self._on_error(exc)
                raise exc
            self._consumer.seek(tp, target)
            self._consumer.resume(tp)


class KafkaConsumerComponent(Component):
    def __init__(
        self,
        *,
        bootstrap_servers: str | list[str],
        group_id: str,
        topics: list[str],
        handler: Callable[[Any], Awaitable[None]] | None = None,
        record_handler: Callable[[KafkaRecord], Awaitable[None]] | None = None,
        start_offset_resolver: Callable[[PartitionAssignment], Awaitable[int]]
        | None = None,
        auto_offset_reset: Literal["earliest", "latest", "none"] = "earliest",
        error_policy: ErrorPolicy | None = None,
        client_id: str | None = None,
    ) -> None:
        if (handler is None) == (record_handler is None):
            raise ValueError(
                "KafkaConsumerComponent requires exactly one of `handler` or "
                "`record_handler`"
            )
        if auto_offset_reset == "none" and start_offset_resolver is not None:
            raise ValueError(
                "auto_offset_reset='none' cannot be combined with "
                "start_offset_resolver: aiokafka establishes each "
                "partition's initial position (raising "
                "NoOffsetForPartitionError under 'none' when there is no "
                "committed offset) before the resolver's seek() can run, "
                "independent of pause state, so this combination can never "
                "work. The resolver already runs for every partition with "
                "no committed offset regardless of auto_offset_reset — use "
                "the default 'earliest' (or 'latest') instead."
            )
        self.using([])
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        self._topics = topics
        self._handler = handler
        self._record_handler = record_handler
        self._start_offset_resolver = start_offset_resolver
        self._auto_offset_reset = auto_offset_reset
        self._error_policy: ErrorPolicy = error_policy or SkipAndLogPolicy()
        self._client_id = client_id
        self._consumer: AIOKafkaConsumer | None = None
        self._started = False
        self._last_error: str | None = None
        self._consume_task: asyncio.Task[None] | None = None
        self._stopping: asyncio.Event = asyncio.Event()
        self._resolver_error: Exception | None = None

    async def start(self) -> None:
        self._stopping = asyncio.Event()
        self._resolver_error = None
        self._consumer = AIOKafkaConsumer(
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            client_id=self._client_id,
            auto_offset_reset=self._auto_offset_reset,
            enable_auto_commit=False,
            value_deserializer=(
                (lambda v: json.loads(v.decode("utf-8")))
                if self._handler is not None
                else None
            ),
        )
        listener = (
            _ResolverRebalanceListener(
                self._consumer, self._start_offset_resolver, self._record_resolver_error
            )
            if self._start_offset_resolver is not None
            else None
        )
        self._consumer.subscribe(topics=self._topics, listener=listener)
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

    def _record_resolver_error(self, exc: Exception) -> None:
        self._resolver_error = exc

    async def _consume_loop(self) -> None:
        assert self._consumer is not None
        while not self._stopping.is_set():
            try:
                if self._resolver_error is not None:
                    # aiokafka's coordinator swallows exceptions raised from
                    # on_partitions_assigned (logs and continues) rather than
                    # propagating them to getone() — so the listener records
                    # the error via _record_resolver_error and we re-raise it
                    # here ourselves to reach the fatal path below.
                    raise self._resolver_error

                try:
                    msg = await asyncio.wait_for(self._consumer.getone(), timeout=1.0)
                except TimeoutError:
                    continue

                try:
                    if self._record_handler is not None:
                        record = KafkaRecord(
                            topic=msg.topic,
                            partition=msg.partition,
                            offset=msg.offset,
                            value=msg.value,
                            key=msg.key,
                            timestamp=msg.timestamp,
                            headers=tuple(msg.headers),
                        )
                        await self._record_handler(record)
                    else:
                        assert self._handler is not None
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
