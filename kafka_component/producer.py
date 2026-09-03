from __future__ import annotations

import json
from typing import Any

from aiokafka import AIOKafkaProducer
from fastapi import APIRouter
from python_components import Component

from kafka_component._routes import build_health_router


class KafkaProducerComponent(Component):
    def __init__(
        self,
        *,
        bootstrap_servers: str | list[str],
        client_id: str | None = None,
    ) -> None:
        self.using([])
        self._bootstrap_servers = bootstrap_servers
        self._client_id = client_id
        self._producer: AIOKafkaProducer | None = None
        self._started = False
        self._last_error: str | None = None

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            client_id=self._client_id,
            acks="all",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        await self._producer.start()
        self._started = True

    async def shutdown(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
        self._started = False

    async def send(self, topic: str, value: Any, *, key: bytes | None = None) -> None:
        if self._producer is None:
            raise RuntimeError("KafkaProducerComponent.send() called before start()")
        try:
            await self._producer.send_and_wait(topic, value=value, key=key)
        except Exception as exc:
            self._last_error = str(exc)
            raise

    def routes(self) -> APIRouter:
        return build_health_router(self.get_status)

    def get_status(self) -> dict[str, Any]:
        return {"connected": self._started, "last_error": self._last_error}
