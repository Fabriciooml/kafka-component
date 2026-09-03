from __future__ import annotations

import asyncio
from typing import Any

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from python_components import System

from kafka_component.consumer import KafkaConsumerComponent
from kafka_component.producer import KafkaProducerComponent


async def test_system_composes_producer_and_consumer_and_mounts_health_routes(
    bootstrap_servers,
):
    topic = "system-integration-topic"
    received: list[Any] = []

    async def handler(value: Any) -> None:
        received.append(value)

    producer = KafkaProducerComponent(bootstrap_servers=bootstrap_servers)
    consumer = KafkaConsumerComponent(
        bootstrap_servers=bootstrap_servers,
        group_id="system-integration-group",
        topics=[topic],
        handler=handler,
    ).using(["producer"])

    system = System({"producer": producer, "consumer": consumer})

    async with system:
        sent_producer = system.get_component("producer")
        assert sent_producer is producer

        await producer.send(topic, {"order_id": 99})

        for _ in range(50):
            if received:
                break
            await asyncio.sleep(0.2)

        assert received == [{"order_id": 99}]

        app = FastAPI()
        app.include_router(producer.routes())

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/health")

        assert response.status_code == 200
        assert response.json() == {"connected": True, "last_error": None}
