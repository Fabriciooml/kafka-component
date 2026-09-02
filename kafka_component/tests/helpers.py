from __future__ import annotations

import asyncio
import json
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer


async def raw_produce(bootstrap_servers: str, topic: str, value: Any) -> None:
    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()
    try:
        await producer.send_and_wait(topic, value=value)
    finally:
        await producer.stop()


async def raw_consume_one(
    bootstrap_servers: str,
    topic: str,
    *,
    group_id: str,
    timeout: float = 10.0,
):
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    await consumer.start()
    try:
        return await asyncio.wait_for(consumer.getone(), timeout=timeout)
    finally:
        await consumer.stop()
