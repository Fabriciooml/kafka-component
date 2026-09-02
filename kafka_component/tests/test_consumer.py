from __future__ import annotations

import asyncio
from typing import Any

from kafka_component.consumer import KafkaConsumerComponent
from kafka_component.tests.helpers import raw_produce


async def test_consumer_receives_preexisting_messages_in_order(bootstrap_servers):
    topic = "consumer-order-topic"
    await raw_produce(bootstrap_servers, topic, {"seq": 1})
    await raw_produce(bootstrap_servers, topic, {"seq": 2})
    await raw_produce(bootstrap_servers, topic, {"seq": 3})

    received: list[Any] = []

    async def handler(value: Any) -> None:
        received.append(value)

    component = KafkaConsumerComponent(
        bootstrap_servers=bootstrap_servers,
        group_id="order-test-group",
        topics=[topic],
        handler=handler,
    )
    await component.start()
    try:
        for _ in range(50):
            if len(received) == 3:
                break
            await asyncio.sleep(0.2)
    finally:
        await component.shutdown()

    assert received == [{"seq": 1}, {"seq": 2}, {"seq": 3}]


async def test_consumer_subscribes_to_multiple_topics(bootstrap_servers):
    topic_a = "multi-topic-a"
    topic_b = "multi-topic-b"
    await raw_produce(bootstrap_servers, topic_a, {"from": "a"})
    await raw_produce(bootstrap_servers, topic_b, {"from": "b"})

    received: list[Any] = []

    async def handler(value: Any) -> None:
        received.append(value)

    component = KafkaConsumerComponent(
        bootstrap_servers=bootstrap_servers,
        group_id="multi-topic-group",
        topics=[topic_a, topic_b],
        handler=handler,
    )
    await component.start()
    try:
        for _ in range(50):
            if len(received) == 2:
                break
            await asyncio.sleep(0.2)
    finally:
        await component.shutdown()

    assert {"from": "a"} in received
    assert {"from": "b"} in received
