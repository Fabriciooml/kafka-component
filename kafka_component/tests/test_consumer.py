from __future__ import annotations

import asyncio
from typing import Any

from aiokafka import AIOKafkaProducer

from kafka_component.consumer import KafkaConsumerComponent
from kafka_component.errors import DeadLetterPolicy
from kafka_component.producer import KafkaProducerComponent
from kafka_component.tests.helpers import raw_consume_one, raw_produce


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


async def test_handler_failure_invokes_error_policy_and_continues(bootstrap_servers):
    topic = "error-policy-topic"
    await raw_produce(bootstrap_servers, topic, {"seq": 1, "fail": True})
    await raw_produce(bootstrap_servers, topic, {"seq": 2, "fail": False})

    handled: list[tuple[Any, Exception]] = []

    class SpyPolicy:
        async def handle(self, message: Any, exc: Exception) -> None:
            handled.append((message.value, exc))

    processed: list[Any] = []

    async def handler(value: Any) -> None:
        if value["fail"]:
            raise ValueError("handler exploded")
        processed.append(value)

    component = KafkaConsumerComponent(
        bootstrap_servers=bootstrap_servers,
        group_id="error-policy-group",
        topics=[topic],
        handler=handler,
        error_policy=SpyPolicy(),
    )
    await component.start()
    try:
        for _ in range(50):
            if len(processed) == 1 and len(handled) == 1:
                break
            await asyncio.sleep(0.2)
    finally:
        await component.shutdown()

    assert processed == [{"seq": 2, "fail": False}]
    assert len(handled) == 1
    assert handled[0][0] == {"seq": 1, "fail": True}
    assert isinstance(handled[0][1], ValueError)


async def test_dead_letter_policy_republishes_failed_message(bootstrap_servers):
    topic = "dlq-source-topic"
    await raw_produce(bootstrap_servers, topic, {"seq": 1, "fail": True})

    producer = KafkaProducerComponent(bootstrap_servers=bootstrap_servers)
    await producer.start()

    async def handler(value: Any) -> None:
        raise ValueError("boom")

    component = KafkaConsumerComponent(
        bootstrap_servers=bootstrap_servers,
        group_id="dlq-test-group",
        topics=[topic],
        handler=handler,
        error_policy=DeadLetterPolicy(producer),
    )
    await component.start()
    try:
        dlq_msg = await raw_consume_one(
            bootstrap_servers, f"{topic}.DLQ", group_id="dlq-verify-group", timeout=15.0
        )
    finally:
        await component.shutdown()
        await producer.shutdown()

    assert dlq_msg.value["original_topic"] == topic
    assert dlq_msg.value["original_value"] == {"seq": 1, "fail": True}
    assert dlq_msg.value["error_type"] == "ValueError"


async def test_shutdown_drains_in_flight_message_before_stopping(bootstrap_servers):
    topic = "drain-topic"
    await raw_produce(bootstrap_servers, topic, {"seq": 1})

    started_processing = asyncio.Event()
    finished_processing = asyncio.Event()

    async def handler(value: Any) -> None:
        started_processing.set()
        await asyncio.sleep(1.0)
        finished_processing.set()

    component = KafkaConsumerComponent(
        bootstrap_servers=bootstrap_servers,
        group_id="drain-group",
        topics=[topic],
        handler=handler,
    )
    await component.start()
    await asyncio.wait_for(started_processing.wait(), timeout=10.0)

    await component.shutdown()

    assert finished_processing.is_set()


async def test_deserialization_failure_marks_disconnected_and_shutdown_does_not_hang(
    bootstrap_servers,
):
    topic = "malformed-value-topic"

    # Bypass raw_produce (which JSON-encodes) to get genuinely malformed
    # bytes onto the topic: a plain producer with no value_serializer.
    raw_producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await raw_producer.start()
    try:
        await raw_producer.send_and_wait(topic, value=b"not valid json")
    finally:
        await raw_producer.stop()

    async def handler(value: Any) -> None:
        pass

    component = KafkaConsumerComponent(
        bootstrap_servers=bootstrap_servers,
        group_id="malformed-value-group",
        topics=[topic],
        handler=handler,
    )
    await component.start()

    for _ in range(50):
        if component.get_status()["connected"] is False:
            break
        await asyncio.sleep(0.2)

    status = component.get_status()
    assert status["connected"] is False
    assert status["last_error"] is not None

    await asyncio.wait_for(component.shutdown(), timeout=5.0)
