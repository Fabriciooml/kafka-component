from __future__ import annotations

import asyncio
import json
from typing import Any

from aiokafka import AIOKafkaProducer

from kafka_component.consumer import (
    KafkaConsumerComponent,
    KafkaRecord,
    PartitionAssignment,
)
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


async def test_record_handler_receives_coordinates_across_partitions(bootstrap_servers):
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic

    topic = "record-handler-topic"
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        await admin.create_topics(
            [NewTopic(name=topic, num_partitions=2, replication_factor=1)]
        )
    finally:
        await admin.close()

    raw_producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await raw_producer.start()
    try:
        await raw_producer.send_and_wait(topic, value={"seq": 1}, partition=0)
        await raw_producer.send_and_wait(topic, value={"seq": 2}, partition=1)
    finally:
        await raw_producer.stop()

    received: list[KafkaRecord] = []

    async def record_handler(record: KafkaRecord) -> None:
        received.append(record)

    component = KafkaConsumerComponent(
        bootstrap_servers=bootstrap_servers,
        group_id="record-handler-group",
        topics=[topic],
        record_handler=record_handler,
    )
    await component.start()
    try:
        for _ in range(50):
            if len(received) == 2:
                break
            await asyncio.sleep(0.2)
    finally:
        await component.shutdown()

    assert len(received) == 2
    by_partition = {r.partition: r for r in received}
    assert set(by_partition) == {0, 1}
    assert by_partition[0].offset == 0
    assert by_partition[1].offset == 0
    for record in received:
        assert record.topic == topic
        assert json.loads(record.value) in [{"seq": 1}, {"seq": 2}]


async def test_record_handler_exception_leaves_offset_uncommitted_then_advances(
    bootstrap_servers,
):
    topic = "record-handler-error-topic"
    await raw_produce(bootstrap_servers, topic, {"seq": 1, "fail": True})
    await raw_produce(bootstrap_servers, topic, {"seq": 2, "fail": False})

    handled: list[KafkaRecord] = []
    processed: list[KafkaRecord] = []

    class SpyPolicy:
        async def handle(self, message, exc) -> None:
            handled.append(message)

    async def record_handler(record: KafkaRecord) -> None:
        value = json.loads(record.value)
        if value["fail"]:
            raise ValueError("handler exploded")
        processed.append(record)

    component = KafkaConsumerComponent(
        bootstrap_servers=bootstrap_servers,
        group_id="record-handler-error-group",
        topics=[topic],
        record_handler=record_handler,
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

    assert len(processed) == 1
    assert processed[0].offset == 1
    assert len(handled) == 1
    assert handled[0].offset == 0


async def test_record_handler_sees_raw_bytes_for_malformed_json(bootstrap_servers):
    topic = "record-handler-malformed-topic"
    raw_producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await raw_producer.start()
    try:
        await raw_producer.send_and_wait(topic, value=b"not valid json")
    finally:
        await raw_producer.stop()

    seen: list[KafkaRecord] = []
    decode_errors: list[Exception] = []

    async def record_handler(record: KafkaRecord) -> None:
        seen.append(record)
        try:
            json.loads(record.value)
        except Exception as exc:
            decode_errors.append(exc)

    component = KafkaConsumerComponent(
        bootstrap_servers=bootstrap_servers,
        group_id="record-handler-malformed-group",
        topics=[topic],
        record_handler=record_handler,
    )
    await component.start()
    try:
        for _ in range(50):
            if len(seen) == 1:
                break
            await asyncio.sleep(0.2)
    finally:
        await component.shutdown()

    assert len(seen) == 1
    assert seen[0].topic == topic
    assert seen[0].partition == 0
    assert seen[0].offset == 0
    assert seen[0].value == b"not valid json"
    assert len(decode_errors) == 1


async def test_resolver_selects_saved_offset_and_skips_earlier_records(
    bootstrap_servers,
):
    topic = "resolver-topic"
    await raw_produce(bootstrap_servers, topic, {"seq": 1})
    await raw_produce(bootstrap_servers, topic, {"seq": 2})
    await raw_produce(bootstrap_servers, topic, {"seq": 3})

    received: list[KafkaRecord] = []

    async def record_handler(record: KafkaRecord) -> None:
        received.append(record)

    async def resolver(assignment: PartitionAssignment) -> int:
        assert assignment.topic == topic
        assert assignment.partition == 0
        return assignment.end_offset - 1  # skip the first two records

    component = KafkaConsumerComponent(
        bootstrap_servers=bootstrap_servers,
        group_id="resolver-group",
        topics=[topic],
        record_handler=record_handler,
        start_offset_resolver=resolver,
    )
    await component.start()
    try:
        for _ in range(50):
            if len(received) == 1:
                break
            await asyncio.sleep(0.2)
    finally:
        await component.shutdown()

    assert len(received) == 1
    assert json.loads(received[0].value) == {"seq": 3}


async def test_resolver_offset_includes_records_produced_before_startup(
    bootstrap_servers,
):
    from aiokafka import AIOKafkaConsumer, TopicPartition

    topic = "resolver-late-produce-topic"
    await raw_produce(bootstrap_servers, topic, {"seq": 1})

    # Simulate "route created, boundary captured" — the application would do
    # this itself (e.g. an admin client's end_offsets call) at route-creation
    # time, well before this component's consumer ever starts.
    probe = AIOKafkaConsumer(bootstrap_servers=bootstrap_servers)
    await probe.start()
    try:
        tp = TopicPartition(topic, 0)
        boundary = (await probe.end_offsets([tp]))[tp]
    finally:
        await probe.stop()

    # More records arrive after the boundary was captured but before this
    # component's consumer starts — they must still be handled.
    await raw_produce(bootstrap_servers, topic, {"seq": 2})

    async def fixed_resolver(assignment: PartitionAssignment) -> int:
        return boundary

    received: list[KafkaRecord] = []

    async def record_handler(record: KafkaRecord) -> None:
        received.append(record)

    component = KafkaConsumerComponent(
        bootstrap_servers=bootstrap_servers,
        group_id="resolver-late-produce-group",
        topics=[topic],
        record_handler=record_handler,
        start_offset_resolver=fixed_resolver,
    )
    await component.start()
    try:
        for _ in range(50):
            if len(received) == 1:
                break
            await asyncio.sleep(0.2)
    finally:
        await component.shutdown()

    assert len(received) == 1
    assert json.loads(received[0].value) == {"seq": 2}


async def test_out_of_range_resolver_offset_is_fatal(bootstrap_servers):
    topic = "resolver-out-of-range-topic"
    await raw_produce(bootstrap_servers, topic, {"seq": 1})

    async def record_handler(record: KafkaRecord) -> None:
        pass

    async def bad_resolver(assignment: PartitionAssignment) -> int:
        return assignment.end_offset + 1000

    component = KafkaConsumerComponent(
        bootstrap_servers=bootstrap_servers,
        group_id="resolver-out-of-range-group",
        topics=[topic],
        record_handler=record_handler,
        start_offset_resolver=bad_resolver,
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


async def test_restart_resumes_from_committed_offset_without_invoking_resolver(
    bootstrap_servers,
):
    topic = "resolver-restart-topic"
    await raw_produce(bootstrap_servers, topic, {"seq": 1})

    resolver_calls: list[PartitionAssignment] = []

    async def resolver(assignment: PartitionAssignment) -> int:
        resolver_calls.append(assignment)
        return assignment.beginning_offset

    first_received: list[KafkaRecord] = []

    async def first_handler(record: KafkaRecord) -> None:
        first_received.append(record)

    first = KafkaConsumerComponent(
        bootstrap_servers=bootstrap_servers,
        group_id="resolver-restart-group",
        topics=[topic],
        record_handler=first_handler,
        start_offset_resolver=resolver,
    )
    await first.start()
    try:
        for _ in range(50):
            if len(first_received) == 1:
                break
            await asyncio.sleep(0.2)
    finally:
        await first.shutdown()

    assert len(first_received) == 1
    assert len(resolver_calls) == 1

    await raw_produce(bootstrap_servers, topic, {"seq": 2})

    second_received: list[KafkaRecord] = []

    async def second_handler(record: KafkaRecord) -> None:
        second_received.append(record)

    second = KafkaConsumerComponent(
        bootstrap_servers=bootstrap_servers,
        group_id="resolver-restart-group",
        topics=[topic],
        record_handler=second_handler,
        start_offset_resolver=resolver,
    )
    await second.start()
    try:
        for _ in range(50):
            if len(second_received) == 1:
                break
            await asyncio.sleep(0.2)
    finally:
        await second.shutdown()

    assert len(second_received) == 1
    assert json.loads(second_received[0].value) == {"seq": 2}
    assert len(resolver_calls) == 1  # not invoked again: committed offset exists


async def test_rebalance_new_partition_invokes_resolver_existing_partition_does_not(
    bootstrap_servers,
):
    from aiokafka.admin import AIOKafkaAdminClient, NewPartitions, NewTopic

    topic = "resolver-rebalance-topic"
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        await admin.create_topics(
            [NewTopic(name=topic, num_partitions=1, replication_factor=1)]
        )
    finally:
        await admin.close()

    await raw_produce(bootstrap_servers, topic, {"seq": 1})

    resolver_calls: list[PartitionAssignment] = []
    received: list[KafkaRecord] = []

    async def resolver(assignment: PartitionAssignment) -> int:
        resolver_calls.append(assignment)
        return assignment.beginning_offset

    async def record_handler(record: KafkaRecord) -> None:
        received.append(record)

    component = KafkaConsumerComponent(
        bootstrap_servers=bootstrap_servers,
        group_id="resolver-rebalance-group",
        topics=[topic],
        record_handler=record_handler,
        start_offset_resolver=resolver,
    )
    await component.start()
    try:
        for _ in range(50):
            if len(received) == 1:
                break
            await asyncio.sleep(0.2)
        assert len(resolver_calls) == 1
        assert resolver_calls[0].partition == 0

        admin2 = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
        await admin2.start()
        try:
            await admin2.create_partitions({topic: NewPartitions(total_count=2)})
        finally:
            await admin2.close()

        await component._consumer._client.force_metadata_update()

        await raw_produce(bootstrap_servers, topic, {"seq": 2})

        for _ in range(50):
            if len(resolver_calls) == 2:
                break
            await asyncio.sleep(0.5)
    finally:
        await component.shutdown()

    assert len(resolver_calls) == 2
    partitions_seen = sorted(call.partition for call in resolver_calls)
    assert partitions_seen == [0, 1]
