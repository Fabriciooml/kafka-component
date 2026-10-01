from __future__ import annotations

import pytest

from kafka_component.consumer import (
    KafkaConsumerComponent,
    KafkaRecord,
    PartitionAssignment,
)


def test_kafka_record_fields():
    record = KafkaRecord(
        topic="orders",
        partition=0,
        offset=5,
        value=b'{"id": 1}',
        key=b"k",
        timestamp=123,
        headers=(("trace-id", b"abc"),),
    )

    assert record.topic == "orders"
    assert record.partition == 0
    assert record.offset == 5
    assert record.value == b'{"id": 1}'
    assert record.key == b"k"
    assert record.timestamp == 123
    assert record.headers == (("trace-id", b"abc"),)


def test_kafka_record_defaults():
    record = KafkaRecord(topic="orders", partition=0, offset=5, value=b"x")

    assert record.key is None
    assert record.timestamp is None
    assert record.headers == ()


def test_partition_assignment_fields():
    assignment = PartitionAssignment(
        topic="orders", partition=0, beginning_offset=10, end_offset=20
    )

    assert assignment.topic == "orders"
    assert assignment.partition == 0
    assert assignment.beginning_offset == 10
    assert assignment.end_offset == 20


async def _noop_handler(value) -> None:
    pass


async def _noop_record_handler(record: KafkaRecord) -> None:
    pass


def test_requires_exactly_one_handler_mode():
    with pytest.raises(ValueError, match="exactly one of"):
        KafkaConsumerComponent(
            bootstrap_servers="localhost:9092",
            group_id="g",
            topics=["t"],
        )


def test_rejects_both_handler_modes():
    with pytest.raises(ValueError, match="exactly one of"):
        KafkaConsumerComponent(
            bootstrap_servers="localhost:9092",
            group_id="g",
            topics=["t"],
            handler=_noop_handler,
            record_handler=_noop_record_handler,
        )


def test_accepts_legacy_handler_alone():
    component = KafkaConsumerComponent(
        bootstrap_servers="localhost:9092",
        group_id="g",
        topics=["t"],
        handler=_noop_handler,
    )
    assert component._handler is _noop_handler
    assert component._record_handler is None


def test_accepts_record_handler_with_resolver():
    async def resolver(assignment):
        return 0

    component = KafkaConsumerComponent(
        bootstrap_servers="localhost:9092",
        group_id="g",
        topics=["t"],
        record_handler=_noop_record_handler,
        start_offset_resolver=resolver,
        auto_offset_reset="latest",
    )
    assert component._record_handler is _noop_record_handler
    assert component._start_offset_resolver is resolver
    assert component._auto_offset_reset == "latest"


def test_resolver_allowed_with_legacy_handler():
    async def resolver(assignment):
        return 0

    component = KafkaConsumerComponent(
        bootstrap_servers="localhost:9092",
        group_id="g",
        topics=["t"],
        handler=_noop_handler,
        start_offset_resolver=resolver,
    )
    assert component._handler is _noop_handler
    assert component._start_offset_resolver is resolver


def test_auto_offset_reset_defaults_to_earliest():
    component = KafkaConsumerComponent(
        bootstrap_servers="localhost:9092",
        group_id="g",
        topics=["t"],
        handler=_noop_handler,
    )
    assert component._auto_offset_reset == "earliest"


def test_rejects_none_auto_offset_reset_with_resolver():
    async def resolver(assignment):
        return 0

    with pytest.raises(ValueError, match="none"):
        KafkaConsumerComponent(
            bootstrap_servers="localhost:9092",
            group_id="g",
            topics=["t"],
            handler=_noop_handler,
            start_offset_resolver=resolver,
            auto_offset_reset="none",
        )
