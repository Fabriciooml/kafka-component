from __future__ import annotations

from kafka_component.consumer import KafkaRecord, PartitionAssignment


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
