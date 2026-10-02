from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from kafka_component.errors import (
    DeadLetterPolicy,
    SkipAndLogPolicy,
    StartOffsetOutOfRangeError,
)


@dataclass
class FakeMessage:
    topic: str
    partition: int
    offset: int
    value: Any


async def test_skip_and_log_policy_logs_and_returns(caplog):
    policy = SkipAndLogPolicy()
    message = FakeMessage(topic="orders", partition=0, offset=7, value={"id": 1})
    exc = ValueError("boom")

    with caplog.at_level(logging.ERROR):
        await policy.handle(message, exc)

    assert len(caplog.records) == 1
    record_text = caplog.records[0].getMessage()
    assert "orders" in record_text
    assert "7" in record_text
    assert "boom" in record_text


class FakeSender:
    def __init__(self) -> None:
        self.sent: list[tuple[str, Any]] = []

    async def send(self, topic: str, value: Any, *, key=None) -> None:
        self.sent.append((topic, value))


async def test_dead_letter_policy_uses_explicit_topic():
    sender = FakeSender()
    policy = DeadLetterPolicy(sender, dlq_topic="custom.dlq")
    message = FakeMessage(topic="orders", partition=0, offset=7, value={"id": 1})
    exc = ValueError("boom")

    await policy.handle(message, exc)

    assert len(sender.sent) == 1
    topic, payload = sender.sent[0]
    assert topic == "custom.dlq"
    assert payload["original_topic"] == "orders"
    assert payload["original_partition"] == 0
    assert payload["original_offset"] == 7
    assert payload["original_value"] == {"id": 1}
    assert payload["error_type"] == "ValueError"
    assert payload["error_message"] == "boom"


async def test_dead_letter_policy_defaults_topic_from_original():
    sender = FakeSender()
    policy = DeadLetterPolicy(sender)
    message = FakeMessage(topic="orders", partition=0, offset=7, value={"id": 1})

    await policy.handle(message, ValueError("boom"))

    topic, _ = sender.sent[0]
    assert topic == "orders.DLQ"


def test_start_offset_out_of_range_error_carries_coordinates():
    exc = StartOffsetOutOfRangeError(
        topic="orders",
        partition=1,
        requested_offset=42,
        beginning_offset=50,
        end_offset=100,
    )

    assert exc.topic == "orders"
    assert exc.partition == 1
    assert exc.requested_offset == 42
    assert exc.beginning_offset == 50
    assert exc.end_offset == 100
    assert "orders" in str(exc)
    assert "42" in str(exc)
