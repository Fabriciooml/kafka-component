from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from kafka_component.errors import SkipAndLogPolicy


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
