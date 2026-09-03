from __future__ import annotations

from kafka_component.producer import KafkaProducerComponent
from kafka_component.tests.helpers import raw_consume_one


async def test_send_publishes_json_encoded_value(bootstrap_servers):
    component = KafkaProducerComponent(bootstrap_servers=bootstrap_servers)
    await component.start()
    try:
        await component.send("producer-test-topic", {"order_id": 42})
    finally:
        await component.shutdown()

    msg = await raw_consume_one(
        bootstrap_servers, "producer-test-topic", group_id="producer-test-group"
    )
    assert msg.value == {"order_id": 42}


async def test_get_status_reflects_lifecycle(bootstrap_servers):
    component = KafkaProducerComponent(bootstrap_servers=bootstrap_servers)
    assert component.get_status() == {"connected": False, "last_error": None}

    await component.start()
    try:
        assert component.get_status() == {"connected": True, "last_error": None}
    finally:
        await component.shutdown()

    assert component.get_status()["connected"] is False


async def test_send_with_bytes_key_round_trips(bootstrap_servers):
    component = KafkaProducerComponent(bootstrap_servers=bootstrap_servers)
    await component.start()
    try:
        await component.send("producer-key-topic", {"order_id": 42}, key=b"order-42")
    finally:
        await component.shutdown()

    msg = await raw_consume_one(
        bootstrap_servers, "producer-key-topic", group_id="producer-key-test-group"
    )
    assert msg.key == b"order-42"
    assert msg.value == {"order_id": 42}


async def test_send_before_start_raises(bootstrap_servers):
    component = KafkaProducerComponent(bootstrap_servers=bootstrap_servers)
    try:
        await component.send("nope", {"x": 1})
        assert False, "expected RuntimeError"
    except RuntimeError:
        pass
