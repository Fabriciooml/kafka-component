from __future__ import annotations

import pytest
from testcontainers.kafka import KafkaContainer


@pytest.fixture(scope="session")
def kafka_container():
    with KafkaContainer("apache/kafka:latest") as container:
        yield container


@pytest.fixture(scope="session")
def bootstrap_servers(kafka_container: KafkaContainer) -> str:
    return kafka_container.get_bootstrap_server()
