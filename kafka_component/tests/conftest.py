from __future__ import annotations

import pytest
from testcontainers.kafka import KafkaContainer


@pytest.fixture(scope="session")
def kafka_container():
    # Using confluentinc/cp-kafka because apache/kafka:latest uses KRaft mode
    # which is not fully supported by testcontainers' wait strategy
    with KafkaContainer("confluentinc/cp-kafka:7.5.0") as container:
        yield container


@pytest.fixture(scope="session")
def bootstrap_servers(kafka_container: KafkaContainer) -> str:
    return kafka_container.get_bootstrap_server()
