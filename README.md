# kafka-component

Async Kafka producer/consumer components for [python-components](https://github.com/lucassant95/python-components).

## Install

```bash
uv add kafka-component
```

## Requirements

- Python >= 3.11
- A running Kafka broker (KRaft or ZooKeeper mode)

## Usage

```python
import asyncio

from python_components import System
from kafka_component import KafkaProducerComponent, KafkaConsumerComponent, DeadLetterPolicy

producer = KafkaProducerComponent(bootstrap_servers="localhost:9092")

async def handle_order(value):
    print("received order", value)

consumer = KafkaConsumerComponent(
    bootstrap_servers="localhost:9092",
    group_id="orders-service",
    topics=["orders"],
    handler=handle_order,
    error_policy=DeadLetterPolicy(producer, dlq_topic="orders.DLQ"),
).using(["producer"])

system = System({"producer": producer, "consumer": consumer})

async def main():
    async with system:
        await producer.send("orders", {"order_id": 1})

asyncio.run(main())
```

The consumer's `.using(["producer"])` declares a start-order dependency on the
producer so `System`'s topological sort always starts the producer first —
it is not read as an injected `self.producer` attribute by
`KafkaConsumerComponent`; the `DeadLetterPolicy` above already holds a
direct reference to the `producer` instance.

## Semantics and caveats

- **At-least-once delivery.** The consumer commits offsets only after the handler succeeds. A crash between a successful handler call and the commit can redeliver a message — handlers should be idempotent if that matters.
- **Sequential processing.** One message is handled at a time, in partition order. There is no built-in concurrency; scale via more consumer instances/partitions, not intra-instance parallelism.
- **Not a supervisor.** On handler failure, the configured `ErrorPolicy` runs (default `SkipAndLogPolicy`, or `DeadLetterPolicy`) and the loop continues — the component itself does not retry, crash-loop, or restart. Health is exposed via `routes()`; process supervision is the caller's job.
- **JSON value-only.** Message values are `json.dumps`/`json.loads`. Keys and headers pass through untouched.
- **Graceful shutdown drains the in-flight message.** `shutdown()` waits for the currently-processing message's handler (and its `ErrorPolicy`, if it fails) to finish before stopping — it does not abandon work mid-message.

## Development

```bash
uv sync --all-groups
uv run pytest
uv run ruff format --check .
uv run ruff check .
```

Integration tests spin up a real Kafka broker via [testcontainers](https://testcontainers.com/modules/kafka/). For local development against a persistent broker instead, run `docker compose up -d`.

## License

MIT
