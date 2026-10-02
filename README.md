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

## Record handler mode and start-offset resolution

For applications that need Kafka coordinates alongside the value (to record a
durable failure, for example) or need to seek a partition to a specific
offset the first time it's assigned, pass `record_handler` instead of
`handler`, optionally with `start_offset_resolver`:

```python
from kafka_component import KafkaConsumerComponent, KafkaRecord, PartitionAssignment

async def handle_record(record: KafkaRecord) -> None:
    print(record.topic, record.partition, record.offset, record.value)

async def resolve_start_offset(assignment: PartitionAssignment) -> int:
    # Called only when this consumer group has no committed offset yet
    # for this partition.
    return assignment.end_offset  # e.g. "only records from now on"

consumer = KafkaConsumerComponent(
    bootstrap_servers="localhost:9092",
    group_id="orders-service",
    topics=["orders"],
    record_handler=handle_record,
    start_offset_resolver=resolve_start_offset,
)
```

- Exactly one of `handler` or `record_handler` must be supplied; passing both
  or neither raises `ValueError`.
- `record_handler` receives raw, undecoded `value: bytes` — decoding and
  handling decode failures (with `record.topic`/`record.partition`/`record.offset`
  still available) is the application's responsibility.
- `start_offset_resolver` runs once per partition, only when the consumer
  group has no committed offset for it yet — on first assignment, and again
  on a later rebalance if a genuinely new partition shows up. A partition
  with a committed offset is never rewound to the resolver's answer.
- A resolver result outside `[beginning_offset, end_offset]` raises
  `StartOffsetOutOfRangeError`, which surfaces the same way other fatal
  consumer errors do: `get_status()` reports `connected: False` with
  `last_error` set.
- `auto_offset_reset` (`"earliest"` / `"latest"` / `"none"`, default
  `"earliest"`) is forwarded to the underlying `AIOKafkaConsumer` and applies
  to any partition the resolver doesn't act on.

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
