---
status: accepted
---

# aiokafka client with explicit delivery semantics

kafka-component needs a Kafka client that fits the family's async-first, no-supervisor stance (components expose health, they don't self-heal). We chose **aiokafka** over confluent-kafka because confluent's asyncio support wraps a threaded producer/consumer rather than being asyncio-native, which fights the async-first design; we accept aiokafka's lower raw throughput and community (not enterprise-SLA) maintenance pace as the trade-off.

On top of that, `KafkaConsumerComponent` commits offsets manually — only after the handler succeeds (at-least-once, no auto-commit) — and `KafkaProducerComponent` defaults to wait-for-ack (`acks=all`) rather than fire-and-forget. Per-message failure handling is pluggable via `ErrorPolicy` (default `SkipAndLogPolicy`, optional `DeadLetterPolicy`) instead of a built-in retry loop, keeping the component from silently losing or endlessly retrying messages.

## Considered Options

- confluent-kafka (rejected: threaded async wrapper, not asyncio-native)
- auto-commit consumer (rejected: risks silent data loss on crash mid-processing)
- fire-and-forget producer, `acks=0`/`1` (rejected as default: conflicts with family's explicit-failure stance; could be exposed later as opt-in)
- built-in retry-N policy (rejected for v1: real design surface — backoff, ordering guarantees — punted; pluggable `ErrorPolicy` leaves room to add without breaking the API)
