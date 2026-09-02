# kafka-component

Component library for Kafka producer/consumer lifecycle, built on python-components' `Component` contract. Provides async producer and consumer components with explicit, no-silent-failure delivery semantics.

## Language

**KafkaProducerComponent**:
A `Component` that publishes JSON-encoded message values to Kafka, waiting for broker acknowledgment before returning.
_Avoid_: Producer, Kafka publisher

**KafkaConsumerComponent**:
A `Component` that consumes JSON-encoded message values from Kafka, committing offsets only after the message handler succeeds.
_Avoid_: Consumer, Kafka subscriber

**ErrorPolicy**:
The strategy a `KafkaConsumerComponent` invokes when a message handler raises, deciding what happens to the failed message.
_Avoid_: failure handler, retry handler

**SkipAndLogPolicy**:
The default `ErrorPolicy` — logs the failure and advances past the message without retrying.
_Avoid_: default policy

**DeadLetterPolicy**:
An `ErrorPolicy` that republishes the failed message and its error metadata to a dead-letter topic, defaulting to `{topic}.DLQ` when the caller doesn't supply one explicitly.
_Avoid_: DLQ handler
