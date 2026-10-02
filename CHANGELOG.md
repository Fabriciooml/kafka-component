# Changelog

## 0.2.0

- `KafkaConsumerComponent` adds `record_handler` mode (receive raw `KafkaRecord` with partition/offset/topic metadata), `start_offset_resolver` for seek-to-beginning-group logic on first assignment, and `auto_offset_reset` passthrough. New exports: `KafkaRecord`, `PartitionAssignment`, `StartOffsetOutOfRangeError`.

## 0.1.0

- Initial release: `KafkaProducerComponent`, `KafkaConsumerComponent`, `ErrorPolicy` (`SkipAndLogPolicy`, `DeadLetterPolicy`).
