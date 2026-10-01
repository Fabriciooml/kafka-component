# Consumer Record Handler and Start-Offset Resolver Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend `KafkaConsumerComponent` with an opt-in full-record handler (`record_handler`), an opt-in per-partition start-offset resolver (`start_offset_resolver`) invoked on first assignment and on rebalance, and a configurable `auto_offset_reset`, while leaving existing `handler(value)` behavior unchanged.

**Architecture:** Two new dataclasses (`KafkaRecord`, `PartitionAssignment`) and a new exception (`StartOffsetOutOfRangeError`) carry data across the new seams. `_consume_loop` branches per message on which handler mode is configured. A new internal `ConsumerRebalanceListener` subclass seeks partitions with no committed offset to whatever `start_offset_resolver` returns, before `aiokafka`'s own fetch-position logic runs for that partition; partitions with a committed offset are left untouched.

**Tech Stack:** Python 3.11+, `aiokafka>=0.14,<1`, `pytest` + `pytest-asyncio` (mode `auto`, see `pytest.ini`), `testcontainers[kafka]` for integration tests against a real broker (`confluentinc/cp-kafka:7.5.0`).

**Spec:** `docs/superpowers/specs/2026-10-01-consumer-record-handler-and-offset-resolver-design.md`

## Global Constraints

- Existing `handler(value)` behavior and tests must pass unchanged (spec, Scope).
- Exactly one of `handler` / `record_handler` must be supplied; both or neither raises `ValueError` at construction (spec, API).
- `start_offset_resolver` works with either handler mode (spec, API).
- `auto_offset_reset` default is `"earliest"` — identical to today's hardcoded value, no behavior change for existing callers who don't pass it (spec, auto_offset_reset).
- Commit semantics (`commit()` with no args after a successful handler call, `ErrorPolicy.handle(msg, exc)` on failure) are unchanged in both handler modes (spec, Handler dispatch).
- `StartOffsetOutOfRangeError` surfaces through the same fatal path `_consume_loop` already uses for deserialization failures: `last_error` set, `connected: False`, loop exits (spec, Error handling).
- Integration tests use the real Kafka broker via the existing `bootstrap_servers` / `kafka_container` fixtures in `kafka_component/tests/conftest.py`; unit tests (no broker) cover constructor validation only (spec, Testing).

---

## File Structure

- Modify `kafka_component/errors.py` — add `StartOffsetOutOfRangeError`.
- Modify `kafka_component/consumer.py` — add `KafkaRecord`, `PartitionAssignment`, `_ResolverRebalanceListener`, and extend `KafkaConsumerComponent`.
- Modify `kafka_component/__init__.py` — export the three new public names.
- Modify `kafka_component/tests/test_consumer.py` — new integration tests for record mode, resolver, rebalance, and `auto_offset_reset`.
- Create `kafka_component/tests/test_consumer_config.py` — unit tests (no broker) for constructor validation.
- Modify `README.md` and `CHANGELOG.md` — document the new API.

---

### Task 1: `KafkaRecord`, `PartitionAssignment`, `StartOffsetOutOfRangeError`

**Files:**
- Modify: `kafka_component/errors.py`
- Modify: `kafka_component/consumer.py`
- Modify: `kafka_component/__init__.py`
- Test: `kafka_component/tests/test_errors.py`, `kafka_component/tests/test_consumer_config.py` (new)

**Interfaces:**
- Produces: `kafka_component.errors.StartOffsetOutOfRangeError(topic: str, partition: int, requested_offset: int, beginning_offset: int, end_offset: int)` — attributes of the same names, `str(exc)` includes all five values.
- Produces: `kafka_component.consumer.KafkaRecord` — frozen dataclass, fields `topic: str, partition: int, offset: int, value: bytes, key: bytes | None = None, timestamp: int | None = None, headers: tuple[tuple[str, bytes], ...] = ()`.
- Produces: `kafka_component.consumer.PartitionAssignment` — frozen dataclass, fields `topic: str, partition: int, beginning_offset: int, end_offset: int`.

- [ ] **Step 1: Write the failing tests**

First, add `StartOffsetOutOfRangeError` to the existing `from kafka_component.errors import DeadLetterPolicy, SkipAndLogPolicy` line at the top of `kafka_component/tests/test_errors.py`:

```python
from kafka_component.errors import DeadLetterPolicy, SkipAndLogPolicy, StartOffsetOutOfRangeError
```

Then append to the end of the file:

```python
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
```

Create `kafka_component/tests/test_consumer_config.py`:

```python
from __future__ import annotations

from kafka_component.consumer import KafkaRecord, PartitionAssignment


def test_kafka_record_fields():
    record = KafkaRecord(
        topic="orders",
        partition=0,
        offset=5,
        value=b'{"id": 1}',
        key=b"k",
        timestamp=123,
        headers=(("trace-id", b"abc"),),
    )

    assert record.topic == "orders"
    assert record.partition == 0
    assert record.offset == 5
    assert record.value == b'{"id": 1}'
    assert record.key == b"k"
    assert record.timestamp == 123
    assert record.headers == (("trace-id", b"abc"),)


def test_kafka_record_defaults():
    record = KafkaRecord(topic="orders", partition=0, offset=5, value=b"x")

    assert record.key is None
    assert record.timestamp is None
    assert record.headers == ()


def test_partition_assignment_fields():
    assignment = PartitionAssignment(
        topic="orders", partition=0, beginning_offset=10, end_offset=20
    )

    assert assignment.topic == "orders"
    assert assignment.partition == 0
    assert assignment.beginning_offset == 10
    assert assignment.end_offset == 20
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest kafka_component/tests/test_errors.py kafka_component/tests/test_consumer_config.py -v`
Expected: `test_start_offset_out_of_range_error_carries_coordinates` FAILs with `ImportError: cannot import name 'StartOffsetOutOfRangeError'`; the two new `test_consumer_config.py` import-dependent tests FAIL with `ImportError: cannot import name 'KafkaRecord'`.

- [ ] **Step 3: Implement `StartOffsetOutOfRangeError`**

In `kafka_component/errors.py`, add after the imports (no other changes to the file):

```python
class StartOffsetOutOfRangeError(Exception):
    def __init__(
        self,
        *,
        topic: str,
        partition: int,
        requested_offset: int,
        beginning_offset: int,
        end_offset: int,
    ) -> None:
        self.topic = topic
        self.partition = partition
        self.requested_offset = requested_offset
        self.beginning_offset = beginning_offset
        self.end_offset = end_offset
        super().__init__(
            f"start_offset_resolver returned offset {requested_offset} for "
            f"topic={topic} partition={partition}, outside valid range "
            f"[{beginning_offset}, {end_offset}]"
        )
```

- [ ] **Step 4: Implement `KafkaRecord` and `PartitionAssignment`**

In `kafka_component/consumer.py`, add after the existing imports and before the `KafkaConsumerComponent` class:

```python
from dataclasses import dataclass


@dataclass(frozen=True)
class KafkaRecord:
    topic: str
    partition: int
    offset: int
    value: bytes
    key: bytes | None = None
    timestamp: int | None = None
    headers: tuple[tuple[str, bytes], ...] = ()


@dataclass(frozen=True)
class PartitionAssignment:
    topic: str
    partition: int
    beginning_offset: int
    end_offset: int
```

(Place the `from dataclasses import dataclass` import alongside the existing `from collections.abc import Awaitable, Callable` line rather than inline — both are stdlib imports at the top of the file.)

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest kafka_component/tests/test_errors.py kafka_component/tests/test_consumer_config.py -v`
Expected: all PASS.

- [ ] **Step 6: Export the new names**

In `kafka_component/__init__.py`, update to:

```python
from __future__ import annotations

from kafka_component.consumer import (
    KafkaConsumerComponent,
    KafkaRecord,
    PartitionAssignment,
)
from kafka_component.errors import (
    DeadLetterPolicy,
    ErrorPolicy,
    Sender,
    SkipAndLogPolicy,
    StartOffsetOutOfRangeError,
)
from kafka_component.producer import KafkaProducerComponent

__all__ = [
    "DeadLetterPolicy",
    "ErrorPolicy",
    "KafkaConsumerComponent",
    "KafkaProducerComponent",
    "KafkaRecord",
    "PartitionAssignment",
    "Sender",
    "SkipAndLogPolicy",
    "StartOffsetOutOfRangeError",
]
```

- [ ] **Step 7: Run the full unit test suite and ruff**

Run: `uv run pytest kafka_component/tests/test_errors.py kafka_component/tests/test_consumer_config.py -v && uv run ruff format --check . && uv run ruff check .`
Expected: all PASS, no formatting/lint issues.

- [ ] **Step 8: Commit**

```bash
git add kafka_component/errors.py kafka_component/consumer.py kafka_component/__init__.py kafka_component/tests/test_errors.py kafka_component/tests/test_consumer_config.py
git commit -m "feat: add KafkaRecord, PartitionAssignment, StartOffsetOutOfRangeError"
```

---

### Task 2: Constructor accepts `record_handler`, `start_offset_resolver`, `auto_offset_reset`

**Files:**
- Modify: `kafka_component/consumer.py`
- Test: `kafka_component/tests/test_consumer_config.py`

**Interfaces:**
- Consumes: `KafkaRecord`, `PartitionAssignment` from Task 1 (import already present in `consumer.py`).
- Produces: `KafkaConsumerComponent.__init__(..., handler=None, record_handler=None, start_offset_resolver=None, auto_offset_reset="earliest", ...)`. Raises `ValueError` if `(handler is None) == (record_handler is None)`. Stores `self._record_handler`, `self._start_offset_resolver`, `self._auto_offset_reset`. No other behavior changes yet — `start()` and `_consume_loop` are untouched in this task.

- [ ] **Step 1: Write the failing tests**

First, update the top-of-file imports in `kafka_component/tests/test_consumer_config.py` (created in Task 1):

```python
from __future__ import annotations

import pytest

from kafka_component.consumer import KafkaConsumerComponent, KafkaRecord, PartitionAssignment
```

Then append to the end of the file:

```python
async def _noop_handler(value) -> None:
    pass


async def _noop_record_handler(record: KafkaRecord) -> None:
    pass


def test_requires_exactly_one_handler_mode():
    with pytest.raises(ValueError, match="exactly one of"):
        KafkaConsumerComponent(
            bootstrap_servers="localhost:9092",
            group_id="g",
            topics=["t"],
        )


def test_rejects_both_handler_modes():
    with pytest.raises(ValueError, match="exactly one of"):
        KafkaConsumerComponent(
            bootstrap_servers="localhost:9092",
            group_id="g",
            topics=["t"],
            handler=_noop_handler,
            record_handler=_noop_record_handler,
        )


def test_accepts_legacy_handler_alone():
    component = KafkaConsumerComponent(
        bootstrap_servers="localhost:9092",
        group_id="g",
        topics=["t"],
        handler=_noop_handler,
    )
    assert component._handler is _noop_handler
    assert component._record_handler is None


def test_accepts_record_handler_with_resolver():
    async def resolver(assignment):
        return 0

    component = KafkaConsumerComponent(
        bootstrap_servers="localhost:9092",
        group_id="g",
        topics=["t"],
        record_handler=_noop_record_handler,
        start_offset_resolver=resolver,
        auto_offset_reset="none",
    )
    assert component._record_handler is _noop_record_handler
    assert component._start_offset_resolver is resolver
    assert component._auto_offset_reset == "none"


def test_resolver_allowed_with_legacy_handler():
    async def resolver(assignment):
        return 0

    component = KafkaConsumerComponent(
        bootstrap_servers="localhost:9092",
        group_id="g",
        topics=["t"],
        handler=_noop_handler,
        start_offset_resolver=resolver,
    )
    assert component._handler is _noop_handler
    assert component._start_offset_resolver is resolver


def test_auto_offset_reset_defaults_to_earliest():
    component = KafkaConsumerComponent(
        bootstrap_servers="localhost:9092",
        group_id="g",
        topics=["t"],
        handler=_noop_handler,
    )
    assert component._auto_offset_reset == "earliest"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest kafka_component/tests/test_consumer_config.py -v`
Expected: `test_requires_exactly_one_handler_mode` FAILs (construction currently requires `handler` positionally with no default, so it raises `TypeError`, not the targeted `ValueError`); `test_rejects_both_handler_modes`, `test_accepts_record_handler_with_resolver`, `test_resolver_allowed_with_legacy_handler`, `test_auto_offset_reset_defaults_to_earliest` FAIL with `TypeError: unexpected keyword argument` since these params don't exist yet.

- [ ] **Step 3: Implement the constructor changes**

In `kafka_component/consumer.py`, update the imports and `__init__`:

```python
from typing import Any, Literal
```

(add `Literal` to the existing `from typing import Any` line)

```python
    def __init__(
        self,
        *,
        bootstrap_servers: str | list[str],
        group_id: str,
        topics: list[str],
        handler: Callable[[Any], Awaitable[None]] | None = None,
        record_handler: Callable[[KafkaRecord], Awaitable[None]] | None = None,
        start_offset_resolver: Callable[[PartitionAssignment], Awaitable[int]]
        | None = None,
        auto_offset_reset: Literal["earliest", "latest", "none"] = "earliest",
        error_policy: ErrorPolicy | None = None,
        client_id: str | None = None,
    ) -> None:
        if (handler is None) == (record_handler is None):
            raise ValueError(
                "KafkaConsumerComponent requires exactly one of `handler` or "
                "`record_handler`"
            )
        self.using([])
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        self._topics = topics
        self._handler = handler
        self._record_handler = record_handler
        self._start_offset_resolver = start_offset_resolver
        self._auto_offset_reset = auto_offset_reset
        self._error_policy: ErrorPolicy = error_policy or SkipAndLogPolicy()
        self._client_id = client_id
        self._consumer: AIOKafkaConsumer | None = None
        self._started = False
        self._last_error: str | None = None
        self._consume_task: asyncio.Task[None] | None = None
        self._stopping: asyncio.Event = asyncio.Event()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest kafka_component/tests/test_consumer_config.py -v`
Expected: all PASS.

- [ ] **Step 5: Run the full existing test suite to confirm no regressions in construction**

Run: `uv run pytest kafka_component/tests/test_consumer_config.py kafka_component/tests/test_errors.py kafka_component/tests/test_producer.py -v`
Expected: all PASS. (Integration tests in `test_consumer.py` and `test_system_integration.py` are left for later steps/tasks since they need the broker and are unaffected by this task.)

- [ ] **Step 6: Commit**

```bash
git add kafka_component/consumer.py kafka_component/tests/test_consumer_config.py
git commit -m "feat: accept record_handler, start_offset_resolver, auto_offset_reset params"
```

---

### Task 3: Subscribe via explicit `subscribe()` call (listener plumbing, no behavior change)

**Files:**
- Modify: `kafka_component/consumer.py` (`start()` method)
- Test: `kafka_component/tests/test_consumer.py` (existing, run as regression)

**Interfaces:**
- Consumes: nothing new.
- Produces: `start()` now constructs `AIOKafkaConsumer` without positional topics and calls `self._consumer.subscribe(topics=self._topics, listener=None)` before `await self._consumer.start()`. Observable behavior for existing `handler(value)` callers is unchanged.

- [ ] **Step 1: Run the existing integration suite to capture the current baseline**

Run: `uv run pytest kafka_component/tests/test_consumer.py -v`
Expected: all PASS (this is the pre-change baseline; no new test is written for this task since it's a pure refactor covered by existing integration tests).

- [ ] **Step 2: Refactor `start()`**

In `kafka_component/consumer.py`, replace the `start()` method:

```python
    async def start(self) -> None:
        self._stopping = asyncio.Event()
        self._consumer = AIOKafkaConsumer(
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            client_id=self._client_id,
            auto_offset_reset=self._auto_offset_reset,
            enable_auto_commit=False,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        self._consumer.subscribe(topics=self._topics, listener=None)
        await self._consumer.start()
        self._started = True
        self._consume_task = asyncio.create_task(self._consume_loop())
```

(This replaces the old `AIOKafkaConsumer(*self._topics, ...)` constructor call and the hardcoded `auto_offset_reset="earliest"` with `self._auto_offset_reset`, which defaults to `"earliest"` — see Task 2. `value_deserializer` stays as-is for now; Task 4 makes it conditional on handler mode.)

- [ ] **Step 3: Run the existing integration suite to confirm no regressions**

Run: `uv run pytest kafka_component/tests/test_consumer.py -v`
Expected: all PASS, identical results to Step 1.

- [ ] **Step 4: Commit**

```bash
git add kafka_component/consumer.py
git commit -m "refactor: subscribe explicitly so a rebalance listener can be attached"
```

---

### Task 4: `record_handler` dispatch in `_consume_loop`

**Files:**
- Modify: `kafka_component/consumer.py` (`start()`, `_consume_loop()`)
- Test: `kafka_component/tests/test_consumer.py`

**Interfaces:**
- Consumes: `KafkaRecord` (Task 1), `self._record_handler` / `self._handler` (Task 2).
- Produces: in record mode, `value_deserializer` is `None` (raw `bytes` values) and `_consume_loop` calls `self._record_handler(KafkaRecord(...))` instead of `self._handler(msg.value)`. Commit and `ErrorPolicy` wiring unchanged.

- [ ] **Step 1: Write the failing tests**

First, update the existing top-of-file imports in `kafka_component/tests/test_consumer.py`:

```python
from __future__ import annotations

import asyncio
import json
from typing import Any

from aiokafka import AIOKafkaProducer

from kafka_component.consumer import KafkaConsumerComponent, KafkaRecord
from kafka_component.errors import DeadLetterPolicy
from kafka_component.producer import KafkaProducerComponent
from kafka_component.tests.helpers import raw_consume_one, raw_produce
```

(This adds `import json` and `KafkaRecord` to the existing import block — everything else in it is unchanged.)

Then append to the end of the file:

```python
async def test_record_handler_receives_coordinates_across_partitions(bootstrap_servers):
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic

    topic = "record-handler-topic"
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        await admin.create_topics(
            [NewTopic(name=topic, num_partitions=2, replication_factor=1)]
        )
    finally:
        await admin.close()

    raw_producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await raw_producer.start()
    try:
        await raw_producer.send_and_wait(topic, value={"seq": 1}, partition=0)
        await raw_producer.send_and_wait(topic, value={"seq": 2}, partition=1)
    finally:
        await raw_producer.stop()

    received: list[KafkaRecord] = []

    async def record_handler(record: KafkaRecord) -> None:
        received.append(record)

    component = KafkaConsumerComponent(
        bootstrap_servers=bootstrap_servers,
        group_id="record-handler-group",
        topics=[topic],
        record_handler=record_handler,
    )
    await component.start()
    try:
        for _ in range(50):
            if len(received) == 2:
                break
            await asyncio.sleep(0.2)
    finally:
        await component.shutdown()

    assert len(received) == 2
    by_partition = {r.partition: r for r in received}
    assert set(by_partition) == {0, 1}
    assert by_partition[0].offset == 0
    assert by_partition[1].offset == 0
    for record in received:
        assert record.topic == topic
        assert json.loads(record.value) in [{"seq": 1}, {"seq": 2}]


async def test_record_handler_exception_leaves_offset_uncommitted_then_advances(
    bootstrap_servers,
):
    topic = "record-handler-error-topic"
    await raw_produce(bootstrap_servers, topic, {"seq": 1, "fail": True})
    await raw_produce(bootstrap_servers, topic, {"seq": 2, "fail": False})

    handled: list[KafkaRecord] = []
    processed: list[KafkaRecord] = []

    class SpyPolicy:
        async def handle(self, message, exc) -> None:
            handled.append(message)

    async def record_handler(record: KafkaRecord) -> None:
        value = json.loads(record.value)
        if value["fail"]:
            raise ValueError("handler exploded")
        processed.append(record)

    component = KafkaConsumerComponent(
        bootstrap_servers=bootstrap_servers,
        group_id="record-handler-error-group",
        topics=[topic],
        record_handler=record_handler,
        error_policy=SpyPolicy(),
    )
    await component.start()
    try:
        for _ in range(50):
            if len(processed) == 1 and len(handled) == 1:
                break
            await asyncio.sleep(0.2)
    finally:
        await component.shutdown()

    assert len(processed) == 1
    assert processed[0].offset == 1
    assert len(handled) == 1
    assert handled[0].offset == 0


async def test_record_handler_sees_raw_bytes_for_malformed_json(bootstrap_servers):
    topic = "record-handler-malformed-topic"
    raw_producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await raw_producer.start()
    try:
        await raw_producer.send_and_wait(topic, value=b"not valid json")
    finally:
        await raw_producer.stop()

    seen: list[KafkaRecord] = []
    decode_errors: list[Exception] = []

    async def record_handler(record: KafkaRecord) -> None:
        seen.append(record)
        try:
            json.loads(record.value)
        except Exception as exc:
            decode_errors.append(exc)

    component = KafkaConsumerComponent(
        bootstrap_servers=bootstrap_servers,
        group_id="record-handler-malformed-group",
        topics=[topic],
        record_handler=record_handler,
    )
    await component.start()
    try:
        for _ in range(50):
            if len(seen) == 1:
                break
            await asyncio.sleep(0.2)
    finally:
        await component.shutdown()

    assert len(seen) == 1
    assert seen[0].topic == topic
    assert seen[0].partition == 0
    assert seen[0].offset == 0
    assert seen[0].value == b"not valid json"
    assert len(decode_errors) == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest kafka_component/tests/test_consumer.py -k record_handler -v`
Expected: all three FAIL — `record_handler` is accepted by the constructor (Task 2) but `_consume_loop` still only calls `self._handler`, so `_handler` is `None` and the loop raises `TypeError: 'NoneType' object is not callable`, caught by the loop's own exception handling and surfacing as `connected: False` with no records ever reaching `record_handler` — the test's polling loop times out with `received`/`seen` still empty, so the `assert len(...) == N` lines fail.

- [ ] **Step 3: Implement the dispatch**

In `kafka_component/consumer.py`, update `start()`'s `value_deserializer`:

```python
            value_deserializer=(
                (lambda v: json.loads(v.decode("utf-8")))
                if self._handler is not None
                else None
            ),
```

(Replace the line `value_deserializer=lambda v: json.loads(v.decode("utf-8")),` from Task 3 with this conditional.)

Update `_consume_loop`'s handler-invocation block:

```python
                try:
                    if self._record_handler is not None:
                        record = KafkaRecord(
                            topic=msg.topic,
                            partition=msg.partition,
                            offset=msg.offset,
                            value=msg.value,
                            key=msg.key,
                            timestamp=msg.timestamp,
                            headers=tuple(msg.headers),
                        )
                        await self._record_handler(record)
                    else:
                        assert self._handler is not None
                        await self._handler(msg.value)
                except Exception as exc:
```

(This replaces the existing `try: await self._handler(msg.value) \n except Exception as exc:` block. Everything after `except Exception as exc:` — the `ErrorPolicy` call and `continue` — is unchanged.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest kafka_component/tests/test_consumer.py -k record_handler -v`
Expected: all three PASS.

- [ ] **Step 5: Run the full integration suite to confirm no regressions**

Run: `uv run pytest kafka_component/tests/test_consumer.py -v`
Expected: all PASS, including the pre-existing legacy-`handler` tests.

- [ ] **Step 6: Commit**

```bash
git add kafka_component/consumer.py kafka_component/tests/test_consumer.py
git commit -m "feat: dispatch to record_handler with raw KafkaRecord"
```

---

### Task 5: `_ResolverRebalanceListener` — first-assignment offset resolution

**Files:**
- Modify: `kafka_component/consumer.py` (new class, `start()`)
- Test: `kafka_component/tests/test_consumer.py`

**Interfaces:**
- Consumes: `PartitionAssignment` (Task 1), `StartOffsetOutOfRangeError` (Task 1), `self._start_offset_resolver` (Task 2).
- Produces: `_ResolverRebalanceListener(consumer: AIOKafkaConsumer, resolver: Callable[[PartitionAssignment], Awaitable[int]])`, an `aiokafka.ConsumerRebalanceListener`. `start()` builds one and passes it to `subscribe(..., listener=...)` whenever `self._start_offset_resolver is not None`.

- [ ] **Step 1: Write the failing tests**

First, add `PartitionAssignment` to the existing `kafka_component.consumer` import line at the top of `kafka_component/tests/test_consumer.py` (added in Task 4):

```python
from kafka_component.consumer import KafkaConsumerComponent, KafkaRecord, PartitionAssignment
```

Then append to the end of the file:

```python
async def test_resolver_selects_saved_offset_and_skips_earlier_records(bootstrap_servers):
    topic = "resolver-topic"
    await raw_produce(bootstrap_servers, topic, {"seq": 1})
    await raw_produce(bootstrap_servers, topic, {"seq": 2})
    await raw_produce(bootstrap_servers, topic, {"seq": 3})

    received: list[KafkaRecord] = []

    async def record_handler(record: KafkaRecord) -> None:
        received.append(record)

    async def resolver(assignment: PartitionAssignment) -> int:
        assert assignment.topic == topic
        assert assignment.partition == 0
        return assignment.end_offset - 1  # skip the first two records

    component = KafkaConsumerComponent(
        bootstrap_servers=bootstrap_servers,
        group_id="resolver-group",
        topics=[topic],
        record_handler=record_handler,
        start_offset_resolver=resolver,
    )
    await component.start()
    try:
        for _ in range(50):
            if len(received) == 1:
                break
            await asyncio.sleep(0.2)
    finally:
        await component.shutdown()

    assert len(received) == 1
    assert json.loads(received[0].value) == {"seq": 3}


async def test_resolver_offset_includes_records_produced_before_startup(bootstrap_servers):
    from aiokafka import AIOKafkaConsumer, TopicPartition

    topic = "resolver-late-produce-topic"
    await raw_produce(bootstrap_servers, topic, {"seq": 1})

    # Simulate "route created, boundary captured" — the application would do
    # this itself (e.g. an admin client's end_offsets call) at route-creation
    # time, well before this component's consumer ever starts.
    probe = AIOKafkaConsumer(bootstrap_servers=bootstrap_servers)
    await probe.start()
    try:
        tp = TopicPartition(topic, 0)
        boundary = (await probe.end_offsets([tp]))[tp]
    finally:
        await probe.stop()

    # More records arrive after the boundary was captured but before this
    # component's consumer starts — they must still be handled.
    await raw_produce(bootstrap_servers, topic, {"seq": 2})

    async def fixed_resolver(assignment: PartitionAssignment) -> int:
        return boundary

    received: list[KafkaRecord] = []

    async def record_handler(record: KafkaRecord) -> None:
        received.append(record)

    component = KafkaConsumerComponent(
        bootstrap_servers=bootstrap_servers,
        group_id="resolver-late-produce-group",
        topics=[topic],
        record_handler=record_handler,
        start_offset_resolver=fixed_resolver,
    )
    await component.start()
    try:
        for _ in range(50):
            if len(received) == 1:
                break
            await asyncio.sleep(0.2)
    finally:
        await component.shutdown()

    assert len(received) == 1
    assert json.loads(received[0].value) == {"seq": 2}


async def test_out_of_range_resolver_offset_is_fatal(bootstrap_servers):
    topic = "resolver-out-of-range-topic"
    await raw_produce(bootstrap_servers, topic, {"seq": 1})

    async def record_handler(record: KafkaRecord) -> None:
        pass

    async def bad_resolver(assignment: PartitionAssignment) -> int:
        return assignment.end_offset + 1000

    component = KafkaConsumerComponent(
        bootstrap_servers=bootstrap_servers,
        group_id="resolver-out-of-range-group",
        topics=[topic],
        record_handler=record_handler,
        start_offset_resolver=bad_resolver,
    )
    await component.start()

    for _ in range(50):
        if component.get_status()["connected"] is False:
            break
        await asyncio.sleep(0.2)

    status = component.get_status()
    assert status["connected"] is False
    assert status["last_error"] is not None

    await asyncio.wait_for(component.shutdown(), timeout=5.0)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest kafka_component/tests/test_consumer.py -k resolver -v`
Expected: all FAIL — `start_offset_resolver` is accepted and stored (Task 2) but never consulted; `test_resolver_selects_saved_offset_and_skips_earlier_records` and `test_resolver_offset_includes_records_produced_before_startup` time out with `received` empty or containing the wrong record (consumer uses default `auto_offset_reset="earliest"`, so it reads records the resolver was supposed to skip); `test_out_of_range_resolver_offset_is_fatal` FAILs because `connected` never becomes `False` — the bad offset is never requested.

- [ ] **Step 3: Implement `_ResolverRebalanceListener`**

In `kafka_component/consumer.py`, update the `aiokafka` import line to:

```python
from aiokafka import AIOKafkaConsumer, ConsumerRebalanceListener
```

Add the import for the new exception alongside the existing `errors` import:

```python
from kafka_component.errors import ErrorPolicy, SkipAndLogPolicy, StartOffsetOutOfRangeError
```

Add the listener class after `PartitionAssignment` and before `KafkaConsumerComponent`:

```python
class _ResolverRebalanceListener(ConsumerRebalanceListener):
    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        resolver: Callable[[PartitionAssignment], Awaitable[int]],
    ) -> None:
        self._consumer = consumer
        self._resolver = resolver

    async def on_partitions_revoked(self, revoked: Any) -> None:
        pass

    async def on_partitions_assigned(self, assigned: Any) -> None:
        for tp in assigned:
            committed = await self._consumer.committed(tp)
            if committed is not None:
                continue
            beginning = (await self._consumer.beginning_offsets([tp]))[tp]
            end = (await self._consumer.end_offsets([tp]))[tp]
            assignment = PartitionAssignment(
                topic=tp.topic,
                partition=tp.partition,
                beginning_offset=beginning,
                end_offset=end,
            )
            target = await self._resolver(assignment)
            if not beginning <= target <= end:
                raise StartOffsetOutOfRangeError(
                    topic=tp.topic,
                    partition=tp.partition,
                    requested_offset=target,
                    beginning_offset=beginning,
                    end_offset=end,
                )
            self._consumer.seek(tp, target)
```

- [ ] **Step 4: Wire the listener into `start()`**

Update `start()`'s subscribe call:

```python
        listener = (
            _ResolverRebalanceListener(self._consumer, self._start_offset_resolver)
            if self._start_offset_resolver is not None
            else None
        )
        self._consumer.subscribe(topics=self._topics, listener=listener)
```

(Replace the existing `self._consumer.subscribe(topics=self._topics, listener=None)` line from Task 3 with these two statements, placed after the `AIOKafkaConsumer(...)` construction and before `await self._consumer.start()`.)

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest kafka_component/tests/test_consumer.py -k resolver -v`
Expected: all PASS.

- [ ] **Step 6: Run the full integration suite to confirm no regressions**

Run: `uv run pytest kafka_component/tests/test_consumer.py -v`
Expected: all PASS.

- [ ] **Step 7: Commit**

```bash
git add kafka_component/consumer.py kafka_component/tests/test_consumer.py
git commit -m "feat: resolve start offset for unassigned-committed partitions on rebalance"
```

---

### Task 6: Committed offset wins on restart and rebalance; resolver only runs for genuinely new partitions

**Files:**
- Modify: `kafka_component/tests/test_consumer.py` (no production code changes expected — this task proves Task 5's `committed is not None: continue` branch)

**Interfaces:**
- Consumes: everything from Task 5. No new production interfaces.

- [ ] **Step 1: Write the failing tests**

Append to `kafka_component/tests/test_consumer.py`:

```python
async def test_restart_resumes_from_committed_offset_without_invoking_resolver(
    bootstrap_servers,
):
    topic = "resolver-restart-topic"
    await raw_produce(bootstrap_servers, topic, {"seq": 1})
    await raw_produce(bootstrap_servers, topic, {"seq": 2})

    resolver_calls: list[PartitionAssignment] = []

    async def resolver(assignment: PartitionAssignment) -> int:
        resolver_calls.append(assignment)
        return assignment.beginning_offset

    first_received: list[KafkaRecord] = []

    async def first_handler(record: KafkaRecord) -> None:
        first_received.append(record)

    first = KafkaConsumerComponent(
        bootstrap_servers=bootstrap_servers,
        group_id="resolver-restart-group",
        topics=[topic],
        record_handler=first_handler,
        start_offset_resolver=resolver,
    )
    await first.start()
    try:
        for _ in range(50):
            if len(first_received) == 1:
                break
            await asyncio.sleep(0.2)
    finally:
        await first.shutdown()

    assert len(first_received) == 1
    assert len(resolver_calls) == 1

    second_received: list[KafkaRecord] = []

    async def second_handler(record: KafkaRecord) -> None:
        second_received.append(record)

    second = KafkaConsumerComponent(
        bootstrap_servers=bootstrap_servers,
        group_id="resolver-restart-group",
        topics=[topic],
        record_handler=second_handler,
        start_offset_resolver=resolver,
    )
    await second.start()
    try:
        for _ in range(50):
            if len(second_received) == 1:
                break
            await asyncio.sleep(0.2)
    finally:
        await second.shutdown()

    assert len(second_received) == 1
    assert json.loads(second_received[0].value) == {"seq": 2}
    assert len(resolver_calls) == 1  # not invoked again: committed offset exists


async def test_rebalance_new_partition_invokes_resolver_existing_partition_does_not(
    bootstrap_servers,
):
    from aiokafka.admin import AIOKafkaAdminClient, NewPartitions, NewTopic

    topic = "resolver-rebalance-topic"
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        await admin.create_topics(
            [NewTopic(name=topic, num_partitions=1, replication_factor=1)]
        )
    finally:
        await admin.close()

    await raw_produce(bootstrap_servers, topic, {"seq": 1})

    resolver_calls: list[PartitionAssignment] = []
    received: list[KafkaRecord] = []

    async def resolver(assignment: PartitionAssignment) -> int:
        resolver_calls.append(assignment)
        return assignment.beginning_offset

    async def record_handler(record: KafkaRecord) -> None:
        received.append(record)

    component = KafkaConsumerComponent(
        bootstrap_servers=bootstrap_servers,
        group_id="resolver-rebalance-group",
        topics=[topic],
        record_handler=record_handler,
        start_offset_resolver=resolver,
    )
    await component.start()
    try:
        for _ in range(50):
            if len(received) == 1:
                break
            await asyncio.sleep(0.2)
        assert len(resolver_calls) == 1
        assert resolver_calls[0].partition == 0

        admin2 = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
        await admin2.start()
        try:
            await admin2.create_partitions({topic: NewPartitions(total_count=2)})
        finally:
            await admin2.close()

        await raw_produce(bootstrap_servers, topic, {"seq": 2})

        for _ in range(50):
            if len(resolver_calls) == 2:
                break
            await asyncio.sleep(0.5)
    finally:
        await component.shutdown()

    assert len(resolver_calls) == 2
    partitions_seen = sorted(call.partition for call in resolver_calls)
    assert partitions_seen == [0, 1]
```

- [ ] **Step 2: Run tests to verify they fail or pass**

Run: `uv run pytest kafka_component/tests/test_consumer.py -k "restart_resumes or rebalance_new_partition" -v`
Expected: `test_restart_resumes_from_committed_offset_without_invoking_resolver` should already PASS (Task 5's `committed is not None: continue` branch already implements this — this test documents and locks in that behavior rather than driving new code). `test_rebalance_new_partition_invokes_resolver_existing_partition_does_not` should also PASS for the same reason — `on_partitions_assigned` is invoked by `aiokafka` on every rebalance, not just the first one, and the existing partition's committed offset makes the listener skip it. If either test fails, treat it as a real bug in Task 5's implementation (likely: `committed()` returning a stale/incorrect result, or the admin partition count change not triggering a rebalance within the polling window — increase the second polling loop's attempts/sleep if it's a timing issue, but do not change the `committed is not None: continue` logic without first confirming via a debug print that `committed()` returned something unexpected).

- [ ] **Step 3: If both pass, no further implementation step is needed. If not, debug per Step 2's guidance and fix.**

- [ ] **Step 4: Run the full integration suite to confirm no regressions**

Run: `uv run pytest kafka_component/tests/test_consumer.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add kafka_component/tests/test_consumer.py
git commit -m "test: verify committed offsets win over resolver on restart and rebalance"
```

---

### Task 7: `auto_offset_reset` passthrough

**Files:**
- Modify: `kafka_component/tests/test_consumer.py`

**Interfaces:**
- Consumes: `self._auto_offset_reset` (Task 2, wired into `start()` in Task 3). No new production interfaces — this task verifies the passthrough end-to-end.

- [ ] **Step 1: Write the failing test**

Append to `kafka_component/tests/test_consumer.py`:

```python
async def test_auto_offset_reset_latest_skips_preexisting_records(bootstrap_servers):
    topic = "auto-offset-reset-latest-topic"
    await raw_produce(bootstrap_servers, topic, {"seq": 1})

    received: list[Any] = []

    async def handler(value: Any) -> None:
        received.append(value)

    component = KafkaConsumerComponent(
        bootstrap_servers=bootstrap_servers,
        group_id="auto-offset-reset-latest-group",
        topics=[topic],
        handler=handler,
        auto_offset_reset="latest",
    )
    await component.start()
    try:
        await asyncio.sleep(2.0)  # let the consumer join the group and set its position
        await raw_produce(bootstrap_servers, topic, {"seq": 2})
        for _ in range(50):
            if len(received) == 1:
                break
            await asyncio.sleep(0.2)
    finally:
        await component.shutdown()

    assert received == [{"seq": 2}]
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest kafka_component/tests/test_consumer.py -k auto_offset_reset_latest -v`
Expected: FAILs — `start()` currently hardcodes `auto_offset_reset="earliest"` as of before Task 3, but Task 3 already made it `self._auto_offset_reset`. If Tasks 3–6 were completed in order, this test should already PASS, since the passthrough was implemented as part of Task 3's refactor. Run it to confirm; if it fails, the bug is in Task 3's `start()` — verify `auto_offset_reset=self._auto_offset_reset` is actually present there (not left as the literal `"earliest"` string).

- [ ] **Step 3: If it fails, fix `start()`**

Confirm `kafka_component/consumer.py`'s `start()` method passes `auto_offset_reset=self._auto_offset_reset` to `AIOKafkaConsumer(...)` (set in Task 3). If it was accidentally left hardcoded, change it now.

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest kafka_component/tests/test_consumer.py -k auto_offset_reset_latest -v`
Expected: PASS.

- [ ] **Step 5: Run the full integration suite to confirm no regressions**

Run: `uv run pytest kafka_component/tests/test_consumer.py -v`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add kafka_component/tests/test_consumer.py
git commit -m "test: verify auto_offset_reset passthrough"
```

---

### Task 8: Documentation

**Files:**
- Modify: `README.md`
- Modify: `CHANGELOG.md`

**Interfaces:**
- Consumes: the finished public API from Tasks 1–7 (`KafkaRecord`, `PartitionAssignment`, `StartOffsetOutOfRangeError`, `record_handler`, `start_offset_resolver`, `auto_offset_reset`).
- Produces: no code.

- [ ] **Step 1: Check the current `CHANGELOG.md` format**

Run: `cat CHANGELOG.md`

Note the existing heading/entry style (e.g. `## [Unreleased]` or version-numbered sections) so the new entry matches.

- [ ] **Step 2: Add a `README.md` section documenting the new consumer modes**

After the existing "Semantics and caveats" section in `README.md` (before "## Development"), add:

```markdown
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
```

- [ ] **Step 3: Add a `CHANGELOG.md` entry**

Add an entry (matching the format found in Step 1) describing: `record_handler` mode, `start_offset_resolver`, `auto_offset_reset`, and the three new exports (`KafkaRecord`, `PartitionAssignment`, `StartOffsetOutOfRangeError`).

- [ ] **Step 4: Run the full suite and lint one more time**

Run: `uv run pytest kafka_component/tests/ -v && uv run ruff format --check . && uv run ruff check .`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add README.md CHANGELOG.md
git commit -m "docs: document record_handler, start_offset_resolver, auto_offset_reset"
```
